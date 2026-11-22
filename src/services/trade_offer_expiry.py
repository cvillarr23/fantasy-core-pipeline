"""Drops trade offers whose approval run expired, in the scheduler's expiry transaction.

A staged offer's approval run (opened by the worker's trade scan) gates a `trade-submit`
run with `params.offer_key`. When the scheduler expires that approval, the offer's
`awaiting_approval` rows are dismissed with `dismissed_by = "expired"`, so the Trades page
stops listing an offer that can no longer be approved.

Kept apart from `trade_offer_service` so the launcher doesn't import the API-side service.
"""

SUBMIT_JOB = "trade-submit"

# Expiring approvals' trade-submit offer keys, as the `params` of their `then` run.
_EXPIRING_KEYS_SQL = """SELECT trigger->'then'->'params'->>'offer_key' FROM run_log
WHERE executor='approval' AND status='waiting' AND expires_at<=%s
AND trigger->'then'->>'job'=%s"""


def lock_expiring_offers(cursor, now):
    """Row-locks the awaiting offers whose approvals expire at `now`.

    Call it before the approvals are expired. Approving or dismissing an offer locks its
    execution_log rows before its run_log row, and so does the worker's supersede step, so
    the sweep must take its locks in that order too or the two could deadlock.
    """
    cursor.execute(
        "SELECT id FROM execution_log WHERE transaction_type='trade_offer_submit' "
        "AND status='awaiting_approval' "
        f"AND counter_offer->>'offer_key' IN ({_EXPIRING_KEYS_SQL}) FOR UPDATE",
        (now, SUBMIT_JOB),
    )
    cursor.fetchall()


def dismiss_expired_offers(cursor, approval_ids):
    """Dismisses the awaiting offers gated by these just-expired approvals; returns their
    offer keys."""
    if not approval_ids:
        return []
    cursor.execute(
        """UPDATE execution_log e SET status='dismissed', outcome_details=
          COALESCE(e.outcome_details,'{}'::jsonb) || jsonb_build_object('dismissal',
            jsonb_build_object('reason','expired','dismissed_by','expired',
              'dismissed_at', to_jsonb(now()), 'approval_run_id', r.id))
        FROM run_log r
        WHERE r.id = ANY(%s) AND r.trigger->'then'->>'job'=%s
        AND e.transaction_type='trade_offer_submit' AND e.status='awaiting_approval'
        AND e.counter_offer->>'offer_key' = r.trigger->'then'->'params'->>'offer_key'
        AND NOT EXISTS (SELECT 1 FROM execution_log sent
          WHERE sent.transaction_type='trade_offer_submit'
          AND sent.counter_offer->>'offer_key' = e.counter_offer->>'offer_key'
          AND sent.status IN ('approval_pending','approved','submitting','submitted'))
        RETURNING e.counter_offer->>'offer_key'""",
        (list(approval_ids), SUBMIT_JOB),
    )
    return sorted({row[0] for row in cursor.fetchall()})
