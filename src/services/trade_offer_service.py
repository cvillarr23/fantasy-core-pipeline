"""Human approvals of immutable ESPN offers; durable, fail-closed claims that queue the send.

A staged offer normally has a `waiting` approval run, opened by the worker's trade scan
(dedupe `approval@<scan_run_id>#<offer_key>`, `then` = trade-submit). Approving resolves it
in the claim's transaction and the send starts from it; dismissing resolves it `dismissed`.
An offer with no approval run (staged before them, or by a scan outside the launcher) is
sent by starting trade-submit directly.
"""

import hashlib
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from typing import NamedTuple

from pydantic import ValidationError

from src.database import get_db_cursor
from src.launcher.runs import Runs
from src.services import approval_service, jobs_service
from src.services.espn_ownership import load_espn_ownership
from src.schemas.trade import ProposalEvaluation

logger = logging.getLogger("TradeOfferService")

ROW_COLUMNS = (
    "id",
    "status",
    "league_id",
    "season",
    "executed_at",
    "outcome_details",
    "counter_offer",
    "dry_run",
)
SELECT_COLUMNS = ", ".join(ROW_COLUMNS)


# A sent offer holds its players until the worker reports how ESPN resolved it. Core is never
# told about offers the worker stops watching, so the hold also lapses after this long.
COMMITTED_STATUSES = ("approved", "submitting", "submitted")
DEFAULT_COMMIT_HOURS = 72


class OfferConflict(ValueError):
    pass


class OfferNotFound(ValueError):
    pass


class PublishUnavailable(RuntimeError):
    pass


# The launcher job that sends an approved offer to ESPN (trade_builder.js --offer-key).
SUBMIT_JOB = "trade-submit"
# start_run raises these before it writes a run_log row, so nothing was queued.
PRE_WRITE_START_ERRORS = (
    jobs_service.CatalogUnavailableError,
    jobs_service.JobNotFoundError,
    jobs_service.RunOverlapError,
    ValueError,
)


class Claim(NamedTuple):
    """A committed `approval_pending` claim whose trade-submit run is still to be queued."""

    row: dict
    offer: dict
    approval: dict
    resumed: bool = False


def compute_offer_key(offer):
    """Mirror trade_builder.js computeOfferKey (ids have already been normalized).

    A counter's related transaction is appended, so an approval names the incoming offer it
    counters. Unlinked offers keep the original eight-field key.
    """
    canonical = [
        "trade-offer-v1",
        str(offer["leagueId"]),
        str(offer["seasonId"]),
        str(offer["fromTeamId"]),
        str(offer["toTeamId"]),
        sorted(map(str, offer["incomingPlayerIds"])),
        sorted(map(str, offer["outgoingPlayerIds"])),
        offer.get("message"),
    ]
    if offer.get("relatedTransactionId"):
        canonical.append(str(offer["relatedTransactionId"]))
    encoded = json.dumps(canonical, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _validated_offer(row, key):
    offer = row["counter_offer"]
    try:

        def identifier(value):
            if not isinstance(value, str) or not value or value != value.strip():
                raise ValueError("invalid identifier")
            return value

        league = identifier(row["league_id"])
        source_team = identifier(offer["from_team_id"])
        target_team = identifier(offer["to_team_id"])
        if source_team == target_team:
            raise ValueError("same team")
        if type(row["season"]) is not int:
            raise ValueError("invalid season")
        if offer["source"] not in ("counter_offer", "proposal", "manual"):
            raise ValueError("invalid source")
        sides = []
        for field in ("incoming_player_ids", "outgoing_player_ids"):
            ids = offer[field]
            if not isinstance(ids, list) or not ids:
                raise ValueError("empty side")
            for player_id in ids:
                if not re.fullmatch(r"(?:[0-9]+|-[1-9][0-9]*)", identifier(player_id)):
                    raise ValueError("invalid ESPN id")
            if len(set(ids)) != len(ids):
                raise ValueError("duplicate ids")
            sides.append(ids)
        if set(sides[0]) & set(sides[1]):
            raise ValueError("overlapping ids")
        message = offer.get("message")
        # The worker normalizes empty/whitespace-only notes to null before staging.
        if message is not None and (
            not isinstance(message, str) or not message.strip()
        ):
            raise ValueError("invalid staged message")
        # Counter linkage. Both fields are optional; the thread root is tracing only.
        related = offer.get("related_transaction_id")
        thread_root = offer.get("thread_root_id")
        if related is not None:
            identifier(related)
        if thread_root is not None:
            if related is None:
                raise ValueError("thread root without a related transaction")
            identifier(thread_root)
        normalized = _offer_key_fields(row, offer, message)
        if offer["offer_key"] != key or compute_offer_key(normalized) != key:
            raise ValueError("offer key mismatch")
        if league != os.getenv("ESPN_LEAGUE_ID") or source_team != os.getenv(
            "ESPN_TEAM_ID"
        ):
            raise ValueError("offer does not match configured worker league/team")
    except (KeyError, TypeError, ValueError, UnicodeError) as error:
        raise OfferConflict(f"Stored offer is invalid or edited: {error}") from error
    return offer


def _read_offers(cursor, key, *, for_update=True):
    cursor.execute(
        f"SELECT {SELECT_COLUMNS} FROM execution_log "
        "WHERE transaction_type = 'trade_offer_submit' AND counter_offer->>'offer_key' = %s "
        "ORDER BY executed_at DESC, id DESC" + (" FOR UPDATE" if for_update else ""),
        (key,),
    )
    return [dict(zip(ROW_COLUMNS, row)) for row in cursor.fetchall()]


def _save_approval(cursor, row_id, status, approval):
    cursor.execute(
        "UPDATE execution_log SET status = %s, outcome_details = "
        "COALESCE(outcome_details, '{}'::jsonb) || jsonb_build_object('approval', %s::jsonb) "
        "WHERE id = %s",
        (status, json.dumps(approval), row_id),
    )


def _save_resolution(cursor, row_id, resolution):
    cursor.execute(
        "UPDATE execution_log SET outcome_details = "
        "COALESCE(outcome_details, '{}'::jsonb) || jsonb_build_object('resolution', %s::jsonb) "
        "WHERE id = %s",
        (json.dumps(resolution), row_id),
    )


def resolve_offer(key, outcome, resolved_at=None, espn_transaction_id=None):
    """Records how ESPN resolved a sent offer, releasing its players for new trades."""
    with get_db_cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
        rows = _read_offers(cursor, key)
        if not rows:
            raise OfferNotFound("Offer not found.")
        sent = [r for r in rows if r["status"] in COMMITTED_STATUSES]
        if not sent:
            raise OfferConflict("Offer was never approved or sent.")
        stored = [
            (r["outcome_details"] or {}).get("resolution")
            for r in sent
            if (r["outcome_details"] or {}).get("resolution")
        ]
        if any(s["outcome"] != outcome for s in stored):
            raise OfferConflict("Offer is already resolved with a different outcome.")
        if len(stored) == len(sent):
            return dict(offer_key=key, duplicate=True, **stored[0])
        resolution = dict(
            outcome=outcome,
            resolved_at=(resolved_at or datetime.now(timezone.utc)).isoformat(),
            espn_transaction_id=espn_transaction_id,
        )
        for r in sent:
            _save_resolution(cursor, r["id"], resolution)
    return dict(offer_key=key, duplicate=False, **resolution)


def _dismissal_audit(dismissed_by):
    return dict(
        reason="manual",
        dismissed_by=dismissed_by,
        dismissed_at=datetime.now(timezone.utc).isoformat(),
    )


def _save_dismissal(cursor, row_id, dismissal):
    cursor.execute(
        "UPDATE execution_log SET status = 'dismissed', outcome_details = "
        "COALESCE(outcome_details, '{}'::jsonb) || jsonb_build_object('dismissal', %s::jsonb) "
        "WHERE id = %s",
        (json.dumps(dismissal), row_id),
    )


def _stored_dismissal(rows):
    """The audit of an earlier dismissal of this key, or None."""
    return next(
        (
            (row["outcome_details"] or {}).get("dismissal")
            for row in rows
            if row["status"] == "dismissed"
        ),
        None,
    )


def dismiss_offer(key, dismissed_by):
    """Retires an unapproved proposal so it leaves the approval list.

    Every awaiting copy of the key is dismissed, not only the newest: approval falls back to
    any awaiting row, so a leftover copy would keep the offer approvable.
    """
    with get_db_cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
        rows = _read_offers(cursor, key)
        if not rows:
            raise OfferNotFound("Offer not found.")
        awaiting = [row for row in rows if row["status"] == "awaiting_approval"]
        if not awaiting:
            stored = _stored_dismissal(rows)
            if stored is None:
                raise OfferConflict("Offer is no longer awaiting approval.")
            return dict(offer_key=key, duplicate=True, **stored)
        dismissal = _dismissal_audit(dismissed_by)
        for row in awaiting:
            _save_dismissal(cursor, row["id"], dismissal)
        for run_id in _approval_runs(cursor, awaiting, key):
            _end_approval_quietly(cursor, run_id, "dismissed", dismissed_by)
    return dict(offer_key=key, duplicate=False, **dismissal)


def committed_outgoing_players(league_id, season):
    """ESPN ids we have promised in unresolved outgoing offers, mapped to their offer keys."""
    hours = int(os.getenv("OUTGOING_COMMIT_HOURS", DEFAULT_COMMIT_HOURS))
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT counter_offer FROM execution_log "
            "WHERE transaction_type = 'trade_offer_submit' AND status = ANY(%s) "
            "AND league_id = %s AND season = %s "
            "AND NOT (COALESCE(outcome_details, '{}'::jsonb) ? 'resolution') "
            "AND executed_at >= NOW() - make_interval(hours => %s)",
            (list(COMMITTED_STATUSES), league_id, season, hours),
        )
        rows = cursor.fetchall()
    committed = {}
    for (offer,) in rows:
        for player_id in offer.get("outgoing_player_ids", []):
            keys = committed.setdefault(player_id, [])
            if offer["offer_key"] not in keys:
                keys.append(offer["offer_key"])
    return committed


def offer_outcome_counts(league_id, season):
    """{counterparty team id: {outcome: distinct offers}} for our resolved outgoing offers
    this season, the acceptance estimate's history (trade_acceptance)."""
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT team_id, outcome, count(*) FROM ("
            "SELECT DISTINCT counter_offer->>'offer_key' AS offer_key, "
            "counter_offer->>'to_team_id' AS team_id, "
            "outcome_details->'resolution'->>'outcome' AS outcome FROM execution_log "
            "WHERE transaction_type = 'trade_offer_submit' AND league_id = %s AND season = %s "
            "AND COALESCE(outcome_details, '{}'::jsonb) ? 'resolution') resolved "
            "WHERE team_id IS NOT NULL AND outcome IS NOT NULL GROUP BY team_id, outcome",
            (league_id, season),
        )
        rows = cursor.fetchall()
    counts = {}
    for team_id, outcome, count in rows:
        counts.setdefault(team_id, {})[outcome] = count
    return counts


def _response(key, approval, duplicate=False):
    return dict(
        offer_key=key,
        status="approved",
        tracking_id=approval["tracking_id"],
        approved_by=approval["approved_by"],
        approved_at=approval["approved_at"],
        run_id=approval.get("run_id"),
        duplicate=duplicate,
    )


def _lock_edited_key(cursor, key):
    # Never wait while holding the original key: two opposing edits could deadlock.
    cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))", (key,))
    if not cursor.fetchone()[0]:
        raise OfferConflict("The edited offer is being approved; retry later.")


def _offer_key_fields(row, offer, message):
    return dict(
        leagueId=row["league_id"],
        seasonId=row["season"],
        fromTeamId=offer["from_team_id"],
        toTeamId=offer["to_team_id"],
        incomingPlayerIds=offer["incoming_player_ids"],
        outgoingPlayerIds=offer["outgoing_player_ids"],
        message=message,
        relatedTransactionId=offer.get("related_transaction_id"),
    )


def _edited_offer_key(row, offer, message):
    return compute_offer_key(_offer_key_fields(row, offer, message))


def _stored_approval(row):
    approval = (row["outcome_details"] or {}).get("approval", {})
    if not all(approval.get(k) for k in ("tracking_id", "approved_by", "approved_at")):
        raise OfferConflict("Approval audit metadata is missing.")
    return approval


def _duplicate_receipt(rows, key, message):
    for row in rows:
        if row["status"] == "approved":
            offer = _validated_offer(row, key)
            if message is not None and message != offer.get("message"):
                raise OfferConflict("An approved offer cannot be edited.")
            return _response(key, _stored_approval(row), duplicate=True)
    return None


def _pending_claim(rows, key, message):
    """The committed claim of an approval whose send may not be queued yet, or None.

    Approving it again resumes that claim, keeping its approver and tracking id: the
    trade-submit dedupe key means the retry finds the run if the first attempt queued it.
    """
    for row in rows:
        if row["status"] == "approval_pending":
            offer = _validated_offer(row, key)
            if message is not None and message != offer.get("message"):
                raise OfferConflict("A pending approval cannot be edited.")
            return Claim(row, offer, _stored_approval(row), resumed=True)
    return None


def _save_edited_offer(cursor, row, rows, offer):
    cursor.execute(
        "INSERT INTO execution_log (transaction_type, bot_name, status, league_id, "
        "season, offering_team_id, counter_offer, dry_run) "
        "VALUES ('trade_offer_submit', 'trade_builder', 'awaiting_approval', "
        "%s, %s, %s, %s::jsonb, %s) RETURNING id",
        (
            row["league_id"],
            row["season"],
            offer["to_team_id"] if offer["source"] == "counter_offer" else None,
            json.dumps(offer),
            row["dry_run"],
        ),
    )
    edited_row = {**row, "id": cursor.fetchone()[0], "counter_offer": offer}
    for original in rows:
        if original["status"] == "awaiting_approval":
            cursor.execute(
                "UPDATE execution_log SET status = 'superseded', counter_offer = "
                "counter_offer || jsonb_build_object('superseded_by', %s::text) WHERE id = %s",
                (offer["offer_key"], original["id"]),
            )
    return edited_row


def _claim_approval(key, approved_by, message):
    """Locks the offer and commits an `approval_pending` claim plus its audit.

    Returns the stored receipt for an offer that is already approved, else the Claim to send.
    """
    # Imported lazily: routes owns the established bot_decisions audit conventions.
    from src.api import routes

    with get_db_cursor() as cursor:
        # Same key and locking primitive as the worker's live-submit claim.
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
        rows = _read_offers(cursor, key)
        if not rows:
            raise OfferNotFound("Offer not found.")
        replacements = {
            row["counter_offer"].get("superseded_by")
            for row in rows
            if row["status"] == "superseded"
        }
        if replacements and not any(
            row["status"]
            in (
                "awaiting_approval",
                "approval_pending",
                "approved",
                "submitting",
                "submitted",
            )
            for row in rows
        ):
            if len(replacements) != 1 or not next(iter(replacements)):
                raise OfferConflict("Superseded offer linkage is invalid.")
            replacement = next(iter(replacements))
            # An echo of the addressed draft means unchanged, even after replacement.
            if message is not None and all(
                message == _validated_offer(row, key).get("message")
                for row in rows
                if row["status"] == "superseded"
            ):
                message = None
            # Confirmed receipts are immutable. A plain read can return one while the
            # worker holds the replacement lock, without risking an opposing-edit deadlock.
            receipt = _duplicate_receipt(
                _read_offers(cursor, replacement, for_update=False),
                replacement,
                message,
            )
            if receipt is not None:
                return receipt
            _lock_edited_key(cursor, replacement)
            rows = _read_offers(cursor, replacement)
            if not rows:
                raise OfferConflict("The replacement offer is missing.")
            key = replacement
            if message is not None and any(
                row["counter_offer"].get("message") != message
                for row in rows
                if row["status"]
                in ("approved", "awaiting_approval", "approval_pending")
            ):
                raise OfferConflict("This offer has already been edited.")
        # Check the entire key, not only the latest dry fill. A new staged row must not
        # hide an earlier pending/confirmed approval or an actual worker submission.
        pending = _pending_claim(rows, key, message)
        if pending is not None:
            return pending
        receipt = _duplicate_receipt(rows, key, message)
        if receipt is not None:
            return receipt
        if any(row["status"] in ("submitting", "submitted") for row in rows):
            raise OfferConflict(
                "The worker has already claimed or submitted this offer."
            )
        row = next((r for r in rows if r["status"] == "awaiting_approval"), rows[0])
        if row["status"] != "awaiting_approval":
            raise OfferConflict("Offer is no longer awaiting approval.")
        offer = _validated_offer(row, key)
        # Reject conflicting staged copies rather than choosing an arbitrary version.
        for other in rows:
            if other["id"] == row["id"]:
                continue
            if other["status"] == "awaiting_approval":
                _validated_offer(other, key)
                if other["counter_offer"]["source"] != offer["source"]:
                    raise OfferConflict("Staged copies disagree on offer source.")
                if other["counter_offer"].get("thread_root_id") != offer.get(
                    "thread_root_id"
                ):
                    raise OfferConflict("Staged copies disagree on counter thread.")
        approval_run_id = _approval_run_id(cursor, row, key)
        superseded_runs = [
            run_id
            for run_id in _approval_runs(cursor, rows, key)
            if run_id != approval_run_id
        ]
        if message is not None and message != offer.get("message"):
            if offer.get("message_edited"):
                raise OfferConflict("This offer has already been edited.")
            # TODO: Replace fallback once fantasy-browser-worker#124 captures the ESPN limit.
            if len(message) > 1000:
                raise ValueError("Changed message must be at most 1000 characters.")
            edited_key = _edited_offer_key(row, offer, message)
            _lock_edited_key(cursor, edited_key)
            if _read_offers(cursor, edited_key):
                raise OfferConflict("An offer with the edited terms already exists.")
            offer = {
                **offer,
                "offer_key": edited_key,
                "message": message,
                "drafted_message": offer.get("message"),
                "message_edited": True,
                "edited_by": approved_by,
            }
            row = _save_edited_offer(cursor, row, rows, offer)
            key = edited_key
        tracking_id = str(uuid.uuid4())
        approval = dict(
            offer_key=key,
            approved_by=approved_by,
            approved_at=datetime.now(timezone.utc).isoformat(),
            tracking_id=tracking_id,
        )
        if approval_run_id is not None:
            _approve_run(cursor, approval_run_id, key, approved_by)
            approval["approval_run_id"] = approval_run_id
        for run_id in superseded_runs:
            _end_approval_quietly(cursor, run_id, "superseded", approved_by)
        payload = dict(
            tracking_id=tracking_id,
            season=row["season"],
            to_team_id=offer["to_team_id"],
            incoming_player_ids=offer["incoming_player_ids"],
            outgoing_player_ids=offer["outgoing_player_ids"],
            source=offer["source"],
            approval=dict(offer_key=key, approved_by=approved_by),
        )
        if offer.get("message") is not None:
            payload["message"] = offer["message"]
        # The approved send, kept as audit; the trade-submit run rebuilds it from the offer.
        # The worker recomputes the key from these, so a counter must keep its link.
        for field in ("related_transaction_id", "thread_root_id"):
            if offer.get(field) is not None:
                payload[field] = offer[field]
        approval["payload"] = payload
        _save_approval(cursor, row["id"], "approval_pending", approval)
        routes._record_bot_decision(
            tracking_id=tracking_id,
            bot_name="trade_builder",
            action="trade_offer",
            team_id=offer["from_team_id"],
            league_id=row["league_id"],
            decision_inputs=dict(
                season=row["season"], execution_log_id=row["id"], **approval
            ),
            cursor=cursor,
        )
    return Claim(row, offer, approval)


def approve_offer(key, approved_by, message=None):
    claimed = _claim_approval(key, approved_by, message)
    if not isinstance(claimed, Claim):
        return claimed
    # The claim and audit are committed before the run is queued. Never queue it in the
    # claiming transaction: a rollback after the run started would permit a second send.
    approval = {**claimed.approval, "run_id": _start_send(claimed)}
    with get_db_cursor() as cursor:
        _save_approval(cursor, claimed.row["id"], "approved", approval)
    return _response(approval["offer_key"], approval)


def _scan_run_id(offer):
    """The run of the scan that staged the offer (worker-written), so the send is its child."""
    run_id = offer.get("scan_run_id")
    return run_id if isinstance(run_id, str) and run_id.strip() else None


def _approval_run_id(cursor, row, key):
    """The approval run the worker's scan opened for this staged copy, or None (a legacy
    offer, staged before approval runs or outside a launcher run)."""
    scan_run_id = _scan_run_id(row["counter_offer"] or {})
    if scan_run_id is None:
        return None
    dedupe_key = approval_service.approval_dedupe_key(scan_run_id, key)
    return Runs(cursor).run_id_for_dedupe_key(dedupe_key)


def _approval_runs(cursor, rows, key):
    """The approval runs of these rows' awaiting copies, one per scan that staged one."""
    found = []
    for row in rows:
        if row["status"] != "awaiting_approval":
            continue
        run_id = _approval_run_id(cursor, row, key)
        if run_id is not None and run_id not in found:
            found.append(run_id)
    return found


def _approve_run(cursor, run_id, key, approved_by):
    """Approves the offer's approval run in the claim's transaction.

    Its `then` is pointed at `key` first: an edited message changes the offer key, and the
    trade-submit run it starts must send the offer that was approved.
    """
    Runs(cursor).retarget_approval_then(run_id, {"offer_key": key})
    try:
        approval_service.resolve_approval(cursor, run_id, "approved", approved_by)
    except approval_service.ApprovalResolvedError as error:
        raise OfferConflict(
            f"The offer's approval is {error.status}; it can no longer be approved."
        ) from error
    except approval_service.ApprovalNotFoundError as error:
        raise OfferConflict("The offer's approval run is missing.") from error


def _end_approval_quietly(cursor, run_id, outcome, decided_by):
    """Ends an approval whose offer copy is being retired. One that already ended (expired,
    or superseded by a newer scan) needs nothing more."""
    try:
        approval_service.resolve_approval(cursor, run_id, outcome, decided_by)
    except (
        approval_service.ApprovalResolvedError,
        approval_service.ApprovalNotFoundError,
    ) as error:
        logger.info("Approval %s left as is: %s", run_id, error)


def _start_send(claim):
    """Queues the send of a committed claim: through its approval run when the scan opened
    one, else directly (offers staged before approval runs)."""
    run_id = claim.approval.get("approval_run_id")
    if run_id is None:
        return _start_submit_run(claim)
    return _start_approved_send(run_id)


def _start_approved_send(approval_run_id):
    """Starts the approved approval run's trade-submit run; returns its run id.

    The claim is never released here: its approval already ended `success`, and the
    scheduler starts a `then` run that didn't start. Approving again resumes the claim, and
    the `trade-submit@<approval id>` dedupe key finds the run if it was queued.
    """
    try:
        started = approval_service.start_approved_run(approval_run_id)
    except Exception as error:
        raise PublishUnavailable(
            "Queuing the send had an unknown outcome; approve again to reconcile."
        ) from error
    if started is None:
        raise PublishUnavailable(f"Approval {approval_run_id} was not approved.")
    return started["run_id"]


def _start_submit_run(claim):
    """Queues the trade-submit launcher run for a committed claim; returns its run id.

    The dedupe key is the offer key, so however often a claim is resumed it maps to one
    run. A failure that provably wrote nothing releases a fresh claim for re-approval;
    any other failure leaves it `approval_pending` for an approve retry to resume.
    """
    key = claim.approval["offer_key"]
    try:
        started = jobs_service.start_run(
            SUBMIT_JOB,
            params={"offer_key": key},
            requested_by=claim.approval["approved_by"],
            parent_run_id=_scan_run_id(claim.offer),
            dedupe_key=f"{SUBMIT_JOB}@{key}",
        )
    except PRE_WRITE_START_ERRORS as error:
        if not claim.resumed:
            _release_claim(claim)
        raise PublishUnavailable(
            "The send could not be queued; approval may be retried."
        ) from error
    except Exception as error:
        raise PublishUnavailable(
            "Queuing the send had an unknown outcome; approve again to reconcile."
        ) from error
    return started["run_id"]


def _release_claim(claim):
    approval = {
        **claim.approval,
        "publish_error": "Failed before the send was queued; approval may be retried.",
    }
    with get_db_cursor() as cursor:
        _save_approval(cursor, claim.row["id"], "awaiting_approval", approval)


EVALUATION_VERSION = "proposal-evaluation-v1"
# Enough of a validation failure to name the bad fields without logging stored values.
LOGGED_ERROR_LOCATIONS = 5


def _stored_evaluation(row, offer):
    """The worker's score evidence for a generated proposal, or None.

    Display-only: a stored evaluation that can't be read must cost the reviewer that one
    panel, not the whole offer list, so failures become None with a warning.
    """
    raw = offer.get("evaluation")
    if raw is None:
        return None
    label = f"row {row['id']} (offer {offer.get('offer_key')})"
    version = raw.get("version") if isinstance(raw, dict) else None
    if version != EVALUATION_VERSION:
        logger.warning(
            "Ignoring stored evaluation on %s: unrecognized version %.80r",
            label,
            version,
        )
        return None
    try:
        return ProposalEvaluation.model_validate(raw)
    except ValidationError as error:
        locations = [
            "/".join(map(str, detail["loc"]))
            for detail in error.errors(include_input=False, include_url=False)
        ]
        logger.warning(
            "Ignoring invalid stored evaluation on %s: %d error(s) at %s",
            label,
            error.error_count(),
            ", ".join(locations[:LOGGED_ERROR_LOCATIONS]),
        )
        return None


def _list_conditions(status, offer_key, league_id, season):
    """WHERE terms on each offer's latest row. Without a status, one offer_key's lookup
    (the trade-submit run reading its offer back) sees that row whatever its status, so it
    can tell an offer that isn't approved from an unknown key."""
    if status is None and offer_key is None:
        status = "awaiting_approval"
    conditions, params = [], []
    for column, value in (
        ("status", status),
        ("counter_offer->>'offer_key'", offer_key),
        ("league_id", league_id),
        ("season", season),
    ):
        if value is not None:
            conditions.append(f"{column} = %s")
            params.append(value)
    return conditions, params


def list_offers(
    status=None, league_id=None, season=None, limit=50, offset=0, *, offer_key=None
):
    # execution_log keeps one row per lifecycle step, so status is filtered on each
    # offer's latest row, never on any row: an offer already submitted must not still
    # list as awaiting approval because its earlier staged row says so.
    conditions, params = _list_conditions(status, offer_key, league_id, season)
    offer_group = "COALESCE(counter_offer->>'offer_key', id::text)"
    with get_db_cursor() as cursor:
        cursor.execute(
            f"SELECT {SELECT_COLUMNS} FROM ("
            f"SELECT DISTINCT ON ({offer_group}) {SELECT_COLUMNS} FROM execution_log "
            "WHERE transaction_type = 'trade_offer_submit' "
            f"ORDER BY {offer_group}, executed_at DESC, id DESC"
            f") latest WHERE {' AND '.join(conditions)} "
            "ORDER BY executed_at DESC, id DESC LIMIT %s OFFSET %s",
            params + [limit, offset],
        )
        rows = [dict(zip(ROW_COLUMNS, row)) for row in cursor.fetchall()]
        ids = {
            pid
            for row in rows
            for side in ("incoming_player_ids", "outgoing_player_ids")
            for pid in (row["counter_offer"] or {}).get(side, [])
        }
        names, internal_ids, ownership_by_season = {}, {}, {}
        if ids:
            cursor.execute(
                "SELECT espn_id, name, player_id FROM players WHERE espn_id = ANY(%s)",
                (list(ids),),
            )
            for espn_id, name, player_id in cursor.fetchall():
                names[espn_id] = name
                internal_ids[espn_id] = player_id
            # An offer's market signal is the newest row of its own season (week 18 caps
            # the loader's "latest week at or before"); one query per distinct season.
            for offer_season in {row["season"] for row in rows if row["season"]}:
                ownership_by_season[offer_season] = load_espn_ownership(
                    cursor, list(internal_ids.values()), offer_season, 18
                )
    result = []
    for row in rows:
        offer = row["counter_offer"] or {}
        item = {
            k: v
            for k, v in row.items()
            if k not in ("counter_offer", "outcome_details", "dry_run")
        }
        item.update(
            {
                k: v
                for k, v in offer.items()
                if k
                in (
                    "offer_key",
                    "source",
                    "from_team_id",
                    "to_team_id",
                    "incoming_player_ids",
                    "outgoing_player_ids",
                    "message",
                    "drafted_message",
                    "message_edited",
                    "edited_by",
                    "related_transaction_id",
                    "thread_root_id",
                )
            }
        )
        for side in ("incoming", "outgoing"):
            ownership = ownership_by_season.get(row["season"], {})
            item[f"{side}_players"] = [
                dict(
                    espn_id=pid,
                    name=names.get(pid),
                    espn=ownership.get(internal_ids.get(pid)),
                )
                for pid in offer.get(f"{side}_player_ids", [])
            ]
        audit = (row["outcome_details"] or {}).get("approval")
        item["approval"] = (
            {k: v for k, v in audit.items() if k != "payload"} if audit else None
        )
        item["resolution"] = (row["outcome_details"] or {}).get("resolution")
        item["evaluation"] = _stored_evaluation(row, offer)
        result.append(item)
    return dict(offers=result, limit=limit, offset=offset)
