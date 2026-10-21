"""Outbox delivery and conservative reconciliation, independent of browser lifetime."""

import asyncio
import json
import logging
import os
import time
import re
import pika
from src.database import get_db_cursor
from src.services.activity_service import action_detail, dependency_failure_reason, record_event
from src.services import run_log_service

logger = logging.getLogger(__name__)


def publish_one(identity=None):
    connection = None
    action_id = None
    try:
        with get_db_cursor() as c:
            c.execute(
                """SELECT o.action_id,o.queue,o.body,o.depends_on FROM activity_outbox o
              WHERE o.published_at IS NULL AND (%s IS NOT NULL OR o.next_attempt_at<=now()) AND (%s IS NULL OR o.action_id=%s)
              AND (o.depends_on IS NULL OR (o.hold_until>=now() AND EXISTS (SELECT 1 FROM activity_actions p
                   WHERE p.id=o.depends_on AND p.outcome='verified_complete')))
              ORDER BY o.next_attempt_at,o.action_id LIMIT 1 FOR UPDATE SKIP LOCKED""",
                (identity, identity, identity),
            )
            row = c.fetchone()
            if not row:
                return False
            action_id, queue, body, depends_on = row
            if isinstance(body, str):
                body = json.loads(body)
            from src.api.routes import AMQP_URL, _pick_slot_already_filled

            params = pika.URLParameters(AMQP_URL)
            params.socket_timeout = 5
            params.blocked_connection_timeout = 5
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.confirm_delivery()
            channel.queue_declare(queue=queue, passive=True)
            expired = queue == "draft_targets" and (
                (
                    body.get("expires_at") is not None
                    and time.time() >= body["expires_at"]
                )
                or _pick_slot_already_filled(
                    body.get("league_id"), body.get("pick_number")
                )
            )
            if expired:
                c.execute(
                    "UPDATE activity_actions SET execution='skipped',outcome='expired',updated_at=now() WHERE id=%s AND execution='queued'",
                    (action_id,),
                )
                c.execute(
                    "INSERT INTO activity_events(action_id,stage) SELECT id,'expired' FROM activity_actions WHERE id=%s AND outcome='expired'",
                    (action_id,),
                )
            else:
                channel.basic_publish(
                    exchange="",
                    routing_key=queue,
                    body=json.dumps(body),
                    mandatory=True,
                    properties=pika.BasicProperties(
                        delivery_mode=2, content_type="application/json",
                        headers={"traceparent": body["traceparent"]} if body.get("traceparent") else {}
                    ),
                )
                c.execute("UPDATE activity_actions SET execution='delivered',updated_at=now() WHERE id=%s AND execution='queued'", (action_id,))
                c.execute(
                    "INSERT INTO activity_events(action_id,stage) VALUES (%s,'delivered_to_worker_queue')",
                    (action_id,),
                )
                if depends_on:
                    c.execute(
                        "INSERT INTO activity_events(action_id,stage,detail) VALUES (%s,'dependency_verified',%s)",
                        (action_id, json.dumps({"depends_on": depends_on})),
                    )
            c.execute(
                "UPDATE activity_outbox SET published_at=now(),attempts=attempts+1 WHERE action_id=%s",
                (action_id,),
            )
        return True
    except Exception as exc:
        if action_id:
            # Failure accounting is a new transaction: the delivery transaction rolled back.
            with get_db_cursor() as c:
                c.execute("""UPDATE activity_outbox SET attempts=attempts+1,
                    next_attempt_at=now()+least(300, 5*power(2,least(attempts,6))) * interval '1 second',
                    last_error=%s WHERE action_id=%s AND published_at IS NULL""", (type(exc).__name__, action_id))
        raise
    finally:
        if connection:
            try:
                connection.close()
            except Exception:
                logger.warning("Could not close outbox connection")


def settle_held_actions():
    """Cancels held actions that must never be sent: their parent failed for good or was
    skipped, or hold_until passed before they were released. Release is publish_one's job;
    this only ever closes a held row. Returns the cancelled action ids."""
    cancelled = []
    with get_db_cursor() as c:
        c.execute(
            """SELECT o.action_id,o.depends_on,o.hold_until<now(),p.execution,p.outcome,p.evidence,
              EXISTS (SELECT 1 FROM activity_events e WHERE e.action_id=o.depends_on
                      AND e.stage='manual_verification_required')
            FROM activity_outbox o LEFT JOIN activity_actions p ON p.id=o.depends_on
            WHERE o.published_at IS NULL AND o.depends_on IS NOT NULL
            FOR UPDATE OF o SKIP LOCKED"""
        )
        for action_id, parent_id, overdue, execution, outcome, evidence, manual in c.fetchall():
            parent = None if execution is None else {
                "execution": execution,
                "outcome": outcome,
                "evidence": evidence,
                "manual_verification_required": manual,
            }
            reason = dependency_failure_reason(parent, overdue=bool(overdue))
            if not reason:
                continue
            detail = json.dumps({
                "reason": reason,
                "depends_on": parent_id,
                "parent_execution": execution,
                "parent_outcome": outcome,
            })
            # The key is released so a later claim held on a fresh IR move isn't answered
            # with this dead one; that claim still needs its own verified parent.
            c.execute(
                """UPDATE activity_actions SET execution='skipped',outcome='cancelled',idempotency_key=NULL,
                evidence=evidence || %s::jsonb,updated_at=now() WHERE id=%s""",
                (detail, action_id),
            )
            c.execute(
                "INSERT INTO activity_events(action_id,stage,detail) VALUES (%s,'dependency_failed',%s)",
                (action_id, detail),
            )
            c.execute(
                "UPDATE activity_outbox SET published_at=now() WHERE action_id=%s",
                (action_id,),
            )
            logger.warning("Cancelled held action %s: %s (depends on %s)", action_id, reason, parent_id)
            cancelled.append(action_id)
    return cancelled


def normalize_slot(slot):
    slot = re.sub(r"\d+$", "", str(slot).upper())
    return {"FLEX": "RB/WR/TE", "BENCH": "BE"}.get(slot, slot)


def reconcile_action(identity, leagues=None):
    action = action_detail(identity)
    if (
        not action
        or action["outcome"]
        in {"verified_complete", "rejected", "cancelled", "expired"}
        or action["execution"] == "skipped"
    ):
        return
    if action["execution"] == "queued":
        publish_one(identity)
        return
    if action["execution"] == "delivered":
        return
    body = action["payload"]
    kind = action["kind"]
    # This read verifies only the explicitly requested changes, never generic worker success.
    if kind not in {"waiver_actions", "lineup_updates", "trade_submissions"}:
        with get_db_cursor() as c:
            c.execute(
                "UPDATE activity_actions SET next_check_at=now()+interval '5 minutes' WHERE id=%s",
                (identity,),
            )
        return
    from src.ingestion.sync_league import build_league

    season = body.get("season") or int(os.getenv("CURRENT_YEAR", time.gmtime().tm_year))
    cache = leagues if leagues is not None else {}
    if season not in cache:
        cache[season] = build_league(season, request_timeout_seconds=10)
    league = cache[season]
    team = next(
        (
            t
            for t in league.teams
            if str(t.team_id) == str(body.get("team_id") or os.getenv("ESPN_TEAM_ID"))
        ),
        None,
    )
    if not team:
        return
    roster = {str(p.playerId): p.lineupSlot for p in team.roster}
    if kind == "waiver_actions":
        complete = str(body.get("player_id")) in roster and (
            not body.get("drop_player_id") or str(body["drop_player_id"]) not in roster
        )
        evidence = {
            "add_present": str(body.get("player_id")) in roster,
            "drop_absent": not body.get("drop_player_id")
            or str(body["drop_player_id"]) not in roster,
            "source": "ESPN current roster",
        }
    elif kind == "trade_submissions":
        other = next(
            (t for t in league.teams if str(t.team_id) == str(body.get("to_team_id"))),
            None,
        )
        other_ids = {str(p.playerId) for p in other.roster} if other else set()
        incoming = set(map(str, body.get("incoming_player_ids", [])))
        outgoing = set(map(str, body.get("outgoing_player_ids", [])))
        complete = (
            bool(incoming and outgoing and other)
            and incoming <= roster.keys()
            and outgoing <= other_ids
            and not outgoing.intersection(roster)
            and not incoming.intersection(other_ids)
        )
        evidence = {
            "source": "ESPN rosters for both trade teams",
            "incoming_present": incoming <= roster.keys(),
            "outgoing_received_by_other_team": outgoing <= other_ids,
        }
    else:
        swaps = body.get("optimized_swaps", [])
        checks = [
            # An IR or slot-move entry moves one player, so only that player's slot is
            # checked; espn_api reports IR as "IR" and bench as "BE".
            {
                "player": s["player_id"],
                "kind": s["kind"],
                "confirmed": normalize_slot(roster.get(str(s["player_id"]))) == normalize_slot(s["slot"]),
            }
            if s.get("kind") in ("ir", "activate_ir", "move")
            else {
                "add": s["add_player_id"],
                "drop": s["drop_player_id"],
                "confirmed": normalize_slot(roster.get(str(s["add_player_id"]))) == normalize_slot(s["slot"])
                and normalize_slot(roster.get(str(s["drop_player_id"]))) == "BE",
            }
            for s in swaps
        ]
        complete = bool(checks) and all(s["confirmed"] for s in checks)
        evidence = {"items": checks, "source": "ESPN current roster"}
    if complete:
        from src.services.read_cache import invalidate_league

        # Invalidate before publishing the outcome: the web UI refetches as soon as it sees
        # verified_complete, and a refetch that lands first would re-cache pre-move reads.
        invalidate_league(action["league_id"])
        record_event(
            identity,
            "verified_complete",
            evidence,
            execution="submitted",
            outcome="verified_complete",
        )
    else:
        # A missing roster change could mean a pending waiver, failed submission or unknown outcome.
        record_event(identity, "status_checked", evidence)
    with get_db_cursor() as c:
        c.execute(
            "UPDATE activity_actions SET next_check_at=now()+interval '5 minutes' WHERE id=%s",
            (identity,),
        )


def tick():
    # Settling only ever cancels, and publish_one releases a held row only once its parent
    # is verified, so a settle failure can delay a cancellation but never send a held row.
    try:
        settle_held_actions()
    except Exception:
        logger.exception("Settling held actions deferred")
    for _ in range(25):
        try:
            if not publish_one():
                break
        except Exception:
            logger.exception("Outbox delivery deferred")
            continue
    with get_db_cursor() as c:
        c.execute(
            """WITH due AS (
              SELECT id FROM activity_actions WHERE outcome NOT IN ('verified_complete','rejected','cancelled','expired')
              AND execution!='skipped' AND NOT (payload @> '{"historical":true}'::jsonb)
              AND next_check_at<=now() ORDER BY next_check_at LIMIT 20 FOR UPDATE SKIP LOCKED
            ) UPDATE activity_actions a SET next_check_at=now()+interval '5 minutes'
              FROM due WHERE a.id=due.id RETURNING a.id"""
        )
        ids = [r[0] for r in c.fetchall()]
    leagues = {}
    for identity in ids:
        try:
            reconcile_action(identity, leagues)
        except Exception:
            logger.exception("Reconciliation deferred for %s", identity)
    days = max(1, int(os.getenv("LLM_CONTENT_RETENTION_DAYS", "90")))
    with get_db_cursor() as c:
        c.execute(
            "UPDATE activity_sessions SET content='[]',status=CASE WHEN status='running' THEN 'unknown' ELSE status END WHERE created_at<now()-(%s * interval '1 day') AND content!='[]'::jsonb",
            (days,),
        )
    try:
        run_log_service.purge_older_than(max(1, int(os.getenv("RUN_LOG_RETENTION_DAYS", "90"))))
    except Exception:
        logger.exception("Run log retention deferred")


async def run(stop):
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=60)
        except asyncio.TimeoutError:
            try:
                await asyncio.to_thread(tick)
            except Exception:
                logger.exception("Activity background tick failed")
