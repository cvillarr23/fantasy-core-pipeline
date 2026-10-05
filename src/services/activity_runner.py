"""Outbox delivery and conservative reconciliation, independent of browser lifetime."""

import asyncio
import json
import logging
import os
import time
import re
import pika
from src.database import get_db_cursor
from src.services.activity_service import action_detail, record_event

logger = logging.getLogger(__name__)


def publish_one(identity=None):
    connection = None
    action_id = None
    try:
        with get_db_cursor() as c:
            c.execute(
                """SELECT o.action_id,o.queue,o.body FROM activity_outbox o
              WHERE o.published_at IS NULL AND (%s IS NOT NULL OR o.next_attempt_at<=now()) AND (%s IS NULL OR o.action_id=%s)
              ORDER BY o.next_attempt_at,o.action_id LIMIT 1 FOR UPDATE SKIP LOCKED""",
                (identity, identity, identity),
            )
            row = c.fetchone()
            if not row:
                return False
            action_id, queue, body = row
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
            {
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
        record_event(
            identity,
            "verified_complete",
            evidence,
            execution="submitted",
            outcome="verified_complete",
        )
        from src.services.read_cache import invalidate_league

        invalidate_league(action["league_id"])
    else:
        # A missing roster change could mean a pending waiver, failed submission or unknown outcome.
        record_event(identity, "status_checked", evidence)
    with get_db_cursor() as c:
        c.execute(
            "UPDATE activity_actions SET next_check_at=now()+interval '5 minutes' WHERE id=%s",
            (identity,),
        )


def tick():
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


async def run(stop):
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=60)
        except asyncio.TimeoutError:
            try:
                await asyncio.to_thread(tick)
            except Exception:
                logger.exception("Activity background tick failed")
