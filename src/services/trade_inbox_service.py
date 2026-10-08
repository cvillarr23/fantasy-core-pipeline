"""Worker-compatible inbox reads and durable claims for human-requested publications.

Inbox status and decision belong to the worker. Core's publish ledger lives separately
in outcome_details so refreshes and response results cannot erase an uncertain send.
"""

import hashlib
import json
import logging
import math
import os
import re
import uuid
from datetime import datetime, timedelta, timezone

from pydantic import ValidationError

from src.database import get_db_cursor
from src.schemas.trade_inbox import InboxScore, ShadowPolicy

logger = logging.getLogger("TradeInboxService")
ROW_COLUMNS = (
    "id",
    "status",
    "league_id",
    "season",
    "executed_at",
    "outcome_details",
    "counter_offer",
)
SELECT_COLUMNS = ", ".join(ROW_COLUMNS)
LIVE_STATUSES = {"open", "unparsed", "responding"}
RESPONSE_MARGIN = timedelta(minutes=5)
# Python strip also removes control characters that JavaScript trim preserves, and
# omits the BOM that JavaScript trims. Approval hashes must use the worker's exact set.
JS_WHITESPACE = "\u0009\u000a\u000b\u000c\u000d\u0020\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000\ufeff"


class InboxConflict(ValueError):
    pass


class InboxNotFound(ValueError):
    pass


class PublishUnavailable(RuntimeError):
    pass


def _now():
    return datetime.now(timezone.utc)


def _date(value):
    """ESPN dates are milliseconds; worker-owned dates are ISO strings."""
    try:
        if isinstance(value, datetime):
            result = value
        elif type(value) in (int, float):
            result = datetime.fromtimestamp(value / 1000, timezone.utc)
        elif isinstance(value, str):
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            return None
        return (
            result.replace(tzinfo=timezone.utc)
            if result.tzinfo is None
            else result.astimezone(timezone.utc)
        )
    except (ValueError, OverflowError, OSError):
        return None


def _time(value):
    date = _date(value)
    return date.timestamp() if date is not None else math.inf


def _hidden(entry, now):
    triage = entry.get("triage") or {}
    until = _date(triage.get("snooze_until"))
    return triage.get("state") == "dismissed" or (
        triage.get("state") == "snoozed" and until is not None and until > now
    )


def _rank(row):
    entry = row["counter_offer"] or {}
    score = entry.get("score")
    final = score.get("final_score") if isinstance(score, dict) else None
    numeric = type(final) in (int, float) and math.isfinite(final)
    return (
        not numeric,
        -final if numeric else 0,
        _time(entry.get("expiration_date")),
        _time(entry.get("proposed_date")),
        entry.get("espn_transaction_id", ""),
    )


def _advisory(row, field, model):
    raw = (row["counter_offer"] or {}).get(field)
    if raw is None:
        return None
    try:
        # JSON validation accepts ISO timestamps while retaining strict numeric types.
        return model.model_validate_json(json.dumps(raw, allow_nan=False))
    except (ValidationError, TypeError, ValueError) as error:
        locations = (
            [
                "/".join(map(str, e["loc"]))
                for e in error.errors(include_input=False, include_url=False)
            ][:5]
            if isinstance(error, ValidationError)
            else []
        )
        logger.warning(
            "Ignoring invalid stored %s on inbox row %s at %s",
            field,
            row["id"],
            ", ".join(locations),
        )
        return None


def _shape(row, names=None):
    names = names or {}
    entry = row["counter_offer"] or {}
    item = {
        k: v for k, v in row.items() if k not in ("counter_offer", "outcome_details")
    }
    fields = (
        "espn_transaction_id",
        "related_transaction_id",
        "thread_root_id",
        "proposing_team_id",
        "our_team_id",
        "espn_type",
        "espn_status",
        "send_espn_ids",
        "receive_espn_ids",
        "other_items",
        "player_names",
        "comment",
        "fingerprint",
        "parse_issues",
        "decision",
        "missed_reads",
        "exit_reason",
    )
    item.update({k: entry[k] for k in fields if k in entry})
    for field in ("proposed_date", "expiration_date", "first_seen_at", "last_seen_at"):
        item[field] = _date(entry.get(field))
    triage = entry.get("triage")
    if isinstance(triage, dict):
        item["triage"] = {
            **triage,
            "at": _date(triage.get("at")),
            "snooze_until": _date(triage.get("snooze_until")),
        }
    roster_names = entry.get("player_names") or {}
    for side in ("send", "receive"):
        item[f"{side}_players"] = [
            dict(espn_id=pid, name=names.get(pid) or roster_names.get(pid))
            for pid in entry.get(f"{side}_espn_ids", [])
        ]
    item["score"] = _advisory(row, "score", InboxScore)
    item["shadow_policy"] = _advisory(row, "shadow_policy", ShadowPolicy)
    item["publications"] = [
        {k: v for k, v in claim.items() if k != "payload"}
        for claim in _publications(row).values()
    ]
    return item


def list_incoming(
    status="live", visibility="visible", league_id=None, season=None, limit=50, offset=0
):
    conditions, params = ["transaction_type = 'trade_inbox'"], []
    league_id = league_id or os.getenv("ESPN_LEAGUE_ID")
    for field, value in (("league_id", league_id), ("season", season)):
        if value is not None:
            conditions.append(f"{field} = %s")
            params.append(value)
    with get_db_cursor() as cursor:
        # Rank the full filtered inbox before slicing; a SQL page first loses the best offers.
        cursor.execute(
            f"SELECT {SELECT_COLUMNS} FROM execution_log WHERE {' AND '.join(conditions)}",
            params,
        )
        rows = [dict(zip(ROW_COLUMNS, r)) for r in cursor.fetchall()]
        now = _now()
        rows = [
            r
            for r in rows
            if (
                status == "all"
                or (
                    r["status"] in LIVE_STATUSES
                    if status == "live"
                    else r["status"] == status
                )
            )
            and (
                visibility == "all"
                or _hidden(r["counter_offer"] or {}, now) == (visibility == "hidden")
            )
        ]
        rows = sorted(rows, key=_rank)[offset : offset + limit]
        names = _player_names(cursor, rows)
    return dict(offers=[_shape(r, names) for r in rows], limit=limit, offset=offset)


def _player_names(cursor, rows):
    ids = {
        pid
        for row in rows
        for side in ("send", "receive")
        for pid in (row["counter_offer"] or {}).get(f"{side}_espn_ids", [])
    }
    if not ids:
        return {}
    cursor.execute(
        "SELECT espn_id, name FROM players WHERE espn_id = ANY(%s)", (list(ids),)
    )
    return dict(cursor.fetchall())


def _scope():
    league, team = os.getenv("ESPN_LEAGUE_ID"), os.getenv("ESPN_TEAM_ID")
    if not all(
        isinstance(v, str) and re.fullmatch(r"[1-9]\d*", v) for v in (league, team)
    ):
        raise InboxConflict("Worker league/team is not configured.")
    return league, team


def _lock(cursor, league, tx):
    cursor.execute(
        "SELECT pg_advisory_xact_lock(hashtext(%s))", (f"trade-inbox:{league}:{tx}",)
    )


def _read_locked(cursor, league, tx):
    cursor.execute(
        f"SELECT {SELECT_COLUMNS} FROM execution_log WHERE transaction_type = 'trade_inbox' "
        "AND league_id = %s AND counter_offer->>'espn_transaction_id' = %s ORDER BY id DESC FOR UPDATE",
        (league, tx),
    )
    return [dict(zip(ROW_COLUMNS, r)) for r in cursor.fetchall()]


def _one(cursor, league, tx):
    _lock(cursor, league, tx)
    rows = _read_locked(cursor, league, tx)
    if not rows:
        raise InboxNotFound("Incoming offer not found.")
    if len(rows) != 1:
        raise InboxConflict("Duplicate inbox rows require reconciliation.")
    return rows[0]


def _save_triage(cursor, row_id, triage):
    cursor.execute(
        "UPDATE execution_log SET counter_offer = jsonb_set(counter_offer, '{triage}', %s::jsonb) WHERE id = %s",
        (json.dumps(triage), row_id),
    )


def triage(tx, state, by, snooze_until=None):
    league, _ = _scope()
    if (
        state not in ("new", "seen", "snoozed", "dismissed")
        or not isinstance(by, str)
        or not by.strip()
    ):
        raise ValueError("Valid triage state and nonblank by are required.")
    if state != "snoozed" and snooze_until is not None:
        raise ValueError("snooze_until is only valid for snoozed.")
    now = _now()
    value = dict(state=state, by=by.strip(), at=now.isoformat())
    if state == "snoozed":
        if snooze_until is None or snooze_until <= now:
            raise ValueError("snooze_until must be in the future.")
        value["snooze_until"] = snooze_until.isoformat()
    with get_db_cursor() as cursor:
        row = _one(cursor, league, tx)
        _save_triage(cursor, row["id"], value)
        row["counter_offer"]["triage"] = value
        names = _player_names(cursor, [row])
    return _shape(row, names)


def _digest(parts):
    return hashlib.sha256(
        json.dumps(
            parts, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    ).hexdigest()


def _comment(value):
    return (value or "").strip(JS_WHITESPACE) or None


def compute_decision_key(league, season, team, tx, action, fingerprint, comment=None):
    return _digest(
        [
            "trade-response-v1",
            str(league),
            str(season),
            str(team),
            str(tx),
            action,
            fingerprint,
            _comment(comment),
        ]
    )


def _publications(row):
    details = row.get("outcome_details") or {}
    claims = details.get("incoming_publications", {})
    if not isinstance(claims, dict) or not all(
        isinstance(c, dict) for c in claims.values()
    ):
        raise InboxConflict(
            "Publication audit metadata is invalid; reconcile before retrying."
        )
    return claims


def _save_publication(cursor, row_id, key, claim):
    cursor.execute(
        "UPDATE execution_log SET outcome_details = COALESCE(outcome_details, '{}'::jsonb) || "
        "jsonb_build_object('incoming_publications', COALESCE(outcome_details->'incoming_publications', '{}'::jsonb) || jsonb_build_object(%s, %s::jsonb)) WHERE id = %s",
        (key, json.dumps(claim), row_id),
    )


def _worker_response_exists(cursor, league, tx):
    # Worker claims use a different lock. Existing response evidence must also block a
    # new action when a worker failure left the inbox status looking open.
    cursor.execute(
        "SELECT id FROM execution_log WHERE transaction_type = 'trade_response' AND league_id = %s "
        "AND counter_offer->>'espn_transaction_id' = %s AND status IN ('responding', 'responded', 'response_unknown', 'rejected') LIMIT 1",
        (league, tx),
    )
    return cursor.fetchone() is not None


def _identity(row, league, team, tx):
    entry = row["counter_offer"]
    if (
        not isinstance(entry, dict)
        or row["league_id"] != league
        or entry.get("our_team_id") != team
        or entry.get("espn_transaction_id") != tx
    ):
        raise InboxConflict("Offer does not match the configured worker scope.")
    if type(row["season"]) is not int or not 2000 <= row["season"] <= 2100:
        raise InboxConflict("Stored season is invalid.")
    if not isinstance(entry.get("fingerprint"), str) or not re.fullmatch(
        r"[0-9a-f]{64}", entry["fingerprint"]
    ):
        raise InboxConflict("Stored fingerprint is invalid.")
    proposer = entry.get("proposing_team_id")
    if (
        not isinstance(proposer, str)
        or not re.fullmatch(r"[1-9]\d*", proposer)
        or proposer == team
    ):
        raise InboxConflict("Stored proposer is invalid.")
    return entry


def _actionable(row, entry):
    if row["status"] != "open":
        raise InboxConflict("Offer is not open.")
    if (
        entry.get("parse_issues") != []
        or entry.get("espn_type") != "TRADE_PROPOSAL"
        or entry.get("espn_status") != "PENDING"
    ):
        raise InboxConflict("Stored offer shape is not actionable.")
    expiry = _date(entry.get("expiration_date"))
    if expiry is None or expiry <= _now() + RESPONSE_MARGIN:
        raise InboxConflict("Offer is expired or expires within five minutes.")
    sides = []
    for side in ("send", "receive"):
        ids = entry.get(f"{side}_espn_ids")
        if (
            not isinstance(ids, list)
            or any(
                not isinstance(pid, str) or not re.fullmatch(r"-?[1-9]\d*", pid)
                for pid in ids
            )
            or len(set(ids)) != len(ids)
        ):
            raise InboxConflict("Stored player ids are invalid.")
        sides.append(set(ids))
    if sides[0] & sides[1]:
        raise InboxConflict("Stored offer has overlapping players.")


def _receipt(tx, claim, duplicate=False):
    return dict(
        espn_transaction_id=tx,
        status="queued",
        kind=claim["kind"],
        tracking_id=claim["tracking_id"],
        approved_by=claim["approved_by"],
        approved_at=claim["approved_at"],
        duplicate=duplicate,
        decision_key=claim.get("decision_key"),
        action=claim.get("action"),
    )


def decide(tx, action, approved_by, comment=None):
    return _request_publication(
        tx, "decision", approved_by, action=action, comment=_comment(comment)
    )


def counter(tx, approved_by, week):
    return _request_publication(tx, "counter", approved_by, week=week)


def _request_publication(
    tx, kind, approved_by, *, action=None, comment=None, week=None
):
    from src.api import routes

    league, team = _scope()
    if not isinstance(approved_by, str) or not approved_by.strip():
        raise ValueError("A human approved_by is required.")
    approved_by = approved_by.strip()
    if kind == "decision" and action not in ("accept", "decline"):
        raise ValueError("action must be accept or decline.")
    if kind == "counter" and (type(week) is not int or not 1 <= week <= 18):
        raise ValueError("week must be between 1 and 18.")
    with get_db_cursor() as cursor:
        row = _one(cursor, league, tx)
        entry = _identity(row, league, team, tx)
        key = (
            compute_decision_key(
                league, row["season"], team, tx, action, entry["fingerprint"], comment
            )
            if kind == "decision"
            else _digest(
                [
                    "trade-inbox-counter-v1",
                    league,
                    str(row["season"]),
                    team,
                    tx,
                    entry["fingerprint"],
                    week,
                ]
            )
        )
        claims = _publications(row)
        existing = claims.get(key)
        if existing and existing.get("status") == "queued":
            if not all(
                existing.get(k)
                for k in (
                    "tracking_id",
                    "approved_by",
                    "approved_at",
                    "kind",
                    "payload",
                )
            ):
                raise InboxConflict("Publication audit metadata is missing.")
            return _receipt(tx, existing, duplicate=True)
        # Even an offer change cannot release an uncertain publication. Pending work
        # might already be in the worker, so another action needs reconciliation first.
        if any(c.get("status") != "failed_before_publish" for c in claims.values()):
            raise InboxConflict(
                "An action is already queued, pending, or outcome-unknown; reconcile before retrying."
            )
        _actionable(row, entry)
        if _worker_response_exists(cursor, league, tx):
            raise InboxConflict("Worker response evidence requires reconciliation.")
        tracking_id = str(uuid.uuid4())
        if kind == "decision":
            if action == "accept":
                score = _advisory(row, "score", InboxScore)
                if score is None or score.scored_fingerprint != entry["fingerprint"]:
                    raise InboxConflict(
                        "Accept requires score evidence for this offer fingerprint."
                    )
                if score.exceeds_roster_limit:
                    raise InboxConflict("Accept would exceed the roster limit.")
            payload = dict(
                tracking_id=tracking_id,
                espn_transaction_id=tx,
                action=action,
                approval=dict(decision_key=key, approved_by=approved_by),
            )
            if comment is not None:
                payload["comment"] = comment
            queue = "trade_responses"
        else:
            score = _advisory(row, "score", InboxScore)
            if (
                score is None
                or score.scored_fingerprint != entry["fingerprint"]
                or score.as_of_week != week
            ):
                raise InboxConflict(
                    "Counter requires player mappings scored for this offer and week."
                )

            def internal(side):
                ids = entry[f"{side}_espn_ids"]
                if not ids:
                    raise InboxConflict("Counter requires players on both sides.")
                mapped = [score.players.get(pid) for pid in ids]
                if any(
                    p is None or not p.player_id or p.player_id != p.player_id.strip()
                    for p in mapped
                ):
                    raise InboxConflict(
                        "Counter has unmapped players; refresh the score."
                    )
                return [p.player_id for p in mapped]

            payload = dict(
                tracking_id=tracking_id,
                season=row["season"],
                week=week,
                offering_team_id=entry["proposing_team_id"],
                incoming_player_ids=internal("receive"),
                outgoing_player_ids=internal("send"),
            )
            incoming, outgoing = (
                payload["incoming_player_ids"],
                payload["outgoing_player_ids"],
            )
            if (
                len(set(incoming)) != len(incoming)
                or len(set(outgoing)) != len(outgoing)
                or set(incoming) & set(outgoing)
            ):
                raise InboxConflict(
                    "Counter player mappings overlap or contain duplicates."
                )
            queue = "trade_negotiations"
        claim = dict(
            kind=kind,
            status="pending",
            tracking_id=tracking_id,
            approved_by=approved_by,
            approved_at=_now().isoformat(),
            fingerprint=entry["fingerprint"],
            payload=payload,
        )
        if kind == "decision":
            claim.update(decision_key=key, action=action)
        _save_publication(cursor, row["id"], key, claim)
        routes._record_bot_decision(
            tracking_id=tracking_id,
            bot_name="trade_inbox",
            action=f"trade_inbox_{action if kind == 'decision' else 'counter'}",
            team_id=team,
            league_id=league,
            decision_inputs=dict(
                season=row["season"], week=week, execution_log_id=row["id"], **claim
            ),
            cursor=cursor,
        )
    # Persist the claim before broker access: rollback after a successful send would
    # allow a second publication of an irreversible response.
    connection, attempted = None, False
    try:
        params = routes.pika.URLParameters(routes.AMQP_URL)
        params.socket_timeout = routes.BROKER_SOCKET_TIMEOUT_SECONDS
        params.blocked_connection_timeout = routes.BROKER_SOCKET_TIMEOUT_SECONDS
        connection = routes.pika.BlockingConnection(params)
        channel = connection.channel()
        channel.confirm_delivery()
        channel.queue_declare(queue=queue, durable=True)
        carrier = {}
        routes.propagate.inject(carrier)
        body = json.dumps({**payload, "traceparent": carrier.get("traceparent")})
        attempted = True
        channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=body,
            mandatory=True,
            properties=routes.pika.BasicProperties(
                delivery_mode=2, content_type="application/json", headers=carrier
            ),
        )
    except Exception as error:
        if not attempted or isinstance(error, routes.PRE_WRITE_BROKER_ERRORS):
            _finish_publication(league, tx, key, claim, "failed_before_publish")
        raise PublishUnavailable(
            "Publication failed; an uncertain outcome requires reconciliation before retrying."
        ) from error
    finally:
        routes._close_broker_connection(connection)
    _finish_publication(league, tx, key, claim, "queued")
    return _receipt(tx, claim)


def _finish_publication(league, tx, key, claim, status):
    with get_db_cursor() as cursor:
        row = _one(cursor, league, tx)
        current = _publications(row).get(key)
        if (
            not current
            or current.get("tracking_id") != claim["tracking_id"]
            or current.get("status") != "pending"
        ):
            raise InboxConflict(
                "Publication claim changed; reconciliation is required."
            )
        claim["status"] = status
        if status == "failed_before_publish":
            claim["publish_error"] = (
                "Failed before publication; this request may be retried."
            )
        _save_publication(cursor, row["id"], key, claim)
