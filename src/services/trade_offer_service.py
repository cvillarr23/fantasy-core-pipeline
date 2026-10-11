"""Human approvals of immutable ESPN offers; durable, fail-closed publish claims."""

import hashlib
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone

from pydantic import ValidationError

from src.queue_contracts import TRADE_SUBMISSIONS
from src.database import get_db_cursor
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
)
SELECT_COLUMNS = ", ".join(ROW_COLUMNS)


class OfferConflict(ValueError):
    pass


class OfferNotFound(ValueError):
    pass


class PublishUnavailable(RuntimeError):
    pass


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
        normalized = dict(
            leagueId=league,
            seasonId=str(row["season"]),
            fromTeamId=source_team,
            toTeamId=target_team,
            incomingPlayerIds=sides[0],
            outgoingPlayerIds=sides[1],
            message=message,
            relatedTransactionId=related,
        )
        if offer["offer_key"] != key or compute_offer_key(normalized) != key:
            raise ValueError("offer key mismatch")
        if league != os.getenv("ESPN_LEAGUE_ID") or source_team != os.getenv(
            "ESPN_TEAM_ID"
        ):
            raise ValueError("offer does not match configured worker league/team")
    except (KeyError, TypeError, ValueError, UnicodeError) as error:
        raise OfferConflict(f"Stored offer is invalid or edited: {error}") from error
    return offer


def _read_offers_for_update(cursor, key):
    cursor.execute(
        f"SELECT {SELECT_COLUMNS} FROM execution_log "
        "WHERE transaction_type = 'trade_offer_submit' AND counter_offer->>'offer_key' = %s "
        "ORDER BY executed_at DESC, id DESC FOR UPDATE",
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


def _response(key, approval, duplicate=False):
    return dict(
        offer_key=key,
        status="approved",
        tracking_id=approval["tracking_id"],
        approved_by=approval["approved_by"],
        approved_at=approval["approved_at"],
        duplicate=duplicate,
    )


def approve_offer(key, approved_by):
    # Imported lazily: routes owns the established AMQP/audit conventions.
    from src.api import routes

    with get_db_cursor() as cursor:
        # Same key and locking primitive as the worker's live-submit claim.
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (key,))
        rows = _read_offers_for_update(cursor, key)
        if not rows:
            raise OfferNotFound("Offer not found.")
        # Check the entire key, not only the latest dry fill. A new staged row must not
        # hide an earlier pending/confirmed publish or an actual worker submission.
        for row in rows:
            if row["status"] == "approval_pending":
                raise OfferConflict(
                    "Approval is pending or its publish outcome is unknown; reconcile before retrying."
                )
        for row in rows:
            if row["status"] == "approved":
                _validated_offer(row, key)
                approval = (row["outcome_details"] or {}).get("approval", {})
                if not all(
                    approval.get(k)
                    for k in ("tracking_id", "approved_by", "approved_at")
                ):
                    raise OfferConflict("Approval audit metadata is missing.")
                return _response(key, approval, duplicate=True)
        if any(row["status"] in ("submitting", "submitted") for row in rows):
            raise OfferConflict(
                "The worker has already claimed or submitted this offer."
            )
        row = rows[0]
        if row["status"] != "awaiting_approval":
            raise OfferConflict("Offer is no longer awaiting approval.")
        offer = _validated_offer(row, key)
        # Reject conflicting staged copies rather than choosing an arbitrary version.
        for other in rows[1:]:
            if other["status"] == "awaiting_approval":
                _validated_offer(other, key)
                if other["counter_offer"]["source"] != offer["source"]:
                    raise OfferConflict("Staged copies disagree on offer source.")
                if other["counter_offer"].get("thread_root_id") != offer.get(
                    "thread_root_id"
                ):
                    raise OfferConflict("Staged copies disagree on counter thread.")
        tracking_id = str(uuid.uuid4())
        approval = dict(
            offer_key=key,
            approved_by=approved_by,
            approved_at=datetime.now(timezone.utc).isoformat(),
            tracking_id=tracking_id,
        )
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
    # The claim and audit are committed before any broker access. Never enclose publish
    # in the claiming transaction: rollback after a successful send would permit replay.
    connection = None
    attempted = False
    try:
        params = routes.pika.URLParameters(routes.AMQP_URL)
        params.socket_timeout = routes.BROKER_SOCKET_TIMEOUT_SECONDS
        params.blocked_connection_timeout = routes.BROKER_SOCKET_TIMEOUT_SECONDS
        connection = routes.pika.BlockingConnection(params)
        channel = connection.channel()
        channel.confirm_delivery()
        # Worker-owned durable queue: declaring also bootstraps KEDA on a fresh broker.
        channel.queue_declare(queue=TRADE_SUBMISSIONS, durable=True)
        carrier = {}
        routes.propagate.inject(carrier)
        body = json.dumps({**payload, "traceparent": carrier.get("traceparent")})
        attempted = True
        channel.basic_publish(
            exchange="",
            routing_key=TRADE_SUBMISSIONS,
            body=body,
            mandatory=True,
            properties=routes.pika.BasicProperties(
                delivery_mode=2, content_type="application/json", headers=carrier
            ),
        )
    except Exception as error:
        if not attempted or isinstance(error, routes.PRE_WRITE_BROKER_ERRORS):
            approval["publish_error"] = (
                "Failed before publication; approval may be retried."
            )
            with get_db_cursor() as cursor:
                _save_approval(cursor, row["id"], "awaiting_approval", approval)
        raise PublishUnavailable(
            "Publication failed. Pending approvals require reconciliation before retrying."
        ) from error
    finally:
        routes._close_broker_connection(connection)
    with get_db_cursor() as cursor:
        _save_approval(cursor, row["id"], "approved", approval)
    return _response(key, approval)


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


def list_offers(
    status="awaiting_approval", league_id=None, season=None, limit=50, offset=0
):
    conditions = ["transaction_type = 'trade_offer_submit'", "status = %s"]
    params = [status]
    for field, value in (("league_id", league_id), ("season", season)):
        if value is not None:
            conditions.append(f"{field} = %s")
            params.append(value)
    with get_db_cursor() as cursor:
        cursor.execute(
            f"SELECT {SELECT_COLUMNS} FROM execution_log WHERE {' AND '.join(conditions)} "
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
        names = {}
        if ids:
            cursor.execute(
                "SELECT espn_id, name FROM players WHERE espn_id = ANY(%s)",
                (list(ids),),
            )
            names = dict(cursor.fetchall())
    result = []
    for row in rows:
        offer = row["counter_offer"] or {}
        item = {
            k: v
            for k, v in row.items()
            if k not in ("counter_offer", "outcome_details")
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
                    "related_transaction_id",
                    "thread_root_id",
                )
            }
        )
        for side in ("incoming", "outgoing"):
            item[f"{side}_players"] = [
                dict(espn_id=pid, name=names.get(pid))
                for pid in offer.get(f"{side}_player_ids", [])
            ]
        audit = (row["outcome_details"] or {}).get("approval")
        item["approval"] = (
            {k: v for k, v in audit.items() if k != "payload"} if audit else None
        )
        item["evaluation"] = _stored_evaluation(row, offer)
        result.append(item)
    return dict(offers=result, limit=limit, offset=offset)
