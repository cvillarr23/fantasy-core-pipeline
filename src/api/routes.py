import os
import json
import logging
import uuid
import pika
from typing import List, Optional
from fastapi import APIRouter, HTTPException, status
from opentelemetry import propagate
from pydantic import BaseModel, Field
from src.api.draft import _redis_client
from src.database import get_db_cursor

logger = logging.getLogger("GatewayRoutes")
router = APIRouter()

# How long a claimed idempotency key blocks a repeat enqueue. Comfortably longer than any
# pick clock (the retry window it guards) and far shorter than a draft, so keys from an
# earlier draft can never collide with a later one.
DRAFT_TRIGGER_IDEMPOTENCY_TTL_SECONDS = 900

# Load AMQP broker settings securely injected into environment variables
# Default vhost must be the percent-encoded "%2f", not a bare trailing "/" - a bare "//"
# parses as vhost "" (empty string) in pika, not "/", and RabbitMQ then rejects the
# connection with "vhost  not found" instead of connecting to the default vhost.
AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@fantasy-broker.databases.svc.cluster.local:5672/%2f")

# ------------------------------------------------------------------------------
# PYDANTIC DATA TRANSFER OBJECTS (SCHEMAS)
# ------------------------------------------------------------------------------
class SwapInstruction(BaseModel):
    drop_player_id: str = Field(..., description="Unique ESPN/Sleeper tracking ID to bench or cut")
    add_player_id: str = Field(..., description="Unique ESPN/Sleeper tracking ID to elevate to active lineup")
    slot: str = Field(..., description="Target lineup position frame (e.g., RB1, WR2, FLEX)")

class LineupTriggerPayload(BaseModel):
    team_id: str = Field(..., description="Target local league team identification number")
    optimized_swaps: List[SwapInstruction] = Field(..., description="Array of mathematical player reallocations")

class DraftPickTriggerPayload(BaseModel):
    team_id: str = Field(..., description="Target local league team identification number")
    player_id: str = Field(..., description="ESPN player id of the recommended draft target. Must be ESPN's, not the internal Sleeper players.player_id: the worker clicks button.Button--draft[data-player-id=...] in the draft room, and the two id spaces overlap numerically, so the wrong namespace silently matches a different player")
    name: str = Field(..., description="Display name of the recommended draft target, as expected by the click bot")
    position: str = Field(..., description="Player's position, for a name+position+team match against ESPN's own player pool")
    pro_team: str = Field(..., description="Player's NFL team abbreviation, for a name+position+team match against ESPN's own player pool")
    idempotency_key: Optional[str] = Field(
        None,
        description=(
            "Stable per-pick key. A publish that fails ambiguously - a read timeout, or a "
            "connection reset after the request was sent - may already have queued a durable "
            "message, so the caller's retry would otherwise enqueue a second draft action for "
            "the same pick and the worker would click twice. Supplying the same key on every "
            "attempt makes the enqueue idempotent. Optional: callers that genuinely want an "
            "independent action (a manual override from the UI, say) simply omit it."
        ),
    )

# ------------------------------------------------------------------------------
# ROUTE ENDPOINTS
# ------------------------------------------------------------------------------
@router.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    """
    Lightweight health probe verifying cluster connection liveness to internal datastores.
    Used natively by Kubernetes liveness and readiness tracking monitors.
    """
    # 1. Validate PostgreSQL connection health
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT 1;")
    except Exception as e:
        logger.error(f"Liveness probe failed database connection check: {e}")
        raise HTTPException(status_code=503, detail="Stateful database tier connection unreachable.")

    # 2. Validate RabbitMQ broker link parameters
    try:
        params = pika.URLParameters(AMQP_URL)
        connection = pika.BlockingConnection(params)
        connection.close()
    except Exception as e:
        logger.error(f"Liveness probe failed message broker connection check: {e}")
        raise HTTPException(status_code=503, detail="Event message bus broker unreachable.")

    return {"status": "healthy", "database": "connected", "broker": "connected"}

@router.post("/trigger/lineup", status_code=status.HTTP_202_ACCEPTED)
def trigger_lineup_swap(payload: LineupTriggerPayload):
    """
    Asymmetric API gateway: Translates incoming high-level HTTP payloads into structured
    persistent queue messages inside RabbitMQ to activate automated browser worker scripts.
    """
    tracking_id = str(uuid.uuid4())
    logger.info(f"Gateway processing roster realignment command sequence for Team ID {payload.team_id} (tracking_id={tracking_id})...")

    try:
        # 1. Establish an on-demand transaction connection channel to the cluster broker
        params = pika.URLParameters(AMQP_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        # 2. Declare and enforce durable queue settings matching the infrastructure layer
        channel.queue_declare(queue="lineup_updates", durable=True)

        # W3C trace context, separate from tracking_id below: tracking_id stays the
        # human/log-facing correlation UUID already returned to callers; traceparent is
        # the machine-facing OTel context so fantasy-browser-worker's eventual consumer
        # span (WORKER-09) can link back into this same trace instead of starting a
        # disconnected one. Per #26's spec this rides in the AMQP JSON body itself
        # (alongside tracking_id), not only in AMQP message properties, so a consumer
        # that only parses the body - not pika headers - can still recover it.
        carrier: dict = {}
        propagate.inject(carrier)

        # 3. Serialize our structured Pydantic body, stamped with a correlation ID and
        # trace context, into a compact system string mapping
        message_body = json.dumps({
            "tracking_id": tracking_id,
            "traceparent": carrier.get("traceparent"),
            **payload.model_dump(),
        })

        # 4. Publish message with high persistence markers to survive unexpected broker reboots
        channel.basic_publish(
            exchange="",
            routing_key="lineup_updates",
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2, # Make message persistent on disk
                content_type="application/json",
                headers=carrier,
            )
        )
        connection.close()
        logger.info(f"Asymmetric event transaction committed securely to RabbitMQ for Team {payload.team_id}.")
        return {
            "status": "accepted",
            "queue": "lineup_updates",
            "team_id": payload.team_id,
            "tracking_id": tracking_id,
        }

    except Exception as e:
        logger.error(f"Gateway failed to push transaction onto system exchange queue: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to submit message to background execution broker: {str(e)}")

NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def _normalized_name(value: str) -> str:
    """Collapse to comparable form: ESPN, Sleeper and DynastyProcess disagree on punctuation
    and generational suffixes ("A.J. Brown" / "AJ Brown", "Kenneth Walker III" / "Kenneth
    Walker"). Trailing suffix tokens are dropped, but never the only token left - "V" alone
    is somebody's whole name somewhere, and an empty string would match everything."""
    tokens = [
        "".join(ch for ch in token.casefold() if ch.isalnum())
        for token in value.split()
    ]
    tokens = [t for t in tokens if t]
    while len(tokens) > 1 and tokens[-1] in NAME_SUFFIXES:
        tokens.pop()
    return "".join(tokens)


def _reject_internal_player_id(player_id: str, name: str) -> None:
    """Guard against the id-namespace inversion that made auto-pick silently draft the wrong
    player. ESPN ids and Sleeper players.player_id values overlap numerically, so a mistake here
    is undetectable downstream - ESPN 12483 is Matthew Stafford, internal 12483 is Jack Bech.

    Existing in players.espn_id is NOT on its own sufficient to accept an id, which is the whole
    danger of the collision: 12483 is a real espn_id, so a caller meaning Jack Bech would sail
    through an existence check and draft Stafford. The payload's own name is the tiebreaker -
    the caller states who it believes it is drafting, so the two can be cross-checked.

    Rejected: an id that is not an espn_id at all but is a known internal player_id - decidably
    the wrong namespace whatever the payload calls it - and an id that is both, where the payload
    names the internal owner rather than the ESPN one.

    Allowed: an identity match; an id in neither column (the crosswalk covers ~82% of players and
    no K or D/ST, so a legitimate ESPN id for an uncovered player must stay draftable); and the
    genuinely ambiguous case, which is logged instead.
    """
    submitted = _normalized_name(name)
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT name FROM players WHERE espn_id = %s LIMIT 1;", (str(player_id),))
            espn_match = cursor.fetchone()
            cursor.execute("SELECT name FROM players WHERE player_id = %s LIMIT 1;", (str(player_id),))
            internal_match = cursor.fetchone()
    except HTTPException:
        raise
    except Exception as e:
        # Fail open: the click bot is the draft-day safety valve and a database blip must not
        # block a manual pick. The check is a guard against a caller bug, not an authz gate.
        logger.warning(f"Could not validate draft-pick id namespace for '{name}' ({player_id}): {e}")
        return

    if espn_match and _normalized_name(espn_match[0]) == submitted:
        return

    # An id absent from players.espn_id but present as a players.player_id is the wrong namespace
    # regardless of the name attached to it: a typo in the payload does not make a Sleeper id an
    # ESPN one. Only when the id lives in BOTH namespaces does the name decide which was meant.
    if internal_match and (not espn_match or _normalized_name(internal_match[0]) == submitted):
        owner = internal_match[0]
        detail = (
            f"player_id '{player_id}' is {owner}'s internal Sleeper id, not an ESPN id. "
            f"The browser worker clicks button.Button--draft[data-player-id=...] in the ESPN "
            f"draft room, so it needs players.espn_id."
        )
        if espn_match:
            detail += (
                f" In ESPN's namespace that id is {espn_match[0]}, so this would have drafted "
                f"the wrong player rather than failing."
            )
        raise HTTPException(status_code=422, detail=detail)

    if espn_match:
        # The id is a real espn_id, the names disagree, and no internal row claims the id at all.
        # Most likely crosswalk or name-format drift rather than a namespace error, and blocking
        # a draft-day pick on a name mismatch is worse than allowing it.
        logger.warning(
            f"Draft-pick payload names '{name}' but espn_id {player_id} maps to "
            f"'{espn_match[0]}'. Allowing - verify the pick landed on the intended player."
        )

    return


def _claim_idempotency_key(key: Optional[str]) -> str:
    """Claims `key` for this request. Returns "claimed", "pending" or "published".

    A publish that fails ambiguously - a read timeout, or a connection reset after the bytes
    went out - may already have committed a durable message. Without this, the caller's retry
    enqueues a second draft action and the worker clicks twice.

    The claim carries a state, not just existence, because a bare claim does not prove a
    message exists. If the first request claims the key and then hangs in BlockingConnection,
    the caller can time out and retry while it is still running: answering "duplicate" there
    would stop the caller, and the original request could then fail pre-publish and release
    the key with nothing ever enqueued. "pending" lets the caller retry instead.

    Fails OPEN. If Redis is unreachable we cannot prove the key is unclaimed, and the choice
    is between a possible duplicate click and a certainly missed pick. A duplicate SELECT for
    a player we already have is rejected by ESPN; a missed pick hands the roster spot to
    autodraft, which is the failure this whole retry path exists to prevent.
    """
    if not key:
        return "claimed"
    redis_key = f"draft:trigger:idempotency:{key}"
    try:
        client = _redis_client()
        if client.set(redis_key, "pending", nx=True, ex=DRAFT_TRIGGER_IDEMPOTENCY_TTL_SECONDS):
            return "claimed"
        # Lost the race. A key with no value left (expired between SET and GET) is treated as
        # still in flight rather than published; the caller retrying is the safe reading.
        return "published" if client.get(redis_key) == "published" else "pending"
    except Exception as e:
        logger.warning(f"Idempotency check unavailable for key {key}; proceeding without it: {e}")
        return "claimed"


def _mark_idempotency_key_published(key: Optional[str]) -> None:
    """Promotes a claim from "pending" once the message provably may exist, so that later
    retries for this pick get "duplicate" instead of being told to try again."""
    if not key:
        return
    try:
        _redis_client().set(
            f"draft:trigger:idempotency:{key}", "published",
            ex=DRAFT_TRIGGER_IDEMPOTENCY_TTL_SECONDS,
        )
    except Exception as e:
        logger.warning(f"Could not mark idempotency key {key} published: {e}")


def _release_idempotency_key(key: Optional[str]) -> None:
    """Releases a claim for a request that provably never enqueued anything.

    Called only from the pre-publish failure path. Leaving the claim there would turn a
    transient broker error into a permanent one: the caller's retry gets "duplicate", reads
    it as success, and stops - so the pick is lost by the mechanism that exists to protect it.
    """
    if not key:
        return
    try:
        _redis_client().delete(f"draft:trigger:idempotency:{key}")
    except Exception as e:
        # The TTL is the backstop. Worst case the retries for this pick all report duplicate
        # until it expires, which is the pre-existing failure and not made worse here.
        logger.warning(f"Could not release idempotency key {key}: {e}")
    """Releases a claim for a request that provably never enqueued anything.

    Called only from the pre-publish failure path. Leaving the claim there would turn a
    transient broker error into a permanent one: the caller's retry gets "duplicate", reads
    it as success, and stops - so the pick is lost by the mechanism that exists to protect it.
    """
    if not key:
        return
    try:
        _redis_client().delete(f"draft:trigger:idempotency:{key}")
    except Exception as e:
        # The TTL is the backstop. Worst case the retries for this pick all report duplicate
        # until it expires, which is the pre-existing failure and not made worse here.
        logger.warning(f"Could not release idempotency key {key}: {e}")


@router.post("/trigger/draft-pick", status_code=status.HTTP_202_ACCEPTED)
def trigger_draft_pick(payload: DraftPickTriggerPayload):
    """
    Draft-day counterpart to /trigger/lineup (CORE-08): publishes the live optimizer's
    recommended draft target to the click-bot queue. Same asymmetric gateway shape -
    HTTP in, durable AMQP message out - the optimizer module never touches pika directly.
    """
    tracking_id = str(uuid.uuid4())
    logger.info(f"Gateway processing draft pick command for Team ID {payload.team_id} -> {payload.name} (tracking_id={tracking_id})...")

    _reject_internal_player_id(payload.player_id, payload.name)

    claim = _claim_idempotency_key(payload.idempotency_key)
    if claim == "published":
        logger.info(
            f"Draft pick for Team {payload.team_id} -> {payload.name} already enqueued under "
            f"idempotency key {payload.idempotency_key}; not queueing a duplicate."
        )
        return {
            "status": "duplicate",
            "queue": "draft_targets",
            "team_id": payload.team_id,
            "tracking_id": tracking_id,
        }
    if claim == "pending":
        # Another request holds the key but has not reached the broker yet. It may still fail
        # pre-publish and release, so reporting success here could lose the pick entirely.
        # 503 is retryable, which sends the caller back for another attempt.
        logger.info(
            f"Draft pick for Team {payload.team_id} -> {payload.name} is still in flight under "
            f"idempotency key {payload.idempotency_key}; asking the caller to retry."
        )
        raise HTTPException(
            status_code=503,
            detail="An earlier request for this pick is still in flight; retry shortly.",
        )

    try:
        publish_attempted = False
        params = pika.URLParameters(AMQP_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.queue_declare(queue="draft_targets", durable=True)

        carrier: dict = {}
        propagate.inject(carrier)

        # idempotency_key is gateway-only state. The click-bot does not participate in
        # deduplication, so forwarding it (or a null for legacy callers) would change the
        # draft_targets JSON contract for no consumer benefit.
        message_body = json.dumps({
            "tracking_id": tracking_id,
            "traceparent": carrier.get("traceparent"),
            **payload.model_dump(exclude={"idempotency_key"}),
        })

        publish_attempted = True
        channel.basic_publish(
            exchange="",
            routing_key="draft_targets",
            body=message_body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                headers=carrier,
            )
        )
        connection.close()
        logger.info(f"Draft pick target committed to RabbitMQ for Team {payload.team_id}: {payload.name}.")
        _mark_idempotency_key_published(payload.idempotency_key)
        return {
            "status": "accepted",
            "queue": "draft_targets",
            "team_id": payload.team_id,
            "tracking_id": tracking_id,
        }

    except Exception as e:
        # Everything up to basic_publish is provably pre-publish: no message can exist, so
        # holding the claim would be actively harmful. The caller's retry would come back
        # "duplicate", hold_and_publish would read that as success and stop retrying, and
        # the pick would be silently ceded to autodraft by the very mechanism meant to
        # prevent it - a broker blip converted into a guaranteed miss. Releasing restores
        # the plain retry. A failure at or after basic_publish is genuinely ambiguous, so
        # the claim stands and a possible duplicate is preferred to a possible miss.
        if not publish_attempted:
            _release_idempotency_key(payload.idempotency_key)
        else:
            # Ambiguous: the bytes may have landed. Promote the claim so a retry gets a clean
            # "duplicate" rather than being told to keep retrying an outcome we cannot resolve.
            _mark_idempotency_key_published(payload.idempotency_key)
        logger.error(f"Gateway failed to push draft pick target onto system exchange queue: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to submit message to background execution broker: {str(e)}")

