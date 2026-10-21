import os
from contextlib import nullcontext
import json
import logging
import secrets
import uuid
import time
import pika
import re
from typing import Annotated, List, Literal, Optional, Union
from fastapi import APIRouter, Depends, Header, HTTPException, status
from opentelemetry import propagate, trace
from pydantic import BaseModel, Discriminator, Field, Tag, model_validator
from src.queue_contracts import LINEUP_UPDATES, WAIVER_ACTIONS, DRAFT_TARGETS
from src.api.draft import _redis_client
from src.database import get_db_cursor
from src.services import waiver_service

logger = logging.getLogger("GatewayRoutes")
router = APIRouter()

# How long a claimed idempotency key blocks a repeat enqueue. Comfortably longer than any
# pick clock (the retry window it guards) and far shorter than a draft, so keys from an
# earlier draft can never collide with a later one.
DRAFT_TRIGGER_IDEMPOTENCY_TTL_SECONDS = 900
# Same guard for /trigger/lineup (#189). It covers lineup_solver's in-process retries and
# the CronJob's pod-level retries, which all finish within minutes. Short enough that a
# deliberate re-run later the same day (after a manual revert on ESPN, say) isn't mistaken
# for a duplicate - and the optimizer's key includes the Job name, so a new Job never is.
LINEUP_TRIGGER_IDEMPOTENCY_TTL_SECONDS = 1800
# /trigger/waiver's guard spans a whole waiver week, not minutes: a claim sits queued on
# ESPN until waivers process, where the worker's roster check can't see it, so any re-run
# that week proposing the same claim would queue a second one. waiver_evaluator's key is
# per claim + season/week (deliberately no Job name, unlike lineup's).
WAIVER_TRIGGER_IDEMPOTENCY_TTL_SECONDS = 7 * 24 * 3600
# ...but only once published. The in-flight "pending" claim outlasts waiver_evaluator's
# retry budget (~2 min) and no more, so a request that dies mid-flight frees the key.
WAIVER_TRIGGER_CLAIM_TTL_SECONDS = 600
# Ceiling on any single broker operation for a deadline-carrying request.
BROKER_SOCKET_TIMEOUT_SECONDS = 10.0

# Load AMQP broker settings securely injected into environment variables
# Default vhost must be the percent-encoded "%2f", not a bare trailing "/" - a bare "//"
# parses as vhost "" (empty string) in pika, not "/", and RabbitMQ then rejects the
# connection with "vhost  not found" instead of connecting to the default vhost.
AMQP_URL = os.getenv(
    "AMQP_URL", "amqp://guest:guest@fantasy-broker.databases.svc.cluster.local:5672/%2f"
)

# Guards /v1/trigger/*, which bypass oauth2-proxy when called in-cluster. No default -
# OpenBao-injected in deployed environments, same convention as DB_PASSWORD.
TRIGGER_API_KEY = os.getenv("TRIGGER_API_KEY")


# Set by trigger_client from the calling job's run_log entry; the queued action is linked
# to that run as an output, which is how the audit log knows which run caused which move.
RunIdHeader = Annotated[
    Optional[str], Header(description="Run audit log id of the calling job, if any.")
]


def require_trigger_api_key(x_api_key: str = Header(...)) -> None:
    if not TRIGGER_API_KEY:
        raise HTTPException(
            status_code=500, detail="TRIGGER_API_KEY is not configured on this service."
        )
    if not secrets.compare_digest(x_api_key, TRIGGER_API_KEY):
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key.")


# ------------------------------------------------------------------------------
# PYDANTIC DATA TRANSFER OBJECTS (SCHEMAS)
# ------------------------------------------------------------------------------
class SwapInstruction(BaseModel):
    # Excluded from the dump so a swap's published body stays exactly what it was before
    # entries had kinds; the worker reads a missing kind as a swap.
    kind: Literal["swap"] = Field(
        "swap",
        exclude=True,
        description="Optional. Omitted or 'swap' means bench drop_player_id and promote add_player_id into slot.",
    )
    drop_player_id: str = Field(
        ...,
        description="ESPN player id of the starter to bench. Must be ESPN's, not the internal Sleeper players.player_id - the two id spaces overlap numerically, so the wrong namespace can silently swap a different player (#187)",
    )
    add_player_id: str = Field(
        ...,
        description="ESPN player id of the player to elevate to the active lineup. Must be ESPN's, not the internal Sleeper players.player_id - same id-namespace trap as drop_player_id",
    )
    slot: str = Field(
        ..., description="Target lineup position frame (e.g., RB1, WR2, FLEX)"
    )
    drop_player_name: Optional[str] = Field(
        None,
        description=(
            "Display name of the drop player. Optional, but lets the gateway's id-namespace "
            "guard reject an id that is both some player's espn_id and this player's internal "
            "player_id - without it only a purely internal id can be caught."
        ),
    )
    add_player_name: Optional[str] = Field(
        None,
        description="Display name of the add player, for the same id-namespace cross-check as drop_player_name.",
    )
    matchup_multiplier: Optional[float] = Field(
        None,
        description=(
            "Opponent-defense-strength multiplier applied to the added player's VORP for "
            "this week (#93), in [0.9, 1.1]. 1.0 (neutral) when MATCHUP_WEIGHTING_ENABLED "
            "is off, the week isn't Week 1, or no defense-strength proxy row was "
            "available - lineup_solver.py's optimizer always sends a concrete value, never "
            "omits the field; None is only possible from a non-optimizer caller. Lets the "
            "UI explain a nudge."
        ),
    )
    decision_inputs: Optional[dict] = Field(
        None,
        description=(
            "CORE-17 (#35): the VORP numbers that justified this specific swap (add/drop "
            "player vorp_score, matchup_multiplier). Lives on the swap, not the request, "
            "because one request carries a list of swaps and this must stay 1:1 with each "
            "one. Recorded to bot_decisions; never forwarded to the click bot."
        ),
    )


IR_MOVE_KINDS = ("ir", "activate_ir")
# bot_decisions.action per IR entry kind. An IR move promotes nobody, so its row has no drop.
IR_MOVE_ACTIONS = {"ir": "lineup_ir_move", "activate_ir": "lineup_ir_activate"}


class IrMoveInstruction(BaseModel):
    kind: Literal["ir", "activate_ir"] = Field(
        ...,
        description=(
            "'ir' moves player_id into an IR slot; 'activate_ir' moves them out of IR into "
            "slot. One player, nobody promoted."
        ),
    )
    player_id: str = Field(
        ...,
        description="ESPN player id of the player to move. Must be ESPN's, not the internal Sleeper players.player_id - same id-namespace trap as SwapInstruction's ids",
    )
    slot: str = Field(
        ...,
        description="Target slot: IR for 'ir'; any non-IR slot (normally BENCH) for 'activate_ir'",
    )
    player_name: Optional[str] = Field(
        None,
        description="Display name of the player, for the gateway's id-namespace cross-check.",
    )
    decision_inputs: Optional[dict] = Field(
        None,
        description="Why this move was proposed. Recorded to bot_decisions; never forwarded to the click bot.",
    )

    @model_validator(mode="after")
    def _slot_matches_kind(self):
        # Normalized the way the worker resolves slots ("RB1" and "RB" are one ESPN slot).
        # IR eligibility, a full IR slot and "not in IR" are left to the worker, which reads
        # ESPN's live roster; core only has a synced snapshot.
        slot = re.sub(r"\d+$", "", self.slot.strip().upper())
        if not slot:
            raise ValueError("slot is required")
        if self.kind == "ir" and slot != "IR":
            raise ValueError(f"kind 'ir' must target slot IR, got '{self.slot}'")
        if self.kind == "activate_ir" and slot == "IR":
            raise ValueError("kind 'activate_ir' moves a player out of IR, so its slot cannot be IR")
        return self


class SlotMoveInstruction(BaseModel):
    kind: Literal["move"] = Field(
        ...,
        description=(
            "Moves player_id to slot, benching nobody. Used to re-slot a starter who is "
            "staying in the lineup so another player's swap has room: a kept WR leaving FLEX "
            "for a WR slot so an RB can start in FLEX."
        ),
    )
    player_id: str = Field(
        ...,
        description="ESPN player id of the player to move. Must be ESPN's, not the internal Sleeper players.player_id - same id-namespace trap as SwapInstruction's ids",
    )
    slot: str = Field(..., description="Target lineup slot (e.g., RB1, WR2, FLEX). Not IR: that is the 'ir' kind's job")
    player_name: Optional[str] = Field(
        None,
        description="Display name of the player, for the gateway's id-namespace cross-check.",
    )
    decision_inputs: Optional[dict] = Field(
        None,
        description="Why this move was proposed. Recorded to bot_decisions; never forwarded to the click bot.",
    )

    @model_validator(mode="after")
    def _slot_is_not_ir(self):
        slot = re.sub(r"\d+$", "", self.slot.strip().upper())
        if not slot:
            raise ValueError("slot is required")
        if slot == "IR":
            raise ValueError("kind 'move' cannot target IR; use kind 'ir', which checks eligibility")
        return self


def _lineup_entry_tag(value) -> str:
    # A plain Field(discriminator="kind") rejects an entry with no kind even when the swap
    # model defaults it, and every existing caller sends none, so the tag is computed here.
    kind = value.get("kind", "swap") if isinstance(value, dict) else getattr(value, "kind", "swap")
    if kind in IR_MOVE_KINDS:
        return "ir_move"
    return "slot_move" if kind == "move" else kind


LineupEntry = Annotated[
    Union[
        Annotated[SwapInstruction, Tag("swap")],
        Annotated[IrMoveInstruction, Tag("ir_move")],
        Annotated[SlotMoveInstruction, Tag("slot_move")],
    ],
    Discriminator(_lineup_entry_tag),
]


class LineupTriggerPayload(BaseModel):
    team_id: str = Field(
        ..., description="Target local league team identification number"
    )
    optimized_swaps: List[LineupEntry] = Field(
        ...,
        description=(
            "Lineup moves, applied in one ESPN transaction. An entry without kind is a bench + "
            "promote swap; kind 'ir' / 'activate_ir' moves one player into or out of IR; kind "
            "'move' re-slots one starter."
        ),
    )
    idempotency_key: Optional[str] = Field(
        None,
        description=(
            "Stable key for this lineup decision (#189). lineup_solver retries a failed publish "
            "and the CronJob retries a failed pod, and a retry after an ambiguous failure (read "
            "timeout, reset after send) could otherwise enqueue the same swaps twice. Repeats "
            "within LINEUP_TRIGGER_IDEMPOTENCY_TTL_SECONDS return status 'duplicate'. Optional: "
            "the web UI's manual swaps omit it."
        ),
    )


class WaiverTriggerPayload(BaseModel):
    idempotency_key: Optional[str] = None
    team_id: str = Field(
        ..., description="Target local league team identification number"
    )
    player_id: str = Field(
        ...,
        description="ESPN player id of the free agent to claim. Must be ESPN's, not the internal Sleeper players.player_id - same id-namespace trap DraftPickTriggerPayload guards against",
    )
    name: str = Field(
        ...,
        description="Display name of the player to claim, as expected by the click bot",
    )
    team: str = Field(
        ...,
        description="NFL team abbreviation of the player to claim, for a name+team match against ESPN's own player pool",
    )
    drop_player_id: Optional[str] = Field(
        None,
        description=(
            "ESPN player id of the roster player to drop to open a slot. Optional: an "
            "add-only claim into an already-open bench slot omits this."
        ),
    )
    dry_run: Optional[bool] = Field(
        None,
        description=(
            "Per-request override of the worker's DRY_RUN setting. The web UI sends an "
            "explicit value; omitted (automated triggers) keeps the worker's default."
        ),
    )
    decision_inputs: Optional[dict] = Field(
        None,
        description=(
            "CORE-17 (#35): the VORP numbers (and season/week) that justified this claim. "
            "Recorded to bot_decisions; never forwarded to the click bot."
        ),
    )
    # Excluded from the dump so the click bot's message is unchanged; the hold lives in
    # the outbox, not the message.
    after_tracking_id: Optional[str] = Field(
        None,
        exclude=True,
        description=(
            "Tracking id of an IR move (a /trigger/lineup action with a kind 'ir' entry) "
            "that makes room for this add-only claim. The claim is held, not delivered, "
            "until that move is verified on ESPN's roster, and cancelled if it fails or "
            "isn't verified in time. Requires drop_player_id to be omitted."
        ),
    )


class DraftPickTriggerPayload(BaseModel):
    team_id: str = Field(
        ..., description="Target local league team identification number"
    )
    player_id: str = Field(
        ...,
        description="ESPN player id of the recommended draft target. Must be ESPN's, not the internal Sleeper players.player_id: the worker clicks button.Button--draft[data-player-id=...] in the draft room, and the two id spaces overlap numerically, so the wrong namespace silently matches a different player",
    )
    name: str = Field(
        ...,
        description="Display name of the recommended draft target, as expected by the click bot",
    )
    position: str = Field(
        ...,
        description="Player's position, for a name+position+team match against ESPN's own player pool",
    )
    pro_team: str = Field(
        ...,
        description="Player's NFL team abbreviation, for a name+position+team match against ESPN's own player pool",
    )
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
    league_id: Optional[str] = Field(
        None,
        description=(
            "League this pick belongs to. With pick_number, lets the gateway revalidate "
            "supersession immediately before publishing."
        ),
    )
    pick_number: Optional[int] = Field(
        None,
        description=(
            "Draft slot this target is for. The caller's cancellation flag cannot reach an "
            "attempt already blocked inside requests.post, so a pick landing mid-flight "
            "would otherwise still be enqueued and clicked against a later slot. The "
            "gateway rechecks the listener board's last_processed_pick_number here."
        ),
    )
    expires_at: Optional[float] = Field(
        None,
        description=(
            "Unix timestamp after which this pick must not be enqueued. The caller's HTTP "
            "timeout only stops the caller waiting; it does not cancel this handler, which "
            "can resume from a blocked broker call and queue a click long after the pick "
            "clock has run out - by then ESPN has autodrafted and the click lands on the "
            "next team's pick. Checked immediately before the message is published."
        ),
    )
    mode: Optional[Literal["live", "practice"]] = Field(
        None,
        description=(
            "Practice/live tag for this pick, forwarded to the click bot so it can safely "
            "submit for real in a practice room even when DRY_RUN is unset (which otherwise "
            "defaults to true everywhere, live included). Sourced from the caller's own "
            "context - draft_matrix.py's listener already knows its mode from the Job's own "
            "label/env, and the web UI knows it from the stored draft config - never guessed "
            "downstream, since a live league misidentified as practice would defeat the "
            "point of the safe default."
        ),
    )
    decision_inputs: Optional[dict] = Field(
        None,
        description=(
            "CORE-17 (#35): the scarcity-adjusted VORP (and season) that justified this "
            "recommendation. Recorded to bot_decisions; never forwarded to the click bot."
        ),
    )


# ------------------------------------------------------------------------------
# ROUTE ENDPOINTS
# ------------------------------------------------------------------------------
@router.get("/livez", status_code=status.HTTP_200_OK)
async def liveness_check():
    """
    Kubernetes liveness probe: answers as long as the event loop is serving requests.
    It never checks Postgres or RabbitMQ, so a dependency outage takes the pod out of
    rotation through /v1/health (readiness) instead of restarting it.
    """
    return {"status": "alive"}


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
        raise HTTPException(
            status_code=503, detail="Stateful database tier connection unreachable."
        )

    # 2. Validate RabbitMQ broker link parameters
    try:
        params = pika.URLParameters(AMQP_URL)
        connection = pika.BlockingConnection(params)
        connection.close()
    except Exception as e:
        logger.error(f"Liveness probe failed message broker connection check: {e}")
        raise HTTPException(
            status_code=503, detail="Event message bus broker unreachable."
        )

    return {"status": "healthy", "database": "connected", "broker": "connected"}


@router.post(
    "/trigger/lineup",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_trigger_api_key)],
)
def trigger_lineup_swap(payload: LineupTriggerPayload, x_run_id: RunIdHeader = None):
    """
    Asymmetric API gateway: Translates incoming high-level HTTP payloads into structured
    persistent queue messages inside RabbitMQ to activate automated browser worker scripts.
    """
    for swap in payload.optimized_swaps:
        if isinstance(swap, (IrMoveInstruction, SlotMoveInstruction)):
            _reject_internal_player_id(swap.player_id, swap.player_name)
            continue
        _reject_internal_player_id(swap.drop_player_id, swap.drop_player_name)
        _reject_internal_player_id(swap.add_player_id, swap.add_player_name)
    return _accept_durable(LINEUP_UPDATES, payload, run_id=x_run_id)


@router.post(
    "/trigger/waiver",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_trigger_api_key)],
)
def trigger_waiver_claim(payload: WaiverTriggerPayload, x_run_id: RunIdHeader = None):
    """
    Waiver-claim counterpart to /trigger/lineup (CORE-15/#117): publishes a live waiver
    evaluation's recommended add/drop to the click-bot queue. Same asymmetric gateway shape
    as /trigger/lineup - HTTP in, durable AMQP message out. No deadline handling (no pick
    clock), but the same idempotency claim protocol, week-scoped: see
    WAIVER_TRIGGER_IDEMPOTENCY_TTL_SECONDS.
    """
    depends_on = _ir_move_to_wait_for(payload) if payload.after_tracking_id else None
    _reject_internal_player_id(payload.player_id, payload.name)
    if payload.drop_player_id:
        _reject_internal_player_id(payload.drop_player_id)
    _reject_player_rostered_elsewhere(payload)
    return _accept_durable(WAIVER_ACTIONS, payload, depends_on=depends_on, run_id=x_run_id)


def _ir_move_to_wait_for(payload) -> str:
    """Checks the IR move an add-only claim is chained to, and returns its id. Unlike the
    other guards this fails CLOSED: a claim whose IR move can't be confirmed would land
    on a full roster, so it is refused rather than queued unheld."""
    from src.services.activity_service import (
        DependencyNotFoundError, DependencyRejectedError, require_ir_move,
    )

    if payload.drop_player_id:
        raise HTTPException(
            status_code=422,
            detail="A claim chained to an IR move is add-only; omit drop_player_id.",
        )
    try:
        return require_ir_move(payload.after_tracking_id, payload.team_id)
    except DependencyNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except DependencyRejectedError as exc:
        raise HTTPException(409, str(exc)) from exc
    except Exception as exc:
        logger.exception("Could not load IR move %s", payload.after_tracking_id)
        raise HTTPException(503, "Could not check the IR move this claim depends on.") from exc


def _reject_player_rostered_elsewhere(payload):
    """A player on another manager's roster can't be claimed; ESPN would only reject it
    later in the click bot. Our own team holding them is allowed through so a retried
    submission of an already-completed claim still dedupes to its original tracking id.
    Lookup failures fail open - the claim proceeds and the worker reports the outcome."""
    from datetime import datetime

    from src.ingestion.sync_league import refresh_rosters_if_stale
    from src.services.waiver_service import rostered_team_for_espn_id

    try:
        refresh_rosters_if_stale(int(os.getenv("CURRENT_YEAR", datetime.now().year)))
        owner = rostered_team_for_espn_id(payload.player_id)
    except Exception as exc:
        logger.warning(f"Could not verify waiver target ownership: {exc}")
        return
    if owner and owner != str(payload.team_id):
        raise HTTPException(
            status_code=409,
            detail=f"{payload.name} is on another team's roster. Propose a trade instead.",
        )


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


def _reject_internal_player_id(player_id: str, name: Optional[str] = None) -> None:
    """Guard against the id-namespace inversion that made auto-pick silently draft the wrong
    player. ESPN ids and Sleeper players.player_id values overlap numerically, so a mistake here
    is undetectable downstream - ESPN 12483 is Matthew Stafford, internal 12483 is Jack Bech.

    Existing in players.espn_id is NOT on its own sufficient to accept an id, which is the whole
    danger of the collision: 12483 is a real espn_id, so a caller meaning Jack Bech would sail
    through an existence check and draft Stafford. The payload's own name is the tiebreaker -
    the caller states who it believes it is acting on, so the two can be cross-checked.

    `name` is optional because not every id this guards has one available - a waiver claim's
    drop_player_id carries no name field in the wire contract. Without a name, the disambiguation
    step below is skipped entirely (there's nothing to cross-check), but the definite-wrong-
    namespace case - an id absent from players.espn_id but present as a players.player_id - is
    still decidable and still rejected regardless.

    Rejected: an id that is not an espn_id at all but is a known internal player_id - decidably
    the wrong namespace whatever the payload calls it - and (when a name is given) an id that is
    both, where the payload names the internal owner rather than the ESPN one.

    Allowed: an identity match; an id in neither column (players.espn_id is not resolved for every
    player, so a legitimate ESPN id for an uncovered player must stay draftable); and the
    genuinely ambiguous case, which is logged instead.
    """
    submitted = _normalized_name(name) if name is not None else None
    try:
        with get_db_cursor() as cursor:
            cursor.execute(
                "SELECT name FROM players WHERE espn_id = %s LIMIT 1;",
                (str(player_id),),
            )
            espn_match = cursor.fetchone()
            cursor.execute(
                "SELECT name FROM players WHERE player_id = %s LIMIT 1;",
                (str(player_id),),
            )
            internal_match = cursor.fetchone()
    except HTTPException:
        raise
    except Exception as e:
        # Fail open: the click bot is the draft-day safety valve and a database blip must not
        # block a manual pick. The check is a guard against a caller bug, not an authz gate.
        logger.warning(
            f"Could not validate player id namespace for '{name}' ({player_id}): {e}"
        )
        return

    if espn_match and (
        submitted is None or _normalized_name(espn_match[0]) == submitted
    ):
        return

    # An id absent from players.espn_id but present as a players.player_id is the wrong namespace
    # regardless of the name attached to it: a typo in the payload does not make a Sleeper id an
    # ESPN one. Only when the id lives in BOTH namespaces (and a name was actually given) does the
    # name decide which was meant.
    if internal_match and (
        not espn_match
        or (submitted is not None and _normalized_name(internal_match[0]) == submitted)
    ):
        owner = internal_match[0]
        detail = (
            f"player_id '{player_id}' is {owner}'s internal Sleeper id, not an ESPN id. "
            f"The browser worker needs players.espn_id here, not players.player_id."
        )
        if espn_match:
            detail += (
                f" In ESPN's namespace that id is {espn_match[0]}, so this would have acted on "
                f"the wrong player rather than failing."
            )
        raise HTTPException(status_code=422, detail=detail)

    if espn_match:
        # The id is a real espn_id, the names disagree (or no name was given), and no internal
        # row claims the id at all. Most likely crosswalk or name-format drift rather than a
        # namespace error, and blocking a live action on a name mismatch is worse than allowing it.
        logger.warning(
            f"Payload names '{name}' but espn_id {player_id} maps to '{espn_match[0]}'. "
            f"Allowing - verify this acted on the intended player."
        )

    return


def _record_bot_decision(
    *,
    tracking_id: str,
    bot_name: str,
    action: str,
    team_id: str,
    league_id: Optional[str] = None,
    target_player_id: Optional[str] = None,
    drop_player_id: Optional[str] = None,
    decision_inputs: Optional[dict] = None,
    cursor=None,
) -> None:
    """Records *why* an already-decided action happened (CORE-17/#35), separate from
    execution_log's whether-it-succeeded record. season/week are pulled out of
    decision_inputs into their own typed columns purely for cheap Grafana filtering.

    Fails OPEN, same philosophy as _reject_internal_player_id above: a DB hiccup logging
    the reasoning must never block the live action it describes. But fail-open is not
    fail-silent (#159): a dropped row logs at ERROR with the traceback and records the
    exception on the request span (plus a `bot_decision.write_failed` attribute), so a
    gap in the audit trail is visible in logs and Tempo. With an explicit cursor,
    participates in the caller's transaction and propagates failures instead.
    """
    # Approval callers supply their transaction: audit failure must roll back the claim.
    transaction_cursor = cursor
    inputs = decision_inputs or {}
    try:
        with (
            nullcontext(transaction_cursor)
            if transaction_cursor is not None
            else get_db_cursor()
        ) as cursor:
            cursor.execute(
                """
                INSERT INTO bot_decisions
                    (tracking_id, bot_name, action, team_id, league_id, season, week,
                     target_player_id, drop_player_id, inputs)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    tracking_id,
                    bot_name,
                    action,
                    team_id,
                    league_id,
                    inputs.get("season"),
                    inputs.get("week"),
                    target_player_id,
                    drop_player_id,
                    json.dumps(inputs),
                ),
            )
    except Exception as e:
        if transaction_cursor is not None:
            raise
        logger.error(
            f"Could not record bot_decisions row for tracking_id={tracking_id} ({bot_name}/{action}): {e}",
            exc_info=True,
        )
        # Span status stays OK on purpose - the live action itself succeeded (202). Guarded
        # because this runs after the AMQP publish committed: a telemetry error escaping
        # here would 500 the request and a retry would publish the action twice.
        try:
            span = trace.get_current_span()
            span.set_attribute("bot_decision.write_failed", True)
            span.record_exception(
                e,
                attributes={
                    "tracking_id": tracking_id,
                    "bot_name": bot_name,
                    "action": action,
                },
            )
        except Exception:
            logger.debug(
                "Could not mark bot_decisions write failure on the span", exc_info=True
            )


def _idempotency_redis_key(namespace: str, key: str) -> str:
    return f"{namespace}:trigger:idempotency:{key}"


def _claim_idempotency_key(
    key: Optional[str],
    token: str,
    namespace: str = "draft",
    ttl: int = DRAFT_TRIGGER_IDEMPOTENCY_TTL_SECONDS,
) -> str:
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
    redis_key = _idempotency_redis_key(namespace, key)
    try:
        client = _redis_client()
        # The value carries this request's token so only this request can release the claim.
        if client.set(redis_key, f"pending:{token}", nx=True, ex=ttl):
            return "claimed"
        # Lost the race. A key with no value left (expired between SET and GET) is treated as
        # still in flight rather than published; the caller retrying is the safe reading.
        return "published" if client.get(redis_key) == "published" else "pending"
    except Exception as e:
        logger.warning(
            f"Idempotency check unavailable for key {key}; proceeding without it: {e}"
        )
        return "claimed"


def _mark_idempotency_key_published(
    key: Optional[str],
    namespace: str = "draft",
    ttl: int = DRAFT_TRIGGER_IDEMPOTENCY_TTL_SECONDS,
) -> None:
    """Promotes a claim from "pending" once the message provably may exist, so that later
    retries for this pick get "duplicate" instead of being told to try again."""
    if not key:
        return
    try:
        _redis_client().set(_idempotency_redis_key(namespace, key), "published", ex=ttl)
    except Exception as e:
        logger.warning(f"Could not mark idempotency key {key} published: {e}")


_RELEASE_CLAIM_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


def _release_idempotency_key(
    key: Optional[str], token: str, namespace: str = "draft"
) -> None:
    """Releases a claim for a request that provably never enqueued anything.

    Called only from the pre-publish failure path. Leaving the claim there would turn a
    transient broker error into a permanent one: the caller's retry gets "duplicate", reads
    it as success, and stops - so the pick is lost by the mechanism that exists to protect it.
    """
    if not key:
        return
    try:
        # Compare-and-delete: a retry may already hold a fresh "pending" claim by the time
        # this runs, and a blind DELETE would drop it and let a third request enqueue too.
        _redis_client().eval(
            _RELEASE_CLAIM_LUA,
            1,
            _idempotency_redis_key(namespace, key),
            f"pending:{token}",
        )
    except Exception as e:
        # The TTL is the backstop. Worst case the retries for this pick all report duplicate
        # until it expires, which is the pre-existing failure and not made worse here.
        logger.warning(f"Could not release idempotency key {key}: {e}")


def _pick_slot_already_filled(
    league_id: Optional[str], pick_number: Optional[int]
) -> bool:
    """Last-moment supersession check. hold_and_publish's cancel_event cannot interrupt an
    attempt already blocked inside requests.post, so the slot can be taken while the request
    is in flight; enqueueing then puts the click on a later team's pick. The listener writes
    last_processed_pick_number as it persists each pick, so the board answers this directly.

    Fails OPEN, like the idempotency claim: an unreachable Redis cannot prove the slot is
    gone, and a missed pick is the worse outcome.
    """
    if not league_id or pick_number is None:
        return False
    try:
        last_processed = _redis_client().hget(
            f"draft:{league_id}:board", "last_processed_pick_number"
        )
        return last_processed is not None and int(last_processed) >= int(pick_number)
    except Exception as e:
        logger.warning(f"Could not check pick supersession for league {league_id}: {e}")
        return False


# Pika raises these before any frame reaches the socket, so no message can exist.
PRE_WRITE_BROKER_ERRORS = (
    pika.exceptions.ChannelWrongStateError,
    pika.exceptions.ConnectionWrongStateError,
)


def _close_broker_connection(connection) -> None:
    """Best-effort close. Already-closed connections raise from pika rather than no-op, and a
    failure to close must never mask the real outcome of the request."""
    if connection is None:
        return
    try:
        if connection.is_open:
            connection.close()
    except Exception as e:
        logger.warning(f"Could not close broker connection cleanly: {e}")


@router.post(
    "/trigger/draft-pick",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_trigger_api_key)],
)
def trigger_draft_pick(payload: DraftPickTriggerPayload, x_run_id: RunIdHeader = None):
    """
    Draft-day counterpart to /trigger/lineup (CORE-08): publishes the live optimizer's
    recommended draft target to the click-bot queue. Same asymmetric gateway shape -
    HTTP in, durable AMQP message out - the optimizer module never touches pika directly.
    """
    _reject_internal_player_id(payload.player_id, payload.name)
    if _pick_slot_already_filled(payload.league_id, payload.pick_number):
        raise HTTPException(409, "The targeted pick slot has already been filled.")
    if payload.expires_at is not None and time.time() >= payload.expires_at:
        raise HTTPException(
            409, "Pick deadline passed before the target could be queued."
        )
    return _accept_durable(DRAFT_TARGETS, payload, run_id=x_run_id)


def _accept_durable(queue, payload, depends_on=None, run_id=None):
    """Persists the action, then delivers it. With depends_on it is only persisted: the
    activity runner delivers it once that action is verified_complete, or cancels it."""
    from src.services.activity_service import create_action
    from src.services.activity_runner import publish_one

    body = payload.model_dump()
    carrier = {}
    propagate.inject(carrier)
    body["traceparent"] = carrier.get("traceparent")
    try:
        proposed_id = str(uuid.uuid4())
        identity = create_action(queue, body, proposed_id, depends_on=depends_on)
    except Exception as exc:
        logger.exception("Could not persist submission")
        raise HTTPException(
            503, "Submission was not accepted. Durable tracking is unavailable."
        ) from exc
    if identity == proposed_id:
        if run_id:
            from src.services import run_log_service

            run_log_service.link("activity_action", [identity], "output", run_id=run_id)
        if queue == LINEUP_UPDATES:
            for swap in payload.optimized_swaps:
                if isinstance(swap, IrMoveInstruction):
                    action, target, drop = IR_MOVE_ACTIONS[swap.kind], swap.player_id, None
                elif isinstance(swap, SlotMoveInstruction):
                    action, target, drop = "lineup_slot_move", swap.player_id, None
                else:
                    action, target, drop = "lineup_swap", swap.add_player_id, swap.drop_player_id
                _record_bot_decision(
                    tracking_id=identity,
                    bot_name="lineup_bot",
                    action=action,
                    team_id=payload.team_id,
                    target_player_id=target,
                    drop_player_id=drop,
                    decision_inputs=swap.decision_inputs,
                )
        else:
            _record_bot_decision(
                tracking_id=identity,
                bot_name="waiver_sniper"
                if queue == WAIVER_ACTIONS
                else "draft_clicker",
                action="waiver_claim" if queue == WAIVER_ACTIONS else "draft_pick",
                team_id=payload.team_id,
                target_player_id=payload.player_id,
                drop_player_id=getattr(payload, "drop_player_id", None),
                decision_inputs=payload.decision_inputs,
            )
    result = {
        "status": "accepted" if identity == proposed_id else "duplicate",
        "queue": queue,
        "team_id": payload.team_id,
        "tracking_id": identity,
    }
    if depends_on:
        # A duplicate may be held on an earlier IR move, so only a new claim names its parent.
        if identity == proposed_id:
            result["after_tracking_id"] = depends_on
        return result
    try:
        publish_one(identity)
    except Exception as exc:
        logger.exception("Submission persisted; background outbox will retry delivery")
        if queue == DRAFT_TARGETS:
            raise HTTPException(503, "Draft target saved but not delivered. Retry immediately with the same idempotency key.") from exc
    return result
