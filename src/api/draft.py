"""
Draft-day browser control plane (CORE-10).

Lets the draft-day pipeline (CORE-08's draft_matrix.py listener) be configured, launched,
paused, and monitored from a browser instead of `kubectl` - the user will be traveling
internationally on the actual draft day. Sits alongside the hot path (browser-worker ->
draft_matrix -> /trigger/draft-pick -> RabbitMQ -> click bot) as an out-of-band control/
observability layer, not part of it.

The listener and the draft-room connector (fantasy-browser-worker's draft_sync.js) run through
the job launcher: /start, /stop and /practice/sync-job request and cancel catalog runs via
src/services/draft_run_service.py, and the launcher creates and stops their pods. This API only
reads k8s (Job phase for /status, pods and logs for /logs). The watcher retires a practice run
whose connector heartbeat has lapsed, which a separate stale-draft sweeper CronJob used to do.

execution_log has no `mode` column, but `_recent_execution_errors`/`_last_clicker_action` are
both scoped by `league_id` (and `_last_clicker_action` by `season` too) - since practice/live
configs always live under distinct league_ids (see CORE-18), a practice run's rows are naturally
excluded from a live league's /status the same way its runs and Redis keys already are, without
needing a dedicated column.

Also hosts /practice/sync-job (cvillarr23/fantasy-web-ui#9): starts the connector against a
practice/mock room URL a human joined by hand. This is a *different* run than /start's
draft_matrix.py listener above.
"""
import asyncio
import json
import logging
import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from math import ceil
from typing import List, Literal, Optional
from urllib.parse import parse_qs, urlparse

import redis
from espn_api.football.constant import POSITION_MAP, PRO_TEAM_MAP
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, field_validator

from src.database import get_db_cursor, get_redis_client
from src.schemas.espn_ownership import EspnOwnership
from src.services import draft_run_service
from src.services.draft_run_service import DraftLauncherUnavailableError
from src.services.espn_ownership import load_espn_ownership
from src.k8s_client import K8S_REQUEST_TIMEOUT_SECONDS
from src.ingestion.sync_kicker_dst import ESPN_TO_INTERNAL_TEAM
from src.optimization.draft_matrix import (
    _set_auto_publish_enabled,
    compute_draft_recommendation,
    compute_on_the_clock_team,
    compute_total_picks,
    load_league_config,
    publish_draft_target,
)

logger = logging.getLogger("DraftControlPlane")
draft_router = APIRouter()

# The real ESPN league (sync_league.py's ESPN_LEAGUE_ID) that practice/mock-draft-room
# configs must never collide with. Optional: if unset, practice-mode validation is skipped
# with a logged warning rather than guessed at.
ESPN_LEAGUE_ID = os.getenv("ESPN_LEAGUE_ID")
# Draft-sync listener defaults to week 1 preseason projections (matches draft_matrix.py's
# own --week CLI default) - draft config doesn't track a week, since a draft has none.
DRAFT_SYNC_WEEK = 1
# The publish-retry deadline in draft_matrix.hold_and_publish is bounded by the pick clock,
# and the two modes do not share one: ESPN practice rooms run 30s while real drafts are 90s+.
# Passed explicitly per run rather than left to the module default, because the failure is
# silent in the dangerous direction - a practice listener inheriting the 90s live default
# keeps retrying for a full minute after the clock it was protecting expired and autodrafted.
DRAFT_PICK_CLOCK_SECONDS_BY_MODE = {
    "live": int(os.getenv("DRAFT_PICK_CLOCK_SECONDS_LIVE", "90")),
    "practice": int(os.getenv("DRAFT_PICK_CLOCK_SECONDS_PRACTICE", "30")),
}
AUTO_DRAFT_DELAY_SECONDS_BY_MODE = {
    "live": int(os.getenv("AUTO_DRAFT_DELAY_SECONDS_LIVE", "45")),
    # Confirmed live 2026-08-27: 20s (66% of the 30s practice clock) left only 10s for the
    # actual publish - draft_matrix.py's own clamp now enforces a floor via
    # MIN_AUTO_PUBLISH_WINDOW_SECONDS regardless of this value, but this default is fixed to
    # match live's ~50% hold ratio too, so a practice room isn't *relying* on that clamp
    # (and the clamp's own warning log) just to get a sane starting point.
    "practice": int(os.getenv("AUTO_DRAFT_DELAY_SECONDS_PRACTICE", "15")),
}

SSE_POLL_INTERVAL_SECONDS = 2
DRAFT_STATUS_STALENESS_SECONDS = int(os.getenv("DRAFT_STATUS_STALENESS_SECONDS", "15"))
RECENT_ERRORS_LIMIT = 5

# GET /logs (#68) caps how much pod log a client can pull in one request - a crash traceback
# is a handful of lines, not a reason to let a browser tab request an unbounded log dump.
DRAFT_LOGS_DEFAULT_TAIL_LINES = 200
DRAFT_LOGS_MAX_TAIL_LINES = 1000

STOP_TIMEOUT_SECONDS = (
    draft_run_service.TERMINATION_MAX_POLLS * draft_run_service.TERMINATION_POLL_INTERVAL_SECONDS
)


def _redis_client() -> redis.Redis:
    return get_redis_client()


# ------------------------------------------------------------------------------
# PYDANTIC DATA TRANSFER OBJECTS (SCHEMAS)
# ------------------------------------------------------------------------------
class DraftConfigPayload(BaseModel):
    league_id: str = Field(..., description="ESPN league identifier the draft listener run should target")
    team_id: str = Field(..., description="Our own team_id within that league")
    season: int = Field(..., description="Season year, e.g. 2026")
    mode: Literal["live", "practice"] = Field("live", description="'practice' for ESPN mock draft rooms; tags the run/status so a practice run is never mistaken for the real Draft Day")
    auto_publish_enabled: bool = Field(True, description="Manual kill switch (#108/#109) for hold_and_publish's auto-publish. Defaults True so existing callers that never send this field see no behavior change.")


class DraftAutoPublishRequest(BaseModel):
    league_id: str = Field(..., description="League whose auto-publish kill switch should be toggled")
    enabled: bool = Field(..., description="False disables auto-publish (recorded as disabled_by='manual'); true re-enables it (clears disabled_by)")


class DraftPickSummary(BaseModel):
    pick_number: int
    round: Optional[int]
    team_id: str
    player_id: Optional[str] = Field(
        None,
        description=(
            "Internal players.player_id (Sleeper-namespaced). Null when the ESPN->internal crosswalk "
            "has no entry for this player - always the case for K and D/ST, which players does not carry."
        ),
    )
    espn_player_id: Optional[str] = Field(
        None,
        description="ESPN's own player id, as it arrived on the draft stream. Null only on picks recorded before this column existed.",
    )
    was_autodrafted: Optional[bool] = Field(
        None,
        description="True if this pick landed on a different player than the last target we actually attempted to publish for its pick_number (or no attempt was ever recorded, e.g. auto-publish was off) - i.e. ESPN's clock ran out and the room picked on our behalf rather than our recommendation landing. Only ever meaningful for our own team's picks; always null for opponent picks, since the concept of 'a target we attempted' does not apply to them.",
    )
    name: Optional[str] = Field(None, description="Joined from players; null if the player row is missing")
    position: Optional[str] = Field(None, description="Joined from players; null if the player row is missing")
    vorp_score: Optional[float] = Field(None, description="Player's most recent weekly_stats.vorp_score - a CURRENT pipeline value, not a snapshot frozen at pick time. Null if the player has no weekly_stats row (e.g. no crosswalk entry) or has no VORP computed yet.")
    projected_points: Optional[float] = Field(None, description="Player's most recent weekly_stats.projected_points, same current-value caveat as vorp_score.")
    scarcity_multiplier: Optional[float] = Field(None, description="Player's most recent weekly_stats.scarcity_multiplier, same current-value caveat as vorp_score.")
    matchup_coefficient: Optional[float] = Field(None, description="Player's most recent weekly_stats.matchup_coefficient, same current-value caveat as vorp_score.")
    espn: Optional[EspnOwnership] = Field(None, description="ESPN's market signal for this player (latest week captured this season); null when ESPN has no row for them or player_id is null.")


class TeamAggregate(BaseModel):
    total_vorp: float = Field(..., description="Sum of vorp_score across this team's drafted players with a weekly_stats row, using each player's most recent row - a CURRENT snapshot recomputed on every /status call, not a running total frozen pick-by-pick.")
    total_projected_points: float = Field(..., description="Sum of projected_points across this team's drafted players, same current-snapshot caveat as total_vorp.")
    position_counts: dict[str, int] = Field(default_factory=dict, description="Drafted player count per position (e.g. {'RB': 3, 'WR': 4}). Players with no players-table match (K/D-ST with no crosswalk entry) are omitted, not bucketed under a synthetic key.")
    most_recent_pick_value_alert: bool = Field(False, description="True if this team's single most recent pick landed VALUE_ROUND_THRESHOLD or more rounds later than its overall-VORP-rank would predict (see most_recent_pick_expected_round) - a 'great value' signal, not a claim the pick was objectively good.")
    most_recent_pick_vorp_rank: Optional[int] = Field(None, description="Overall vorp_score rank (1 = highest) of this team's most recent pick, across every player with a weekly_stats row - drafted or not. Null if that player has no vorp_score.")
    most_recent_pick_expected_round: Optional[int] = Field(None, description="ceil(most_recent_pick_vorp_rank / team_count) - the round a player at this VORP rank would be expected to go in a straight VORP-order draft. Null if vorp_rank or team_count is unavailable.")


class DraftStatusResponse(BaseModel):
    league_id: str
    season: int
    mode: str = Field(..., description="'live' or 'practice', sourced from the draft_config row for this league")
    auto_publish_enabled: bool = Field(True, description="Manual kill switch (#108/#109) for hold_and_publish's auto-publish; True (fail-open) when the draft_config row is unavailable")
    auto_publish_disabled_by: Optional[str] = Field(None, description="'manual' if an operator disabled auto-publish via POST /v1/draft/auto-publish, 'circuit_breaker' if a future automatic safety net tripped it, null if auto-publish has never been disabled for this league")
    job_state: str = Field(..., description="Active/Succeeded/Failed/Unknown, from the listener run's k8s Job, or its run_log row once the Job is gone")
    recommendation: dict = Field(..., description="Latest recommendation from CORE-08's solver (draft:{league_id}:board), empty if none yet")
    last_clicker_action: Optional[str] = Field(None, description="Human-readable outcome of WORKER-06's most recent click for this league (e.g. 'Drafted Breece Hall at 14:32:05', or a failure message), sourced from execution_log; null until the click bot has confirmed a pick")
    recent_picks: List[DraftPickSummary]
    team_ids: List[str] = Field(
        default_factory=list,
        description="Every team_id in the league, from draft_slots, in slot order - independent of who has picked so far. Unlike team_rosters/team_aggregates (which only ever gain a key once a team's first pick lands), this is the source of truth for rendering one comparison column per team, including teams with zero picks. Empty if draft_slots hasn't been synced yet.",
    )
    team_rosters: dict[str, List[DraftPickSummary]] = Field(
        default_factory=dict,
        description="All draft picks grouped by team_id for full-roster rendering, ordered by pick_number ascending within each team",
    )
    team_aggregates: dict[str, TeamAggregate] = Field(
        default_factory=dict,
        description="Per-team VORP/projected-points totals, position breakdown, and a value-pick alert for that team's most recent pick - keyed by team_id, same keys as team_rosters.",
    )
    on_the_clock_team_id: Optional[str] = Field(None, description="fantasy-browser-worker's live SELECTING WS frame when available (real-time, no chicken-and-egg gap for a team's own round-1 pick); falls back to our own snake-draft derivation, which is null until every team's round-1 slot is known, and again once the draft is complete - there is no next pick to be on the clock for")
    autodraft_active_team_id: Optional[str] = Field(None, description="Team whose ESPN autodraft is currently toggled on, from fantasy-browser-worker's live AUTODRAFT WS frame. Null if no team currently has it active or the signal hasn't been seen yet - this is a live toggle state, not a history of past autodrafted picks")
    draft_state: Literal["waiting", "active", "complete"] = Field(..., description="'waiting' before the first pick, 'active' while picks remain, 'complete' once picks_seen reaches every team's full roster (#82) - independent of pipeline_health, so a finished draft is never rendered as a degraded connection")
    pipeline_health: str = Field(..., description="'healthy' iff Postgres/Redis both respond and, while the draft is still active, the listener run's Job is Active and the listener's last-processed timestamp is fresh; a 'complete' draft only requires Postgres/Redis, since its run is expected to have exited, else 'degraded'")
    connector_health: Optional[dict] = Field(
        None,
        description=(
            "fantasy-browser-worker's draft:{league_id}:health heartbeat, forwarded verbatim "
            "(ws_connected, dom_observer, pick_silence_ms, last_pick_seen_at, picks_known, "
            "ledger_stats, per-tier ingest counters). Deliberately independent of "
            "pipeline_health - an evicted/blind connector and an unhealthy listener are "
            "different failures that must stay separately visible. Null if the connector has "
            "never written a heartbeat, its 1h TTL expired, or the key was malformed."
        ),
    )
    recent_errors: List[dict] = Field(default_factory=list, description=f"Most recent draft-related error rows from execution_log (limit {RECENT_ERRORS_LIMIT})")
    listener_state: Literal["Active", "Starting", "Failed", "Unknown", "Complete"] = Field(
        ...,
        description="League-scoped listener lifecycle state derived from the draft board hash and Job status. 'Complete' (#82) is the one terminal value the board's own explicit state - not job_state - can win with even though the Job is no longer Active.",
    )
    listener_last_event_at: Optional[str] = Field(
        None,
        description="Most recent listener event timestamp as ISO-8601 UTC string.",
    )
    listener_error: Optional[str] = Field(
        None,
        description="Most recent listener error text, if one was reported by the draft listener runtime.",
    )


class DraftPauseRequest(BaseModel):
    league_id: str = Field(..., description="League whose auto-draft should be paused/resumed")
    pick_number: Optional[int] = Field(None, description="Pick to pause; defaults to the next pick currently on the recommendation board")


class PracticeSyncJobRequest(BaseModel):
    room_url: str = Field(..., description="ESPN mock/practice draft room URL, obtained by manually joining the room in a real browser first (auto-join is blocked by ESPN's reCAPTCHA)")

    @field_validator("room_url")
    @classmethod
    def _room_url_must_be_https(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError("room_url must be an https:// URL")
        return v


# ------------------------------------------------------------------------------
# Status aggregation (shared by GET /status and GET /stream)
# ------------------------------------------------------------------------------
def _listener_run(league_id: str):
    """(the league's newest listener run or None, whether the lookup actually succeeded).

    Absent and unreadable have to stay distinguishable. Callers use absence to decide that
    leftover Redis state describes a finished run; treating a database failure the same way
    would suppress a live run's error banner exactly when something is already wrong.
    """
    try:
        return draft_run_service.latest_listener(league_id), True
    except Exception as e:
        logger.error(f"Failed to look up the draft listener run for league {league_id}: {e}")
        return None, False


def _listeners_on_other_leagues(target_league_id: str) -> list:
    """The active listener runs (either mode) watching some *other* league.

    Never fails open. An unreadable run_log is not "no conflicts" - treating it as such lets a
    transient blip slip a second listener past the duplicate guard, and two consumers on one
    Redis group split the pick stream between them."""
    return [
        run
        for run in draft_run_service.active_listeners()
        if run.params.get("league_id") != target_league_id
    ]


def _run_ids(runs) -> list:
    return [run.run_id for run in runs]


def _redis_glob_escape(value: str) -> str:
    """Escapes Redis glob metacharacters so a league id can be interpolated into a `scan_iter`
    match pattern literally.

    league_id reaches us from DraftConfigPayload as an arbitrary string. Unescaped, a value of
    `*` turns `draft:{id}:pause:*` into `draft:*:pause:*` and wipes every league's pause flags -
    including a live draft's - before Kubernetes ever gets a chance to reject it as a label."""
    return re.sub(r"([\\*?\[\]^-])", r"\\\1", value)


def _clear_league_draft_state(league_id: str, include_pick_stream: bool = False, raise_on_error: bool = False) -> int:
    """Deletes a league's recommendation board and pause flags - the same Redis state /stop
    clears, minus /stop's insistence that failure is fatal.

    The board holds listener_state, listener_error and the last recommendation, with no TTL.
    Nothing expires it, so once a run ends its final state sits there indefinitely and the UI
    reports a finished run's fatal error, and its stale top-picks, as though they were current.
    draft:{league_id}:suggested (fantasy-browser-worker's scraped ESPN-suggested-pick status
    key, issue #80) has the exact same no-TTL, single-overwrite shape and the same staleness
    risk, so it's cleared alongside the board rather than left to leak the previous run's
    suggestion into a new one. draft:{league_id}:selecting/:autodraft (ESPN's live on-the-clock
    and autodraft-toggle signals) are the same shape again - cleared here for the same reason,
    otherwise a new run could open showing a team as on the clock or autodrafting from a draft
    that already ended. Callers here are cleanup and startup paths, where a Redis
    hiccup must not abort the operation itself - refusing to start a draft because leftover
    state could not be cleared would be worse than starting with it. Logged loudly and
    reported instead.

    include_pick_stream additionally drops the connector's pick ledger, its health key and the
    pick Stream itself. Those are only safe to remove once the run is definitively over: the
    room connector starts BEFORE the listener does, so a pick published in that window would be
    lost if startup cleared the Stream. Teardown has no such race, and clearing there matters -
    the connector seeds its ledger from draft:{league}:picks and the listener replays the
    Stream backlog from id 0, so re-using a league id would otherwise resurrect the old run's
    picks and suppress the new ones as duplicates.

    raise_on_error flips that tolerance for the one caller that cannot afford it: ?force=true
    goes on to start a NEW connector, which seeds its ledger from draft:{league}:picks. Swallowing
    a failed delete there means the replacement silently inherits the previous run's picks and
    drops the real ones as duplicates - the precise failure this cleanup exists to prevent, so
    force fails closed where the cleanup paths fail open.
    """
    try:
        r_client = _redis_client()
        keys = [
            f"draft:{league_id}:board",
            f"draft:{league_id}:suggested",
            f"draft:{league_id}:selecting",
            f"draft:{league_id}:autodraft",
        ]
        keys.extend(r_client.scan_iter(match=f"draft:{_redis_glob_escape(league_id)}:pause:*"))
        if include_pick_stream:
            keys.extend([f"draft:{league_id}:picks", f"draft:{league_id}:health", f"draft_picks:{league_id}"])
        cleared = r_client.delete(*keys) if keys else 0
        if cleared:
            logger.info(f"Cleared {cleared} stale Redis draft key(s) for league {league_id}.")
        return cleared
    except Exception as e:
        if raise_on_error:
            raise
        logger.warning(
            f"Could not clear stale Redis draft state for league {league_id}: {e}. A previous "
            "run's listener error or recommendation may briefly show until the new listener "
            "overwrites it."
        )
        return 0


def _retire_practice_listeners(except_league_id: Optional[str] = None, clear_pick_stream: bool = False) -> list:
    """Cancels practice listener runs, optionally sparing one league's, and waits for their pods.

    Listeners are Redis Stream consumers that block until the draft completes, so a practice
    room nobody finished keeps its listener. Left running, a stale listener both consumes a
    league nobody is drafting and blocks the next practice run (start_draft_sync refuses to run
    two listeners at once). Only practice listeners are ever touched here - a live draft's
    listener must never be collected as a side effect of practice cleanup.

    clear_pick_stream additionally drops each retired league's pick ledger and Stream. It is
    off by default because it is only safe once the league's *publisher* is also gone: the
    room connector writes to those keys, and deleting them underneath a running connector
    either loses picks or lets it immediately recreate them. Only _teardown_practice_run, which
    waits for the connector to stop first, may pass True.

    Returns the cancelled run ids. Raises when a run can't be cancelled or outlives the wait:
    a caller that treats failed cleanup as success either starts a second listener alongside
    the one it failed to reclaim, or reports a 200 teardown while the listener still consumes."""
    runs = [
        run
        for run in draft_run_service.active_practice_listeners()
        if except_league_id is None or run.params.get("league_id") != except_league_id
    ]
    if runs and not draft_run_service.cancel_and_wait(_run_ids(runs)):
        raise RuntimeError(
            f"Practice draft-sync listener run(s) {', '.join(_run_ids(runs))} did not stop "
            f"within {STOP_TIMEOUT_SECONDS}s."
        )
    if runs:
        logger.info(f"Cancelled stale practice draft-sync listener runs: {', '.join(_run_ids(runs))}")
    # The run is only half the footprint. Its board outlives it with no TTL, so leaving it
    # behind is what makes a dead run's error and recommendation keep surfacing as current.
    for league_id in {run.params.get("league_id") for run in runs} - {None}:
        _clear_league_draft_state(league_id, include_pick_stream=clear_pick_stream)
    return _run_ids(runs)


def _to_iso_timestamp(value) -> Optional[str]:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OSError):
        return None


def _listener_status(job_state: str, recommendation: dict, listener_job_absent: bool = False) -> tuple:
    explicit_state = recommendation.get("listener_state")
    valid_states = {"Active", "Starting", "Failed", "Unknown", "Complete"}
    listener_state = explicit_state if explicit_state in valid_states else None

    listener_last_event_at = _to_iso_timestamp(
        recommendation.get("listener_last_event_at", recommendation.get("last_processed_pick_at"))
    )
    listener_error = recommendation.get("listener_error") or None

    if listener_job_absent:
        # No listener Job exists for this league, so whatever the board says describes a run
        # that has already ended. Reporting its final "Failed" and error text as the current
        # state is how a long-finished run keeps showing a fatal banner with nothing running -
        # there is no listener left to overwrite the board, and it has no TTL. The timestamp
        # is kept because "last seen at" is still true; the state and error are not.
        #
        # "Complete" (#82) is the one exception: unlike a stale "Active"/"Failed", it
        # describes a real, permanent fact about the run - the Job being gone (reaped/GC'd
        # after Succeeded) does not make a finished draft any less finished, so it must
        # survive here rather than fall back to "Unknown" the way every other explicit
        # state does.
        if listener_state == "Complete":
            return "Complete", listener_last_event_at, None
        return "Unknown", listener_last_event_at, None

    if job_state == "Failed":
        listener_state = "Failed"
    elif listener_state is None:
        if job_state == "Active":
            listener_state = "Active" if listener_last_event_at is not None else "Starting"
        else:
            listener_state = "Unknown"

    return listener_state, listener_last_event_at, listener_error


def _practice_room_identifier(run) -> Optional[str]:
    room_url = (run.params or {}).get("room_url") if run is not None else None
    if not room_url:
        return None
    parsed = urlparse(room_url)
    if parsed.fragment:
        return parsed.fragment
    return parse_qs(parsed.query).get("draftRoomId", [None])[0]


def _room_connected(league_id: Optional[str]) -> bool:
    if league_id is None:
        return False
    try:
        return bool(_redis_client().exists(f"draft:{league_id}:connected"))
    except Exception as e:
        logger.error(f"Failed to check draft room heartbeat for league {league_id}: {e}")
        return False


def _recommendation(r_client: redis.Redis, league_id: str) -> dict:
    """top_candidates rides the board hash as a JSON string (redis hashes are flat
    string->string) - decode it back into a real list here so API consumers get actual
    JSON, not a JSON-encoded string inside JSON.

    Also picks up two more pieces for CORE-19's ESPN-suggested-pick column: the board's own
    `vorp_by_espn_id` lookup (also JSON-encoded, written by draft_matrix.py's
    handle_pick_event alongside top_candidates) and fantasy-browser-worker's separately-keyed
    `draft:{league_id}:suggested` status snapshot. Resolving the suggestion into a real
    espn_suggestion object needs a `players` join this function has no DB cursor for, so it's
    only staged here (under a private key popped before the response is built) and finished
    by _resolve_espn_suggestion in _build_status, which does have one."""
    board = r_client.hgetall(f"draft:{league_id}:board") or {}
    if "top_candidates" in board:
        try:
            board["top_candidates"] = json.loads(board["top_candidates"])
        except (TypeError, ValueError):
            logger.error(f"Malformed top_candidates JSON on draft:{league_id}:board; dropping it.")
            board["top_candidates"] = []
    for key in ("auto_publish_at", "pick_clock_expires_at"):
        if key not in board:
            continue
        try:
            board[key] = int(float(board[key]))
        except (TypeError, ValueError):
            logger.error(f"Malformed {key} value on draft:{league_id}:board; dropping it.")
            board.pop(key, None)

    vorp_by_espn_id = {}
    if "vorp_by_espn_id" in board:
        raw_vorp = board.pop("vorp_by_espn_id")
        try:
            vorp_by_espn_id = json.loads(raw_vorp)
            if not isinstance(vorp_by_espn_id, dict):
                raise TypeError(f"expected an object, got {type(vorp_by_espn_id).__name__}")
        except (TypeError, ValueError):
            logger.error(f"Malformed vorp_by_espn_id JSON on draft:{league_id}:board; dropping it.")
            vorp_by_espn_id = {}

    # json.loads happily accepts a bare list/string/number as valid JSON - not just a
    # malformed string - so a decode success alone doesn't guarantee _resolve_espn_suggestion
    # can safely call .get()/["player_id"] on the result. Reviewer-caught (PR #97): without
    # this isinstance check, a non-object payload staged here would raise AttributeError
    # inside _build_status's Postgres try block, which misreports pipeline_health="degraded"
    # as though the database had failed rather than dropping the one malformed field.
    try:
        raw_suggested = r_client.get(f"draft:{league_id}:suggested")
        suggested = json.loads(raw_suggested) if raw_suggested else None
        if suggested is not None and not isinstance(suggested, dict):
            raise TypeError(f"expected an object, got {type(suggested).__name__}")
    except (TypeError, ValueError):
        logger.error(f"Malformed ESPN-suggested-pick JSON on draft:{league_id}:suggested; dropping it.")
        suggested = None

    if suggested is not None:
        board["_espn_suggestion_staging"] = {"raw": suggested, "vorp_by_espn_id": vorp_by_espn_id}
    return board


def _connector_health(league_id: str) -> Optional[dict]:
    """Reads fantasy-browser-worker's `draft:{league_id}:health` heartbeat (its issue #52,
    this repo's #81) - a JSON string the room connector SETs on a fixed cadence with a 1h
    TTL, describing WS/DOM/REST ingest-tier health (ws_connected, dom_observer, picks_known,
    pick_silence_ms, last_pick_seen_at, ledger_stats, per-tier counters). Opaque pass-through:
    nothing in this repo parses or depends on its internal shape, so a field the connector
    adds shows up here with no schema change, the same way `recommendation` passes CORE-08's
    board straight through. None is a normal state - never written yet, expired, or the
    connector hasn't started - not an error; callers must not treat it as a Redis failure."""
    raw = _redis_client().get(f"draft:{league_id}:health")
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        logger.error(f"Malformed connector health JSON on draft:{league_id}:health; dropping it.")
        return None


def _espn_selecting_team_id(league_id: str) -> Optional[str]:
    """Reads fantasy-browser-worker's `draft:{league_id}:selecting` status snapshot - ESPN's own
    live SELECTING <teamId> <clockMs> WS turn-notification, forwarded verbatim as JSON. This is
    the authoritative on-the-clock signal where available: unlike our own snake-draft derivation
    (_on_the_clock_team_id, which needs every earlier team's round-1 slot already recorded),
    ESPN tells us directly and in real time, so there is no chicken-and-egg gap for a team's own
    first pick. None is a normal state (older worker version, key not written yet), not an error."""
    raw = _redis_client().get(f"draft:{league_id}:selecting")
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
        return str(parsed["team_id"])
    except (TypeError, ValueError, KeyError):
        logger.error(f"Malformed on-the-clock JSON on draft:{league_id}:selecting; dropping it.")
        return None


def _espn_autodraft_active_team_id(league_id: str) -> Optional[str]:
    """Reads fantasy-browser-worker's `draft:{league_id}:autodraft` status snapshot - ESPN's own
    live AUTODRAFT <teamId> <bool> WS toggle broadcast. There was previously no live signal for
    this at all: the only existing autodraft detection (draft_pick_ledger's classifyPickOrigin)
    only tells you a pick already happened via autodraft, after the fact - too late for a UI
    warning meant to fire before a team's own clock runs out. `active: false` (a team's autodraft
    being explicitly turned back off) reports as None here, same as never having seen the frame -
    both mean "no team's autodraft is on right now," which is the only state this field means to
    capture."""
    raw = _redis_client().get(f"draft:{league_id}:autodraft")
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
        return str(parsed["team_id"]) if parsed.get("active") else None
    except (TypeError, ValueError, KeyError):
        logger.error(f"Malformed autodraft JSON on draft:{league_id}:autodraft; dropping it.")
        return None


# Same "most recent weekly_stats row per player" LATERAL pattern as players.py's
# search_players - vorp_score/scarcity_multiplier/matchup_coefficient are only ever stored
# per (player, season, week), so "current value" means the latest row that exists, not a
# value scoped to the draft's own season/week (a draft happens before that week's stats do).
#
# vorp_score and projected_points both DEFAULT 0.0 in the schema, so a row the projection
# ingester created but the VORP pipeline hasn't reached yet reads as a real, if terrible,
# score rather than "not computed" - a materially different claim. scarcity_multiplier has
# no default and is written by that same VORP update, so it's null exactly until computation
# completes; gating on it (rather than trusting the raw score) turns "not computed yet" back
# into a genuine null instead of a misleading zero.
_WEEKLY_STATS_LATERAL_JOIN = """
    LEFT JOIN LATERAL (
        SELECT
            CASE WHEN scarcity_multiplier IS NOT NULL THEN vorp_score END AS vorp_score,
            CASE WHEN scarcity_multiplier IS NOT NULL THEN projected_points END AS projected_points,
            scarcity_multiplier, matchup_coefficient
        FROM weekly_stats
        WHERE player_id = p.player_id
        ORDER BY season DESC, week DESC
        LIMIT 1
    ) w ON true
"""


def _recent_picks(cursor, league_id: str, season: int, limit: int) -> list:
    """Left-joins players so the pick feed can render name/position without a separate
    per-pick lookup - LEFT JOIN (not INNER) so a pick still shows up even if its player
    row is somehow missing, just with name/position null rather than the whole pick
    vanishing from the feed. That is the normal case for K and D/ST, which the players
    table doesn't carry at all: espn_player_id is returned alongside so the feed can still
    identify the pick when the crosswalk has nothing to join to.

    Also LATERAL-joins weekly_stats for vorp_score/projected_points/scarcity_multiplier/
    matchup_coefficient - these are each player's most recent computed values (CURRENT
    pipeline state), not a snapshot frozen at the moment this pick happened. Null for any
    pick whose player has no weekly_stats row (same K/D-ST case as above, or a player with
    no VORP computed yet)."""
    cursor.execute(f"""
        SELECT d.pick_number, d.round, d.team_id, d.player_id, d.espn_player_id, p.name, p.position,
               w.vorp_score, w.projected_points, w.scarcity_multiplier, w.matchup_coefficient, d.was_autodrafted
        FROM draft_picks d
        LEFT JOIN players p ON d.player_id = p.player_id
        {_WEEKLY_STATS_LATERAL_JOIN}
        WHERE d.league_id = %s AND d.season = %s
        ORDER BY d.pick_number DESC
        LIMIT %s;
    """, (league_id, season, limit))
    return [
        {"pick_number": row[0], "round": row[1], "team_id": row[2], "player_id": row[3],
         "espn_player_id": row[4], "name": row[5], "position": row[6],
         "vorp_score": row[7], "projected_points": row[8],
         "scarcity_multiplier": row[9], "matchup_coefficient": row[10],
         "was_autodrafted": row[11]}
        for row in cursor.fetchall()
    ]


def _league_team_ids(cursor, league_id: str, season: int) -> list:
    """Every team_id in the league, in draft-slot order - the only source that knows about a
    team before its first pick lands. team_rosters/team_aggregates are built by grouping
    draft_picks rows, so a team with zero picks so far never gets a key in either dict; this
    is what lets the frontend still render a (empty) column for it."""
    cursor.execute("SELECT team_id FROM draft_slots WHERE league_id = %s AND season = %s ORDER BY slot_number ASC;", (league_id, season))
    return [row[0] for row in cursor.fetchall()]


def _team_rosters(cursor, league_id: str, season: int) -> dict:
    """Same query/columns as _recent_picks (including the weekly_stats LATERAL join), just
    unbounded and grouped by team_id instead of limited and DESC - see that function's
    docstring for the join rationale and the current-vs-historical-value caveat."""
    cursor.execute(f"""
        SELECT d.pick_number, d.round, d.team_id, d.player_id, d.espn_player_id, p.name, p.position,
               w.vorp_score, w.projected_points, w.scarcity_multiplier, w.matchup_coefficient, d.was_autodrafted
        FROM draft_picks d
        LEFT JOIN players p ON d.player_id = p.player_id
        {_WEEKLY_STATS_LATERAL_JOIN}
        WHERE d.league_id = %s AND d.season = %s
        ORDER BY d.team_id ASC, d.pick_number ASC;
    """, (league_id, season))

    rosters: dict[str, list] = {}
    for row in cursor.fetchall():
        team_id = row[2]
        if team_id not in rosters:
            rosters[team_id] = []
        rosters[team_id].append(
            {
                "pick_number": row[0],
                "round": row[1],
                "team_id": team_id,
                "player_id": row[3],
                "espn_player_id": row[4],
                "name": row[5],
                "position": row[6],
                "vorp_score": row[7],
                "projected_points": row[8],
                "scarcity_multiplier": row[9],
                "matchup_coefficient": row[10],
                "was_autodrafted": row[11],
            }
        )
    return rosters


# A pick landing this many rounds later than its overall-VORP-rank would predict counts as
# a "great value" alert. Module-level constant so it's one tunable spot, not a magic number
# buried in the query below.
VALUE_ROUND_THRESHOLD = 2


def _team_aggregates(cursor, league_id: str, season: int, team_count: Optional[int]) -> dict:
    """Per-team VORP/projected-points totals, position counts, and a value-pick alert for
    each team's single most recent pick.

    All three are read in one shot: a position-count/sum GROUP BY, a pool-wide VORP rank
    (drafted or not, so a pick can be ranked against the whole player pool without re-running
    the live scarcity/ILP recompute draft_matrix.py uses for recommendations - that pipeline
    answers "what should we draft next", not "how good, historically, was any given pick"),
    and each team's MAX(pick_number) to find "most recent". team_count is None whenever
    _resolve_draft_size couldn't determine it (see that function) - the value alert is
    skipped in that case (expected_round is undefined without it) rather than guessing.
    """
    cursor.execute(f"""
        SELECT d.team_id, p.position, COUNT(*) AS player_count,
               COALESCE(SUM(w.vorp_score), 0.0) AS position_vorp,
               COALESCE(SUM(w.projected_points), 0.0) AS position_projected_points
        FROM draft_picks d
        LEFT JOIN players p ON d.player_id = p.player_id
        {_WEEKLY_STATS_LATERAL_JOIN}
        WHERE d.league_id = %s AND d.season = %s
        GROUP BY d.team_id, p.position;
    """, (league_id, season))

    aggregates: dict[str, dict] = {}
    for team_id, position, player_count, position_vorp, position_projected_points in cursor.fetchall():
        team = aggregates.setdefault(team_id, {
            "total_vorp": 0.0,
            "total_projected_points": 0.0,
            "position_counts": {},
            "most_recent_pick_value_alert": False,
            "most_recent_pick_vorp_rank": None,
            "most_recent_pick_expected_round": None,
        })
        team["total_vorp"] += float(position_vorp)
        team["total_projected_points"] += float(position_projected_points)
        if position is not None:
            team["position_counts"][position] = player_count

    if team_count:
        # ranked_pool is scoped to this draft's own (season, DRAFT_SYNC_WEEK) row per player -
        # the same (season, week) load_undrafted_pool and compute_draft_recommendation use for
        # draft-time projections - rather than each player's all-time most recent weekly_stats
        # row. The shared _WEEKLY_STATS_LATERAL_JOIN deliberately ignores season for per-pick
        # display (showing a drafted player's current value), but reusing that here would pull
        # in a retired or otherwise-absent player's stale historical VORP as if it were live,
        # inflating the ranked pool and skewing every expected_round calculation that follows.
        # scarcity_multiplier IS NOT NULL excludes rows the VORP pipeline hasn't reached yet,
        # for the same reason _WEEKLY_STATS_LATERAL_JOIN gates on it above.
        cursor.execute(f"""
            WITH ranked_pool AS (
                SELECT player_id, RANK() OVER (ORDER BY vorp_score DESC) AS vorp_rank
                FROM weekly_stats
                WHERE season = %s AND week = %s AND scarcity_multiplier IS NOT NULL
            ),
            most_recent_picks AS (
                SELECT DISTINCT ON (d.team_id) d.team_id, d.round, d.player_id
                FROM draft_picks d
                WHERE d.league_id = %s AND d.season = %s
                ORDER BY d.team_id, d.pick_number DESC
            )
            SELECT mrp.team_id, mrp.round, r.vorp_rank
            FROM most_recent_picks mrp
            JOIN ranked_pool r ON r.player_id = mrp.player_id;
        """, (season, DRAFT_SYNC_WEEK, league_id, season))
        for team_id, pick_round, vorp_rank in cursor.fetchall():
            team = aggregates.setdefault(team_id, {
                "total_vorp": 0.0,
                "total_projected_points": 0.0,
                "position_counts": {},
                "most_recent_pick_value_alert": False,
                "most_recent_pick_vorp_rank": None,
                "most_recent_pick_expected_round": None,
            })
            expected_round = ceil(vorp_rank / team_count)
            team["most_recent_pick_vorp_rank"] = vorp_rank
            team["most_recent_pick_expected_round"] = expected_round
            if pick_round is not None:
                team["most_recent_pick_value_alert"] = (pick_round - expected_round) >= VALUE_ROUND_THRESHOLD

    return aggregates


def _resolve_draft_size(cursor, league_id: str, season: int) -> tuple:
    """(team_count, total_picks, picks_seen) for this league/season - resolved once here
    instead of separately in _on_the_clock_team_id and _build_status's draft_state (#82).

    team_count resolution mirrors draft_matrix.league_team_count's own order (the synced
    ESPN value in league_settings, falling back to the season-scoped draft_config value a
    practice/mock room derives from pick arrivals) rather than calling that function
    directly - it takes a psycopg2 connection, not a cursor, and every caller here only
    ever holds a cursor (see get_db_cursor). Not vorp_matrix.load_league_config's own
    team_count field either, which is a hardcoded LIMIT-1 fallback (default 10) meant for
    CORE-05's generic pre-draft VORP math, not per-league accuracy - using it here would
    silently compute the wrong total_picks for any mock room that isn't exactly 10 teams.
    total_picks is None whenever team_count is unknown - "unknown", not "zero", since a
    None here must never be read as "already at zero picks needed"."""
    cursor.execute("SELECT team_count FROM league_settings WHERE league_id = %s;", (league_id,))
    row = cursor.fetchone()
    if row is None or not row[0]:
        cursor.execute("SELECT team_count FROM draft_config WHERE league_id = %s AND season = %s;", (league_id, season))
        row = cursor.fetchone()
    team_count = row[0] if row and row[0] else None

    total_picks = None
    if team_count:
        league_config = load_league_config(cursor)
        total_picks = compute_total_picks(team_count, league_config)

    cursor.execute("SELECT COALESCE(MAX(pick_number), 0) FROM draft_picks WHERE league_id = %s AND season = %s;", (league_id, season))
    picks_seen_row = cursor.fetchone()
    picks_seen = picks_seen_row[0] if picks_seen_row else 0

    return team_count, total_picks, picks_seen


def _on_the_clock_team_id(cursor, league_id: str, season: int, team_count: Optional[int], total_picks: Optional[int], picks_seen: int) -> Optional[str]:
    if team_count is None:
        return None
    if total_picks is not None and picks_seen >= total_picks:
        # #82: the draft is over - there is no next pick to be "on the clock" for, and
        # computing one anyway (the old behavior) is exactly what let a finished draft
        # keep reporting a team id for a pick number that will never happen.
        return None

    cursor.execute("SELECT team_id, slot_number FROM draft_slots WHERE league_id = %s AND season = %s;", (league_id, season))
    slot_map = {team_id: slot_number for team_id, slot_number in cursor.fetchall()}
    if not slot_map:
        return None

    return compute_on_the_clock_team(picks_seen + 1, team_count, slot_map)


def _recent_execution_errors(cursor, league_id: str, season: int, limit: int) -> list:
    """Scoped by league_id so a practice room's failures never surface in a live league's
    /status - execution_log has no dedicated mode column (see the module docstring), but
    practice/live configs always use distinct league_ids, so this filter alone keeps the
    two from ever mixing here.

    Also scoped by season, for the same reason _last_clicker_action is: a reactivated league
    keeps its league_id year over year, so without this next year's draft would open showing
    last year's failures as current. Rows with a null season are still included - producers
    other than the draft path do not set it, and dropping them would silently narrow what
    /status has always reported."""
    cursor.execute("""
        SELECT transaction_type, bot_name, error_message, executed_at
        FROM execution_log
        WHERE status <> 'success'
          AND error_message IS NOT NULL
          AND league_id = %s
          AND (season = %s OR season IS NULL)
          AND (transaction_type ILIKE 'draft%%' OR COALESCE(bot_name, '') ILIKE 'draft%%')
        ORDER BY executed_at DESC
        LIMIT %s;
    """, (league_id, season, limit))
    return [
        {
            "transaction_type": row[0],
            "bot_name": row[1],
            "error_message": row[2],
            "executed_at": row[3],
        }
        for row in cursor.fetchall()
    ]


def _last_clicker_action(cursor, league_id: str, season: int) -> Optional[str]:
    """Most recent draft_pick_click confirmation from fantasy-browser-worker's click bot
    (WORKER-06) for this league/season, formatted for direct display - not the raw row,
    since the dashboard has no other use for execution_log's internal shape here.

    Scoped by season, not just league_id: draft_picks/draft_slots are both keyed by
    (league_id, season) because a reactivated league keeps the same league_id year over
    year - without this, a new season's /status would show last season's final click
    until the first confirmation of the new draft comes in."""
    cursor.execute("""
        SELECT player_name, status, error_message, executed_at
        FROM execution_log
        WHERE transaction_type = 'draft_pick_click' AND league_id = %s AND season = %s
        ORDER BY executed_at DESC
        LIMIT 1;
    """, (league_id, season))
    row = cursor.fetchone()
    if row is None:
        return None
    player_name, row_status, error_message, executed_at = row
    if row_status == "success":
        return f"Drafted {player_name} at {executed_at:%H:%M:%S}"
    if row_status == "skipped_dry_run":
        # Was previously indistinguishable from silence: DRY_RUN wrote no row at all, so a
        # skipped pick looked exactly like nothing had happened yet. Surfacing it here is the
        # whole point - this status exists so a DRY_RUN skip is never mistaken for a timeout.
        return f"Skipped {player_name} at {executed_at:%H:%M:%S} - DRY_RUN was on, no submission was attempted"
    return f"Failed to draft {player_name}: {error_message}"


def _is_stale(job_state: str, recommendation: dict) -> bool:
    if job_state != "Active":
        return False
    last_processed_pick_at = recommendation.get("last_processed_pick_at")
    if last_processed_pick_at is None:
        return True
    try:
        return (time.time() - float(last_processed_pick_at)) > DRAFT_STATUS_STALENESS_SECONDS
    except (TypeError, ValueError):
        return True


# A draft happens before any week is played; week 18 makes the loader take the newest row of the season.
DRAFT_OWNERSHIP_WEEK = 18


def _attach_espn_ownership(cursor, season: int, recent_picks: list, team_rosters: dict, recommendation: dict) -> None:
    """Sets `espn` on every pick, ranked candidate and the ESPN suggestion, from one batched
    query. Applied on read because the market signal moves during a draft."""
    candidates = recommendation.get("top_candidates") or []
    suggestion = recommendation.get("espn_suggestion")
    targets = (
        recent_picks
        + [pick for picks in team_rosters.values() for pick in picks]
        + [c for c in candidates if isinstance(c, dict)]
        + ([suggestion] if suggestion else [])
    )
    ownership = load_espn_ownership(cursor, [t.get("player_id") for t in targets], season, DRAFT_OWNERSHIP_WEEK)
    for target in targets:
        espn = ownership.get(target.get("player_id"))
        target["espn"] = espn.model_dump() if espn is not None else None


def _resolve_espn_suggestion(cursor, staging: dict) -> Optional[dict]:
    """Joins fantasy-browser-worker's scraped ESPN suggested-pick (issue #80) against our own
    players/VORP data - the "API half" of CORE-19. staging is
    {"raw": <worker's draft:{league_id}:suggested payload>, "vorp_by_espn_id": {...}}; raw
    always carries at least an ESPN player_id, but its name/position_id/pro_team_id degrade
    to null if the worker's own player-pool lookup failed (see that repo's
    handleAutosuggestFrame) - handled the same way here, by falling through to the
    espn_id-less branch below.

    A `players` match (the common case - the worker's catalog uses ESPN's own ids, the same
    ones players.espn_id already carries) wins over the worker's own fields, since our row is
    authoritative. No match (K/D-ST, or any player the crosswalk hasn't caught up to) falls
    back to translating ESPN's numeric position_id/pro_team_id ourselves - reusing
    espn_api's own POSITION_MAP/PRO_TEAM_MAP and sync_kicker_dst's WSH->WAS normalization,
    the same way sync_league.py/sync_kicker_dst.py already do - rather than dropping the
    suggestion the issue asks to keep (null score, not a missing row).
    """
    raw = staging["raw"]
    espn_player_id = raw.get("player_id")
    if espn_player_id is None:
        return None
    espn_player_id = str(espn_player_id)

    cursor.execute("SELECT player_id, name, position, team FROM players WHERE espn_id = %s;", (espn_player_id,))
    row = cursor.fetchone()
    if row:
        player_id, name, position, team = row
    else:
        player_id = None
        name = raw.get("name")
        position = POSITION_MAP.get(raw.get("position_id"))
        pro_team = PRO_TEAM_MAP.get(raw.get("pro_team_id"))
        team = ESPN_TO_INTERNAL_TEAM.get(pro_team, pro_team)

    return {
        "espn_player_id": espn_player_id,
        "player_id": player_id,
        "name": name,
        "position": position,
        "team": team,
        "vorp_score": staging["vorp_by_espn_id"].get(espn_player_id),
        "suggested_at": raw.get("timestamp"),
    }


def _build_status(league_id: str, season: int, limit: int) -> dict:
    """Every datastore read is isolated in its own try/except: a Postgres or Redis outage
    must surface as pipeline_health="degraded" in a normal 200 response (so the browser can
    make the documented auto-picker fallback decision), not escape as an unhandled
    exception that turns the whole endpoint into a 500."""
    # One lookup serves both: the run's Job phase, and whether a listener describes the board.
    listener_run, listener_query_ok = _listener_run(league_id)
    job_state = draft_run_service.run_job_state(listener_run)

    redis_ok = True
    recommendation: dict = {}
    try:
        recommendation = _recommendation(_redis_client(), league_id)
    except Exception as e:
        logger.error(f"Failed to read Redis recommendation board for league {league_id}: {e}")
        redis_ok = False

    # Deliberately its own try/except, isolated from redis_ok/pipeline_health above: the
    # connector and the listener fail independently, and conflating them is exactly what let
    # #81 hide an evicted, blind connector behind an otherwise-healthy listener.
    connector_health: Optional[dict] = None
    try:
        connector_health = _connector_health(league_id)
    except Exception as e:
        logger.error(f"Failed to read Redis connector health for league {league_id}: {e}")

    # Isolated from redis_ok/pipeline_health for the same reason connector_health is: a read
    # failure here must not misreport the whole pipeline as degraded over a signal that's
    # allowed to simply be absent (older worker version, or the frame hasn't arrived yet).
    espn_selecting_team_id: Optional[str] = None
    autodraft_active_team_id: Optional[str] = None
    try:
        espn_selecting_team_id = _espn_selecting_team_id(league_id)
        autodraft_active_team_id = _espn_autodraft_active_team_id(league_id)
    except Exception as e:
        logger.error(f"Failed to read Redis on-the-clock/autodraft state for league {league_id}: {e}")

    db_ok = True
    recent_picks: list = []
    team_ids: list = []
    team_rosters: dict = {}
    team_aggregates: dict = {}
    on_the_clock_team_id = None
    recent_errors: list = []
    last_clicker_action = None
    # Neither "picks have started" nor "the total is known" until proven otherwise - a
    # failed read below must default draft_state to "waiting", not accidentally "complete".
    total_picks = None
    picks_seen = 0
    # Popped here (unconditionally, before the DB block below can fail) rather than inside
    # the try, so this internal staging key can never leak into the response regardless of
    # whether the players join that resolves it succeeds.
    espn_suggestion_staging = recommendation.pop("_espn_suggestion_staging", None)
    # "unknown" (not "live") is the pre-query default: a Postgres outage must not let a
    # practice run get silently reported as mode="live" just because the mode SELECT never
    # ran - that would route a practice run into live monitoring precisely when db_ok is
    # already about to go false. Only a successful query that finds no draft_config row
    # falls back to "live" (today's pre-CORE-18 default for an unconfigured league).
    mode = "unknown"
    auto_publish_enabled = True
    auto_publish_disabled_by = None
    try:
        with get_db_cursor() as cursor:
            cursor.execute(
                "SELECT mode, auto_publish_enabled, auto_publish_disabled_by FROM draft_config WHERE league_id = %s;",
                (league_id,),
            )
            mode_row = cursor.fetchone()
            if mode_row is not None:
                mode, auto_publish_enabled, auto_publish_disabled_by = mode_row
            else:
                mode = "live"
            recent_picks = _recent_picks(cursor, league_id, season, limit)
            team_ids = _league_team_ids(cursor, league_id, season)
            team_rosters = _team_rosters(cursor, league_id, season)
            team_count, total_picks, picks_seen = _resolve_draft_size(cursor, league_id, season)
            team_aggregates = _team_aggregates(cursor, league_id, season, team_count)
            # ESPN's own live signal wins when present - see _espn_selecting_team_id's header
            # for why it's authoritative where our own derivation has a structural gap. Only
            # falls back to the derived value when the draft is still active (not "complete"):
            # ESPN's key has no TTL and nothing clears it once a draft ends the same way
            # draft:{league_id}:suggested didn't before PR #97, so trusting it unconditionally
            # here would resurrect a finished draft's last-known "on the clock" team forever.
            on_the_clock_team_id = espn_selecting_team_id if picks_seen < (total_picks or float("inf")) else None
            if on_the_clock_team_id is None:
                on_the_clock_team_id = _on_the_clock_team_id(cursor, league_id, season, team_count, total_picks, picks_seen)
            recent_errors = _recent_execution_errors(cursor, league_id, season, RECENT_ERRORS_LIMIT)
            last_clicker_action = _last_clicker_action(cursor, league_id, season)
            if espn_suggestion_staging is not None:
                espn_suggestion = _resolve_espn_suggestion(cursor, espn_suggestion_staging)
                if espn_suggestion is not None:
                    recommendation["espn_suggestion"] = espn_suggestion
            _attach_espn_ownership(cursor, season, recent_picks, team_rosters, recommendation)
    except Exception as e:
        logger.error(f"Failed to read draft picks/slots/mode for league {league_id}: {e}")
        db_ok = False

    # #82: "complete" is a first-class terminal state, not "degraded" - a finished draft's
    # k8s Job is expected to no longer be Active (draft_matrix.run_draft_listener exits
    # cleanly once it observes the same total_picks threshold), so requiring job_state ==
    # "Active" here is exactly what used to make a normal finished run report "degraded".
    if total_picks is not None and picks_seen >= total_picks:
        draft_state = "complete"
    elif picks_seen == 0:
        draft_state = "waiting"
    else:
        draft_state = "active"

    if draft_state == "complete":
        pipeline_health = "healthy" if db_ok and redis_ok else "degraded"
        # Unlike top_candidates/recommended_player_id (a real record of the last thing this
        # pipeline recommended, harmless to keep showing after the draft ends), ESPN's
        # suggested pick is an artifact of its live draft-room UI specifically saying "here's
        # who's up next" - and nothing ever clears draft:{league_id}:suggested once the draft
        # is over (it has no TTL and isn't touched by draft completion, only by /start or
        # /stop - reviewer-caught, PR #97). Once complete, there is no "next" anymore, so
        # showing the last-seen suggestion (and its VORP) as though still current is actively
        # misleading rather than merely stale.
        recommendation.pop("espn_suggestion", None)
    else:
        pipeline_health = "healthy" if job_state == "Active" and db_ok and redis_ok and not _is_stale(job_state, recommendation) else "degraded"
    listener_job_absent = listener_query_ok and draft_run_service.ended_without_outcome(listener_run)
    listener_state, listener_last_event_at, listener_error = _listener_status(
        job_state,
        recommendation,
        # Only a successful query that found nothing counts as absent. A failed read leaves
        # this False so a genuine error stays visible.
        listener_job_absent=listener_job_absent,
    )
    if listener_job_absent:
        # The board's recommendation is as dead as its listener_state: recommended player,
        # pick number and top_candidates all describe a run that has already ended, and there
        # is nothing left to refresh them. Suppressing the error while still serving the stale
        # top picks would just make the staleness harder to notice.
        recommendation = {}

    return {
        "league_id": league_id,
        "season": season,
        "mode": mode,
        "auto_publish_enabled": auto_publish_enabled,
        "auto_publish_disabled_by": auto_publish_disabled_by,
        "job_state": job_state,
        "recommendation": recommendation,
        "last_clicker_action": last_clicker_action,
        "recent_picks": recent_picks,
        "team_ids": team_ids,
        "team_rosters": team_rosters,
        "team_aggregates": team_aggregates,
        "on_the_clock_team_id": on_the_clock_team_id,
        "autodraft_active_team_id": autodraft_active_team_id if draft_state != "complete" else None,
        "draft_state": draft_state,
        "pipeline_health": pipeline_health,
        "connector_health": connector_health,
        "recent_errors": recent_errors,
        "listener_state": listener_state,
        "listener_last_event_at": listener_last_event_at,
        "listener_error": listener_error,
    }


# ------------------------------------------------------------------------------
# ROUTE ENDPOINTS
# ------------------------------------------------------------------------------
@draft_router.post("/config", status_code=status.HTTP_200_OK)
def set_draft_config(payload: DraftConfigPayload):
    """Stores league_id/team_id/season/mode so /start can launch the listener run from it."""
    if payload.mode == "practice":
        if ESPN_LEAGUE_ID is None:
            logger.warning(f"ESPN_LEAGUE_ID is not configured - skipping the real-league collision check for practice config league_id={payload.league_id}.")
        elif payload.league_id == ESPN_LEAGUE_ID:
            raise HTTPException(status_code=400, detail=f"league_id {payload.league_id} matches the real ESPN_LEAGUE_ID - a practice/mock-draft-room config must use the mock room's own league_id.")

    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO draft_config (league_id, team_id, season, mode, auto_publish_enabled)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (league_id) DO UPDATE
                SET team_id = EXCLUDED.team_id, season = EXCLUDED.season, mode = EXCLUDED.mode,
                    auto_publish_enabled = EXCLUDED.auto_publish_enabled,
                    -- A derived team count belongs to the season it was derived from. Reusing
                    -- a room for a new season must start over rather than inherit it.
                    team_count = CASE WHEN draft_config.season IS DISTINCT FROM EXCLUDED.season
                                      THEN NULL ELSE draft_config.team_count END,
                    updated_at = CURRENT_TIMESTAMP;
            """, (payload.league_id, payload.team_id, payload.season, payload.mode, payload.auto_publish_enabled))
    except Exception as e:
        logger.error(f"Failed to persist draft config for league {payload.league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to store draft config: {str(e)}")

    return {"status": "stored", "league_id": payload.league_id, "team_id": payload.team_id, "season": payload.season, "mode": payload.mode, "auto_publish_enabled": payload.auto_publish_enabled}


@draft_router.get("/config", status_code=status.HTTP_200_OK)
def get_draft_config():
    """Returns the most recently stored live config, so the web UI's league/team settings
    follow the operator across browsers/devices instead of living only in localStorage.
    Practice rows are excluded - they're mock-room configs, never the real league."""
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT league_id, team_id, season, mode FROM draft_config WHERE mode = 'live' ORDER BY updated_at DESC LIMIT 1;")
            row = cursor.fetchone()
    except Exception as e:
        logger.error(f"Failed to read draft config: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read draft config: {str(e)}")

    if row is None:
        raise HTTPException(status_code=404, detail="No live draft config stored yet.")
    league_id, team_id, season, mode = row
    return {"league_id": league_id, "team_id": team_id, "season": season, "mode": mode}


def _stored_draft_config(league_id: Optional[str]) -> tuple:
    """(league_id, team_id, season, mode) of that league's config, or the newest one."""
    try:
        with get_db_cursor() as cursor:
            if league_id is not None:
                cursor.execute("SELECT league_id, team_id, season, mode FROM draft_config WHERE league_id = %s;", (league_id,))
            else:
                cursor.execute("SELECT league_id, team_id, season, mode FROM draft_config ORDER BY updated_at DESC LIMIT 1;")
            row = cursor.fetchone()
    except Exception as e:
        logger.error(f"Failed to read draft config before starting the draft listener: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read draft config: {str(e)}")
    if row is None:
        raise HTTPException(status_code=400, detail="No draft config stored yet - POST /v1/draft/config first.")
    return row


def _check_publisher_request(with_publisher: bool, league_id: str, mode: str) -> None:
    """with_publisher is live-only, and the live connector reads its league from ESPN_LEAGUE_ID
    (the worker's espn env bundle), so it can only serve that league."""
    if not with_publisher:
        return
    if mode == "practice":
        raise HTTPException(
            status_code=400,
            detail=(
                "with_publisher is only valid for a live config - a practice connector needs a "
                "manually-joined room URL, so use POST /v1/draft/practice/sync-job instead."
            ),
        )
    if ESPN_LEAGUE_ID and str(league_id) != ESPN_LEAGUE_ID:
        raise HTTPException(
            status_code=400,
            detail=(
                f"with_publisher starts the connector for ESPN_LEAGUE_ID ({ESPN_LEAGUE_ID}), "
                f"not league {league_id}."
            ),
        )


def _reclaim_or_refuse_other_listeners(league_id: str, mode: str) -> None:
    """Refuses (409) while a listener runs on another league, except that a practice start
    retires stale *practice* listeners: practice is a singleton by design (one room
    connector, one config row), so a new practice room simply replaces the old one. A live
    listener is never collected this way - drafting for real is never something to interrupt
    on an implicit signal."""
    try:
        conflicting = _listeners_on_other_leagues(league_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Could not determine whether another draft-sync listener is active: {e}. Refusing to start "
                f"league {league_id} rather than risk a second listener splitting the pick stream."
            ),
        )
    if not conflicting:
        return
    live_conflicts = [run for run in conflicting if run.job_name != draft_run_service.LISTENER_JOBS["practice"]]
    if mode == "practice" and not live_conflicts:
        try:
            _retire_practice_listeners(except_league_id=league_id)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=(
                    f"Could not reclaim the stale practice draft-sync listener: {e}. Starting league "
                    f"{league_id} anyway would leave two listeners running."
                ),
            )
        return
    conflicting_league_id = (live_conflicts or conflicting)[0].params.get("league_id")
    raise HTTPException(
        status_code=409,
        detail=(
            f"A draft-sync listener is already Active for league {conflicting_league_id}; "
            f"stop it before starting league {league_id}."
        ),
    )


def _start_listener_run(league_id: str, team_id: str, season: int, mode: str) -> dict:
    try:
        return draft_run_service.start_listener(
            mode,
            league_id=league_id,
            team_id=team_id,
            season=season,
            week=DRAFT_SYNC_WEEK,
            pick_clock_seconds=DRAFT_PICK_CLOCK_SECONDS_BY_MODE[mode],
            auto_draft_delay_seconds=AUTO_DRAFT_DELAY_SECONDS_BY_MODE[mode],
        )
    except DraftLauncherUnavailableError as e:
        logger.error(f"Job launcher cannot start the draft listener for league {league_id}: {e}")
        raise HTTPException(status_code=503, detail=f"Job launcher cannot start the draft listener: {e}")
    except Exception as e:
        logger.error(f"Failed to request the draft listener run for league {league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to request the draft listener run: {str(e)}")


def _start_live_connector_run(league_id: str, listener: dict) -> Optional[dict]:
    """The live connector run, or None when one is already active for the league.

    The listener and connector aren't a single unit - a listener that already exited (or was
    never started) doesn't prove the connector is gone too, so check rather than
    double-publish into the same league's pick Stream. A failure is not rolled back: the
    listener is already queued for a Stream that has a connector on the way one way or
    another, and surfacing it loudly lets the caller retry just the connector."""
    try:
        if draft_run_service.active_live_connectors(league_id):
            logger.info(f"Live connector already active for league {league_id}; not starting a second one.")
            return None
        return draft_run_service.start_live_connector(league_id)
    except Exception as e:
        logger.error(f"Listener requested but the live connector run failed for league {league_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=(
                f"draft-sync listener run {listener['run_id']} was requested, but the live connector run could "
                f"not be: {e}. Retry with with_publisher=true."
            ),
        )


@draft_router.post("/start", status_code=status.HTTP_202_ACCEPTED)
def start_draft_sync(league_id: Optional[str] = None, with_publisher: bool = False):
    """Requests the draft listener run (draft_matrix.py's listener, a `class: service` job in
    the launcher's catalog) from a stored /config. If `league_id` is omitted, falls back to the
    most recently stored config - pass it explicitly to pick a specific stored config, e.g. to
    launch a practice room without disturbing whichever config was updated last.

    with_publisher=True additionally requests fantasy-browser-worker's draft_sync.js as the
    live connector run (issue #66) - only valid for a live config, since practice has its own
    launch path (/practice/sync-job) for the room URL a human has to join by hand first.

    A second listener for the same league would share the browser-worker's Redis Stream
    consumer name (draft-matrix-{team_id}) but keep an entirely separate in-process
    pending_state - a newer pick handled by one process can't cancel the other's pending
    delayed publish, so two listeners for one league can race and double-publish a stale draft
    target. An active one is reported as already_running instead of duplicated."""
    league_id, team_id, season, mode = _stored_draft_config(league_id)
    _check_publisher_request(with_publisher, league_id, mode)

    listener_run, listener_query_ok = _listener_run(league_id)
    if not listener_query_ok:
        raise HTTPException(status_code=500, detail=f"Could not look up the draft listener run for league {league_id}.")
    if draft_run_service.is_active(listener_run):
        return {
            "status": "accepted",
            "already_running": True,
            "run_id": listener_run.run_id,
            "league_id": league_id,
            "team_id": team_id,
            "season": season,
            "mode": mode,
        }

    _reclaim_or_refuse_other_listeners(league_id, mode)
    # A previous run for this league leaves its board behind with no TTL, so without this the
    # new run opens showing the old one's recommendation, its last-processed pick number and,
    # if it died, its error. Clearing before the run is requested means the listener's own
    # "Starting" write is the first thing on a fresh board.
    _clear_league_draft_state(league_id)
    listener = _start_listener_run(league_id, team_id, season, mode)
    publisher = _start_live_connector_run(league_id, listener) if with_publisher else None

    return {
        "status": "accepted",
        "already_running": False,
        "run_id": listener["run_id"],
        "job_name": listener["job_name"],
        "publisher_run_id": publisher["run_id"] if publisher else None,
        "publisher_job_name": publisher["job_name"] if publisher else None,
        "league_id": league_id,
        "team_id": team_id,
        "season": season,
        "mode": mode,
    }


def _stop_timed_out(what: str, league_id: str) -> HTTPException:
    return HTTPException(
        status_code=504,
        detail=f"{what} for league {league_id} did not stop within {STOP_TIMEOUT_SECONDS}s - "
               "Redis state was NOT cleared; retry /stop or check the run manually.",
    )


@draft_router.post("/stop", status_code=status.HTTP_200_OK)
def stop_draft(league_id: str, confirm_publisher_stopped: bool = False):
    """Cancels a league's listener and connector runs and clears its Redis state -
    mode-agnostic (works for both live and practice). Does NOT delete the draft_config row:
    /status and a later /start re-launch both still need it, and there is no other stored copy
    of team_id/season/mode for that league once it's gone.

    Waits for each cancelled run's pod to be gone before clearing Redis - a still-running
    listener could otherwise recreate the recommendation board or publish a target after this
    endpoint has already reported "stopped". The listener goes first, then the connector.

    draft:{league}:picks/health and draft_picks:{league} are only cleared automatically when this
    call itself found the connector run and confirmed its pod is gone. A connector started
    out-of-band (a manual `kubectl apply`) is invisible here, and clearing those keys while it
    might still run risks losing a live pick or letting it recreate them right after.
    confirm_publisher_stopped=True remains the fallback for that case: the caller asserting the
    publisher is truly stopped. Without one or the other, restarting a listener against a
    reused live league id replays the previous draft's picks and silently drops the new ones
    as duplicates."""
    try:
        listeners, connectors = draft_run_service.active_league_runs(league_id)
        if listeners and not draft_run_service.cancel_and_wait(_run_ids(listeners)):
            raise _stop_timed_out("The draft-sync listener", league_id)
        # Unlike a pick-stream clear gated on confirm_publisher_stopped, a connector this call
        # just failed to stop must not fall through: the caller who set that flag can't have
        # verified a state this call just disproved.
        if connectors and not draft_run_service.cancel_and_wait(_run_ids(connectors)):
            raise _stop_timed_out("The draft room connector", league_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to stop draft runs for league {league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to stop draft runs: {str(e)}")

    pick_stream_cleared = confirm_publisher_stopped or bool(connectors)
    try:
        cleared_keys = _clear_league_draft_state(
            league_id, include_pick_stream=pick_stream_cleared, raise_on_error=True
        )
    except Exception as e:
        logger.error(f"Failed to clear Redis draft state for league {league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to clear Redis draft state: {str(e)}")

    return {
        "status": "stopped",
        "league_id": league_id,
        "job_deleted": bool(listeners),
        "publisher_job_deleted": bool(connectors),
        "cancelled_run_ids": _run_ids(listeners) + _run_ids(connectors),
        "redis_keys_cleared": cleared_keys,
        "pick_stream_cleared": pick_stream_cleared,
    }


@draft_router.get("/status", response_model=DraftStatusResponse)
def get_draft_status(league_id: str, season: int, limit: int = 10):
    try:
        return _build_status(league_id, season, limit)
    except Exception as e:
        logger.error(f"Failed to aggregate draft status for league {league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to aggregate draft status: {str(e)}")


@draft_router.get("/stream")
async def stream_draft_status(request: Request, league_id: str, season: int, limit: int = 10):
    """SSE polling loop: no pub/sub exists for this data today, so this just re-runs the
    same aggregation GET /status uses on an interval and only emits when it actually
    changes, rather than spamming a client with identical events every 2 seconds."""
    return StreamingResponse(_draft_status_event_generator(request, league_id, season, limit), media_type="text/event-stream")


async def _draft_status_event_generator(request: Request, league_id: str, season: int, limit: int):
    """Split out from stream_draft_status so it can be driven directly in tests without
    relying on a real ASGI transport's disconnect detection.

    _build_status does blocking k8s/Postgres/Redis I/O - calling it inline here would stall
    the event loop thread for every connected SSE client every poll interval. Offloaded via
    asyncio.to_thread so slow infra doesn't stall unrelated requests or other streams."""
    last_payload = None
    while True:
        if await request.is_disconnected():
            break
        try:
            payload = await asyncio.to_thread(_build_status, league_id, season, limit)
        except Exception as e:
            logger.error(f"Draft status stream aggregation failed for league {league_id}: {e}")
            payload = {"error": str(e)}
        if payload != last_payload:
            yield f"data: {json.dumps(payload, default=str)}\n\n"
            last_payload = payload
        heartbeat = {
            "league_id": league_id,
            "emitted_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        yield f"event: heartbeat\ndata: {json.dumps(heartbeat)}\n\n"
        await asyncio.sleep(SSE_POLL_INTERVAL_SECONDS)


@draft_router.post("/pause", status_code=status.HTTP_200_OK)
def pause_draft(payload: DraftPauseRequest):
    """SETs draft:{league_id}:pause:{pick_number} - draft_matrix.py's wait_for_pause_or_timeout
    polls this exact key to hold off an auto-publish for that pick."""
    r_client = _redis_client()
    pick_number = payload.pick_number
    if pick_number is None:
        board = r_client.hgetall(f"draft:{payload.league_id}:board")
        if not board or "pick_number" not in board:
            raise HTTPException(status_code=400, detail="No pending pick_number on the recommendation board and none was provided explicitly.")
        pick_number = int(board["pick_number"])

    r_client.set(f"draft:{payload.league_id}:pause:{pick_number}", "1")
    return {"status": "paused", "league_id": payload.league_id, "pick_number": pick_number}


@draft_router.post("/resume", status_code=status.HTTP_200_OK)
def resume_draft(payload: DraftPauseRequest):
    """Clearing the pause key alone does not resume anything: draft_matrix.py's
    wait_for_pause_or_timeout returns False as soon as it observes the key set, at which
    point hold_and_publish has already exited with nothing left to notice a later deletion
    - draft_matrix.py runs in a separate Job Pod this API process has no other channel into.
    So /resume recomputes a fresh recommendation and republishes it immediately - the human
    already decided to proceed, so there's no cancellable delay window to wait through this
    time, unlike the normal auto-draft path.

    Passes the league's stored draft_config.mode explicitly to publish_draft_target: this
    handler runs in the FastAPI gateway process, which (unlike the per-league draft-sync
    Job) never has DRAFT_MODE set in its own env, so publish_draft_target's env-var default
    would otherwise always resolve to "live" here - silently defaulting a practice-league
    resume to DRY_RUN in draft_clicker.js with no error surfaced anywhere."""
    r_client = _redis_client()
    pick_number = payload.pick_number
    if pick_number is None:
        board = r_client.hgetall(f"draft:{payload.league_id}:board")
        if not board or "pick_number" not in board:
            raise HTTPException(status_code=400, detail="No pending pick_number on the recommendation board and none was provided explicitly.")
        pick_number = int(board["pick_number"])

    r_client.delete(f"draft:{payload.league_id}:pause:{pick_number}")

    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT team_id, season, mode FROM draft_config WHERE league_id = %s;", (payload.league_id,))
            row = cursor.fetchone()
            if row is None:
                raise HTTPException(status_code=400, detail="No draft config stored for this league - POST /v1/draft/config first.")
            team_id, season, mode = row
            target = compute_draft_recommendation(cursor, payload.league_id, season, DRAFT_SYNC_WEEK, team_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to recompute draft recommendation on resume for league {payload.league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to recompute draft recommendation: {str(e)}")

    if target is None:
        return {"status": "resumed", "league_id": payload.league_id, "pick_number": pick_number, "published": False, "detail": "No feasible recommendation for the current undrafted pool."}

    try:
        publish_draft_target(team_id, target, mode=mode)
    except Exception as e:
        logger.error(f"Failed to publish resumed draft target for league {payload.league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to publish resumed draft target: {str(e)}")

    return {"status": "resumed", "league_id": payload.league_id, "pick_number": pick_number, "published": True, "player_id": target["player_id"], "name": target["name"]}


@draft_router.post("/auto-publish", status_code=status.HTTP_200_OK)
def set_auto_publish(payload: DraftAutoPublishRequest):
    """Manual kill switch (#108/#109): flips draft_config.auto_publish_enabled so
    hold_and_publish abandons a scheduled auto-publish instead of sending it, without
    touching the pause/resume mechanism above (a paused pick can still be resumed manually
    even while auto-publish is globally off for the league).

    Delegates the actual write to _set_auto_publish_enabled in draft_matrix.py rather than
    running its own SQL - the same shared helper (in a later task) a circuit breaker will
    call with disabled_by='circuit_breaker'. This endpoint is the only caller allowed to
    pass disabled_by='manual', since it is the one path an actual human clicked."""
    try:
        _set_auto_publish_enabled(payload.league_id, payload.enabled, disabled_by="manual" if not payload.enabled else None)
    except Exception as e:
        logger.error(f"Failed to update auto_publish_enabled for league {payload.league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update auto-publish setting: {str(e)}")

    return {
        "status": "updated",
        "league_id": payload.league_id,
        "auto_publish_enabled": payload.enabled,
        "auto_publish_disabled_by": "manual" if not payload.enabled else None,
    }


# ------------------------------------------------------------------------------
# GET /v1/draft/logs (cvillarr23/fantasy-core-pipeline#68)
# ------------------------------------------------------------------------------
# Best-effort line-level redaction, not a guarantee - these are the specific secret shapes
# this file's own Jobs are known to inject (ESPN session cookies via _draft_sync_env/the
# room-sync container's env, DB_PASSWORD via secretKeyRef) and that could plausibly surface
# in a traceback or a debug log line. Case-insensitive; value can be quoted, bare, or
# separated by '=' or ':'.
_LOG_REDACTION_PATTERNS = [
    re.compile(r"(?i)(espn_s2\s*[:=]\s*)['\"]?[^'\"\s,}]+"),
    re.compile(r"(?i)(swid\s*[:=]\s*)['\"]?[^'\"\s,}]+"),
    re.compile(r"(?i)(db_password\s*[:=]\s*)['\"]?[^'\"\s,}]+"),
    re.compile(r"(?i)(authorization\s*[:=]\s*)['\"]?[^'\"\s,}]+"),
]


def _redact_log_line(line: str) -> str:
    for pattern in _LOG_REDACTION_PATTERNS:
        line = pattern.sub(r"\1[REDACTED]", line)
    return line


def _log_run(league_id: str, job: str):
    """The run whose pod log /logs reads: the league's newest listener, or the newest practice
    room connector (a global singleton, so league_id is ignored for it)."""
    try:
        if job == "listener":
            return draft_run_service.latest_listener(league_id)
        return draft_run_service.latest_practice_connector()
    except Exception as e:
        logger.error(f"Failed to look up the {job} run for logs (league {league_id}): {e}")
        raise HTTPException(status_code=500, detail=f"Failed to look up the {job} run: {str(e)}")


@draft_router.get("/logs")
def get_draft_logs(league_id: str, job: Literal["listener", "room-sync"], tail: int = DRAFT_LOGS_DEFAULT_TAIL_LINES):
    """Tail of the run's pod log, for diagnosing a crash without cluster access.

    Finds the run (the league's newest listener, or the newest practice room connector), then
    its pod by the launcher's `fantasy.run-id` label, and reads the log directly - this works
    even after the pod has terminated (service runs have one attempt and restart_policy=Never,
    so there is no live-vs-previous-container ambiguity), as long as the pod hasn't been
    garbage-collected: the launcher's Jobs carry ttl_seconds_after_finished of a day.

    job="room-sync" intentionally ignores league_id: the connector run only carries a
    league_id when the practice room URL included a leagueId query param, and practice is a
    singleton (/practice/sync-job refuses a second active connector). league_id stays a
    required query param for consistency with job="listener", where it is the only way to find
    the right run.
    """
    tail = max(1, min(tail, DRAFT_LOGS_MAX_TAIL_LINES))
    run = _log_run(league_id, job)
    if run is None:
        raise HTTPException(status_code=404, detail=f"No {job} run found for league {league_id}")

    try:
        pods = draft_run_service.run_pods(run.run_id)
    except Exception as e:
        logger.error(f"Failed to list pods for {job} logs (league {league_id}): {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list pods for {job}: {str(e)}")
    if not pods:
        raise HTTPException(status_code=404, detail=f"No {job} pod found for league {league_id}")
    pod = max(pods, key=lambda p: p.metadata.creation_timestamp)

    try:
        raw_log = draft_run_service.read_pod_log(pod.metadata.name, tail)
    except Exception as e:
        logger.error(f"Failed to read log for pod {pod.metadata.name} ({job}, league {league_id}): {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read pod log: {str(e)}")

    container_statuses = pod.status.container_statuses or []
    terminated = None
    if container_statuses:
        terminated = getattr(container_statuses[0].state, "terminated", None)

    return {
        "league_id": league_id,
        "job": job,
        "pod_name": pod.metadata.name,
        "pod_phase": pod.status.phase,
        "container_exit_code": terminated.exit_code if terminated else None,
        "container_reason": terminated.reason if terminated else None,
        "lines": [_redact_log_line(line) for line in raw_log.splitlines()],
    }


# ------------------------------------------------------------------------------
# POST/GET/DELETE /v1/draft/practice/sync-job (cvillarr23/fantasy-web-ui#9)
# ------------------------------------------------------------------------------
PRACTICE_START_LOCK_KEY = "draft:practice:start-lock"
# Derived from the real worst case, not guessed. A start can run two full termination waits (the
# connector's and the listener's), each TERMINATION_MAX_POLLS iterations of a bounded apiserver
# call plus a sleep, then the run requests. A lease shorter than that expires mid-start, which
# lets a second request acquire the lock and start a connector alongside the first - the overlap
# the lock exists to prevent, reintroduced by the timeout.
PRACTICE_START_LOCK_TTL_SECONDS = int(
    2 * draft_run_service.TERMINATION_MAX_POLLS
    * (K8S_REQUEST_TIMEOUT_SECONDS + draft_run_service.TERMINATION_POLL_INTERVAL_SECONDS)
    + 4 * K8S_REQUEST_TIMEOUT_SECONDS
)
PRACTICE_START_LOCK_RENEW_INTERVAL_SECONDS = max(1, PRACTICE_START_LOCK_TTL_SECONDS // 4)

# Compare-and-delete: release only if we still hold the lease. A plain DELETE would drop whoever
# owns it now, which after an expiry is a DIFFERENT request - so the naive version actively
# creates the double-start it is meant to prevent.
_RELEASE_LOCK_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""

_RENEW_LOCK_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], ARGV[2])
end
return 0
"""


@contextmanager
def _practice_start_lock():
    """Serialises teardown-then-create so two concurrent starts cannot both find the room empty
    and then both start a connector.

    The check and the run request are separated by up to a minute of termination polling, so
    the window is wide, and two connectors publishing into the same Stream is exactly what every
    other guard here exists to prevent.

    Best-effort by design: if Redis is unreachable we proceed unlocked rather than refusing to
    start a draft, since the active-run check still catches the common case and an
    unavailable lock service must not be a hard dependency of starting a run. That fail-open only
    holds because _redis_client() now has finite socket timeouts - without them an unresponsive
    Redis would hang here forever instead of raising."""
    token = str(uuid.uuid4())
    r_client = None
    try:
        r_client = _redis_client()
        acquired = bool(r_client.set(PRACTICE_START_LOCK_KEY, token, nx=True, ex=PRACTICE_START_LOCK_TTL_SECONDS))
    except Exception as e:
        logger.warning(f"Could not acquire the practice start lock ({e}); proceeding without it.")
        yield
        return

    if not acquired:
        raise HTTPException(
            status_code=409,
            detail="Another practice draft start is already in progress. Retry in a few seconds.",
        )
    stop_renewal = threading.Event()

    def _renew_lock_lease():
        while not stop_renewal.wait(PRACTICE_START_LOCK_RENEW_INTERVAL_SECONDS):
            try:
                renewed = r_client.eval(
                    _RENEW_LOCK_LUA,
                    1,
                    PRACTICE_START_LOCK_KEY,
                    token,
                    PRACTICE_START_LOCK_TTL_SECONDS,
                )
                if not renewed:
                    logger.warning("Practice start lock lease is no longer owned; stopping lease renewal.")
                    return
            except Exception as e:
                logger.warning(f"Could not renew the practice start lock lease ({e}); will retry.")

    renewal_thread = threading.Thread(target=_renew_lock_lease, daemon=True)
    renewal_thread.start()
    try:
        yield
    finally:
        stop_renewal.set()
        renewal_thread.join(timeout=1)
        try:
            r_client.eval(_RELEASE_LOCK_LUA, 1, PRACTICE_START_LOCK_KEY, token)
        except Exception as e:
            logger.warning(f"Could not release the practice start lock ({e}); it expires in {PRACTICE_START_LOCK_TTL_SECONDS}s.")


@draft_router.post("/practice/sync-job", status_code=status.HTTP_202_ACCEPTED)
def start_practice_sync_job(payload: PracticeSyncJobRequest, force: bool = False):
    """Requests fantasy-browser-worker's draft_sync.js as the practice room connector run,
    pointed at a manually-joined ESPN mock/practice draft room. Distinct from /start's
    listener run; see module docstring.

    force=true retires whatever practice run is already there instead of refusing. Without it
    a wedged run is a dead end: the 409 tells the user to end the run first, and ending it is
    exactly what is failing. This is the escape hatch."""
    with _practice_start_lock():
        return _start_practice_sync_job_locked(payload, force)


def _force_teardown_for_start() -> None:
    """Retires the existing practice run for ?force=true, insisting the connector is gone.

    Teardown reports success as long as the listener was retired, even when the connector
    outlived its wait. Starting a replacement then would leave two connectors publishing into
    draft_picks:{league} at once - precisely the overlap this guard exists to prevent."""
    try:
        teardown = _teardown_practice_run(strict_cleanup=True)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not tear down the existing practice run: {e}")
    if not teardown["connector_gone"]:
        raise HTTPException(
            status_code=409,
            detail=(
                f"The existing practice room connector ({teardown['job_name']}) was cancelled but has not "
                "stopped yet. Starting another now would let two connectors publish into the same "
                "draft Stream. Retry in a few seconds."
            ),
        )


def _refuse_while_practice_active() -> None:
    try:
        active = draft_run_service.active_practice_connectors()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not check for an active practice run: {e}")
    if active:
        raise HTTPException(status_code=409, detail="A practice draft-sync run is already active - end it before starting another, or retry with force=true.")


def _clear_room_league_for_new_connector(room_league_id: Optional[str]) -> None:
    """Clears the room league's pick ledger and Stream before a new connector seeds from them.

    No connector is running at this point, so this is the one safe moment, and it covers a run
    the watcher retired (lapsed heartbeat) without anyone calling DELETE. It's skipped while a
    practice listener still consumes that league - a connector restarted mid-draft should pick
    up where the old one left off - and fails closed, since a new connector seeding from the
    previous run's ledger drops the real picks as duplicates."""
    if room_league_id is None:
        return
    try:
        listening = any(
            run.params.get("league_id") == room_league_id
            for run in draft_run_service.active_practice_listeners()
        )
        if not listening:
            _clear_league_draft_state(room_league_id, include_pick_stream=True, raise_on_error=True)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Could not clear the previous practice run's Redis state for league {room_league_id} "
                f"({e}). Starting a connector now would let it seed from that run's pick ledger."
            ),
        )


def _start_practice_sync_job_locked(payload: PracticeSyncJobRequest, force: bool):
    """The body of the endpoint, held under the start lock so the active check / teardown and
    the run request cannot be interleaved by a concurrent request."""
    if force:
        _force_teardown_for_start()
    else:
        _refuse_while_practice_active()

    room_league_id = parse_qs(urlparse(payload.room_url).query).get("leagueId", [None])[0]
    _clear_room_league_for_new_connector(room_league_id)
    try:
        started = draft_run_service.start_practice_connector(payload.room_url, room_league_id)
    except DraftLauncherUnavailableError as e:
        logger.error(f"Job launcher cannot start the practice room connector: {e}")
        raise HTTPException(status_code=503, detail=f"Job launcher cannot start the practice room connector: {e}")
    except Exception as e:
        logger.error(f"Failed to request the practice room connector run: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to request the practice room connector run: {str(e)}")

    return {"status": "accepted", **started}


@draft_router.get("/practice/sync-job/status")
def get_practice_sync_job_status():
    try:
        run = draft_run_service.latest_practice_connector()
    except Exception as e:
        logger.error(f"Failed to look up the practice room connector run: {e}")
        run = None
    job_state = draft_run_service.run_job_state(run)
    active_league_id = run.params.get("league_id") if run is not None else None
    room_connected = job_state == "Active" and _room_connected(active_league_id)
    return {
        "job_state": job_state,
        "pod_active": job_state == "Active",
        "room_connected": room_connected,
        "room_handshake_complete": room_connected,
        "active_league_id": active_league_id,
        "room_identifier": _practice_room_identifier(run),
        "run_id": run.run_id if run is not None else None,
    }


def _clear_connector_leagues(league_ids: list, strict_cleanup: bool) -> None:
    """Clears each retired connector's league. _retire_practice_listeners only clears leagues
    it found a *listener* for, and the listener may have finished already while the connector
    was still up. Idempotent, so re-clearing a league costs a no-op DELETE."""
    for league_id in league_ids:
        try:
            _clear_league_draft_state(league_id, include_pick_stream=True, raise_on_error=strict_cleanup)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=(
                    f"The practice run was retired but its Redis state for league {league_id} could not be "
                    f"cleared ({e}). Starting a replacement now would let it seed from the previous run's "
                    "pick ledger, so it has been stopped instead. Retry once Redis is reachable."
                ),
            )


def _teardown_practice_run(strict_cleanup: bool = False) -> dict:
    """Retires the practice room connector and its listener, and clears their Redis state.

    Split out of the DELETE endpoint so POST ?force=true can reuse it. Returns the connector's
    k8s Job name (None if there was none), the listener runs cancelled, and whether the
    connector actually stopped. Deliberately does NOT treat "nothing was running" as an error -
    only the endpoint decides whether that deserves a 404.

    Every active connector run, not just the newest: there is normally one, but concurrent
    starts can leave more, and retiring only the newest would report connector_gone=True while
    an older publisher was still writing to the same Stream - exactly the guarantee
    ?force=true relies on before starting a replacement.

    connector_gone=False means a connector outlived the wait and may still be publishing. That
    is tolerable for DELETE, whose job is to retire the listener too, but NOT for ?force=true.
    The listener is retired either way (the connector is only half the run, and nothing else
    stops a listener mid-draft); only the pick-stream cleanup, the part the connector can race,
    is skipped.

    strict_cleanup makes a failed Redis delete fatal rather than logged. Redis cleanup is
    best-effort everywhere else on purpose, but a force start that proceeds after it failed
    hands the new connector the old run's ledger to seed from."""
    try:
        connectors = draft_run_service.active_practice_connectors()
    except Exception as e:
        logger.error(f"Failed to list practice room connector runs for teardown: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list practice room connector runs: {str(e)}")

    connector_name = None
    connector_gone = True
    if connectors:
        connector_name = draft_run_service.k8s_job_name(connectors[0].job_name, connectors[0].run_id)
        try:
            connector_gone = draft_run_service.cancel_and_wait(_run_ids(connectors))
        except Exception as e:
            logger.error(f"Failed to cancel practice room connector runs: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to cancel the practice room connector: {str(e)}")
        if not connector_gone:
            logger.warning(
                f"Practice room connector run(s) {', '.join(_run_ids(connectors))} did not stop in time; "
                "retiring the listener anyway but leaving the pick ledger and Stream in place, since the "
                "connector may still be publishing to them."
            )

    try:
        cancelled_listeners = _retire_practice_listeners(clear_pick_stream=connector_gone)
    except Exception as e:
        detail = (
            f"Room connector {connector_name} was cancelled, but the draft-sync listener could not be: {e}"
            if connector_name
            else f"Failed to cancel the practice draft-sync listener: {e}"
        )
        raise HTTPException(status_code=500, detail=detail)

    if connector_gone:
        league_ids = list(dict.fromkeys(run.params["league_id"] for run in connectors if run.params.get("league_id")))
        _clear_connector_leagues(league_ids, strict_cleanup)

    return {
        "job_name": connector_name,
        "connector_run_ids": _run_ids(connectors),
        "listener_runs_cancelled": cancelled_listeners,
        "connector_gone": connector_gone,
    }


@draft_router.delete("/practice/sync-job", status_code=status.HTTP_200_OK)
def end_practice_sync_job():
    """Ends the practice run so leftover runs/Redis state can't interfere with a later run -
    the cleanup concern the issue explicitly calls out."""
    result = _teardown_practice_run()
    if result["job_name"] is None and not result["listener_runs_cancelled"]:
        raise HTTPException(status_code=404, detail="No practice draft-sync run exists.")
    return {"status": "deleted", **result}
