"""
Draft-day browser control plane (CORE-10).

Lets the draft-day pipeline (CORE-08's draft_matrix.py listener) be configured, launched,
paused, and monitored from a browser instead of `kubectl` - the user will be traveling
internationally on the actual draft day. Sits alongside the hot path (browser-worker ->
draft_matrix -> /trigger/draft-pick -> RabbitMQ -> click bot) as an out-of-band control/
observability layer, not part of it.

Two known gaps, both flagged rather than guessed at:
  - /start's Job image and its DB/Redis/API env config come from required env vars with no
    defaults - the real image ref and Secret/ConfigMap names live in fantasy-infra, out of
    this repo's scope, same "flag the stale/missing default instead of guessing" pattern as
    CORE_API_BASE_URL.
  - execution_log has no `mode` column, but `_recent_execution_errors`/`_last_clicker_action`
    are both scoped by `league_id` (and `_last_clicker_action` by `season` too) - since
    practice/live configs always live under distinct league_ids (see CORE-18), a practice
    run's rows are naturally excluded from a live league's /status the same way its k8s Job
    and Redis keys already are, without needing a dedicated column.

Also hosts /practice/sync-job (cvillarr23/fantasy-web-ui#9): creates fantasy-browser-worker's
draft_sync.js Job (the actual ESPN draft-room connector) against a practice/mock room URL a
human joined by hand - replacing the manual `kubectl apply` of
fantasy-browser-worker/k8s/test-draft-sync-job.yaml. This is a *different* Job than /start's
own draft_matrix.py listener above, and is independent of CORE-18's practice-mode work on
/start (cvillarr23/fantasy-core-pipeline#47) - it reuses the same BatchV1Api Job-creation
pattern rather than depending on that ticket landing first.
"""
import asyncio
import json
import logging
import os
import re
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import List, Literal, Optional
from urllib.parse import parse_qs, urlparse

import redis
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException
from pydantic import BaseModel, Field, field_validator

from src.database import get_db_cursor
from src.optimization.draft_matrix import compute_draft_recommendation, compute_on_the_clock_team, publish_draft_target

logger = logging.getLogger("DraftControlPlane")
draft_router = APIRouter()

REDIS_HOST = os.getenv("REDIS_HOST", "fantasy-cache.default.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "default")
DRAFT_SYNC_IMAGE = os.getenv("DRAFT_SYNC_IMAGE")
# Names of the k8s Secret/ConfigMap supplying the draft-sync Job's ESPN session and league
# config. These intentionally do NOT carry the database/cache connection settings - those
# are set explicitly by _draft_sync_env() below, because envFrom cannot rename a Secret's
# own key into the DB_PASSWORD name src/database.py reads. No defaults: the actual
# Secret/ConfigMap names are fantasy-infra's to define, not this repo's to guess.
DRAFT_SYNC_ENV_SECRET = os.getenv("DRAFT_SYNC_ENV_SECRET")
DRAFT_SYNC_ENV_CONFIGMAP = os.getenv("DRAFT_SYNC_ENV_CONFIGMAP")
# DB_PASSWORD cannot ride envFrom: the k8s Secret that holds it stores it under its own
# key name (postgres-password), and envFrom would surface it under that key rather than
# the DB_PASSWORD name src/database.py actually reads. So it needs an explicit
# secretKeyRef mapping - the same one this API's own Deployment uses. Overridable because
# the Secret/key names are fantasy-infra's to own, defaulted so a correct cluster works
# without extra wiring.
DRAFT_SYNC_DB_SECRET_NAME = os.getenv("DRAFT_SYNC_DB_SECRET_NAME", "database-credentials")
DRAFT_SYNC_DB_SECRET_KEY = os.getenv("DRAFT_SYNC_DB_SECRET_KEY", "postgres-password")
# Non-secret connection settings the listener Pod needs. Propagated from this API process
# so the Job always talks to the same database/cache/API this process does, instead of
# silently falling back to src.database / draft_matrix module defaults that may not match
# the cluster it is actually running in.
DRAFT_SYNC_INHERITED_ENV_VARS = (
    "DB_HOST",
    "DB_NAME",
    "DB_USER",
    "REDIS_HOST",
    "REDIS_PORT",
    "CORE_API_BASE_URL",
)
# The real ESPN league (sync_league.py's ESPN_LEAGUE_ID) that practice/mock-draft-room
# configs must never collide with. Optional: if unset, practice-mode validation is skipped
# with a logged warning rather than guessed at - same idiom as DRAFT_SYNC_IMAGE above.
ESPN_LEAGUE_ID = os.getenv("ESPN_LEAGUE_ID")
# Draft-sync listener defaults to week 1 preseason projections (matches draft_matrix.py's
# own --week CLI default) - draft config doesn't track a week, since a draft has none.
DRAFT_SYNC_WEEK = 1

SSE_POLL_INTERVAL_SECONDS = 2
DRAFT_STATUS_STALENESS_SECONDS = int(os.getenv("DRAFT_STATUS_STALENESS_SECONDS", "15"))
RECENT_ERRORS_LIMIT = 5

# fantasy-browser-worker's draft_sync.js image ref - no default, same "fail fast on
# missing infra-owned config" pattern as DRAFT_SYNC_IMAGE above.
BROWSER_WORKER_IMAGE = os.getenv("BROWSER_WORKER_IMAGE")
# Real Secret name, not env-configurable: fantasy-browser-worker/k8s/test-draft-sync-job.yaml
# already references this exact name directly (not via an env-configurable Secret ref), so
# there's nothing to guess here the way there is for DRAFT_SYNC_ENV_SECRET/CONFIGMAP.
ESPN_SESSION_SECRET_NAME = "espn-session-secrets"
PRACTICE_SYNC_APP_LABEL = "practice-draft-sync"

# Every Kubernetes call in this module is made from a synchronous FastAPI endpoint, which runs
# on a bounded threadpool. The kubernetes client applies no timeout of its own, so a control
# plane that accepts the TCP connection but never answers - the exact failure mode of an
# overloaded apiserver - parks a thread indefinitely. Enough parked threads and every sync
# endpoint stops responding, the liveness probe starts failing, and a degraded control plane
# becomes a hard outage of the whole API. Bounded so those calls fail fast and the endpoint can
# report "unknown" instead of hanging.
K8S_REQUEST_TIMEOUT_SECONDS = 10


def _redis_client() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def _draft_sync_env() -> list:
    """Explicit env for the draft-sync Job's Pod.

    The Job's Pod does not inherit this API Pod's environment, and draft_matrix.py imports
    src.database, which builds a connection pool at import time - so a missing DB_PASSWORD
    is not a degraded listener, it is an immediate crash before any pick is ever consumed.
    """
    env = [
        client.V1EnvVar(
            name="DB_PASSWORD",
            value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(
                    name=DRAFT_SYNC_DB_SECRET_NAME,
                    key=DRAFT_SYNC_DB_SECRET_KEY,
                )
            ),
        )
    ]
    for name in DRAFT_SYNC_INHERITED_ENV_VARS:
        value = os.getenv(name)
        if value:
            env.append(client.V1EnvVar(name=name, value=value))
    return env


def _dns1123_slug(value: str) -> str:
    """Job `metadata.name` must be a lowercase RFC 1123 DNS subdomain - league_id itself
    doesn't have to be (it's an opaque ESPN identifier, and practice-room IDs aren't under
    our control), so this only sanitizes the Job *name*, never the `league-id` label value
    (labels allow uppercase/mixed case) or the CLI arg passed to draft_matrix.py."""
    slug = re.sub(r"[^a-z0-9-]", "-", value.lower()).strip("-")
    return slug or "league"


def _batch_v1_api() -> client.BatchV1Api:
    """in-cluster config in a deployed pod, falls back to local kubeconfig for dev."""
    try:
        config.load_incluster_config()
    except ConfigException:
        config.load_kube_config()
    return client.BatchV1Api()


# ------------------------------------------------------------------------------
# PYDANTIC DATA TRANSFER OBJECTS (SCHEMAS)
# ------------------------------------------------------------------------------
class DraftConfigPayload(BaseModel):
    league_id: str = Field(..., description="ESPN league identifier the draft-sync Job should target")
    team_id: str = Field(..., description="Our own team_id within that league")
    season: int = Field(..., description="Season year, e.g. 2026")
    mode: Literal["live", "practice"] = Field("live", description="'practice' for ESPN mock draft rooms; tags the Job/status so a practice run is never mistaken for the real Draft Day")


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
    name: Optional[str] = Field(None, description="Joined from players; null if the player row is missing")
    position: Optional[str] = Field(None, description="Joined from players; null if the player row is missing")


class DraftStatusResponse(BaseModel):
    league_id: str
    season: int
    mode: str = Field(..., description="'live' or 'practice', sourced from the draft_config row for this league")
    job_state: str = Field(..., description="Active/Succeeded/Failed/Unknown, from the k8s Job's own status")
    recommendation: dict = Field(..., description="Latest recommendation from CORE-08's solver (draft:{league_id}:board), empty if none yet")
    last_clicker_action: Optional[str] = Field(None, description="Human-readable outcome of WORKER-06's most recent click for this league (e.g. 'Drafted Breece Hall at 14:32:05', or a failure message), sourced from execution_log; null until the click bot has confirmed a pick")
    recent_picks: List[DraftPickSummary]
    on_the_clock_team_id: Optional[str] = Field(None, description="Null until every team's round-1 slot is known")
    pipeline_health: str = Field(..., description="'healthy' iff the draft-sync Job is Active, Postgres/Redis both respond, and the listener's last-processed timestamp is fresh, else 'degraded'")
    recent_errors: List[dict] = Field(default_factory=list, description=f"Most recent draft-related error rows from execution_log (limit {RECENT_ERRORS_LIMIT})")
    listener_state: Literal["Active", "Starting", "Failed", "Unknown"] = Field(
        ...,
        description="League-scoped listener lifecycle state derived from the draft board hash and Job status.",
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
def _job_phase(job_status) -> str:
    if job_status.active:
        return "Active"
    if job_status.succeeded:
        return "Succeeded"
    if job_status.failed:
        return "Failed"
    return "Unknown"


def _latest_listener_job(league_id: str):
    """(latest draft-sync Job for the league or None, whether the query actually succeeded).

    Absent and unreadable have to stay distinguishable. Callers use absence to decide that
    leftover Redis state describes a finished run; treating an apiserver failure the same way
    would suppress a live run's error banner exactly when something is already wrong.
    """
    try:
        batch_api = _batch_v1_api()
        jobs = batch_api.list_namespaced_job(K8S_NAMESPACE, label_selector=f"app=draft-sync,league-id={league_id}", _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS)
    except Exception as e:
        logger.error(f"Failed to query draft-sync Job status for league {league_id}: {e}")
        return None, False

    items = getattr(jobs, "items", []) or []
    if not items:
        return None, True
    # Most recently created Job for this league is the one we care about.
    return max(items, key=lambda j: j.metadata.creation_timestamp), True


def _job_state(league_id: str) -> str:
    job, _query_ok = _latest_listener_job(league_id)
    if job is None:
        return "Unknown"
    return _job_phase(job.status)


def _practice_sync_job_state() -> str:
    try:
        batch_api = _batch_v1_api()
        jobs = batch_api.list_namespaced_job(K8S_NAMESPACE, label_selector=f"app={PRACTICE_SYNC_APP_LABEL}", _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS)
    except Exception as e:
        logger.error(f"Failed to query practice draft-sync Job status: {e}")
        return "Unknown"

    if not jobs.items:
        return "Unknown"
    latest = max(jobs.items, key=lambda j: j.metadata.creation_timestamp)
    return _job_phase(latest.status)


def _latest_practice_sync_job():
    try:
        batch_api = _batch_v1_api()
        jobs = batch_api.list_namespaced_job(K8S_NAMESPACE, label_selector=f"app={PRACTICE_SYNC_APP_LABEL}", _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS)
    except Exception as e:
        logger.error(f"Failed to query practice draft-sync Job status: {e}")
        return None
    items = getattr(jobs, "items", []) or []
    if not isinstance(items, list) or not items:
        return None
    return max(items, key=lambda j: j.metadata.creation_timestamp)


def _practice_sync_job_league_id() -> Optional[str]:
    latest = _latest_practice_sync_job()
    if latest is None:
        return None
    return (latest.metadata.labels or {}).get("league-id")


def _active_listeners_on_other_leagues(target_league_id: str) -> list:
    """Returns the active draft-sync listener Jobs watching some *other* league.

    More than one can exist: every practice run before this fix left its listener behind, so
    a long-lived cluster accumulates them."""
    # Never fails open. An unreadable listener state is not "no conflicts" - treating it as
    # such lets a transient apiserver blip slip a second listener past the duplicate guard,
    # and two consumers on one Redis group split the pick stream between them.
    batch_api = _batch_v1_api()
    try:
        jobs = batch_api.list_namespaced_job(K8S_NAMESPACE, label_selector="app=draft-sync", _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS)
    except Exception as e:
        logger.error(f"Failed to query active draft-sync listeners: {e}")
        raise
    items = getattr(jobs, "items", []) or []
    if not isinstance(items, list):
        raise RuntimeError(f"Unexpected response listing draft-sync Jobs: items was {type(items).__name__}, not a list.")
    conflicting = []
    for job in items:
        if _job_phase(job.status) != "Active":
            continue
        job_league_id = (job.metadata.labels or {}).get("league-id")
        if job_league_id and job_league_id != target_league_id:
            conflicting.append(job)
    return conflicting


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
    Callers here are cleanup and startup paths, where a Redis hiccup must not abort the
    operation itself - refusing to start a draft because leftover state could not be cleared
    would be worse than starting with it. Logged loudly and reported instead.

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
        keys = [f"draft:{league_id}:board"]
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


def _wait_for_jobs_gone(batch_api: client.BatchV1Api, label_selector: str, job_names: list) -> bool:
    """Like _wait_for_job_termination, but waits for specific Jobs rather than for the whole
    selector to empty - the caller may be intentionally leaving other matching Jobs in place."""
    remaining = set(job_names)
    for _ in range(STOP_JOB_TERMINATION_MAX_POLLS):
        jobs = batch_api.list_namespaced_job(K8S_NAMESPACE, label_selector=label_selector, _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS)
        present = {j.metadata.name for j in (getattr(jobs, "items", []) or [])}
        if not (remaining & present):
            return True
        time.sleep(STOP_JOB_TERMINATION_POLL_INTERVAL_SECONDS)
    return False


def _delete_practice_listener_jobs(except_league_id: Optional[str] = None, clear_pick_stream: bool = False) -> list:
    """Deletes practice draft-sync listener Jobs, optionally sparing one league's.

    Listeners are Redis Stream consumers that block forever, so nothing ever retires one on
    its own. Left running, a stale listener both consumes a league nobody is drafting and
    blocks the next practice run from starting at all (start_draft_sync refuses to run two
    listeners at once). Only practice listeners are ever touched here - a live draft's
    listener must never be collected as a side effect of practice cleanup.

    clear_pick_stream additionally drops each retired league's pick ledger and Stream. It is
    off by default because it is only safe once the league's *publisher* is also gone: the
    room connector writes to those keys, and deleting them underneath a running connector
    either loses picks or lets it immediately recreate them. Only end_practice_sync_job, which
    waits for the connector Pod to terminate first, may pass True."""
    try:
        batch_api = _batch_v1_api()
        jobs = batch_api.list_namespaced_job(K8S_NAMESPACE, label_selector="app=draft-sync,mode=practice", _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS)
    except Exception as e:
        logger.error(f"Failed to list practice draft-sync listener Jobs: {e}")
        raise

    deleted = []
    deleted_league_ids = []
    for job in getattr(jobs, "items", []) or []:
        labels = job.metadata.labels or {}
        if except_league_id is not None and labels.get("league-id") == except_league_id:
            continue
        try:
            batch_api.delete_namespaced_job(
                job.metadata.name,
                K8S_NAMESPACE,
                # Foreground, matching /stop: Background returns before the Pod is gone, and
                # /start creates the replacement the moment this returns. The old and new
                # listeners would then briefly share a Redis consumer group and split the
                # pick stream between them - the very overlap this reclaim exists to avoid.
                propagation_policy="Foreground",
                _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS,
            )
            deleted.append(job.metadata.name)
            if labels.get("league-id"):
                deleted_league_ids.append(labels["league-id"])
        except Exception as e:
            # Never swallow this. A caller that treats failed cleanup as success either
            # starts a second listener alongside the one it failed to reclaim, or reports a
            # 200 teardown while the listener is still consuming.
            logger.error(f"Failed to delete practice draft-sync listener Job {job.metadata.name}: {e}")
            raise

    # Waits on the deleted names specifically, not the label selector. The selector also
    # matches the target league's own Job, which except_league_id deliberately spared - a
    # completed or failed one lingers there indefinitely, so a selector-based wait would
    # never see an empty list and every reclaim would time out into a 500.
    if deleted and not _wait_for_jobs_gone(batch_api, "app=draft-sync,mode=practice", deleted):
        raise RuntimeError(
            f"Practice draft-sync listener Job(s) {', '.join(deleted)} did not terminate within "
            f"{STOP_JOB_TERMINATION_MAX_POLLS * STOP_JOB_TERMINATION_POLL_INTERVAL_SECONDS}s."
        )
    if deleted:
        logger.info(f"Deleted stale practice draft-sync listener Jobs: {', '.join(deleted)}")
    # The Job is only half the run's footprint. Its board outlives it with no TTL, so leaving
    # it behind is what makes a dead run's error and recommendation keep surfacing as current.
    for dead_league_id in deleted_league_ids:
        _clear_league_draft_state(dead_league_id, include_pick_stream=clear_pick_stream)
    return deleted


def _to_iso_timestamp(value) -> Optional[str]:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OSError):
        return None


def _listener_status(job_state: str, recommendation: dict, listener_job_absent: bool = False) -> tuple:
    explicit_state = recommendation.get("listener_state")
    valid_states = {"Active", "Starting", "Failed", "Unknown"}
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
        return "Unknown", listener_last_event_at, None

    if job_state == "Failed":
        listener_state = "Failed"
    elif listener_state is None:
        if job_state == "Active":
            listener_state = "Active" if listener_last_event_at is not None else "Starting"
        else:
            listener_state = "Unknown"

    return listener_state, listener_last_event_at, listener_error


def _practice_room_identifier(job) -> Optional[str]:
    containers = getattr(getattr(getattr(job, "spec", None), "template", None), "spec", None)
    if containers is None:
        return None
    for container in getattr(containers, "containers", []) or []:
        for env_var in getattr(container, "env", []) or []:
            if env_var.name != "DRAFT_ROOM_URL" or env_var.value is None:
                continue
            parsed = urlparse(env_var.value)
            if parsed.fragment:
                return parsed.fragment
            draft_room_id = parse_qs(parsed.query).get("draftRoomId", [None])[0]
            if draft_room_id:
                return draft_room_id
    return None


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
    JSON, not a JSON-encoded string inside JSON."""
    board = r_client.hgetall(f"draft:{league_id}:board") or {}
    if "top_candidates" in board:
        try:
            board["top_candidates"] = json.loads(board["top_candidates"])
        except (TypeError, ValueError):
            logger.error(f"Malformed top_candidates JSON on draft:{league_id}:board; dropping it.")
            board["top_candidates"] = []
    return board


def _recent_picks(cursor, league_id: str, season: int, limit: int) -> list:
    """Left-joins players so the pick feed can render name/position without a separate
    per-pick lookup - LEFT JOIN (not INNER) so a pick still shows up even if its player
    row is somehow missing, just with name/position null rather than the whole pick
    vanishing from the feed. That is the normal case for K and D/ST, which the players
    table doesn't carry at all: espn_player_id is returned alongside so the feed can still
    identify the pick when the crosswalk has nothing to join to."""
    cursor.execute("""
        SELECT d.pick_number, d.round, d.team_id, d.player_id, d.espn_player_id, p.name, p.position
        FROM draft_picks d
        LEFT JOIN players p ON d.player_id = p.player_id
        WHERE d.league_id = %s AND d.season = %s
        ORDER BY d.pick_number DESC
        LIMIT %s;
    """, (league_id, season, limit))
    return [
        {"pick_number": row[0], "round": row[1], "team_id": row[2], "player_id": row[3],
         "espn_player_id": row[4], "name": row[5], "position": row[6]}
        for row in cursor.fetchall()
    ]


def _on_the_clock_team_id(cursor, league_id: str, season: int) -> Optional[str]:
    # Mirrors draft_matrix.league_team_count's resolution order rather than reading
    # league_settings alone: that table only ever holds the one real ESPN_LEAGUE_ID, so for a
    # practice/mock room the count derived from pick arrivals is the only one there is, and
    # reading only league_settings here would report "nobody on the clock" for every practice
    # draft no matter how well the listener was working. The draft_config read is scoped to
    # the season for the same reason as in draft_matrix: that table is keyed by league_id
    # alone, so a room reused next season would otherwise inherit the old count.
    cursor.execute("SELECT team_count FROM league_settings WHERE league_id = %s;", (league_id,))
    row = cursor.fetchone()
    if row is None or not row[0]:
        cursor.execute("SELECT team_count FROM draft_config WHERE league_id = %s AND season = %s;", (league_id, season))
        row = cursor.fetchone()
    if row is None or not row[0]:
        return None
    team_count = row[0]

    cursor.execute("SELECT team_id, slot_number FROM draft_slots WHERE league_id = %s AND season = %s;", (league_id, season))
    slot_map = {team_id: slot_number for team_id, slot_number in cursor.fetchall()}
    if not slot_map:
        return None

    cursor.execute("SELECT COALESCE(MAX(pick_number), 0) FROM draft_picks WHERE league_id = %s AND season = %s;", (league_id, season))
    latest_pick_number = cursor.fetchone()[0]

    return compute_on_the_clock_team(latest_pick_number + 1, team_count, slot_map)


def _recent_execution_errors(cursor, league_id: str, limit: int) -> list:
    """Scoped by league_id so a practice room's failures never surface in a live league's
    /status - execution_log has no dedicated mode column (see the module docstring), but
    practice/live configs always use distinct league_ids, so this filter alone keeps the
    two from ever mixing here."""
    cursor.execute("""
        SELECT transaction_type, bot_name, error_message, executed_at
        FROM execution_log
        WHERE status <> 'success'
          AND error_message IS NOT NULL
          AND league_id = %s
          AND (transaction_type ILIKE 'draft%%' OR COALESCE(bot_name, '') ILIKE 'draft%%')
        ORDER BY executed_at DESC
        LIMIT %s;
    """, (league_id, limit))
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


def _build_status(league_id: str, season: int, limit: int) -> dict:
    """Every datastore read is isolated in its own try/except: a Postgres or Redis outage
    must surface as pipeline_health="degraded" in a normal 200 response (so the browser can
    make the documented auto-picker fallback decision), not escape as an unhandled
    exception that turns the whole endpoint into a 500."""
    # One apiserver read serves both: the Job's phase, and whether a listener exists at all.
    listener_job, listener_job_query_ok = _latest_listener_job(league_id)
    job_state = _job_phase(listener_job.status) if listener_job is not None else "Unknown"

    redis_ok = True
    recommendation: dict = {}
    try:
        recommendation = _recommendation(_redis_client(), league_id)
    except Exception as e:
        logger.error(f"Failed to read Redis recommendation board for league {league_id}: {e}")
        redis_ok = False

    db_ok = True
    recent_picks: list = []
    on_the_clock_team_id = None
    recent_errors: list = []
    last_clicker_action = None
    # "unknown" (not "live") is the pre-query default: a Postgres outage must not let a
    # practice run get silently reported as mode="live" just because the mode SELECT never
    # ran - that would route a practice run into live monitoring precisely when db_ok is
    # already about to go false. Only a successful query that finds no draft_config row
    # falls back to "live" (today's pre-CORE-18 default for an unconfigured league).
    mode = "unknown"
    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT mode FROM draft_config WHERE league_id = %s;", (league_id,))
            mode_row = cursor.fetchone()
            mode = mode_row[0] if mode_row is not None else "live"
            recent_picks = _recent_picks(cursor, league_id, season, limit)
            on_the_clock_team_id = _on_the_clock_team_id(cursor, league_id, season)
            recent_errors = _recent_execution_errors(cursor, league_id, RECENT_ERRORS_LIMIT)
            last_clicker_action = _last_clicker_action(cursor, league_id, season)
    except Exception as e:
        logger.error(f"Failed to read draft picks/slots/mode for league {league_id}: {e}")
        db_ok = False

    pipeline_health = "healthy" if job_state == "Active" and db_ok and redis_ok and not _is_stale(job_state, recommendation) else "degraded"
    listener_job_absent = listener_job_query_ok and listener_job is None
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
        "job_state": job_state,
        "recommendation": recommendation,
        "last_clicker_action": last_clicker_action,
        "recent_picks": recent_picks,
        "on_the_clock_team_id": on_the_clock_team_id,
        "pipeline_health": pipeline_health,
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
    """Stores league_id/team_id/season/mode so /start's draft-sync Job can read it at launch."""
    if payload.mode == "practice":
        if ESPN_LEAGUE_ID is None:
            logger.warning(f"ESPN_LEAGUE_ID is not configured - skipping the real-league collision check for practice config league_id={payload.league_id}.")
        elif payload.league_id == ESPN_LEAGUE_ID:
            raise HTTPException(status_code=400, detail=f"league_id {payload.league_id} matches the real ESPN_LEAGUE_ID - a practice/mock-draft-room config must use the mock room's own league_id.")

    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO draft_config (league_id, team_id, season, mode)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (league_id) DO UPDATE
                SET team_id = EXCLUDED.team_id, season = EXCLUDED.season, mode = EXCLUDED.mode,
                    -- A derived team count belongs to the season it was derived from. Reusing
                    -- a room for a new season must start over rather than inherit it.
                    team_count = CASE WHEN draft_config.season IS DISTINCT FROM EXCLUDED.season
                                      THEN NULL ELSE draft_config.team_count END,
                    updated_at = CURRENT_TIMESTAMP;
            """, (payload.league_id, payload.team_id, payload.season, payload.mode))
    except Exception as e:
        logger.error(f"Failed to persist draft config for league {payload.league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to store draft config: {str(e)}")

    return {"status": "stored", "league_id": payload.league_id, "team_id": payload.team_id, "season": payload.season, "mode": payload.mode}


@draft_router.post("/start", status_code=status.HTTP_202_ACCEPTED)
def start_draft_sync(league_id: Optional[str] = None):
    """Creates the draft-sync Job (a wrapper around draft_matrix.py's listener) from a
    stored /config, replacing the manual `kubectl apply` step. If `league_id` is omitted,
    falls back to the most recently stored config (today's behavior, unchanged) - pass it
    explicitly to pick a specific stored config, e.g. to launch a practice room without
    disturbing whichever config was updated last."""
    if not DRAFT_SYNC_IMAGE:
        raise HTTPException(status_code=500, detail="DRAFT_SYNC_IMAGE is not configured - the draft-sync container image ref must be supplied via env var.")
    if not DRAFT_SYNC_ENV_SECRET or not DRAFT_SYNC_ENV_CONFIGMAP:
        raise HTTPException(status_code=500, detail="DRAFT_SYNC_ENV_SECRET and DRAFT_SYNC_ENV_CONFIGMAP must both be configured - the Job's Pod does not inherit this API Pod's own DB/Redis/API environment.")

    try:
        with get_db_cursor() as cursor:
            if league_id is not None:
                cursor.execute("SELECT league_id, team_id, season, mode FROM draft_config WHERE league_id = %s;", (league_id,))
            else:
                cursor.execute("SELECT league_id, team_id, season, mode FROM draft_config ORDER BY updated_at DESC LIMIT 1;")
            row = cursor.fetchone()
    except Exception as e:
        logger.error(f"Failed to read draft config before starting draft-sync Job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read draft config: {str(e)}")

    if row is None:
        raise HTTPException(status_code=400, detail="No draft config stored yet - POST /v1/draft/config first.")
    league_id, team_id, season, mode = row

    # A second listener for the same league would share the browser-worker's Redis Stream
    # consumer name (draft-matrix-{team_id}) but keep an entirely separate in-process
    # pending_state - a newer pick handled by one process can't cancel the other's pending
    # delayed publish, so two Active Jobs for one league can race and double-publish a
    # stale draft target. Reject rather than silently creating a duplicate.
    existing_state = _job_state(league_id)
    if existing_state == "Active":
        return {
            "status": "accepted",
            "already_running": True,
            "league_id": league_id,
            "team_id": team_id,
            "season": season,
            "mode": mode,
        }

    try:
        conflicting = _active_listeners_on_other_leagues(league_id)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Could not determine whether another draft-sync listener is active: {e}. Refusing to start "
                f"league {league_id} rather than risk a second listener splitting the pick stream."
            ),
        )
    if conflicting:
        # A practice run starting while stale *practice* listeners linger is the normal case,
        # not a conflict: the user has simply opened a new practice room, and practice is a
        # singleton by design (one room connector, one config row). Reclaim them so the run
        # can proceed. A live listener is never collected this way - drafting for real is
        # never something to interrupt on an implicit signal, so that still fails loudly.
        live_conflicts = [j for j in conflicting if (j.metadata.labels or {}).get("mode") != "practice"]
        if mode == "practice" and not live_conflicts:
            try:
                _delete_practice_listener_jobs(except_league_id=league_id)
            except Exception as e:
                raise HTTPException(
                    status_code=500,
                    detail=(
                        f"Could not reclaim the stale practice draft-sync listener: {e}. Starting league "
                        f"{league_id} anyway would leave two listeners running."
                    ),
                )
        else:
            conflicting_league_id = (live_conflicts or conflicting)[0].metadata.labels.get("league-id")
            raise HTTPException(
                status_code=409,
                detail=(
                    f"A draft-sync Job is already Active for league {conflicting_league_id}; "
                    f"stop it before starting league {league_id}."
                ),
            )

    job_prefix = "draft-sync-practice" if mode == "practice" else "draft-sync"
    job_name = f"{job_prefix}-{_dns1123_slug(league_id)}-{int(time.time())}"

    # Reached only when a new listener is genuinely being created - the already_running branch
    # returned long before this. A previous run for this league leaves its board behind with no
    # TTL, so without this the new run opens showing the old one's recommendation, its
    # last-processed pick number and, if it died, its error. Clearing before the Job is created
    # means the listener's own "Starting" write is the first thing on a fresh board.
    _clear_league_draft_state(league_id)

    job_labels = {"app": "draft-sync", "league-id": league_id, "mode": mode}
    job = client.V1Job(
        metadata=client.V1ObjectMeta(name=job_name, labels=job_labels),
        spec=client.V1JobSpec(
            backoff_limit=0,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels=job_labels),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    containers=[
                        client.V1Container(
                            name="draft-sync",
                            image=DRAFT_SYNC_IMAGE,
                            command=[
                                "python", "-m", "src.optimization.draft_matrix",
                                "--league-id", str(league_id),
                                "--team-id", str(team_id),
                                "--season", str(season),
                            ],
                            env_from=[
                                client.V1EnvFromSource(config_map_ref=client.V1ConfigMapEnvSource(name=DRAFT_SYNC_ENV_CONFIGMAP)),
                                client.V1EnvFromSource(secret_ref=client.V1SecretEnvSource(name=DRAFT_SYNC_ENV_SECRET)),
                            ],
                            env=_draft_sync_env(),
                        )
                    ],
                ),
            ),
        ),
    )

    try:
        batch_api = _batch_v1_api()
        batch_api.create_namespaced_job(K8S_NAMESPACE, job, _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS)
    except Exception as e:
        logger.error(f"Failed to create draft-sync Job for league {league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create draft-sync Job: {str(e)}")

    return {
        "status": "accepted",
        "already_running": False,
        "job_name": job_name,
        "league_id": league_id,
        "team_id": team_id,
        "season": season,
        "mode": mode,
    }


STOP_JOB_TERMINATION_MAX_POLLS = 15
STOP_JOB_TERMINATION_POLL_INTERVAL_SECONDS = 2


def _wait_for_job_termination(batch_api: client.BatchV1Api, label_selector: str) -> bool:
    """Foreground-propagation deletion doesn't complete synchronously - the Job object (and
    its dependent Pods) can still be terminating for a few seconds after delete_namespaced_job
    returns. Poll until it's actually gone, bounded so a wedged finalizer/apiserver hiccup
    can't hang this request forever. Returns False on timeout - callers must not treat that
    as a successful stop."""
    for _ in range(STOP_JOB_TERMINATION_MAX_POLLS):
        jobs = batch_api.list_namespaced_job(K8S_NAMESPACE, label_selector=label_selector, _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS)
        if not jobs.items:
            return True
        time.sleep(STOP_JOB_TERMINATION_POLL_INTERVAL_SECONDS)
    return False


@draft_router.post("/stop", status_code=status.HTTP_200_OK)
def stop_draft(league_id: str):
    """Tears down a draft-sync Job and its Redis state for one league - mode-agnostic
    (works for both live and practice). Does NOT delete the draft_config row: /status and
    a later /start re-launch both still need it, and there is no other stored copy of
    team_id/season/mode for that league once it's gone.

    Scoped to app=draft-sync (not just league-id) so this can never delete some other
    workload that happens to share the league-id label. Waits for the Job's Pods to fully
    terminate (Foreground propagation + polling) before clearing Redis - a still-running
    listener could otherwise recreate the recommendation board or publish a target after
    this endpoint has already reported "stopped"."""
    label_selector = f"app=draft-sync,league-id={league_id}"
    deleted_job = False
    try:
        batch_api = _batch_v1_api()
        jobs = batch_api.list_namespaced_job(K8S_NAMESPACE, label_selector=label_selector, _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS)
        for job in jobs.items:
            batch_api.delete_namespaced_job(
                name=job.metadata.name,
                namespace=K8S_NAMESPACE,
                propagation_policy="Foreground",  # blocks the Job's own deletion until its Pods are gone
                _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS,
            )
            deleted_job = True

        if deleted_job and not _wait_for_job_termination(batch_api, label_selector):
            raise HTTPException(
                status_code=504,
                detail=f"draft-sync Job for league {league_id} did not terminate within "
                       f"{STOP_JOB_TERMINATION_MAX_POLLS * STOP_JOB_TERMINATION_POLL_INTERVAL_SECONDS}s - "
                       "Redis state was NOT cleared; retry /stop or check the Job manually.",
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete draft-sync Job(s) for league {league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete draft-sync Job: {str(e)}")

    cleared_keys = 0
    try:
        r_client = _redis_client()
        keys_to_delete = [f"draft:{league_id}:board"]
        keys_to_delete.extend(r_client.scan_iter(match=f"draft:{_redis_glob_escape(league_id)}:pause:*"))
        if keys_to_delete:
            cleared_keys = r_client.delete(*keys_to_delete)
    except Exception as e:
        logger.error(f"Failed to clear Redis draft state for league {league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to clear Redis draft state: {str(e)}")

    return {"status": "stopped", "league_id": league_id, "job_deleted": deleted_job, "redis_keys_cleared": cleared_keys}


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
    time, unlike the normal auto-draft path."""
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
            cursor.execute("SELECT team_id, season FROM draft_config WHERE league_id = %s;", (payload.league_id,))
            row = cursor.fetchone()
            if row is None:
                raise HTTPException(status_code=400, detail="No draft config stored for this league - POST /v1/draft/config first.")
            team_id, season = row
            target = compute_draft_recommendation(cursor, payload.league_id, season, DRAFT_SYNC_WEEK, team_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to recompute draft recommendation on resume for league {payload.league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to recompute draft recommendation: {str(e)}")

    if target is None:
        return {"status": "resumed", "league_id": payload.league_id, "pick_number": pick_number, "published": False, "detail": "No feasible recommendation for the current undrafted pool."}

    try:
        publish_draft_target(team_id, target)
    except Exception as e:
        logger.error(f"Failed to publish resumed draft target for league {payload.league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to publish resumed draft target: {str(e)}")

    return {"status": "resumed", "league_id": payload.league_id, "pick_number": pick_number, "published": True, "player_id": target["player_id"], "name": target["name"]}


# ------------------------------------------------------------------------------
# POST/GET/DELETE /v1/draft/practice/sync-job (cvillarr23/fantasy-web-ui#9)
# ------------------------------------------------------------------------------
PRACTICE_START_LOCK_KEY = "draft:practice:start-lock"
# Long enough to cover a full teardown (connector delete + termination polls) plus the create,
# short enough that a process killed mid-flight cannot wedge starts for long. The lock is an
# optimisation over the race, not a correctness boundary on its own - it is always paired with
# the checks below.
PRACTICE_START_LOCK_TTL_SECONDS = 120


@contextmanager
def _practice_start_lock():
    """Serialises teardown-then-create so two concurrent starts cannot both find the room empty
    and then both create a connector.

    The check and the create are separated by seconds of termination polling, so the window is
    wide, and two connectors publishing into the same Stream is exactly what every other guard
    here exists to prevent. Job names are second-resolution, so colliding requests do not even
    collide on name.

    Best-effort by design: if Redis is unreachable we proceed unlocked rather than refusing to
    start a draft, since the k8s-side Active check still catches the common case and an
    unavailable lock service must not be a hard dependency of starting a run."""
    r_client = None
    acquired = False
    try:
        r_client = _redis_client()
        acquired = bool(r_client.set(PRACTICE_START_LOCK_KEY, str(time.time()), nx=True, ex=PRACTICE_START_LOCK_TTL_SECONDS))
    except Exception as e:
        logger.warning(f"Could not acquire the practice start lock ({e}); proceeding without it.")
        yield
        return

    if not acquired:
        raise HTTPException(
            status_code=409,
            detail="Another practice draft start is already in progress. Retry in a few seconds.",
        )
    try:
        yield
    finally:
        try:
            r_client.delete(PRACTICE_START_LOCK_KEY)
        except Exception as e:
            logger.warning(f"Could not release the practice start lock ({e}); it expires in {PRACTICE_START_LOCK_TTL_SECONDS}s.")


@draft_router.post("/practice/sync-job", status_code=status.HTTP_202_ACCEPTED)
def start_practice_sync_job(payload: PracticeSyncJobRequest, force: bool = False):
    """Creates fantasy-browser-worker's draft_sync.js Job pointed at a manually-joined
    ESPN mock/practice draft room - replacing the manual `kubectl apply` of
    k8s/test-draft-sync-job.yaml. Distinct from /start's own draft-sync Job; see module
    docstring.

    force=true retires whatever practice run is already there instead of refusing. Without it
    a wedged run - a Job stuck Terminating, or one stranded by a control plane that stopped
    answering - is a dead end: the 409 tells the user to end the run first, and ending it is
    exactly what is failing. This is the escape hatch."""
    if not BROWSER_WORKER_IMAGE:
        raise HTTPException(status_code=500, detail="BROWSER_WORKER_IMAGE is not configured - the browser-worker container image ref must be supplied via env var.")

    with _practice_start_lock():
        return _start_practice_sync_job_locked(payload, force)


def _start_practice_sync_job_locked(payload: PracticeSyncJobRequest, force: bool):
    """The body of the endpoint, held under the start lock so the Active check / teardown and the
    create cannot be interleaved by a concurrent request."""
    if force:
        try:
            teardown = _teardown_practice_run(strict_cleanup=True)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Could not tear down the existing practice run: {e}")
        # Teardown reports success as long as the listener was retired, even when the connector
        # outlived its termination poll. Starting a replacement then would leave two connectors
        # publishing into draft_picks:{league} at once - precisely the overlap this guard exists
        # to prevent - so force has to insist on a positive result where DELETE does not.
        if not teardown["connector_gone"]:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"The existing practice room connector ({teardown['job_name']}) was deleted but has not "
                    "terminated yet. Starting another now would let two connectors publish into the same "
                    "draft Stream. Retry in a few seconds."
                ),
            )
    elif _practice_sync_job_state() == "Active":
        raise HTTPException(status_code=409, detail="A practice draft-sync Job is already Active - end it before starting another, or retry with force=true.")

    parsed_room_url = urlparse(payload.room_url)
    room_league_id = parse_qs(parsed_room_url.query).get("leagueId", [None])[0]
    job_name = f"practice-draft-sync-{int(time.time())}"
    job_labels = {"app": PRACTICE_SYNC_APP_LABEL}
    if room_league_id is not None:
        job_labels["league-id"] = room_league_id
    job = client.V1Job(
        metadata=client.V1ObjectMeta(name=job_name, labels=job_labels),
        spec=client.V1JobSpec(
            backoff_limit=0,
            ttl_seconds_after_finished=3600,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels=job_labels),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    image_pull_secrets=[client.V1LocalObjectReference(name="ghcr-pull-secret")],
                    containers=[
                        client.V1Container(
                            name="draft-sync",
                            image=BROWSER_WORKER_IMAGE,
                            command=["node", "src/bots/draft_sync.js"],
                            env=[
                                client.V1EnvVar(name="DRAFT_ROOM_URL", value=payload.room_url),
                                client.V1EnvVar(name="REDIS_HOST", value=REDIS_HOST),
                                client.V1EnvVar(name="REDIS_PORT", value=str(REDIS_PORT)),
                                client.V1EnvVar(
                                    name="ESPN_S2",
                                    value_from=client.V1EnvVarSource(
                                        secret_key_ref=client.V1SecretKeySelector(name=ESPN_SESSION_SECRET_NAME, key="espn_s2")
                                    ),
                                ),
                                client.V1EnvVar(
                                    name="SWID",
                                    value_from=client.V1EnvVarSource(
                                        secret_key_ref=client.V1SecretKeySelector(name=ESPN_SESSION_SECRET_NAME, key="swid")
                                    ),
                                ),
                            ],
                        )
                    ],
                ),
            ),
        ),
    )

    try:
        batch_api = _batch_v1_api()
        batch_api.create_namespaced_job(K8S_NAMESPACE, job, _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS)
    except Exception as e:
        logger.error(f"Failed to create practice draft-sync Job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create practice draft-sync Job: {str(e)}")

    return {"status": "accepted", "job_name": job_name}


@draft_router.get("/practice/sync-job/status")
def get_practice_sync_job_status():
    job = _latest_practice_sync_job()
    job_state = _job_phase(job.status) if job is not None else "Unknown"
    active_league_id = (job.metadata.labels or {}).get("league-id") if job is not None else None
    room_identifier = _practice_room_identifier(job) if job is not None else None
    room_connected = False
    if job_state == "Active":
        room_connected = _room_connected(active_league_id)
    return {
        "job_state": job_state,
        "pod_active": job_state == "Active",
        "room_connected": room_connected,
        "room_handshake_complete": room_connected,
        "active_league_id": active_league_id,
        "room_identifier": room_identifier,
    }


def _teardown_practice_run(strict_cleanup: bool = False) -> dict:
    """Retires the practice room connector and its listener, and clears their Redis state.

    Split out of the DELETE endpoint so POST ?force=true can reuse it. Returns the connector
    name (None if there was none), the listener Jobs deleted, and whether the connector
    actually terminated. Deliberately does NOT treat "nothing was running" as an error - only
    the endpoint decides whether that deserves a 404.

    connector_gone=False means the connector outlived its termination poll and may still be
    publishing. That is tolerable for DELETE, whose job is to retire the listener, but NOT for
    ?force=true, which goes on to start a second connector against the same Redis Stream.

    strict_cleanup makes a failed Redis delete fatal rather than logged. Redis cleanup is
    best-effort everywhere else on purpose, but a force start that proceeds after it failed hands
    the new connector the old run's ledger to seed from."""
    try:
        batch_api = _batch_v1_api()
        jobs = batch_api.list_namespaced_job(K8S_NAMESPACE, label_selector=f"app={PRACTICE_SYNC_APP_LABEL}", _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS)
    except Exception as e:
        logger.error(f"Failed to list practice draft-sync Jobs for teardown: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list practice draft-sync Jobs: {str(e)}")

    # The connector is only half the run: the listener that consumes its Redis Stream is a
    # separate Job, and nothing else ever stops it - it blocks on the stream forever.
    #
    # Crucially the listener is NOT gated on the connector still existing. The connector
    # carries ttl_seconds_after_finished, so Kubernetes garbage-collects it an hour after it
    # exits, while the listener is still running. Returning 404 in that window would leave
    # the leaked listener in place with no way to reach it - the exact failure this endpoint
    # is meant to prevent, reintroduced by the ordering.
    # Every connector, not just the newest. There is normally one, but concurrent starts or a
    # manual `kubectl apply` during recovery can leave more, and retiring only the newest would
    # report connector_gone=True while an older publisher was still writing to the same Stream -
    # which is exactly the guarantee ?force=true relies on before starting a replacement.
    connector_name = None
    connector_league_ids = []
    connector_gone = True
    if jobs.items:
        ordered = sorted(jobs.items, key=lambda j: j.metadata.creation_timestamp, reverse=True)
        connector_name = ordered[0].metadata.name
        deleted_connectors = []
        for job in ordered:
            try:
                # Foreground, not Background: the connector is the publisher for
                # draft:{league}:picks and draft_picks:{league}, and the cleanup below deletes
                # them. Background returns while the Pod is still running, so it could publish
                # into - or recreate - those keys after they were cleared, contaminating the next
                # run with the previous one's picks.
                batch_api.delete_namespaced_job(job.metadata.name, K8S_NAMESPACE, propagation_policy="Foreground", _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS)
            except Exception as e:
                logger.error(f"Failed to delete practice draft-sync Job {job.metadata.name}: {e}")
                raise HTTPException(status_code=500, detail=f"Failed to delete practice draft-sync Job: {str(e)}")
            deleted_connectors.append(job.metadata.name)
            league_id = (job.metadata.labels or {}).get("league-id")
            if league_id and league_id not in connector_league_ids:
                connector_league_ids.append(league_id)
        # One wait for the whole set rather than per Job, so the polls overlap instead of
        # summing - N connectors would otherwise take N x the termination budget.
        connector_gone = _wait_for_jobs_gone(batch_api, f"app={PRACTICE_SYNC_APP_LABEL}", deleted_connectors)
        if not connector_gone:
            # Not fatal: the listener still has to be retired, and reporting failure here
            # would leave it leaked - the exact bug this endpoint exists to prevent. Only the
            # pick-stream cleanup is skipped, since that is the part the connector can race.
            logger.warning(
                f"Practice room connector(s) {', '.join(deleted_connectors)} did not terminate in time; retiring the "
                "listener anyway but leaving the pick ledger and Stream in place, since the connector "
                "may still be publishing to them."
            )

    try:
        deleted_listeners = _delete_practice_listener_jobs(clear_pick_stream=connector_gone)
    except Exception as e:
        detail = (
            f"Room connector {connector_name} was deleted, but the draft-sync listener could not be: {e}"
            if connector_name
            else f"Failed to delete the practice draft-sync listener: {e}"
        )
        raise HTTPException(status_code=500, detail=detail)

    # _delete_practice_listener_jobs only clears leagues it found a *listener* for. The listener
    # carries ttl_seconds_after_finished and can already be gone while the connector is still up,
    # in which case nothing above would clear this league at all. Idempotent, so re-clearing a
    # league the listener sweep already handled costs a no-op DELETE.
    if connector_gone:
        for league_id in connector_league_ids:
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

    return {"job_name": connector_name, "listener_jobs_deleted": deleted_listeners, "connector_gone": connector_gone}


@draft_router.delete("/practice/sync-job", status_code=status.HTTP_200_OK)
def end_practice_sync_job():
    """Tears down the practice draft-sync Job so leftover Job/Redis state can't interfere
    with a later run - the cleanup concern the issue explicitly calls out."""
    result = _teardown_practice_run()
    if result["job_name"] is None and not result["listener_jobs_deleted"]:
        raise HTTPException(status_code=404, detail="No practice draft-sync Job exists.")
    return {"status": "deleted", **result}
