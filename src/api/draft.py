"""
Draft-day browser control plane (CORE-10).

Lets the draft-day pipeline (CORE-08's draft_matrix.py listener) be configured, launched,
paused, and monitored from a browser instead of `kubectl` - the user will be traveling
internationally on the actual draft day. Sits alongside the hot path (browser-worker ->
draft_matrix -> /trigger/draft-pick -> RabbitMQ -> click bot) as an out-of-band control/
observability layer, not part of it.

One known gap, flagged rather than guessed at:
  - /start's Job image and its DB/Redis/API env config come from required env vars with no
    defaults - the real image ref and Secret/ConfigMap names live in fantasy-infra, out of
    this repo's scope, same "flag the stale/missing default instead of guessing" pattern as
    CORE_API_BASE_URL.
"""
import asyncio
import json
import logging
import os
import time
from typing import List, Optional

import redis
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException
from pydantic import BaseModel, Field

from src.database import get_db_cursor
from src.optimization.draft_matrix import compute_draft_recommendation, compute_on_the_clock_team, publish_draft_target

logger = logging.getLogger("DraftControlPlane")
draft_router = APIRouter()

REDIS_HOST = os.getenv("REDIS_HOST", "fantasy-cache.default.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "default")
DRAFT_SYNC_IMAGE = os.getenv("DRAFT_SYNC_IMAGE")
# Names of the k8s Secret/ConfigMap the draft-sync Job's container should pull DB_PASSWORD,
# DB_HOST/NAME/USER, REDIS_HOST/PORT, and CORE_API_BASE_URL from - the Job's Pod does not
# inherit this API Pod's own environment, and draft_matrix.py imports src.database, which
# opens a connection pool at import time and needs these to be real. No defaults: the
# actual Secret/ConfigMap names are fantasy-infra's to define, not this repo's to guess.
DRAFT_SYNC_ENV_SECRET = os.getenv("DRAFT_SYNC_ENV_SECRET")
DRAFT_SYNC_ENV_CONFIGMAP = os.getenv("DRAFT_SYNC_ENV_CONFIGMAP")
# Draft-sync listener defaults to week 1 preseason projections (matches draft_matrix.py's
# own --week CLI default) - draft config doesn't track a week, since a draft has none.
DRAFT_SYNC_WEEK = 1

SSE_POLL_INTERVAL_SECONDS = 2
DRAFT_STATUS_STALENESS_SECONDS = int(os.getenv("DRAFT_STATUS_STALENESS_SECONDS", "15"))
RECENT_ERRORS_LIMIT = 5


def _redis_client() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


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


class DraftPickSummary(BaseModel):
    pick_number: int
    round: Optional[int]
    team_id: str
    player_id: str
    name: Optional[str] = Field(None, description="Joined from players; null if the player row is missing")
    position: Optional[str] = Field(None, description="Joined from players; null if the player row is missing")


class DraftStatusResponse(BaseModel):
    league_id: str
    season: int
    job_state: str = Field(..., description="Active/Succeeded/Failed/Unknown, from the k8s Job's own status")
    recommendation: dict = Field(..., description="Latest recommendation from CORE-08's solver (draft:{league_id}:board), empty if none yet")
    last_clicker_action: Optional[str] = Field(None, description="Human-readable outcome of WORKER-06's most recent click for this league (e.g. 'Drafted Breece Hall at 14:32:05', or a failure message), sourced from execution_log; null until the click bot has confirmed a pick")
    recent_picks: List[DraftPickSummary]
    on_the_clock_team_id: Optional[str] = Field(None, description="Null until every team's round-1 slot is known")
    pipeline_health: str = Field(..., description="'healthy' iff the draft-sync Job is Active, Postgres/Redis both respond, and the listener's last-processed timestamp is fresh, else 'degraded'")
    recent_errors: List[dict] = Field(default_factory=list, description=f"Most recent draft-related error rows from execution_log (limit {RECENT_ERRORS_LIMIT})")


class DraftPauseRequest(BaseModel):
    league_id: str = Field(..., description="League whose auto-draft should be paused/resumed")
    pick_number: Optional[int] = Field(None, description="Pick to pause; defaults to the next pick currently on the recommendation board")


# ------------------------------------------------------------------------------
# Status aggregation (shared by GET /status and GET /stream)
# ------------------------------------------------------------------------------
def _job_state(league_id: str) -> str:
    try:
        batch_api = _batch_v1_api()
        jobs = batch_api.list_namespaced_job(K8S_NAMESPACE, label_selector=f"league-id={league_id}")
    except Exception as e:
        logger.error(f"Failed to query draft-sync Job status for league {league_id}: {e}")
        return "Unknown"

    if not jobs.items:
        return "Unknown"
    # Most recently created Job for this league is the one we care about.
    latest = max(jobs.items, key=lambda j: j.metadata.creation_timestamp)
    job_status = latest.status
    if job_status.active:
        return "Active"
    if job_status.succeeded:
        return "Succeeded"
    if job_status.failed:
        return "Failed"
    return "Unknown"


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
    vanishing from the feed."""
    cursor.execute("""
        SELECT d.pick_number, d.round, d.team_id, d.player_id, p.name, p.position
        FROM draft_picks d
        LEFT JOIN players p ON d.player_id = p.player_id
        WHERE d.league_id = %s AND d.season = %s
        ORDER BY d.pick_number DESC
        LIMIT %s;
    """, (league_id, season, limit))
    return [
        {"pick_number": row[0], "round": row[1], "team_id": row[2], "player_id": row[3], "name": row[4], "position": row[5]}
        for row in cursor.fetchall()
    ]


def _on_the_clock_team_id(cursor, league_id: str, season: int) -> Optional[str]:
    cursor.execute("SELECT team_count FROM league_settings WHERE league_id = %s;", (league_id,))
    row = cursor.fetchone()
    if row is None:
        return None
    team_count = row[0]

    cursor.execute("SELECT team_id, slot_number FROM draft_slots WHERE league_id = %s AND season = %s;", (league_id, season))
    slot_map = {team_id: slot_number for team_id, slot_number in cursor.fetchall()}
    if not slot_map:
        return None

    cursor.execute("SELECT COALESCE(MAX(pick_number), 0) FROM draft_picks WHERE league_id = %s AND season = %s;", (league_id, season))
    latest_pick_number = cursor.fetchone()[0]

    return compute_on_the_clock_team(latest_pick_number + 1, team_count, slot_map)


def _recent_execution_errors(cursor, limit: int) -> list:
    cursor.execute("""
        SELECT transaction_type, bot_name, error_message, executed_at
        FROM execution_log
        WHERE status <> 'success'
          AND error_message IS NOT NULL
          AND (transaction_type ILIKE 'draft%%' OR COALESCE(bot_name, '') ILIKE 'draft%%')
        ORDER BY executed_at DESC
        LIMIT %s;
    """, (limit,))
    return [
        {
            "transaction_type": row[0],
            "bot_name": row[1],
            "error_message": row[2],
            "executed_at": row[3],
        }
        for row in cursor.fetchall()
    ]


def _last_clicker_action(cursor, league_id: str) -> Optional[str]:
    """Most recent draft_pick_click confirmation from fantasy-browser-worker's click bot
    (WORKER-06) for this league, formatted for direct display - not the raw row, since the
    dashboard has no other use for execution_log's internal shape here."""
    cursor.execute("""
        SELECT player_name, status, error_message, executed_at
        FROM execution_log
        WHERE transaction_type = 'draft_pick_click' AND league_id = %s
        ORDER BY executed_at DESC
        LIMIT 1;
    """, (league_id,))
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
    job_state = _job_state(league_id)

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
    try:
        with get_db_cursor() as cursor:
            recent_picks = _recent_picks(cursor, league_id, season, limit)
            on_the_clock_team_id = _on_the_clock_team_id(cursor, league_id, season)
            recent_errors = _recent_execution_errors(cursor, RECENT_ERRORS_LIMIT)
            last_clicker_action = _last_clicker_action(cursor, league_id)
    except Exception as e:
        logger.error(f"Failed to read draft picks/slots for league {league_id}: {e}")
        db_ok = False

    pipeline_health = "healthy" if job_state == "Active" and db_ok and redis_ok and not _is_stale(job_state, recommendation) else "degraded"

    return {
        "league_id": league_id,
        "season": season,
        "job_state": job_state,
        "recommendation": recommendation,
        "last_clicker_action": last_clicker_action,
        "recent_picks": recent_picks,
        "on_the_clock_team_id": on_the_clock_team_id,
        "pipeline_health": pipeline_health,
        "recent_errors": recent_errors,
    }


# ------------------------------------------------------------------------------
# ROUTE ENDPOINTS
# ------------------------------------------------------------------------------
@draft_router.post("/config", status_code=status.HTTP_200_OK)
def set_draft_config(payload: DraftConfigPayload):
    """Stores league_id/team_id/season so /start's draft-sync Job can read it at launch."""
    try:
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO draft_config (league_id, team_id, season)
                VALUES (%s, %s, %s)
                ON CONFLICT (league_id) DO UPDATE
                SET team_id = EXCLUDED.team_id, season = EXCLUDED.season, updated_at = CURRENT_TIMESTAMP;
            """, (payload.league_id, payload.team_id, payload.season))
    except Exception as e:
        logger.error(f"Failed to persist draft config for league {payload.league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to store draft config: {str(e)}")

    return {"status": "stored", "league_id": payload.league_id, "team_id": payload.team_id, "season": payload.season}


@draft_router.post("/start", status_code=status.HTTP_202_ACCEPTED)
def start_draft_sync():
    """Creates the draft-sync Job (a wrapper around draft_matrix.py's listener) from the
    most recently stored /config, replacing the manual `kubectl apply` step."""
    if not DRAFT_SYNC_IMAGE:
        raise HTTPException(status_code=500, detail="DRAFT_SYNC_IMAGE is not configured - the draft-sync container image ref must be supplied via env var.")
    if not DRAFT_SYNC_ENV_SECRET or not DRAFT_SYNC_ENV_CONFIGMAP:
        raise HTTPException(status_code=500, detail="DRAFT_SYNC_ENV_SECRET and DRAFT_SYNC_ENV_CONFIGMAP must both be configured - the Job's Pod does not inherit this API Pod's own DB/Redis/API environment.")

    try:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT league_id, team_id, season FROM draft_config ORDER BY updated_at DESC LIMIT 1;")
            row = cursor.fetchone()
    except Exception as e:
        logger.error(f"Failed to read draft config before starting draft-sync Job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to read draft config: {str(e)}")

    if row is None:
        raise HTTPException(status_code=400, detail="No draft config stored yet - POST /v1/draft/config first.")
    league_id, team_id, season = row

    # A second listener for the same league would share the browser-worker's Redis Stream
    # consumer name (draft-matrix-{team_id}) but keep an entirely separate in-process
    # pending_state - a newer pick handled by one process can't cancel the other's pending
    # delayed publish, so two Active Jobs for one league can race and double-publish a
    # stale draft target. Reject rather than silently creating a duplicate.
    existing_state = _job_state(league_id)
    if existing_state == "Active":
        raise HTTPException(status_code=409, detail=f"A draft-sync Job for league {league_id} is already Active - stop it before starting another.")

    job_name = f"draft-sync-{league_id}-{int(time.time())}"
    job = client.V1Job(
        metadata=client.V1ObjectMeta(name=job_name, labels={"app": "draft-sync", "league-id": league_id}),
        spec=client.V1JobSpec(
            backoff_limit=0,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": "draft-sync", "league-id": league_id}),
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
                        )
                    ],
                ),
            ),
        ),
    )

    try:
        batch_api = _batch_v1_api()
        batch_api.create_namespaced_job(K8S_NAMESPACE, job)
    except Exception as e:
        logger.error(f"Failed to create draft-sync Job for league {league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create draft-sync Job: {str(e)}")

    return {"status": "accepted", "job_name": job_name, "league_id": league_id, "team_id": team_id, "season": season}


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
