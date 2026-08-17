"""
Draft-day browser control plane (CORE-10).

Lets the draft-day pipeline (CORE-08's draft_matrix.py listener) be configured, launched,
paused, and monitored from a browser instead of `kubectl` - the user will be traveling
internationally on the actual draft day. Sits alongside the hot path (browser-worker ->
draft_matrix -> /trigger/draft-pick -> RabbitMQ -> click bot) as an out-of-band control/
observability layer, not part of it.

Two known gaps, both flagged rather than guessed at:
  - `last_clicker_action` in /status has no data source yet - fantasy-browser-worker's
    click bot isn't wired to consume `draft_targets` yet (WORKER-06, unshipped elsewhere).
    Always null here until that ships.
  - /start's Job image comes from a required DRAFT_SYNC_IMAGE env var with no default -
    the real image ref lives in fantasy-infra, out of this repo's scope, same "flag the
    stale/missing default instead of guessing" pattern as CORE_API_BASE_URL.
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
from src.optimization.draft_matrix import compute_on_the_clock_team

logger = logging.getLogger("DraftControlPlane")
draft_router = APIRouter()

REDIS_HOST = os.getenv("REDIS_HOST", "fantasy-cache.default.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "default")
DRAFT_SYNC_IMAGE = os.getenv("DRAFT_SYNC_IMAGE")

SSE_POLL_INTERVAL_SECONDS = 2


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


class DraftStatusResponse(BaseModel):
    league_id: str
    season: int
    job_state: str = Field(..., description="Active/Succeeded/Failed/Unknown, from the k8s Job's own status")
    recommendation: dict = Field(..., description="Latest recommendation from CORE-08's solver (draft:{league_id}:board), empty if none yet")
    last_clicker_action: Optional[str] = Field(None, description="Pending WORKER-06 - fantasy-browser-worker's click bot isn't wired to the draft_targets queue yet, so this is always null today")
    recent_picks: List[DraftPickSummary]
    on_the_clock_team_id: Optional[str] = Field(None, description="Null until every team's round-1 slot is known")
    pipeline_health: str = Field(..., description="'healthy' iff the draft-sync Job is Active and Postgres/Redis both respond, else 'degraded'")


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
    return r_client.hgetall(f"draft:{league_id}:board") or {}


def _recent_picks(cursor, league_id: str, season: int, limit: int) -> list:
    cursor.execute("""
        SELECT pick_number, round, team_id, player_id
        FROM draft_picks
        WHERE league_id = %s AND season = %s
        ORDER BY pick_number DESC
        LIMIT %s;
    """, (league_id, season, limit))
    return [
        {"pick_number": row[0], "round": row[1], "team_id": row[2], "player_id": row[3]}
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


def _pipeline_health(job_state: str, cursor) -> str:
    try:
        cursor.execute("SELECT 1;")
        db_ok = True
    except Exception:
        db_ok = False
    return "healthy" if job_state == "Active" and db_ok else "degraded"


def _build_status(league_id: str, season: int, limit: int) -> dict:
    r_client = _redis_client()
    job_state = _job_state(league_id)
    with get_db_cursor() as cursor:
        recent_picks = _recent_picks(cursor, league_id, season, limit)
        on_the_clock_team_id = _on_the_clock_team_id(cursor, league_id, season)
        pipeline_health = _pipeline_health(job_state, cursor)

    return {
        "league_id": league_id,
        "season": season,
        "job_state": job_state,
        "recommendation": _recommendation(r_client, league_id),
        "last_clicker_action": None,
        "recent_picks": recent_picks,
        "on_the_clock_team_id": on_the_clock_team_id,
        "pipeline_health": pipeline_health,
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
    relying on a real ASGI transport's disconnect detection."""
    last_payload = None
    while True:
        if await request.is_disconnected():
            break
        try:
            payload = _build_status(league_id, season, limit)
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
    r_client = _redis_client()
    pick_number = payload.pick_number
    if pick_number is None:
        board = r_client.hgetall(f"draft:{payload.league_id}:board")
        if not board or "pick_number" not in board:
            raise HTTPException(status_code=400, detail="No pending pick_number on the recommendation board and none was provided explicitly.")
        pick_number = int(board["pick_number"])

    r_client.delete(f"draft:{payload.league_id}:pause:{pick_number}")
    return {"status": "resumed", "league_id": payload.league_id, "pick_number": pick_number}
