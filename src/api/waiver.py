"""
Waiver recommendation REST endpoint (fantasy-web-ui's waiver recommendation panel).

Thin controller over src.services.waiver_service, mirroring src/api/roster.py's pattern
of splitting a REST surface out of the MCP-only layer.
"""
import logging
from typing import List, Literal, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.services.waiver_service import WaiverInputsUnavailableError, compute_waiver_recommendations
from src.services.free_agent_service import browse_free_agents

logger = logging.getLogger("WaiverRoutes")
waiver_router = APIRouter()


class FreeAgentPlayer(BaseModel):
    player_id: str
    espn_id: str
    name: str
    position: str
    pro_team: str
    injury_status: Optional[str] = None
    projected_points: Optional[float] = None
    vorp_score: Optional[float] = None


class FreeAgentEntry(FreeAgentPlayer):
    bench_player: Optional[FreeAgentPlayer] = None
    vorp_delta: Optional[float] = None


class FreeAgentsResponse(BaseModel):
    season: int
    week: int
    free_agents: List[FreeAgentEntry]
    total: int
    limit: int
    offset: int
    source_mode: Literal["target_week", "rolling_prior_weeks"]
    source_weeks: List[int]


@waiver_router.get("/free-agents", response_model=FreeAgentsResponse)
def get_free_agents(
    season: int = Query(..., ge=1),
    week: int = Query(..., ge=1, le=18),
    position: Optional[str] = Query(None, min_length=1),
    q: Optional[str] = None,
    sort: Literal["vorp", "projected"] = "vorp",
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    try:
        return browse_free_agents(season, week, position, q, sort, limit, offset)
    except WaiverInputsUnavailableError as exc:
        raise HTTPException(status_code=503, detail=f"Free agents unavailable: {exc}")
    except Exception as exc:
        logger.exception("Failed to browse free agents for season=%s week=%s", season, week)
        raise HTTPException(status_code=500, detail=f"Failed to browse free agents: {exc}")


class WaiverPlayerSummary(BaseModel):
    espn_id: Optional[str] = None
    name: str
    pro_team: str
    injury_status: Optional[str] = None
    net_vorp: float


class WaiverRecommendation(BaseModel):
    # "swap" (add free_agent, drop bench_player) or "ir_and_add" (move ir_player to IR,
    # add free_agent, no drop).
    kind: str = "swap"
    position: str
    vorp_delta: float
    free_agent: WaiverPlayerSummary
    bench_player: Optional[WaiverPlayerSummary] = None
    ir_player: Optional[WaiverPlayerSummary] = None
    rumor_sentiment: Optional[str] = None
    rumor_confidence: Optional[float] = None
    rumor_suggested_action: Optional[str] = None


class WaiverIrMove(BaseModel):
    action: str
    position: str
    player: WaiverPlayerSummary


class WaiverRecommendationsResponse(BaseModel):
    season: int
    week: int
    recommendations: List[WaiverRecommendation]
    # IR-eligible bench players worth stashing while an IR slot is open.
    open_ir_slots: int = 0
    ir_moves: List[WaiverIrMove] = []
    # "target_week", or "rolling_prior_weeks" before the week's projections land (#185).
    source_mode: Optional[str] = None
    source_weeks: Optional[List[int]] = None


@waiver_router.get("/recommendations", response_model=WaiverRecommendationsResponse)
def get_waiver_recommendations(season: int, week: int):
    try:
        return compute_waiver_recommendations(season, week)
    except WaiverInputsUnavailableError as e:
        raise HTTPException(status_code=503, detail=f"Waiver recommendations unavailable: {e}")
    except Exception as e:
        logger.error(f"Failed to compute waiver recommendations for season={season} week={week}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to compute waiver recommendations: {str(e)}")
