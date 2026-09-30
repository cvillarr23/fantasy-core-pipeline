"""
Waiver recommendation REST endpoint (fantasy-web-ui's waiver recommendation panel).

Thin controller over src.services.waiver_service, mirroring src/api/roster.py's pattern
of splitting a REST surface out of the MCP-only layer.
"""
import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.services.waiver_service import WaiverInputsUnavailableError, compute_waiver_recommendations

logger = logging.getLogger("WaiverRoutes")
waiver_router = APIRouter()


class WaiverPlayerSummary(BaseModel):
    espn_id: Optional[str] = None
    name: str
    pro_team: str
    injury_status: Optional[str] = None
    net_vorp: float


class WaiverRecommendation(BaseModel):
    position: str
    vorp_delta: float
    free_agent: WaiverPlayerSummary
    bench_player: WaiverPlayerSummary
    rumor_sentiment: Optional[str] = None
    rumor_confidence: Optional[float] = None
    rumor_suggested_action: Optional[str] = None


class WaiverRecommendationsResponse(BaseModel):
    season: int
    week: int
    recommendations: List[WaiverRecommendation]
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
