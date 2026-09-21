"""
VORP browsing REST endpoint.

Unauthenticated-at-this-layer REST controller over src.services.stats_service.
"""
import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from src.schemas.stats import VorpSort, VorpStat
from src.services.stats_service import browse_vorp

logger = logging.getLogger("StatsRoutes")
stats_router = APIRouter()

# Every filter is optional, so an unscoped request must still be bounded - weekly_stats
# grows every player/week/season and has no other cap on rows returned.
DEFAULT_VORP_LIMIT = 200
MAX_VORP_LIMIT = 1000


@stats_router.get("/vorp", response_model=List[VorpStat])
def browse_vorp_endpoint(
    season: Optional[int] = None,
    week: Optional[int] = None,
    position: Optional[str] = None,
    sort: VorpSort = VorpSort.vorp,
    limit: int = Query(DEFAULT_VORP_LIMIT, ge=1, le=MAX_VORP_LIMIT),
):
    try:
        return browse_vorp(season, week, position, sort, limit)
    except Exception as e:
        logger.error(f"Failed to browse VORP stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to browse VORP stats.")
