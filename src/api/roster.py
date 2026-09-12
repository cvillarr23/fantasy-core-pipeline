"""
Roster deficit REST endpoint (fantasy-web-ui#1 roster/lineup management view support).

Unauthenticated-at-this-layer REST controller over src.services.roster_service, mirroring
src/api/players.py's pattern for splitting a REST surface out of the MCP-only layer.
"""
import logging
from typing import Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.services.roster_service import compute_roster_deficits

logger = logging.getLogger("RosterRoutes")
roster_router = APIRouter()


class RosterPositionDeficit(BaseModel):
    rostered: int
    starters_required: int
    deficit: int
    net_vorp: float


class RosterDeficitsResponse(BaseModel):
    season: int
    week: int
    roster_snapshot: str
    teams: Dict[str, Dict[str, RosterPositionDeficit]]


@roster_router.get("/deficits", response_model=RosterDeficitsResponse)
def get_roster_deficits(season: int, week: int):
    try:
        return compute_roster_deficits(season, week)
    except Exception as e:
        logger.error(f"Failed to compute roster deficits for season={season} week={week}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to compute roster deficits: {str(e)}")
