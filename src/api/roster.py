"""
Roster deficit REST endpoint (fantasy-web-ui#1 roster/lineup management view support).

Unauthenticated-at-this-layer REST controller over src.services.roster_service, mirroring
src/api/players.py's pattern for splitting a REST surface out of the MCP-only layer.
"""
import logging
import os
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.api.trade import _raise_for_trade_read_error
from src.schemas.roster_outlook import TeamByeOutlook
from src.services.roster_outlook_service import get_bye_outlook
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


@roster_router.get("/bye-outlook", response_model=TeamByeOutlook)
def get_roster_bye_outlook(
    season: int = Query(ge=2000, le=2100),
    week: int = Query(ge=1, le=18),
    horizon: int = Query(4, ge=1, le=18),
    league_id: Optional[str] = Query(None, pattern=r"^[1-9]\d*$"),
    team_id: Optional[str] = Query(None, pattern=r"^[1-9]\d*$"),
):
    """Which upcoming weeks NFL byes leave the lineup short. Defaults to our own league
    and team."""
    league_id = league_id or os.getenv("ESPN_LEAGUE_ID") or ""
    team_id = team_id or os.getenv("ESPN_TEAM_ID") or ""
    try:
        return get_bye_outlook(league_id, season, week, horizon, team_id)
    except Exception as error:
        _raise_for_trade_read_error(error)
