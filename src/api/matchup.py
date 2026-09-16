"""
Weekly head-to-head matchup REST endpoint (fantasy-web-ui's Matchup page).

Thin controller over src.services.matchup_service, mirroring src/api/roster.py's
pattern of splitting a REST surface out of the MCP-only layer.
"""
import logging
from typing import Optional

from espn_api.requests.espn_requests import ESPNAccessDenied, ESPNInvalidLeague, ESPNUnknownError
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.services.matchup_service import NoMatchupFoundError, get_current_team_matchup

logger = logging.getLogger("MatchupRoutes")
matchup_router = APIRouter()


class PlayerLine(BaseModel):
    player_id: str
    espn_id: str
    name: str
    position: str
    jersey: Optional[str] = None
    slot: str
    points: float
    projected_points: float
    headshot_url: str
    deficit_flag: bool
    suggested_swap: Optional[dict] = None


class TeamInfo(BaseModel):
    name: str
    logo_url: str
    score: float
    projected: float


class TeamSide(BaseModel):
    team: TeamInfo
    starters: list[PlayerLine]
    bench: list[PlayerLine]


class MatchupResponse(BaseModel):
    season: int
    week: int
    our_team: TeamSide
    opponent: TeamSide


@matchup_router.get("", response_model=MatchupResponse)
def get_matchup(season: int, week: int):
    try:
        return get_current_team_matchup(season, week)
    except NoMatchupFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except (ESPNAccessDenied, ESPNInvalidLeague, ESPNUnknownError) as e:
        logger.error(f"ESPN unreachable while building matchup for season={season} week={week}: {e}")
        raise HTTPException(status_code=502, detail="Could not reach ESPN")
    except Exception as e:
        logger.error(f"Failed to compute matchup for season={season} week={week}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to compute matchup: {str(e)}")
