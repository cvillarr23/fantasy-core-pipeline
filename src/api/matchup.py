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

from src.nfl_schedule import RegularSeasonOverError, WeekResolutionError
from src.services.matchup_service import (
    ESPNTimeoutError,
    NoMatchupFoundError,
    get_current_team_matchup,
    get_current_team_roster,
    get_league_scoreboard,
)

logger = logging.getLogger("MatchupRoutes")
matchup_router = APIRouter()
scoreboard_router = APIRouter()


class PlayerLine(BaseModel):
    player_id: str
    espn_id: str
    name: str
    position: str
    jersey: Optional[str] = None
    slot: str
    points: Optional[float]
    projected_points: Optional[float]
    # The player's NFL bye week; None when the schedule couldn't be read.
    bye_week: Optional[int] = None
    headshot_url: str
    deficit_flag: bool
    suggested_swap: Optional[dict] = None


class TeamInfo(BaseModel):
    name: str
    logo_url: str
    score: float
    projected: Optional[float]


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
    except ESPNTimeoutError as e:
        logger.error(f"ESPN timed out while building matchup for season={season} week={week}: {e}")
        raise HTTPException(status_code=504, detail="ESPN request timed out")
    except (ESPNAccessDenied, ESPNInvalidLeague, ESPNUnknownError) as e:
        logger.error(f"ESPN unreachable while building matchup for season={season} week={week}: {e}")
        raise HTTPException(status_code=502, detail="Could not reach ESPN")
    except Exception as e:
        logger.error(f"Failed to compute matchup for season={season} week={week}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to compute matchup: {str(e)}")


class ScoreboardTeamInfo(BaseModel):
    team_id: str
    name: str
    logo_url: str
    score: float
    projected: Optional[float]


class ScoreboardMatchup(BaseModel):
    home: Optional[ScoreboardTeamInfo] = None
    away: Optional[ScoreboardTeamInfo] = None


class ScoreboardResponse(BaseModel):
    season: int
    week: int
    matchups: list[ScoreboardMatchup]


@matchup_router.get("/scoreboard", response_model=ScoreboardResponse)
@scoreboard_router.get("", response_model=ScoreboardResponse)
def get_scoreboard(season: int, week: int):
    try:
        return get_league_scoreboard(season, week)
    except ESPNTimeoutError as e:
        logger.error(f"ESPN timed out while building scoreboard for season={season} week={week}: {e}")
        raise HTTPException(status_code=504, detail="ESPN request timed out")
    except (ESPNAccessDenied, ESPNInvalidLeague, ESPNUnknownError) as e:
        logger.error(f"ESPN unreachable while building scoreboard for season={season} week={week}: {e}")
        raise HTTPException(status_code=502, detail="Could not reach ESPN")
    except Exception as e:
        logger.error(f"Failed to compute scoreboard for season={season} week={week}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to compute scoreboard: {str(e)}")


class LineupPlayerLine(BaseModel):
    player_id: str
    espn_id: str
    name: str
    position: str
    jersey: Optional[str] = None
    slot: str
    pro_team: str
    injury_status: Optional[str] = None
    # The player's NFL bye week; None when the schedule couldn't be read.
    bye_week: Optional[int] = None
    headshot_url: str
    deficit_flag: bool
    suggested_swap: Optional[dict] = None
    net_vorp: Optional[float] = None
    projected_points: Optional[float] = None
    rumor_sentiment: Optional[str] = None
    rumor_confidence: Optional[float] = None
    rumor_suggested_action: Optional[str] = None
    # {"action": "move_to_ir" | "activate_from_ir", "reason": ...}; display-only.
    ir_suggestion: Optional[dict] = None


class RosterResponse(BaseModel):
    starters: list[LineupPlayerLine]
    bench: list[LineupPlayerLine]
    # An active player sitting in IR; ESPN rejects new transactions until they leave it.
    ir_blocking_transactions: bool = False
    # The league's lineup shape keyed by slot label ("QB", "BE", "IR", ...), so the lineup
    # page can show bench and IR capacity. None when the league's settings aren't synced.
    lineup_slot_counts: Optional[dict[str, int]] = None


@matchup_router.get("/roster", response_model=RosterResponse)
def get_lineup_roster(season: int, week: int | None = None):
    try:
        return get_current_team_roster(season, week) if week is not None else get_current_team_roster(season)
    except (ESPNAccessDenied, ESPNInvalidLeague, ESPNUnknownError) as e:
        logger.error(f"ESPN unreachable while building roster for season={season}: {e}")
        raise HTTPException(status_code=502, detail="Could not reach ESPN")
    except WeekResolutionError as e:
        logger.warning(f"Upcoming week unavailable for roster season={season}: {e}")
        raise HTTPException(status_code=503, detail="Current week is unavailable. Retry or select a week manually.")
    except RegularSeasonOverError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to compute roster for season={season}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to compute roster: {str(e)}")
