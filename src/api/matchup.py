"""
Weekly head-to-head matchup REST endpoint (fantasy-web-ui's Matchup page).

Thin controller over src.services.matchup_service, mirroring src/api/roster.py's
pattern of splitting a REST surface out of the MCP-only layer.
"""
import logging
from typing import Literal, Optional

from espn_api.requests.espn_requests import ESPNAccessDenied, ESPNInvalidLeague, ESPNUnknownError
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from src.nfl_schedule import RegularSeasonOverError, WeekResolutionError
from src.schemas.espn_ownership import EspnOwnership
from src.schemas.live import GameDetailResponse, LiveBlock, MatchupFeedResponse
from src.services import live_service
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
    # Where the player's NFL game stands. None when the scoreboard can't be read, the team has no
    # game this week (bye; see bye_week), or the week is already stored as completed history.
    game_status: Optional[Literal["not_started", "in_progress", "final"]] = None
    # The poller's live snapshot (score, clock, stat line, live points). None when there is none
    # for this player; when set, `live.status` and `game_status` agree.
    live: Optional[LiveBlock] = None
    headshot_url: str
    deficit_flag: bool
    suggested_swap: Optional[dict] = None


class TeamInfo(BaseModel):
    name: str
    logo_url: str
    score: float
    # ESPN's live projected total (totalProjectedPointsLive); when ESPN sends none (no game under
    # way) espn_api falls back to the sum of the starters' static pregame projections.
    projected: Optional[float]
    # ESPN's chance (0-1, two decimals) this team wins the matchup. The two sides sum to 1. ESPN
    # only sends it for the matchup period in progress; None for a finished or future week, a
    # stored completed week, or when ESPN can't be read.
    win_probability: Optional[float] = None


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
        return live_service.overlay_matchup(get_current_team_matchup(season, week))
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


@matchup_router.get("/feed", response_model=MatchupFeedResponse)
def get_matchup_feed(
    season: int,
    week: int,
    limit: int = Query(50, ge=1, le=200),
    before: Optional[str] = None,
):
    """Plays and recent news for both sides' players, newest first. Empty unless the poller is
    running this season and week."""
    try:
        return live_service.load_feed(season, week, limit, before)
    except Exception as e:
        logger.error(f"Failed to build matchup feed for season={season} week={week}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to build matchup feed: {str(e)}")


@matchup_router.get("/games/{event_id}", response_model=GameDetailResponse)
def get_matchup_game(event_id: str, season: int, week: int):
    """One NFL game in this week's matchup: header, each side's starters' fantasy points from
    it, the matchup players in it with their scoring breakdown, and its plays."""
    try:
        return live_service.load_game_detail(event_id, season, week)
    except live_service.LiveGameNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except live_service.LiveSnapshotUnavailableError as e:
        logger.error(f"Live snapshot unavailable for game {event_id}: {e}")
        raise HTTPException(status_code=503, detail="The live snapshot is unavailable")


class ScoreboardTeamInfo(BaseModel):
    team_id: str
    name: str
    logo_url: str
    score: float
    projected: Optional[float]
    # Same meaning as TeamInfo.win_probability.
    win_probability: Optional[float] = None


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
    espn: Optional[EspnOwnership] = None


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
