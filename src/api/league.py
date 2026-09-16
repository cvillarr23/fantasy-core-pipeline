"""
Team-id -> display-name lookup (fantasy-web-ui uses this to replace "Team {id}"
placeholders with real ESPN team names). Thin controller over
src.services.league_service, mirroring src/api/roster.py's pattern.
"""
import logging
import os

from fastapi import APIRouter, HTTPException, Query

from src.services.league_service import get_team_names

logger = logging.getLogger("LeagueRoutes")
league_router = APIRouter()


@league_router.get("/teams")
def get_teams(league_id: str = Query(default_factory=lambda: os.getenv("ESPN_LEAGUE_ID"))):
    try:
        return get_team_names(league_id)
    except Exception as e:
        logger.error(f"Failed to fetch team names for league_id={league_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch team names: {str(e)}")
