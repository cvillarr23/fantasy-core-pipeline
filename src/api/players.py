"""
Player summary REST endpoints (CORE-14 web UI support).

Plain JSON reads of player metadata + recent CORE-13 news, for fantasy-web-ui's
draft-day pick feed. The existing read surface for player data is two MCP tools
(get_league_roster_deficits, evaluate_proposed_trade) behind Google OAuth - not a fit
for a browser fetching a summary per pick, so this is a separate, unauthenticated-at-
this-layer REST router (see draft-day plan's OIDC/ingress follow-up for how it's
actually protected once deployed).

Unauthenticated-at-this-layer REST controller over src.services.players_service.
"""
import logging
from typing import List

from fastapi import APIRouter, HTTPException

from src.schemas.players import PlayerSearchResult, PlayerSummaryResponse
from src.services.players_service import PlayerNotFoundError, get_player_summary, search_players

logger = logging.getLogger("PlayerRoutes")
player_router = APIRouter()


@player_router.get("/search", response_model=List[PlayerSearchResult])
def search_players_endpoint(q: str):
    """Name lookup for the web UI's manual-pick picker, so a draft-day override can be typed
    as a player name instead of a raw id. See src.services.players_service.search_players
    for why both player_id and espn_id are returned."""
    query = q.strip()
    if len(query) < 2:
        return []
    try:
        return search_players(query)
    except Exception as e:
        logger.error(f"Failed to search players for '{query}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to search players: {str(e)}")


@player_router.get("/{player_id}/summary", response_model=PlayerSummaryResponse)
def get_player_summary_endpoint(player_id: str):
    try:
        return get_player_summary(player_id)
    except PlayerNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to load player summary for {player_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load player summary: {str(e)}")
