"""
Player summary REST endpoints (CORE-14 web UI support).

Plain JSON reads of player metadata + recent CORE-13 news, for fantasy-web-ui's
draft-day pick feed. The existing read surface for player data is two MCP tools
(get_league_roster_deficits, evaluate_proposed_trade) behind Google OAuth - not a fit
for a browser fetching a summary per pick, so this is a separate, unauthenticated-at-
this-layer REST router (see draft-day plan's OIDC/ingress follow-up for how it's
actually protected once deployed).
"""
import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.database import get_db_cursor

logger = logging.getLogger("PlayerRoutes")
player_router = APIRouter()

RECENT_NEWS_LIMIT = 5


class RecentNewsItem(BaseModel):
    source: str
    raw_text: str
    url: Optional[str] = None
    published_at: Optional[str] = None


class PlayerSummaryResponse(BaseModel):
    player_id: str
    name: str
    position: str
    team: str
    status: str
    injury_status: Optional[str] = None
    recent_news: List[RecentNewsItem]


def _player_row(cursor, player_id: str):
    cursor.execute("""
        SELECT player_id, name, position, team, status, injury_status
        FROM players
        WHERE player_id = %s;
    """, (player_id,))
    return cursor.fetchone()


def _recent_news(cursor, player_id: str, limit: int) -> list:
    cursor.execute("""
        SELECT source, raw_text, url, published_at
        FROM raw_signals
        WHERE player_id = %s
        ORDER BY published_at DESC NULLS LAST
        LIMIT %s;
    """, (player_id, limit))
    return [
        {"source": row[0], "raw_text": row[1], "url": row[2], "published_at": row[3].isoformat() if row[3] else None}
        for row in cursor.fetchall()
    ]


@player_router.get("/{player_id}/summary", response_model=PlayerSummaryResponse)
def get_player_summary(player_id: str):
    try:
        with get_db_cursor() as cursor:
            row = _player_row(cursor, player_id)
            if row is None:
                raise HTTPException(status_code=404, detail=f"Unknown player_id '{player_id}'.")
            recent_news = _recent_news(cursor, player_id, RECENT_NEWS_LIMIT)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to load player summary for {player_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load player summary: {str(e)}")

    return {
        "player_id": row[0],
        "name": row[1],
        "position": row[2],
        "team": row[3],
        "status": row[4],
        "injury_status": row[5],
        "recent_news": recent_news,
    }
