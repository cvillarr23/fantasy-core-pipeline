"""Player news feed REST endpoints: thin controller over src.services.news_service."""
import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from src.schemas.news import NewsFeedResponse, RecentNewsResponse
from src.services import news_service

logger = logging.getLogger("NewsRoutes")
news_router = APIRouter()

MAX_PLAYER_IDS = 50


def _parse_player_ids(player_ids: str) -> List[str]:
    ids = list(dict.fromkeys(p.strip() for p in player_ids.split(",") if p.strip()))
    if not ids:
        raise HTTPException(status_code=422, detail="player_ids must list at least one id")
    if len(ids) > MAX_PLAYER_IDS:
        raise HTTPException(status_code=422, detail=f"at most {MAX_PLAYER_IDS} player_ids")
    return ids


def _naive_utc(value: Optional[datetime]) -> Optional[datetime]:
    """raw_signals.published_at is a naive UTC timestamp."""
    if value is not None and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


@news_router.get("", response_model=NewsFeedResponse)
def get_news_endpoint(
    player_ids: str,
    since: Optional[datetime] = None,
    limit: int = Query(20, ge=1, le=100),
):
    ids = _parse_player_ids(player_ids)
    try:
        return {"items": news_service.get_news(ids, _naive_utc(since), limit)}
    except Exception as e:
        logger.error(f"Failed to load news: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load news: {str(e)}")


@news_router.get("/recent", response_model=RecentNewsResponse)
def get_recent_news_endpoint(player_ids: str):
    ids = _parse_player_ids(player_ids)
    try:
        return {"has_news_24h": news_service.has_recent_news(ids)}
    except Exception as e:
        logger.error(f"Failed to load recent news flags: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load recent news: {str(e)}")
