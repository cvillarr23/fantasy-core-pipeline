"""Pydantic response models for the player news feed (src/api/news.py)."""
from datetime import datetime
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel

NewsSource = Literal["espn_news", "bluesky", "rotowire", "reddit"]


class NewsItem(BaseModel):
    id: int
    player_id: str
    source: NewsSource
    author: Optional[str] = None
    headline: str
    url: Optional[str] = None
    published_at: datetime
    # Reddit's score. raw_signals doesn't store it yet, so this is always null.
    score: Optional[int] = None
    # Other sources that reported the same story, set only when the story was collapsed.
    also_reported_by: List[NewsSource] = []


class NewsFeedResponse(BaseModel):
    items: List[NewsItem]


class RecentNewsResponse(BaseModel):
    has_news_24h: Dict[str, bool]
