"""Pydantic response models for the player summary REST endpoints (src/api/players.py)."""
from typing import List, Optional

from pydantic import BaseModel


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


class PlayerSearchResult(BaseModel):
    player_id: str
    # Nullable because espn_id is a DynastyProcess crosswalk that covers ~82% of players and
    # no K or D/ST at all. A null here means the player cannot be manually drafted through the
    # click bot, which the UI must surface rather than silently sending the wrong id space.
    espn_id: Optional[str] = None
    name: str
    position: str
    team: str
    injury_status: Optional[str] = None
    vorp_score: Optional[float] = None
    projected_points: Optional[float] = None
