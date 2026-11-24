"""Pydantic response models for the player summary REST endpoints (src/api/players.py)."""
from typing import List, Optional

from pydantic import BaseModel

from src.schemas.espn_ownership import EspnOwnership
from src.schemas.live import PlayerLiveSummary, WeeklyLogEntry


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
    # Same nullability and meaning as PlayerSearchResult.espn_id.
    espn_id: Optional[str] = None
    recent_news: List[RecentNewsItem]
    espn: Optional[EspnOwnership] = None
    # The poller's snapshot for this week's game; null for a player outside the live matchup.
    live: Optional[PlayerLiveSummary] = None
    # Mean actual points over the player's played weeks of their latest season; null with none.
    season_avg: Optional[float] = None
    # Last 4 played weeks of that season, oldest first.
    weekly_log: List[WeeklyLogEntry] = []


class PlayerSearchResult(BaseModel):
    player_id: str
    # Nullable because not every player has an espn_id resolved: the DynastyProcess crosswalk
    # covers ~82% of players, sync_skill_position_crosswalk backfills most other QB/RB/WR/TE,
    # and sync_kicker_dst fills the K and D/ST it can match on ESPN (a D/ST's id is
    # negative, e.g. Eagles -16021).
    # A null here means the player cannot be manually drafted through the click bot, which
    # the UI must surface rather than silently sending the wrong id space.
    espn_id: Optional[str] = None
    name: str
    position: str
    team: str
    injury_status: Optional[str] = None
    vorp_score: Optional[float] = None
    projected_points: Optional[float] = None
    espn: Optional[EspnOwnership] = None
