"""Pydantic response models for the live game endpoints (src/api/live.py, the matchup feed and
the `live` block on matchup and summary players)."""
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel

GameStatus = Literal["not_started", "in_progress", "final"]


class LiveBlock(BaseModel):
    """One player's live snapshot, merged from `live:player` and the player's `live:game`.
    Every field is nullable; the whole block is null when no snapshot exists."""
    status: Optional[GameStatus] = None
    period: Optional[int] = None
    clock: Optional[str] = None
    home_abbr: Optional[str] = None
    home_score: Optional[int] = None
    away_abbr: Optional[str] = None
    away_score: Optional[int] = None
    possession: Optional[str] = None
    yard_line: Optional[str] = None
    red_zone: Optional[bool] = None
    stat_line: Optional[Dict[str, float]] = None
    live_points: Optional[float] = None
    updated_at: Optional[str] = None


class LivePlayersResponse(BaseModel):
    # Keyed by the requested player id; null when that player has no snapshot.
    players: Dict[str, Optional[LiveBlock]]


class PlayerLiveSummary(BaseModel):
    """The `live` field of a player summary (what the web UI's popover reads)."""
    live_points: Optional[float] = None
    projected_points: Optional[float] = None
    game_label: Optional[str] = None
    game_status: Optional[GameStatus] = None
    event_id: Optional[str] = None


class WeeklyLogEntry(BaseModel):
    week: int
    points: Optional[float] = None


class FeedItem(BaseModel):
    type: Literal["play", "news"]
    # Opaque; pass the last item's value as `before` for the next page.
    cursor: str
    # When core recorded the play (ESPN gives no wall time) or the news was published, ISO-8601 UTC.
    at: Optional[str] = None
    player_id: str
    espn_id: Optional[str] = None
    player_name: Optional[str] = None
    position: Optional[str] = None
    team: Optional[str] = None
    side: Literal["mine", "theirs"]
    # Play items.
    play_id: Optional[str] = None
    event_id: Optional[str] = None
    game_label: Optional[str] = None
    period: Optional[int] = None
    clock: Optional[str] = None
    text: Optional[str] = None
    scoring: Optional[bool] = None
    # Change in the player's live points from this play; null when it can't be told (see
    # src/ingestion/live_games.py, `attach_play_deltas`).
    fantasy_delta: Optional[float] = None
    # News items.
    news_id: Optional[int] = None
    source: Optional[str] = None
    headline: Optional[str] = None
    url: Optional[str] = None


class MatchupFeedResponse(BaseModel):
    items: List[FeedItem]
    # Pass as `before` for the next page; null when there are no older items.
    next_before: Optional[str] = None
