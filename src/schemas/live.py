"""Pydantic response models for the live game endpoints (src/api/live.py, the matchup feed and
the `live` block on matchup and summary players)."""
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel

GameStatus = Literal["not_started", "in_progress", "final"]


class ScoreBreakdownLine(BaseModel):
    """One stat's share of a player's live points (src/espn_scoring.py). `points` is ESPN's own
    figure, so the lines sum to `live_points` (within 0.05). A `stat_key` of `other` carries
    whatever ESPN scored that isn't itemized, with null `qty` and `unit`."""
    stat_key: str
    label: str
    # The raw count (98 for 98 rushing yards; 1 for a range stat like "7–13 points allowed").
    # Null when the snapshot predates quantities, and on the `other` line.
    qty: Optional[float] = None
    unit: Optional[str] = None
    # The league rule in plain English, e.g. "1 pt per 10 rushing yds".
    rule_label: Optional[str] = None
    points: float


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
    # The game's scheduled start, ISO-8601 UTC.
    kickoff: Optional[str] = None
    stat_line: Optional[Dict[str, float]] = None
    live_points: Optional[float] = None
    # Null when the player has no live points yet or the league's rules can't be read.
    breakdown: Optional[List[ScoreBreakdownLine]] = None
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
    # The same lines as the `live` block's breakdown.
    breakdown: Optional[List[ScoreBreakdownLine]] = None


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


class GameTeam(BaseModel):
    abbr: Optional[str] = None
    # Null before kickoff.
    score: Optional[int] = None


class LiveGame(BaseModel):
    """The poller's `live:game` snapshot."""
    event_id: str
    status: Optional[GameStatus] = None
    # ESPN's short status, e.g. "3rd 8:42" or "Final".
    detail: Optional[str] = None
    period: Optional[int] = None
    clock: Optional[str] = None
    home: GameTeam
    away: GameTeam
    possession: Optional[str] = None
    yard_line: Optional[str] = None
    red_zone: Optional[bool] = None
    kickoff: Optional[str] = None
    updated_at: Optional[str] = None
    # "TB @ ATL" before kickoff, "TB 14 – 17 ATL" once there is a score.
    game_label: Optional[str] = None


class GamePlayer(BaseModel):
    """A matchup player (either roster, bench included) whose NFL team plays in the game."""
    player_id: str
    espn_id: Optional[str] = None
    player_name: Optional[str] = None
    position: Optional[str] = None
    team: Optional[str] = None
    side: Literal["mine", "theirs"]
    # ESPN lineup slot name (`QB`, `RB/WR/TE`, `BE`, `IR`, ...).
    slot: Optional[str] = None
    # False for bench and IR, whose points don't count toward the matchup.
    starter: bool
    live_points: Optional[float] = None
    projected_points: Optional[float] = None
    stat_line: Optional[Dict[str, float]] = None
    breakdown: Optional[List[ScoreBreakdownLine]] = None


class GameDetailResponse(BaseModel):
    season: int
    week: int
    game: LiveGame
    # Sum of each side's starters' live points from this game; bench never counts. Null
    # before kickoff.
    mine_points: Optional[float] = None
    theirs_points: Optional[float] = None
    players: List[GamePlayer]
    # This game's plays in the matchup feed's item shape, newest first.
    plays: List[FeedItem]
