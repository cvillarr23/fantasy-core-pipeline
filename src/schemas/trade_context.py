"""League trade context: ESPN ownership, scoped metrics and fantasy schedule evidence."""

from typing import Literal
from pydantic import BaseModel


class PlayerWeekMetrics(BaseModel):
    vorp_score: float | None
    projected_points: float | None


class TradeContextPlayer(BaseModel):
    espn_id: str
    player_id: str | None
    name: str | None
    position: str | None
    nfl_team: str | None
    slot: str | None
    eligible_slots: list[str]
    injury_status: str | None
    metrics: dict[str, PlayerWeekMetrics]


class TeamStrength(BaseModel):
    points_per_week: float | None
    completed_matchups: int
    completed_scoring_weeks: int
    unavailable_history_periods: int
    z_score: float | None


class FantasyScheduleWeek(BaseModel):
    week: int
    matchup_period: int
    status: Literal["scheduled", "bye", "unavailable"]
    opponent_team_id: str | None
    opponent_name: str | None
    opponent_strength_z: float | None


class ScheduleCoverage(BaseModel):
    scheduled_weeks: int
    bye_weeks: int
    unavailable_weeks: int
    strength_known_weeks: int


class TradeContextTeam(BaseModel):
    team_id: str
    name: str
    roster: list[TradeContextPlayer]
    strength: TeamStrength
    schedule: list[FantasyScheduleWeek]
    schedule_coverage: ScheduleCoverage
    schedule_strength_z: float | None


class RosterSnapshot(BaseModel):
    kind: Literal["current"]
    source: str
    fetched_at: str
    observed_week: int
    matches_as_of_week: bool


class StrengthMetric(BaseModel):
    name: Literal["completed_fantasy_points_per_week_z"]
    source: str
    league_mean: float | None
    league_stddev: float | None
    teams_with_strength: int


class ContextCoverage(BaseModel):
    unmapped_espn_ids: list[str]
    missing_metric_count: int


class LeagueTradeContext(BaseModel):
    league_id: str
    season: int
    as_of_week: int
    start_week: int
    end_week: int
    regular_season_end_week: int
    lineup_slot_counts: dict[str, int]
    roster_snapshot: RosterSnapshot
    strength_metric: StrengthMetric
    coverage: ContextCoverage
    teams: dict[str, TradeContextTeam]
