"""Deterministic schedule-aware trade score: request and both-team evidence response."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from src.schemas.trade_context import RosterSnapshot, StrengthMetric

EspnPlayerId = Annotated[str, StringConstraints(pattern=r"^-?[1-9]\d*$")]
EspnTeamId = Annotated[str, StringConstraints(pattern=r"^[1-9]\d*$")]


class TradeScoreRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    league_id: str = Field(pattern=r"^[1-9]\d*$")
    season: int = Field(ge=2000, le=2100)
    week: int = Field(ge=1, le=18)
    horizon: int = Field(4, ge=1, le=18)
    our_team_id: EspnTeamId
    counterparty_team_id: EspnTeamId
    send_espn_ids: list[EspnPlayerId] = Field(min_length=1)
    receive_espn_ids: list[EspnPlayerId] = Field(min_length=1)


class ScoreTeam(BaseModel):
    team_id: str
    name: str


class TradedPlayer(BaseModel):
    espn_id: str
    player_id: str | None
    name: str | None
    position: str | None
    nfl_team: str | None


class RawVorp(BaseModel):
    week: int
    incoming_total: float
    outgoing_total: float
    delta_vorp: float
    missing_player_ids: list[str]
    unmapped_espn_ids: list[str]


class UnscoredWeek(BaseModel):
    week: int
    reasons: list[str]


class ScoreHorizon(BaseModel):
    start_week: int
    end_week: int
    scored_weeks: list[int]
    unscored_weeks: list[UnscoredWeek]
    complete_horizon: bool


class ScoreWeights(BaseModel):
    strength_weight: float
    z_clamp: float
    indirect_factor: float


class Starter(BaseModel):
    espn_id: str
    player_id: str
    name: str | None
    position: str
    vorp_score: float


class ExcludedPlayer(BaseModel):
    espn_id: str
    reason: Literal["out", "nfl_bye", "no_metrics", "unmapped", "unknown_position"]


class LineupWeek(BaseModel):
    week: int
    before: float
    after: float
    delta: float
    starters_before: list[Starter]
    starters_after: list[Starter]
    held_slots: list[str]
    excluded_players: list[ExcludedPlayer]


class TeamRosterImpact(BaseModel):
    total_delta: float | None
    roster_size_before: int
    roster_size_after: int
    roster_limit: int
    exceeds_roster_limit: bool
    weeks: list[LineupWeek]


class RosterImpact(BaseModel):
    us: TeamRosterImpact
    counterparty: TeamRosterImpact


class ScheduleWeek(BaseModel):
    week: int
    matchup_period: int | None
    head_to_head: bool
    our_status: str
    our_opponent_team_id: str | None
    our_opponent_z: float | None
    our_multiplier: float | None
    counterparty_status: str
    counterparty_opponent_team_id: str | None
    counterparty_opponent_z: float | None
    counterparty_weight: float | None


class ScheduleTerms(BaseModel):
    weeks: list[ScheduleWeek]


class ScoreTerms(BaseModel):
    our_lineup_gain: float | None
    counterparty_lineup_change: float | None
    our_weighted_gain: float | None
    our_schedule_adjustment: float | None
    counterparty_weighted_benefit: float | None
    final_score: float | None
    final_score_unavailable_reasons: list[str]


class ScoreCoverage(BaseModel):
    unmapped_traded_espn_ids: list[str]
    partial_roster_weeks: list[int]
    nfl_schedule_unavailable_weeks: list[int]


class TradeScore(BaseModel):
    scoring_version: Literal["trade-score-v1"]
    league_id: str
    season: int
    as_of_week: int
    our_team: ScoreTeam
    counterparty_team: ScoreTeam
    send: list[TradedPlayer]
    receive: list[TradedPlayer]
    raw_vorp: RawVorp
    horizon: ScoreHorizon
    weights: ScoreWeights
    roster_impact: RosterImpact
    schedule: ScheduleTerms
    score: ScoreTerms
    coverage: ScoreCoverage
    roster_snapshot: RosterSnapshot
    strength_metric: StrengthMetric
