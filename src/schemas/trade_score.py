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
    # Absent from stored evidence scored before the rest-of-season term (#328).
    ros_regular_weight: float | None = None
    playoff_week_weight: float | None = None
    # trade-score-v4+.
    questionable_availability: float | None = None
    doubtful_availability: float | None = None


class Starter(BaseModel):
    espn_id: str
    player_id: str
    name: str | None
    position: str
    vorp_score: float


class ExcludedPlayer(BaseModel):
    espn_id: str
    reason: Literal["out", "nfl_bye", "no_metrics", "unmapped", "unknown_position"]


class InjuryAdjustment(BaseModel):
    """A Questionable/Doubtful player's VORP scaled by factor for that week's solve."""
    espn_id: str
    factor: float
    reason: str


class LineupWeek(BaseModel):
    week: int
    before: float
    after: float
    delta: float
    starters_before: list[Starter]
    starters_after: list[Starter]
    held_slots: list[str]
    excluded_players: list[ExcludedPlayer]
    # trade-score-v4+; empty in older stored evidence.
    injury_adjustments: list[InjuryAdjustment] = []


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


class TeamByeCoverage(BaseModel):
    crunch_weeks_before: list[int]
    crunch_weeks_after: list[int]
    fixed: list[int]
    created: list[int]


class ByeCoverage(BaseModel):
    us: TeamByeCoverage
    counterparty: TeamByeCoverage


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


# Every scoring_version a stored evaluation may carry, oldest first; the last one is what the
# live scorer emits (trade_score_service.SCORING_VERSION). A score change appends one entry.
# v4 (#330 part 2): injury timelines shape each week's lineup inputs.
# v5 (#331): roster_spot joins final_score.
KNOWN_SCORING_VERSIONS = (
    "trade-score-v1", "trade-score-v2", "trade-score-v3", "trade-score-v4", "trade-score-v5",
)
ScoringVersion = Literal[KNOWN_SCORING_VERSIONS]


class ScoreComponent(BaseModel):
    """One term of final_score (or, with in_final_score false, a reported-only signal)."""
    value: float | None
    in_final_score: bool
    unavailable_reasons: list[str]


class RestOfSeasonWeek(BaseModel):
    week: int
    kind: Literal["regular", "playoff"]
    weight: float
    our_delta: float | None
    counterparty_delta: float | None


class RestOfSeason(BaseModel):
    """The R term: weeks after the horizon through the last fantasy playoff week."""
    weeks: list[RestOfSeasonWeek]
    scored_weeks: list[int]
    unscored_weeks: list[UnscoredWeek]
    complete: bool
    our_lineup_gain: float | None
    playoff_lineup_gain: float | None
    counterparty_lineup_change: float | None
    value: float | None
    unavailable_reasons: list[str]
    costs_playoff_value: bool


class SpotWeek(BaseModel):
    week: int
    delta: float


class RosterSpot(BaseModel):
    """Evidence for roster_spot_value: the free agent added or player dropped, and the
    unweighted per-week lineup deltas behind it."""

    kind: Literal["freed_spot", "forced_drop", "none"]
    player: TradedPlayer | None
    weeks: list[SpotWeek]
    total: float
    candidates_considered: int
    unavailable_reason: str | None


class ScoreTerms(BaseModel):
    our_lineup_gain: float | None
    counterparty_lineup_change: float | None
    our_weighted_gain: float | None
    our_schedule_adjustment: float | None
    counterparty_weighted_benefit: float | None
    # Null when rest-of-season weeks exist but none could be scored (unavailable, not 0).
    rest_of_season_value: float | None = None
    # Absent from stored v1/v2 evidence, which predates the roster-spot term.
    roster_spot_value: float | None = None
    final_score: float | None
    final_score_unavailable_reasons: list[str]
    # Keyed by component name; set from trade-score-v2 on, absent from stored v1 evidence.
    components: dict[str, ScoreComponent] | None = None


class PlayerInjury(BaseModel):
    """ESPN's live status plus the public injuries feed's expected return, for one traded
    player; return_date and expected_return_week are null when the feed has no date."""
    injury_status: str | None
    slot: str | None
    return_date: str | None
    expected_return_week: int | None
    injury_type: str | None
    reported_at: str | None
    # Every scored week he misses, horizon and rest of season.
    weeks_out: list[int]
    as_of_week_availability: float


class ScoreCoverage(BaseModel):
    unmapped_traded_espn_ids: list[str]
    partial_roster_weeks: list[int]
    nfl_schedule_unavailable_weeks: list[int]


class TradeScore(BaseModel):
    # Older versions stay accepted so stored worker evidence keeps reading.
    scoring_version: ScoringVersion
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
    bye_coverage: ByeCoverage
    # Traded players only, keyed by ESPN id (trade-score-v4+; absent from older evidence).
    injuries: dict[str, PlayerInjury] | None = None
    roster_spot: RosterSpot | None = None
    schedule: ScheduleTerms
    rest_of_season: RestOfSeason | None = None
    score: ScoreTerms
    coverage: ScoreCoverage
    roster_snapshot: RosterSnapshot
    strength_metric: StrengthMetric
