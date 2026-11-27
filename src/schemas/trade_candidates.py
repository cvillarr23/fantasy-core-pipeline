"""League-wide trade candidates served by GET /v1/trade/candidates."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from src.schemas.trade_score import AcceptanceEstimate, ByeCoverage, ScoreTeam

CandidateShape = Literal["1for1", "2for1", "1for2"]
CandidateStatus = Literal["proposable", "needs_drop", "one_sided"]
CandidateStatusFilter = Literal["proposable", "needs_drop", "one_sided", "all"]
CandidateSort = Literal[
    "final_score", "our_gain", "their_benefit", "rest_of_season", "expected_score", "acceptance",
]


class CandidatePlayer(BaseModel):
    espn_id: str
    player_id: str | None
    name: str | None
    position: str | None
    nfl_team: str | None
    vorp: float | None
    nfl_bye_week: int | None
    # ESPN's live status and src/injury_timeline's reading of it; absent from older runs.
    injury_status: str | None = None
    expected_return_week: int | None = None
    weeks_out_in_horizon: int = 0


class TradeCandidate(BaseModel):
    candidate_key: str
    counterparty: ScoreTeam
    shape: CandidateShape
    receive: list[CandidatePlayer]
    send: list[CandidatePlayer]
    final_score: float | None
    our_lineup_gain: float | None
    our_weighted_gain: float | None
    counterparty_lineup_change: float | None
    counterparty_weighted_benefit: float | None
    our_schedule_adjustment: float | None
    # R, the rest-of-season term in final_score: null when unavailable (not 0).
    rest_of_season_value: float | None
    playoff_lineup_gain: float | None
    # Our fantasy playoff weeks lose lineup value. Informational, never a gate.
    costs_playoff_value: bool
    rest_of_season_weeks: list[int]
    rest_of_season_scored_weeks: list[int]
    roster_spot_value: float | None
    scored_weeks: list[int]
    horizon_weeks: list[int]
    complete_horizon: bool
    bye_coverage: ByeCoverage
    # Null on rows stored before the acceptance estimate existed.
    acceptance_likelihood: float | None = None
    # final_score x acceptance_likelihood; null when either is.
    expected_score: float | None = None
    acceptance: AcceptanceEstimate | None = None
    status: CandidateStatus
    reasons: list[str]


class TeamCandidateCounts(BaseModel):
    team_id: str
    name: str
    proposable: int
    needs_drop: int
    one_sided: int


class TradeCandidateList(BaseModel):
    league_id: str
    season: int
    week: int
    horizon: int
    run_id: str
    computed_at: datetime
    total: int
    team_counts: list[TeamCandidateCounts]
    items: list[TradeCandidate]
