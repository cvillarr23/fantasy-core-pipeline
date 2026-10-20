"""League-wide trade candidates served by GET /v1/trade/candidates."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from src.schemas.trade_score import ByeCoverage, ScoreTeam

CandidateShape = Literal["1for1", "2for1", "1for2"]
CandidateStatus = Literal["proposable", "needs_drop", "one_sided"]
CandidateStatusFilter = Literal["proposable", "needs_drop", "one_sided", "all"]
CandidateSort = Literal["final_score", "our_gain", "their_benefit"]


class CandidatePlayer(BaseModel):
    espn_id: str
    player_id: str | None
    name: str | None
    position: str | None
    nfl_team: str | None
    vorp: float | None
    nfl_bye_week: int | None


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
    scored_weeks: list[int]
    horizon_weeks: list[int]
    complete_horizon: bool
    bye_coverage: ByeCoverage
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
