"""ESPN-id trade offer review and approval contracts."""

from datetime import datetime
from typing import Literal
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from src.schemas.trade_score import TradeScore, TradeScoreRequest

OfferStatus = Literal[
    "awaiting_approval",
    "approval_pending",
    "approved",
    "superseded",
    "submit_blocked",
    "dry_run_filled",
    "submitting",
    "submitted",
    "failure",
]


class TradeApprovalRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    approved_by: str = Field(
        min_length=1,
        max_length=200,
        description="Self-reported approver; trade routes have no application-layer authentication.",
    )

    # TODO: Replace fallback once fantasy-browser-worker#124 captures the ESPN limit.
    message: str | None = Field(None, min_length=1, max_length=1000)

    @field_validator("message")
    @classmethod
    def nonblank_message(cls, value):
        if value is not None and not value.strip():
            raise ValueError("message must not be blank")
        return value

    @field_validator("approved_by")
    @classmethod
    def nonempty_approver(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("approved_by must not be blank")
        return value


class TradeApprovalResponse(BaseModel):
    offer_key: str
    status: Literal["approved"]
    tracking_id: str
    approved_by: str
    approved_at: datetime
    duplicate: bool = False


class OfferPlayer(BaseModel):
    espn_id: str
    name: str | None


class ProposalTarget(BaseModel):
    team_id: str
    position: str
    deficit: int
    severity: float


class ProposalRanking(BaseModel):
    basis: Literal["final_score", "our_lineup_gain"]
    value_per_scored_week: float
    rank: int
    packages_proposed: int
    packages_scored: int
    packages_eligible: int


class PitchEvidence(BaseModel):
    id: str
    path: str
    text: str


class ProposalPitch(BaseModel):
    message: str
    evidence: list[PitchEvidence]
    cited_evidence_ids: list[str]


class ProposalEvaluation(BaseModel):
    """Score evidence the worker stored with a generated proposal. `score` is the verbatim
    POST /v1/trade/score response, and each pitch evidence `path` is a JSON Pointer into it.
    """

    version: Literal["proposal-evaluation-v1"]
    evaluated_at: AwareDatetime
    request: TradeScoreRequest
    score: TradeScore
    target: ProposalTarget
    ranking: ProposalRanking
    pitch: ProposalPitch


class TradeOffer(BaseModel):
    id: int
    status: str
    league_id: str | None
    season: int | None
    executed_at: datetime
    offer_key: str | None = None
    source: str | None = None
    from_team_id: str | None = None
    to_team_id: str | None = None
    incoming_player_ids: list[str] = Field(default_factory=list)
    outgoing_player_ids: list[str] = Field(default_factory=list)
    incoming_players: list[OfferPlayer]
    outgoing_players: list[OfferPlayer]
    message: str | None = None
    drafted_message: str | None = None
    message_edited: bool = False
    # Set on a counter: the incoming ESPN offer it answers (part of offer_key) and that offer's
    # thread root (tracing only).
    related_transaction_id: str | None = None
    thread_root_id: str | None = None
    approval: dict | None = None
    # Advisory only: it is not part of offer_key and is never sent on approval.
    evaluation: ProposalEvaluation | None = None


class TradeOfferList(BaseModel):
    offers: list[TradeOffer]
    limit: int
    offset: int
