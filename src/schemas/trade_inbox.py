"""Incoming inbox entries and human-authorized publication receipts."""

from datetime import datetime
from typing import Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    field_validator,
    model_validator,
)

from src.schemas.trade import OfferPlayer
from src.schemas.trade_score import TradeScore, TradeScoreRequest

InboxStatus = Literal[
    "open",
    "unparsed",
    "responding",
    "accepted_pending_processing",
    "accepted",
    "declined",
    "countered",
    "vetoed",
    "expired",
    "withdrawn",
    "closed_unknown",
]
InboxFilter = Literal[
    "live",
    "all",
    "open",
    "unparsed",
    "responding",
    "accepted_pending_processing",
    "accepted",
    "declined",
    "countered",
    "vetoed",
    "expired",
    "withdrawn",
    "closed_unknown",
]
Visibility = Literal["visible", "hidden", "all"]
TriageState = Literal["new", "seen", "snoozed", "dismissed"]


class TriageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    state: TriageState
    by: str = Field(min_length=1, max_length=200)
    snooze_until: AwareDatetime | None = None

    @field_validator("by")
    @classmethod
    def nonblank(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("by must not be blank")
        return value

    @model_validator(mode="after")
    def snooze_required(self):
        if self.state == "snoozed" and self.snooze_until is None:
            raise ValueError("snoozed requires snooze_until")
        if self.state != "snoozed" and self.snooze_until is not None:
            raise ValueError("snooze_until is only valid for snoozed")
        return self


class HumanRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    approved_by: str = Field(
        min_length=1,
        max_length=200,
        description="Required human approver audit label; the API key authenticates the caller, not this identity.",
    )

    @field_validator("approved_by")
    @classmethod
    def nonblank(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("approved_by must not be blank")
        return value


class ReviewedRequest(HumanRequest):
    # Required: without it, a refresh between the human's read and the locked publish
    # could apply their approval to an offer they never saw.
    review_token: str = Field(
        pattern=r"^[0-9a-f]{64}$",
        description="`review_token` from the GET /v1/trade/incoming row the human reviewed.",
    )


class DecisionRequest(ReviewedRequest):
    action: Literal["accept", "decline"]
    comment: str | None = Field(None, max_length=2000)


class RetryRequest(ReviewedRequest):
    of_claim_id: int = Field(
        ge=1, description="The worker trade_response row to retry, from `response.id`."
    )
    verified_not_answered: bool = Field(
        False,
        description="Required for an unknown outcome: a human checked ESPN and the offer wasn't answered.",
    )


class CounterRequest(ReviewedRequest):
    week: int = Field(ge=1, le=18)


class InboxScorePlayer(BaseModel):
    model_config = ConfigDict(strict=True)
    player_id: str | None
    name: str | None


class InboxScore(BaseModel):
    model_config = ConfigDict(strict=True, allow_inf_nan=False)
    state: Literal[
        "scored", "incomplete", "unavailable", "unscorable", "stale_ownership"
    ]
    scoring_version: Literal["trade-score-v1"] | None
    as_of_week: int
    final_score: StrictFloat | StrictInt | None
    complete_horizon: bool | None
    unavailable_reasons: list[str]
    raw_delta_vorp: StrictFloat | StrictInt | None
    exceeds_roster_limit: bool | None
    unmapped_espn_ids: list[str]
    partial_reasons: list[str]
    players: dict[str, InboxScorePlayer]
    scored_fingerprint: str
    scored_at: AwareDatetime
    retry: bool
    stale: bool
    error: str | None


class InboxScoreRequest(TradeScoreRequest):
    # The worker stores the request exactly as sent and omits horizon, so null means the
    # scorer used its default, not that a value was sent.
    horizon: int | None = Field(None, ge=1, le=18)


class IncomingEvaluation(BaseModel):
    """The worker's full score evidence for an incoming offer: the exact POST
    /v1/trade/score request and its unmodified response.

    Display-only. It is exposed only while it describes the same offer version, refresh
    and score as `score`, which alone drives ranking, mappings and publication checks.
    """

    version: Literal["incoming-trade-evaluation-v1"]
    evaluated_at: AwareDatetime = Field(description="Equals `score.scored_at`.")
    scored_fingerprint: str = Field(
        pattern=r"^[0-9a-f]{64}$",
        description="Equals the offer's `fingerprint` and `score.scored_fingerprint`.",
    )
    request: InboxScoreRequest
    score: TradeScore


class ShadowThresholds(BaseModel):
    model_config = ConfigDict(strict=True, allow_inf_nan=False)
    accept_min: StrictFloat | StrictInt | None
    decline_max: StrictFloat | StrictInt | None


class ShadowPolicy(BaseModel):
    would: Literal["accept", "decline", "none"]
    reason: str
    thresholds: ShadowThresholds
    policy_version: Literal["shadow-v1"]


class InboxTriage(BaseModel):
    state: TriageState
    by: str | None = None
    at: datetime | None = None
    snooze_until: datetime | None = None


class PublicationReceipt(BaseModel):
    espn_transaction_id: str
    status: Literal["queued"]
    kind: Literal["decision", "counter"]
    tracking_id: str
    approved_by: str
    approved_at: datetime
    duplicate: bool = False
    decision_key: str | None = None
    action: Literal["accept", "decline"] | None = None
    retry_of: int | None = None


class InboxPublication(BaseModel):
    kind: Literal["decision", "counter"]
    status: Literal["pending", "queued", "failed_before_publish"]
    tracking_id: str
    approved_by: str
    approved_at: datetime
    fingerprint: str
    review_token: str | None = None
    decision_key: str | None = None
    action: Literal["accept", "decline"] | None = None
    publish_error: str | None = None
    retry_of: int | None = None
    verified_not_answered: bool | None = None


class InboxResponse(BaseModel):
    """The newest worker response row that blocks the offer: what a retry names."""

    id: int
    status: Literal["responding", "responded", "response_unknown", "rejected"]
    action: str | None = None
    decision_key: str | None = None


ThreadState = Literal[
    "awaiting_us", "awaiting_them", "agreed", "rejected", "expired", "abandoned"
]


class StoredCounter(BaseModel):
    """The worker's `our_counter`: our counter to an incoming offer, as ESPN showed it."""

    espn_transaction_id: str = Field(min_length=1)
    status: InboxStatus
    send_espn_ids: list[str] = Field(default_factory=list)
    receive_espn_ids: list[str] = Field(default_factory=list)
    player_names: dict[str, str | None] = Field(default_factory=dict)
    proposed_date: StrictInt | str | None = None
    expiration_date: StrictInt | str | None = None
    missed_reads: StrictInt = 0
    exit_reason: str | None = None
    seen_at: str | None = None


class OurCounter(BaseModel):
    espn_transaction_id: str
    status: InboxStatus
    send_players: list[OfferPlayer]
    receive_players: list[OfferPlayer]
    proposed_date: datetime | None = None
    expiration_date: datetime | None = None
    missed_reads: int = 0
    exit_reason: str | None = None
    seen_at: datetime | None = None


class ThreadTurn(BaseModel):
    side: Literal["theirs", "ours"]
    espn_transaction_id: str
    status: str
    proposing_team_id: str | None = None
    counters: str | None = Field(
        None, description="On our turns, the offer this counter answered."
    )
    comment: str | None = None
    send_players: list[OfferPlayer]
    receive_players: list[OfferPlayer]
    proposed_date: datetime | None = None
    expiration_date: datetime | None = None
    exit_reason: str | None = None


class NegotiationThread(BaseModel):
    thread_root_id: str
    state: ThreadState | None
    our_counters: int = Field(
        description="Counters from us that ESPN has shown; staged-only counters don't count."
    )
    counter_limit: int = Field(
        description="Counters the worker sends per thread before refusing another."
    )
    turns: list[ThreadTurn]


class IncomingTrade(BaseModel):
    id: int
    status: str
    league_id: str | None
    season: int | None
    executed_at: datetime
    espn_transaction_id: str
    related_transaction_id: str | None = None
    thread_root_id: str | None = None
    proposing_team_id: str | None = None
    our_team_id: str | None = None
    espn_type: str | None = None
    espn_status: str | None = None
    send_espn_ids: list[str] = Field(default_factory=list)
    receive_espn_ids: list[str] = Field(default_factory=list)
    send_players: list[OfferPlayer]
    receive_players: list[OfferPlayer]
    other_items: list[dict] = Field(default_factory=list)
    player_names: dict[str, str | None] = Field(default_factory=dict)
    proposed_date: datetime | None = None
    expiration_date: datetime | None = None
    comment: str | None = None
    fingerprint: str | None = None
    review_token: str = Field(
        description="Send this back on decision, counter and retry; it changes when the offer does."
    )
    parse_issues: list[str] = Field(default_factory=list)
    triage: InboxTriage | None = None
    decision: dict | None = None
    score: InboxScore | None = None
    evaluation: IncomingEvaluation | None = None
    shadow_policy: ShadowPolicy | None = None
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    missed_reads: int = 0
    exit_reason: str | None = None
    publications: list[InboxPublication] = Field(default_factory=list)
    response: InboxResponse | None = None
    our_counter: OurCounter | None = None


class IncomingTradeList(BaseModel):
    offers: list[IncomingTrade]
    limit: int
    offset: int
