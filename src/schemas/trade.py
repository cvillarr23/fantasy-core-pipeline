"""ESPN-id trade offer review and approval contracts."""

from datetime import datetime
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator

OfferStatus = Literal[
    "awaiting_approval",
    "approval_pending",
    "approved",
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
    approval: dict | None = None


class TradeOfferList(BaseModel):
    offers: list[TradeOffer]
    limit: int
    offset: int
