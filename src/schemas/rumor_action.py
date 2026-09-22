"""Pydantic request/response models for the rumor-action trigger endpoint (src/api/rumor_action.py)."""
from typing import Optional

from pydantic import BaseModel


class RumorActionTriggerRequest(BaseModel):
    team_id: Optional[str] = None
    season: Optional[int] = None
    week: Optional[int] = None


class RumorActionTriggerResponse(BaseModel):
    considered: int
    waiver_claims_published: list
    lineup_swaps_published: list
    watermark: int
