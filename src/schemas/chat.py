"""Pydantic request/response models for the chat query endpoint (src/api/chat.py)."""
from typing import List, Optional

from pydantic import BaseModel


class TradeProposal(BaseModel):
    incoming_player_ids: List[str]
    outgoing_player_ids: List[str]


class ChatQueryRequest(BaseModel):
    session_id: Optional[str] = None
    question: str
    season: int
    week: int
    # Whose perspective the model answers from -- nothing in this codebase has a fixed "my
    # team" concept, every other endpoint takes team_id explicitly (see lineup_solver.py,
    # draft_matrix.py, draft.py).
    team_id: str
    trade_proposal: Optional[TradeProposal] = None


class ChatGrounding(BaseModel):
    team_roster: dict
    roster_deficits: dict
    trade_evaluation: Optional[dict] = None


class ChatQueryResponse(BaseModel):
    session_id: Optional[str] = None
    answer: str
    provider: str
    model: str
    latency_ms: int
    grounding: ChatGrounding
