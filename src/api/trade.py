"""
Trade evaluation REST endpoint (issue #115).

evaluate_proposed_trade already exists as an MCP tool (src/api/mcp_server.py) behind
Google OAuth - not a fit for fantasy-web-ui's trade evaluation view calling straight
from a browser, so this is a separate, unauthenticated-at-this-layer REST route, same
pattern as src/api/players.py (protected later at ingress, not here).
"""
import logging
from typing import List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.api.mcp_server import evaluate_proposed_trade

logger = logging.getLogger("TradeRoutes")
trade_router = APIRouter()


class TradeEvaluateRequest(BaseModel):
    incoming_player_ids: List[str] = Field(..., description="player_ids joining this team's roster")
    outgoing_player_ids: List[str] = Field(..., description="player_ids leaving this team's roster")
    season: int = Field(..., description="Season year the trade would take effect in")
    week: int = Field(..., description="Week whose weekly_stats.vorp_score values price the trade")


class TradeEvaluateResponse(BaseModel):
    incoming_total: float
    outgoing_total: float
    delta_vorp: float
    missing_player_ids: List[str]


@trade_router.post("/evaluate", response_model=TradeEvaluateResponse)
def evaluate_trade(payload: TradeEvaluateRequest):
    try:
        return evaluate_proposed_trade(
            incoming_player_ids=payload.incoming_player_ids,
            outgoing_player_ids=payload.outgoing_player_ids,
            season=payload.season,
            week=payload.week,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to evaluate trade: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to evaluate trade: {str(e)}")
