"""
Trade evaluation REST endpoint (fantasy-web-ui#1 trade evaluation view support).

Unauthenticated-at-this-layer REST controller over src.services.trade_service, mirroring
src/api/players.py's pattern for splitting a REST surface out of the MCP-only layer.
"""
import logging
from typing import List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.services.trade_service import evaluate_trade

logger = logging.getLogger("TradeRoutes")
trade_router = APIRouter()


class TradeEvaluationRequest(BaseModel):
    incoming_player_ids: List[str]
    outgoing_player_ids: List[str]
    season: int
    week: int


class TradeEvaluationResponse(BaseModel):
    incoming_total: float
    outgoing_total: float
    delta_vorp: float
    missing_player_ids: List[str]


@trade_router.post("/evaluate", response_model=TradeEvaluationResponse)
def evaluate_trade_endpoint(payload: TradeEvaluationRequest):
    try:
        return evaluate_trade(
            payload.incoming_player_ids, payload.outgoing_player_ids, payload.season, payload.week
        )
    except ValueError as e:
        # Malformed proposal (duplicate/overlapping ids) is a caller-input error, not a
        # server fault - 422 matches routes.py's existing convention for this case.
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to evaluate trade: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to evaluate trade: {str(e)}")
