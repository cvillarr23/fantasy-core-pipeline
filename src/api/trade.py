"""
Trade evaluation and staged-offer approval REST endpoints.

Unauthenticated-at-this-layer REST controller over src.services.trade_service, mirroring
src/api/players.py's pattern for splitting a REST surface out of the MCP-only layer.
"""

import logging
from typing import List

import requests
from espn_api.requests.espn_requests import (
    ESPNAccessDenied,
    ESPNInvalidLeague,
    ESPNUnknownError,
)

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response
from pydantic import BaseModel

from src.services.trade_service import evaluate_trade
from src.schemas.trade_context import LeagueTradeContext
from src.schemas.trade_score import TradeScore, TradeScoreRequest
from src.services.trade_context_service import (
    TradeContextUnavailable,
    get_league_trade_context,
)
from src.services.trade_score_service import TradeOwnershipError, score_trade

from src.schemas.trade import (
    OfferStatus,
    TradeApprovalRequest,
    TradeApprovalResponse,
    TradeOfferList,
)
from src.services import trade_offer_service
from src.services import trade_inbox_service
from src.api.routes import require_trigger_api_key
from src.schemas.trade_inbox import (
    CounterRequest,
    DecisionRequest,
    InboxFilter,
    IncomingTrade,
    IncomingTradeList,
    PublicationReceipt,
    TriageRequest,
    Visibility,
)

logger = logging.getLogger("TradeRoutes")
trade_router = APIRouter()


@trade_router.get("/incoming", response_model=IncomingTradeList)
def list_incoming_trades(
    status: InboxFilter = "live",
    visibility: Visibility = "visible",
    league_id: str | None = Query(None, pattern=r"^[1-9]\d*$"),
    season: int | None = Query(None, ge=2000, le=2100),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return trade_inbox_service.list_incoming(
        status, visibility, league_id, season, limit, offset
    )


def _incoming_error(error):
    if isinstance(error, trade_inbox_service.InboxNotFound):
        raise HTTPException(404, str(error)) from error
    if isinstance(error, trade_inbox_service.InboxConflict):
        raise HTTPException(409, str(error)) from error
    if isinstance(error, trade_inbox_service.PublishUnavailable):
        raise HTTPException(503, str(error)) from error
    if isinstance(error, ValueError):
        raise HTTPException(422, str(error)) from error
    raise error


@trade_router.post(
    "/incoming/{espn_transaction_id}/triage",
    response_model=IncomingTrade,
    dependencies=[Depends(require_trigger_api_key)],
)
def triage_incoming_trade(
    payload: TriageRequest,
    espn_transaction_id: str = Path(min_length=1, max_length=200),
):
    try:
        return trade_inbox_service.triage(
            espn_transaction_id, payload.state, payload.by, payload.snooze_until
        )
    except Exception as error:
        _incoming_error(error)


@trade_router.post(
    "/incoming/{espn_transaction_id}/decision",
    response_model=PublicationReceipt,
    status_code=202,
    dependencies=[Depends(require_trigger_api_key)],
)
def decide_incoming_trade(
    payload: DecisionRequest,
    response: Response,
    espn_transaction_id: str = Path(min_length=1, max_length=200),
):
    try:
        result = trade_inbox_service.decide(
            espn_transaction_id, payload.action, payload.approved_by, payload.comment
        )
    except Exception as error:
        _incoming_error(error)
    if result["duplicate"]:
        response.status_code = 200
    return result


@trade_router.post(
    "/incoming/{espn_transaction_id}/counter",
    response_model=PublicationReceipt,
    status_code=202,
    dependencies=[Depends(require_trigger_api_key)],
)
def counter_incoming_trade(
    payload: CounterRequest,
    response: Response,
    espn_transaction_id: str = Path(min_length=1, max_length=200),
):
    try:
        result = trade_inbox_service.counter(
            espn_transaction_id, payload.approved_by, payload.week
        )
    except Exception as error:
        _incoming_error(error)
    if result["duplicate"]:
        response.status_code = 200
    return result


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
            payload.incoming_player_ids,
            payload.outgoing_player_ids,
            payload.season,
            payload.week,
        )
    except ValueError as e:
        # Malformed proposal (duplicate/overlapping ids) is a caller-input error, not a
        # server fault - 422 matches routes.py's existing convention for this case.
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to evaluate trade: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to evaluate trade: {str(e)}"
        )


# Like /evaluate, these routes rely on the deployment's trust boundary. approved_by
# is an explicit audit label, not an authenticated identity.


@trade_router.get("/offers", response_model=TradeOfferList)
def list_trade_offers(
    status: OfferStatus = "awaiting_approval",
    league_id: str | None = None,
    season: int | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return trade_offer_service.list_offers(status, league_id, season, limit, offset)


@trade_router.post(
    "/offers/{offer_key}/approve", response_model=TradeApprovalResponse, status_code=202
)
def approve_trade_offer(
    payload: TradeApprovalRequest,
    response: Response,
    offer_key: str = Path(pattern=r"^[0-9a-f]{64}$"),
):
    try:
        result = trade_offer_service.approve_offer(offer_key, payload.approved_by)
    except trade_offer_service.OfferNotFound as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except trade_offer_service.OfferConflict as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except trade_offer_service.PublishUnavailable as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    if result["duplicate"]:
        response.status_code = 200
    return result


@trade_router.get("/context", response_model=LeagueTradeContext)
def league_trade_context(
    league_id: str = Query(pattern=r"^[1-9]\d*$"),
    season: int = Query(ge=2000, le=2100),
    week: int = Query(ge=1, le=18),
    horizon: int = Query(4, ge=1, le=18),
):
    try:
        return get_league_trade_context(league_id, season, week, horizon)
    except Exception as error:
        _raise_for_trade_read_error(error)


@trade_router.post("/score", response_model=TradeScore)
def score_trade_endpoint(payload: TradeScoreRequest):
    try:
        return score_trade(
            payload.league_id,
            payload.season,
            payload.week,
            payload.horizon,
            payload.our_team_id,
            payload.counterparty_team_id,
            payload.send_espn_ids,
            payload.receive_espn_ids,
        )
    except Exception as error:
        _raise_for_trade_read_error(error)


def _raise_for_trade_read_error(error: Exception):
    """Shared status mapping for routes built on the live ESPN trade context."""
    if isinstance(error, ValueError):
        raise HTTPException(422, str(error)) from error
    if isinstance(error, TradeOwnershipError):
        raise HTTPException(409, str(error)) from error
    if isinstance(error, TradeContextUnavailable):
        raise HTTPException(503, str(error)) from error
    if isinstance(error, requests.Timeout):
        raise HTTPException(504, "ESPN request timed out") from error
    if isinstance(error, (ESPNAccessDenied, ESPNInvalidLeague, ESPNUnknownError)):
        raise HTTPException(502, "Could not reach ESPN") from error
    raise error
