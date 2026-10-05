"""
Chat query REST endpoint (CORE-153, fantasy-web-ui#97's backend).

Unauthenticated-at-this-layer REST controller over src.services.chat_service, mirroring
src/api/trade.py's pattern.
"""

import logging

from fastapi import APIRouter, HTTPException

from src.llm_client import LlmProviderChainExhaustedError
from src.schemas.chat import ChatQueryRequest, ChatQueryResponse
from src.services.chat_service import answer_chat_query

logger = logging.getLogger("ChatRoutes")
chat_router = APIRouter()


@chat_router.post("/query", response_model=ChatQueryResponse)
def query_endpoint(payload: ChatQueryRequest):
    try:
        return answer_chat_query(
            payload.question,
            payload.season,
            payload.week,
            payload.team_id,
            payload.trade_proposal,
            session_id=payload.session_id,
        )
    except ValueError as e:
        # Malformed trade proposal (duplicate/overlapping ids) is a caller-input error, not a
        # server fault - 422 matches trade.py's existing convention for this case.
        raise HTTPException(status_code=422, detail=str(e))
    except LlmProviderChainExhaustedError as e:
        logger.error(f"LLM provider chain exhausted answering chat query: {e}")
        raise HTTPException(
            status_code=503,
            detail="No LLM provider is currently available. Try again shortly.",
        )
    except Exception as e:
        logger.error(f"Failed to answer chat query: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to answer chat query: {str(e)}"
        )
