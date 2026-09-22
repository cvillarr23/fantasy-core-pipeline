"""
Rumor-signal action trigger REST endpoint (#124), meant to be hit by a daily CronJob
(fantasy-browser-worker#81, not built in this repo) rather than a human - mirrors
src/api/trade.py's thin-controller-over-a-service pattern.
"""
import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.optimization.lineup_solver import resolve_default_week
from src.services import rumor_action_service

logger = logging.getLogger("RumorActionRoutes")
rumor_action_router = APIRouter()


class RumorActionTriggerRequest(BaseModel):
    team_id: Optional[str] = None
    season: Optional[int] = None
    week: Optional[int] = None


class RumorActionTriggerResponse(BaseModel):
    considered: int
    waiver_claims_published: list
    lineup_swaps_published: list
    watermark: int


@rumor_action_router.post("/trigger/rumor-action", response_model=RumorActionTriggerResponse)
def trigger_rumor_action(payload: Optional[RumorActionTriggerRequest] = None):
    payload = payload or RumorActionTriggerRequest()
    team_id = payload.team_id or os.getenv("ESPN_TEAM_ID")
    if not team_id:
        raise HTTPException(status_code=422, detail="team_id is required (or set the ESPN_TEAM_ID env var).")
    season = payload.season or int(os.getenv("CURRENT_YEAR", datetime.now().year))
    week = payload.week or resolve_default_week()

    try:
        return rumor_action_service.evaluate_and_publish_rumor_actions(team_id, season, week)
    except Exception as e:
        logger.error(f"Failed to evaluate/publish rumor actions: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to evaluate rumor actions: {str(e)}")
