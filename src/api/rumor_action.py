"""
Rumor-signal action trigger REST endpoint (#124). The daily CronJob invokes the
shared service directly via python -m; this is the HTTP entrypoint. Mirrors
src/api/trade.py's thin-controller-over-a-service pattern.
"""
import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException

from src.nfl_schedule import RegularSeasonOverError, WeekResolutionError, require_upcoming_week
from src.schemas.rumor_action import RumorActionTriggerRequest, RumorActionTriggerResponse
from src.services import rumor_action_service

logger = logging.getLogger("RumorActionRoutes")
rumor_action_router = APIRouter()


@rumor_action_router.post("/trigger/rumor-action", response_model=RumorActionTriggerResponse)
def trigger_rumor_action(payload: Optional[RumorActionTriggerRequest] = None):
    payload = payload or RumorActionTriggerRequest()
    team_id = payload.team_id or os.getenv("ESPN_TEAM_ID")
    if not team_id:
        raise HTTPException(status_code=422, detail="team_id is required (or set the ESPN_TEAM_ID env var).")
    season = payload.season or int(os.getenv("CURRENT_YEAR", datetime.now().year))
    try:
        # One attempt: an HTTP caller can retry or pass a week; it shouldn't wait out the
        # CronJob backoff, and must never get actions for a guessed week.
        week = payload.week or require_upcoming_week(season, attempts=1)
    except WeekResolutionError as e:
        logger.warning(f"Upcoming week unavailable for season={season}: {e}")
        raise HTTPException(status_code=503, detail="Current week is unavailable. Retry or pass week explicitly.")
    except RegularSeasonOverError as e:
        raise HTTPException(status_code=409, detail=str(e))

    try:
        return rumor_action_service.evaluate_and_publish_rumor_actions(team_id, season, week, source="http")
    except Exception as e:
        logger.error(f"Failed to evaluate/publish rumor actions: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to evaluate rumor actions: {str(e)}")
