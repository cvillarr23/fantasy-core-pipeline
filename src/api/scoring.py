"""League scoring rules REST endpoint (the web UI's Scoring guide): thin controller over
src.services.scoring_service."""
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException

from src.schemas.scoring import ScoringRulesResponse
from src.services import scoring_service

logger = logging.getLogger("ScoringRoutes")
scoring_router = APIRouter()


@scoring_router.get("/rules", response_model=ScoringRulesResponse)
def get_scoring_rules_endpoint(season: Optional[int] = None):
    """The league's rules in plain English. Without `season`, the latest synced season."""
    try:
        return scoring_service.get_scoring_rules(season)
    except scoring_service.ScoringRulesNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to load scoring rules for season={season}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load scoring rules: {str(e)}")
