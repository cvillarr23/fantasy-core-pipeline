"""
Backtest results REST endpoints (CORE-19 web UI support).

Unauthenticated-at-this-layer REST controller over src.services.backtest_service,
mirroring src/api/trade.py's pattern - route handlers only parse params and map service
exceptions to HTTP status codes; src.services.backtest_service does the actual DB work.
This is view-only: it does not add any way to trigger a backtest run.
"""
import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException

from src.schemas.backtest import BacktestResultDetail, BacktestResultSummary
from src.services.backtest_service import (
    BacktestResultNotFoundError,
    get_backtest_result,
    list_backtest_results,
)

logger = logging.getLogger("BacktestRoutes")
backtest_router = APIRouter()


@backtest_router.get("/results", response_model=List[BacktestResultSummary])
def list_backtest_results_endpoint(season: Optional[int] = None):
    try:
        return list_backtest_results(season)
    except Exception as e:
        logger.error(f"Failed to list backtest results: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list backtest results: {str(e)}")


@backtest_router.get("/results/{season}", response_model=BacktestResultDetail)
def get_backtest_result_endpoint(season: int):
    try:
        return get_backtest_result(season)
    except BacktestResultNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to load backtest result for season {season}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load backtest result: {str(e)}")
