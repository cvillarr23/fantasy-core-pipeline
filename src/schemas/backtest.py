"""Pydantic response models for the backtest results REST endpoints (src/api/backtest.py)."""
from typing import Optional

from pydantic import BaseModel


class BacktestResultSummary(BaseModel):
    season: int
    run_at: Optional[str] = None
    our_lineup_points: Optional[float] = None
    optimal_lineup_points: Optional[float] = None
    naive_baseline_points: Optional[float] = None
    our_waiver_points: Optional[float] = None
    no_moves_points: Optional[float] = None
    our_draft_points: Optional[float] = None
    pure_adp_draft_points: Optional[float] = None


class BacktestResultDetail(BacktestResultSummary):
    summary_json: dict
