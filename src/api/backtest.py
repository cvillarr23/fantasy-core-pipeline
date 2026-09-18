"""
Backtest results REST endpoints (CORE-19 web UI support).

Plain read-only surface over the `backtest_results` table CORE-15's
`src/backtesting/season_replay.py` persists to - mirrors `src/api/players.py`'s
pattern of a separate unauthenticated-at-this-layer router, not the MCP/OAuth surface.
This is view-only: it does not add any way to trigger a backtest run.
"""
import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.database import get_db_cursor

logger = logging.getLogger("BacktestRoutes")
backtest_router = APIRouter()

_RESULT_COLUMNS = """
    season, run_at, our_lineup_points, optimal_lineup_points, naive_baseline_points,
    our_waiver_points, no_moves_points, our_draft_points, pure_adp_draft_points
"""


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


def _row_to_summary(row) -> dict:
    return {
        "season": row[0],
        "run_at": row[1].isoformat() if row[1] else None,
        "our_lineup_points": row[2],
        "optimal_lineup_points": row[3],
        "naive_baseline_points": row[4],
        "our_waiver_points": row[5],
        "no_moves_points": row[6],
        "our_draft_points": row[7],
        "pure_adp_draft_points": row[8],
    }


@backtest_router.get("/results", response_model=List[BacktestResultSummary])
def list_backtest_results(season: Optional[int] = None):
    try:
        with get_db_cursor() as cursor:
            if season is not None:
                cursor.execute(f"""
                    SELECT {_RESULT_COLUMNS}
                    FROM backtest_results
                    WHERE season = %s
                    ORDER BY run_at DESC;
                """, (season,))
            else:
                cursor.execute(f"""
                    SELECT {_RESULT_COLUMNS}
                    FROM backtest_results
                    ORDER BY run_at DESC;
                """)
            rows = cursor.fetchall()
    except Exception as e:
        logger.error(f"Failed to list backtest results: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to list backtest results: {str(e)}")

    return [_row_to_summary(row) for row in rows]


@backtest_router.get("/results/{season}", response_model=BacktestResultDetail)
def get_backtest_result(season: int):
    try:
        with get_db_cursor() as cursor:
            cursor.execute(f"""
                SELECT {_RESULT_COLUMNS}, summary_json
                FROM backtest_results
                WHERE season = %s
                ORDER BY run_at DESC
                LIMIT 1;
            """, (season,))
            row = cursor.fetchone()
    except Exception as e:
        logger.error(f"Failed to load backtest result for season {season}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load backtest result: {str(e)}")

    if row is None:
        raise HTTPException(status_code=404, detail=f"No backtest results for season {season}.")

    summary = _row_to_summary(row)
    summary["summary_json"] = row[9]
    return summary
