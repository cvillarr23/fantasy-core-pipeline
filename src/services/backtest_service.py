"""
Backtest results read logic, split out of src/api/backtest.py so that router stays a thin
HTTP controller (parsing, status codes) over this module doing the actual DB work - mirrors
src/services/roster_service.py's split.
"""
from typing import Optional

from src.database import get_db_cursor

_RESULT_COLUMNS = """
    season, run_at, our_lineup_points, optimal_lineup_points, naive_baseline_points,
    our_waiver_points, no_moves_points, our_draft_points, pure_adp_draft_points
"""


class BacktestResultNotFoundError(Exception):
    """No backtest_results row exists for the requested season."""


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


def list_backtest_results(season: Optional[int] = None) -> list:
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

    return [_row_to_summary(row) for row in rows]


def get_backtest_result(season: int) -> dict:
    with get_db_cursor() as cursor:
        cursor.execute(f"""
            SELECT {_RESULT_COLUMNS}, summary_json
            FROM backtest_results
            WHERE season = %s
            ORDER BY run_at DESC
            LIMIT 1;
        """, (season,))
        row = cursor.fetchone()

    if row is None:
        raise BacktestResultNotFoundError(f"No backtest results for season {season}.")

    summary = _row_to_summary(row)
    summary["summary_json"] = row[9]
    return summary
