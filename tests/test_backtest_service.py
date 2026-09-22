from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.services import backtest_service


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


def test_list_backtest_results_orders_most_recent_first():
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        (2025, datetime(2026, 1, 5, 9, 0, 0), 1500.0, 1600.0, 1400.0, 200.0, 150.0, 1800.0, 1700.0),
        (2024, datetime(2025, 1, 6, 9, 0, 0), 1450.0, 1550.0, 1350.0, 190.0, 140.0, 1750.0, 1650.0),
    ]

    with patch("src.services.backtest_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        results = backtest_service.list_backtest_results()

    assert len(results) == 2
    assert results[0]["season"] == 2025
    assert results[0]["run_at"] == "2026-01-05T09:00:00"
    assert results[0]["pure_adp_draft_points"] == 1700.0


def test_list_backtest_results_filters_by_season():
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        (2025, datetime(2026, 1, 5, 9, 0, 0), 1500.0, 1600.0, 1400.0, 200.0, 150.0, 1800.0, 1700.0),
    ]

    with patch("src.services.backtest_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        results = backtest_service.list_backtest_results(season=2025)

    assert len(results) == 1
    params = cursor.execute.call_args[0][1]
    assert params == (2025,)


def test_get_backtest_result_includes_summary_json():
    cursor = MagicMock()
    summary_json = {
        "lineup": {"our_lineup_points": 1500.0, "optimal_lineup_points": 1600.0, "naive_baseline_points": 1400.0},
        "waiver": {"our_waiver_points": 200.0, "no_moves_points": 150.0},
        "draft": {"our_draft_points": 1800.0, "pure_adp_draft_points": 1700.0},
        "weekly": [{"our_points": 100.0, "optimal_points": 110.0, "naive_points": 90.0, "week": 1}],
    }
    cursor.fetchone.return_value = (
        2025, datetime(2026, 1, 5, 9, 0, 0), 1500.0, 1600.0, 1400.0, 200.0, 150.0, 1800.0, 1700.0, summary_json,
    )

    with patch("src.services.backtest_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = backtest_service.get_backtest_result(2025)

    assert result["season"] == 2025
    assert result["summary_json"] == summary_json


def test_get_backtest_result_raises_not_found_for_no_rows():
    cursor = MagicMock()
    cursor.fetchone.return_value = None

    with patch("src.services.backtest_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        with pytest.raises(backtest_service.BacktestResultNotFoundError):
            backtest_service.get_backtest_result(2025)
