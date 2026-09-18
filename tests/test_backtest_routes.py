from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


def test_list_backtest_results_success(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (2025, datetime(2026, 1, 5, 9, 0, 0), 1500.0, 1600.0, 1400.0, 200.0, 150.0, 1800.0, 1700.0),
        (2024, datetime(2025, 1, 6, 9, 0, 0), 1450.0, 1550.0, 1350.0, 190.0, 140.0, 1750.0, 1650.0),
    ]

    with patch("src.api.backtest.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/backtest/results")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    assert body[0]["season"] == 2025
    assert body[0]["run_at"] == "2026-01-05T09:00:00"
    assert body[0]["our_lineup_points"] == 1500.0
    assert body[0]["pure_adp_draft_points"] == 1700.0


def test_list_backtest_results_filters_by_season(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        (2025, datetime(2026, 1, 5, 9, 0, 0), 1500.0, 1600.0, 1400.0, 200.0, 150.0, 1800.0, 1700.0),
    ]

    with patch("src.api.backtest.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/backtest/results?season=2025")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["season"] == 2025
    params = mock_cursor.execute.call_args[0][1]
    assert params == (2025,)


def test_list_backtest_results_db_failure(client):
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.api.backtest.get_db_cursor", _broken_cursor):
        response = client.get("/v1/backtest/results")

    assert response.status_code == 500


def test_get_backtest_result_success(client):
    mock_cursor = MagicMock()
    summary_json = {
        "lineup": {"our_lineup_points": 1500.0, "optimal_lineup_points": 1600.0, "naive_baseline_points": 1400.0},
        "waiver": {"our_waiver_points": 200.0, "no_moves_points": 150.0},
        "draft": {"our_draft_points": 1800.0, "pure_adp_draft_points": 1700.0},
        "weekly": [{"our_points": 100.0, "optimal_points": 110.0, "naive_points": 90.0, "week": 1}],
    }
    mock_cursor.fetchone.return_value = (
        2025, datetime(2026, 1, 5, 9, 0, 0), 1500.0, 1600.0, 1400.0, 200.0, 150.0, 1800.0, 1700.0, summary_json,
    )

    with patch("src.api.backtest.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/backtest/results/2025")

    assert response.status_code == 200
    body = response.json()
    assert body["season"] == 2025
    assert body["summary_json"] == summary_json


def test_get_backtest_result_no_rows_returns_404(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    with patch("src.api.backtest.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/backtest/results/2025")

    assert response.status_code == 404


def test_get_backtest_result_db_failure(client):
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.api.backtest.get_db_cursor", _broken_cursor):
        response = client.get("/v1/backtest/results/2025")

    assert response.status_code == 500
