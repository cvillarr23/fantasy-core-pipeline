import json
import os
from unittest.mock import MagicMock

from src.backtesting import report


def test_write_json_creates_file_with_summary(tmp_path):
    summary = {"lineup": {"our_lineup_points": 100.0}}
    path = report.write_json(summary, 2023, out_dir=str(tmp_path))
    assert os.path.exists(path)
    with open(path) as f:
        assert json.load(f) == summary


def test_write_csv_creates_file_with_weekly_rows(tmp_path):
    weekly = [
        {"week": 1, "our_points": 10.0, "optimal_points": 15.0, "naive_points": 8.0},
        {"week": 2, "our_points": 12.0, "optimal_points": 14.0, "naive_points": 9.0},
    ]
    path = report.write_csv(weekly, 2023, out_dir=str(tmp_path))
    with open(path) as f:
        lines = f.read().strip().split("\n")
    assert lines[0] == "week,our_points,optimal_points,naive_points"
    assert len(lines) == 3


def test_persist_backtest_results_inserts_row():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    summary = {
        "lineup": {"our_lineup_points": 100.0, "optimal_lineup_points": 120.0, "naive_baseline_points": 90.0},
        "waiver": {"our_waiver_points": 10.0, "no_moves_points": 8.0},
        "draft": {"our_draft_points": 200.0, "pure_adp_draft_points": 190.0},
    }
    report.persist_backtest_results(mock_conn, 2023, summary)
    args, _ = mock_cursor.execute.call_args
    params = args[1]
    assert params[0] == 2023
    assert params[1] == 100.0
    assert params[7] == 190.0
