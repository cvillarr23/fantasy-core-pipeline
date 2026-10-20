from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from src.services import trade_service


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


def test_evaluate_trade_computes_strict_delta():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("p1", 10.0), ("p2", 4.0), ("p3", 6.0)]

    with patch("src.services.trade_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = trade_service.evaluate_trade(
            incoming_player_ids=["p1"],
            outgoing_player_ids=["p2", "p3"],
            season=2026,
            week=3,
        )

    assert result["incoming_total"] == 10.0
    assert result["outgoing_total"] == 10.0
    assert result["delta_vorp"] == 0.0
    assert result["missing_player_ids"] == []


def test_evaluate_trade_reports_missing_players_instead_of_scoring_zero():
    cursor = MagicMock()
    # p2 has no weekly_stats row for this season/week (bye week, unsynced, or bad ID).
    cursor.fetchall.return_value = [("p1", 10.0)]

    with patch("src.services.trade_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = trade_service.evaluate_trade(
            incoming_player_ids=["p1"],
            outgoing_player_ids=["p2"],
            season=2026,
            week=3,
        )

    assert result["incoming_total"] == 10.0
    assert result["outgoing_total"] == 0.0
    assert result["delta_vorp"] == 10.0
    assert result["missing_player_ids"] == ["p2"]


def test_evaluate_trade_rejects_duplicate_incoming_ids():
    with pytest.raises(ValueError, match="duplicate"):
        trade_service.evaluate_trade(
            incoming_player_ids=["p1", "p1"],
            outgoing_player_ids=["p2"],
            season=2026,
            week=3,
        )


def test_evaluate_trade_rejects_duplicate_outgoing_ids():
    with pytest.raises(ValueError, match="duplicate"):
        trade_service.evaluate_trade(
            incoming_player_ids=["p1"],
            outgoing_player_ids=["p2", "p2"],
            season=2026,
            week=3,
        )


def test_evaluate_trade_rejects_player_on_both_sides():
    with pytest.raises(ValueError, match="both sides"):
        trade_service.evaluate_trade(
            incoming_player_ids=["p1", "p2"],
            outgoing_player_ids=["p2", "p3"],
            season=2026,
            week=3,
        )


def test_load_vorp_by_player_reads_every_id_in_one_query():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("p1", 10.0), ("p2", -1.5)]

    with patch("src.services.trade_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = trade_service.load_vorp_by_player(2026, 3, ("p1", "p2", "p3"))

    assert result == {"p1": 10.0, "p2": -1.5}
    cursor.execute.assert_called_once()
    assert cursor.execute.call_args.args[1] == (2026, 3, ["p1", "p2", "p3"])


def test_summarize_raw_vorp_matches_evaluate_trade():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("p1", 10.0), ("p3", 6.0)]
    with patch("src.services.trade_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        evaluated = trade_service.evaluate_trade(["p1"], ["p2", "p3"], 2026, 3)

    summarized = trade_service.summarize_raw_vorp({"p1": 10.0, "p3": 6.0, "p9": 1.0}, ["p1"], ["p2", "p3"])
    assert summarized == evaluated == {
        "incoming_total": 10.0,
        "outgoing_total": 6.0,
        "delta_vorp": 4.0,
        "missing_player_ids": ["p2"],
    }
