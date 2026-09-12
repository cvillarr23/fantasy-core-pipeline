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
