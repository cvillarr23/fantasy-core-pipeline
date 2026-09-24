from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from src.services import waiver_service


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


def _players_rows(*rows):
    """rows: (player_id, espn_id, name, team, injury_status)"""
    return list(rows)


def test_compute_waiver_recommendations_sorted_by_vorp_delta_desc(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.return_value = _players_rows(
        ("rb_fa", "101", "FA RB", "SEA", None),
        ("rb_bench", "102", "Bench RB", "NYJ", "Questionable"),
        ("wr_fa", "201", "FA WR", "DAL", None),
        ("wr_bench", "202", "Bench WR", "MIA", None),
    )

    monkeypatch.setattr(
        waiver_service.waiver_strategy,
        "load_bench_by_position",
        lambda cur, team_id, season, week: {"placeholder": True},
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy,
        "load_best_free_agent_by_position",
        lambda cur, season, week: {"placeholder": True},
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy,
        "propose_waiver_moves",
        lambda bench, free_agents, margin: [
            {
                "position": "RB",
                "add_player_id": "rb_fa",
                "drop_player_id": "rb_bench",
                "add_vorp_score": 5.0,
                "drop_vorp_score": 4.0,
            },
            {
                "position": "WR",
                "add_player_id": "wr_fa",
                "drop_player_id": "wr_bench",
                "add_vorp_score": 9.0,
                "drop_vorp_score": 1.0,
            },
        ],
    )

    with patch("src.services.waiver_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = waiver_service.compute_waiver_recommendations(season=2026, week=3)

    assert result["season"] == 2026
    assert result["week"] == 3
    positions = [r["position"] for r in result["recommendations"]]
    assert positions == ["WR", "RB"]
    assert result["recommendations"][0]["vorp_delta"] == 8.0
    assert result["recommendations"][0]["free_agent"] == {
        "espn_id": "201",
        "name": "FA WR",
        "pro_team": "DAL",
        "injury_status": None,
        "net_vorp": 9.0,
    }
    assert result["recommendations"][1]["bench_player"]["injury_status"] == "Questionable"
    assert result["recommendations"][0]["rumor_sentiment"] is None
    assert result["recommendations"][0]["rumor_confidence"] is None
    assert result["recommendations"][0]["rumor_suggested_action"] is None


def test_compute_waiver_recommendations_empty_when_no_moves(monkeypatch):
    cursor = MagicMock()
    cursor.fetchall.return_value = []

    monkeypatch.setattr(
        waiver_service.waiver_strategy, "load_bench_by_position", lambda cur, team_id, season, week: {}
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy, "load_best_free_agent_by_position", lambda cur, season, week: {}
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy, "propose_waiver_moves", lambda bench, free_agents, margin: []
    )

    with patch("src.services.waiver_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = waiver_service.compute_waiver_recommendations(season=2026, week=3)

    assert result["recommendations"] == []


def test_compute_waiver_recommendations_uses_display_margin_not_live_threshold(monkeypatch):
    """Regression guard: display recommendations must pass margin=0.0, not
    waiver_strategy.MARGIN_THRESHOLD - a marginal upgrade that wouldn't auto-publish
    live should still surface here for a manager to decide on."""
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    captured_margin = {}

    monkeypatch.setattr(
        waiver_service.waiver_strategy, "load_bench_by_position", lambda cur, team_id, season, week: {}
    )
    monkeypatch.setattr(
        waiver_service.waiver_strategy, "load_best_free_agent_by_position", lambda cur, season, week: {}
    )

    def _capture_margin(bench, free_agents, margin):
        captured_margin["value"] = margin
        return []

    monkeypatch.setattr(waiver_service.waiver_strategy, "propose_waiver_moves", _capture_margin)

    with patch("src.services.waiver_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        waiver_service.compute_waiver_recommendations(season=2026, week=3)

    assert captured_margin["value"] == 0.0
