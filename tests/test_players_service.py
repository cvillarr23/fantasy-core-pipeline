from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.schemas.espn_ownership import EspnOwnership
from src.services import players_service


REAL_PLAYED_WEEKS = players_service._played_weeks
ESPN = EspnOwnership(pos_rank=12, percent_owned=91.0, percent_started=80.0, percent_bench=11.0, percent_change=3.0)


@pytest.fixture(autouse=True)
def _no_espn_ownership(monkeypatch):
    """Cursor mocks here return fixed rows; the loader has its own tests."""
    monkeypatch.setattr(players_service, "load_latest_espn_ownership", lambda *a, **k: {})
    monkeypatch.setattr(players_service, "_played_weeks", lambda *a, **k: [])


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


def test_get_player_summary_success():
    cursor = MagicMock()
    cursor.fetchone.return_value = ("p1", "Some Back", "RB", "KC", "ACTIVE", "Questionable", "4362628")
    cursor.fetchall.return_value = [
        ("rotowire", "Some Back is questionable for Sunday.", "https://example.com/1", datetime(2026, 8, 17, 12, 0, 0)),
    ]

    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        summary = players_service.get_player_summary("p1")

    assert summary["player_id"] == "p1"
    assert summary["espn_id"] == "4362628"
    assert summary["injury_status"] == "Questionable"
    assert len(summary["recent_news"]) == 1
    assert summary["recent_news"][0]["source"] == "rotowire"
    assert summary["recent_news"][0]["published_at"] == "2026-08-17T12:00:00"


def test_get_player_summary_returns_negative_dst_espn_id():
    """A D/ST's ESPN id is negative (-(16000 + proTeamId)); it must pass through verbatim so the
    browser worker can stage a trade or lineup move involving a defense."""
    cursor = MagicMock()
    cursor.fetchone.return_value = ("PHI", "Philadelphia Eagles", "D/ST", "PHI", "ACTIVE", None, "-16021")
    cursor.fetchall.return_value = []

    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        summary = players_service.get_player_summary("PHI")

    assert summary["espn_id"] == "-16021"


def test_get_player_summary_returns_null_espn_id_when_unresolved():
    cursor = MagicMock()
    cursor.fetchone.return_value = ("p2", "Some Kicker", "K", "LV", "ACTIVE", None, None)
    cursor.fetchall.return_value = []

    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        summary = players_service.get_player_summary("p2")

    assert summary["espn_id"] is None


def test_get_player_summary_raises_not_found_for_unknown_player():
    cursor = MagicMock()
    cursor.fetchone.return_value = None

    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        with pytest.raises(players_service.PlayerNotFoundError):
            players_service.get_player_summary("unknown")


def test_search_players_success():
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("p1", "e1", "Some Back", "RB", "KC", "Questionable", 12.5, 15.2),
        ("p2", None, "Some Backup", "RB", "SF", None, None, None),
    ]

    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        results = players_service.search_players("Some")

    assert len(results) == 2
    assert results[0]["player_id"] == "p1"
    assert results[0]["espn_id"] == "e1"
    # No espn_id resolved yet - the UI must see this and disable manual pick
    # rather than fall back to the internal id, which would draft a different, real player.
    assert results[1]["espn_id"] is None


def test_summary_and_search_carry_espn_ownership_or_none(monkeypatch):
    seen = []

    def fake_loader(cur, ids):
        seen.append(list(ids))
        return {"p1": ESPN}

    monkeypatch.setattr(players_service, "load_latest_espn_ownership", fake_loader)
    cursor = MagicMock()
    cursor.fetchone.return_value = ("p1", "Some Back", "RB", "KC", "ACTIVE", None, "1")
    cursor.fetchall.return_value = []
    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        assert players_service.get_player_summary("p1")["espn"] == ESPN

    cursor.fetchall.return_value = [
        ("p1", "1", "Some Back", "RB", "KC", None, 1.0, 2.0),
        ("p2", None, "Backup", "RB", "SF", None, None, None),
    ]
    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        results = players_service.search_players("S")
    assert results[0]["espn"] == ESPN and results[1]["espn"] is None
    assert seen == [["p1"], ["p1", "p2"]]


def test_played_weeks_keep_only_the_latest_season_oldest_first():
    cursor = MagicMock()
    cursor.fetchall.return_value = [(2026, 6, 10.0), (2026, 5, 20.0), (2026, 2, 15.0), (2025, 17, 99.0)]

    played = REAL_PLAYED_WEEKS(cursor, "p1")

    assert played == [(2, 15.0), (5, 20.0), (6, 10.0)]
    assert "actual_points_source IS NOT NULL" in cursor.execute.call_args[0][0]


def test_season_avg_and_weekly_log_cover_the_last_four_played_weeks():
    played = [(1, 5.0), (2, 15.0), (4, 0.0), (5, 20.0), (6, 10.0)]

    assert players_service._season_avg(played) == 10.0
    assert players_service._weekly_log(played) == [
        {"week": 2, "points": 15.0}, {"week": 4, "points": 0.0}, {"week": 5, "points": 20.0}, {"week": 6, "points": 10.0},
    ]


def test_season_avg_is_null_and_log_empty_without_played_weeks():
    assert players_service._season_avg([]) is None
    assert players_service._weekly_log([]) == []


def test_get_player_summary_includes_live_block_and_history(monkeypatch):
    cursor = MagicMock()
    cursor.fetchone.return_value = ("p1", "Some Back", "RB", "KC", "ACTIVE", None, "1")
    cursor.fetchall.return_value = []
    live = {"live_points": 7.5, "projected_points": 14.0, "game_label": "KC @ DEN", "game_status": "in_progress", "event_id": "9"}
    monkeypatch.setattr(players_service, "_played_weeks", lambda *a: [(1, 12.0), (2, 8.0)])
    monkeypatch.setattr(players_service.live_service, "player_live_summary", lambda cur, pid: live)

    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        summary = players_service.get_player_summary("p1")

    assert summary["live"] == live
    assert summary["season_avg"] == 10.0
    assert summary["weekly_log"] == [{"week": 1, "points": 12.0}, {"week": 2, "points": 8.0}]
