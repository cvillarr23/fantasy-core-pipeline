from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.services import players_service


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


def test_get_player_summary_success():
    cursor = MagicMock()
    cursor.fetchone.return_value = ("p1", "Some Back", "RB", "KC", "ACTIVE", "Questionable")
    cursor.fetchall.return_value = [
        ("rotowire", "Some Back is questionable for Sunday.", "https://example.com/1", datetime(2026, 8, 17, 12, 0, 0)),
    ]

    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        summary = players_service.get_player_summary("p1")

    assert summary["player_id"] == "p1"
    assert summary["injury_status"] == "Questionable"
    assert len(summary["recent_news"]) == 1
    assert summary["recent_news"][0]["source"] == "rotowire"
    assert summary["recent_news"][0]["published_at"] == "2026-08-17T12:00:00"


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
    # No crosswalk entry (every K and D/ST) - the UI must see this and disable manual pick
    # rather than fall back to the internal id, which would draft a different, real player.
    assert results[1]["espn_id"] is None
