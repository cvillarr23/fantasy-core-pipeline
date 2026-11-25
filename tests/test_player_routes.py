from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.schemas.espn_ownership import EspnOwnership
from src.services import players_service


ESPN = EspnOwnership(pos_rank=12, percent_owned=91.0, percent_started=80.0, percent_bench=11.0, percent_change=3.0)
ESPN_JSON = {"pos_rank": 12, "percent_owned": 91.0, "percent_started": 80.0, "percent_bench": 11.0, "percent_change": 3.0}


@pytest.fixture(autouse=True)
def _no_espn_ownership(monkeypatch):
    monkeypatch.setattr(players_service, "load_latest_espn_ownership", lambda *a, **k: {})
    monkeypatch.setattr(players_service, "_played_weeks", lambda *a, **k: [])


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


def test_get_player_summary_success(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("p1", "Some Back", "RB", "KC", "ACTIVE", "Questionable", "4362628")
    mock_cursor.fetchall.return_value = [
        ("rotowire", "Some Back is questionable for Sunday.", "https://example.com/1", datetime(2026, 8, 17, 12, 0, 0)),
    ]

    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/players/p1/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["player_id"] == "p1"
    assert body["name"] == "Some Back"
    assert body["espn_id"] == "4362628"
    assert body["injury_status"] == "Questionable"
    assert len(body["recent_news"]) == 1
    assert body["recent_news"][0]["source"] == "rotowire"


def test_get_player_summary_serializes_real_datetime_published_at(client):
    """published_at comes back from psycopg2 as a real datetime for a TIMESTAMP column, not
    a string - a raw datetime object passed straight through would fail FastAPI's Pydantic
    response validation (str field) and turn this endpoint into a 500."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("p1", "Some Back", "RB", "KC", "ACTIVE", None, None)
    mock_cursor.fetchall.return_value = [
        ("rotowire", "Some Back looked good in practice.", None, datetime(2026, 8, 17, 12, 0, 0)),
    ]

    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/players/p1/summary")

    assert response.status_code == 200
    assert response.json()["recent_news"][0]["published_at"] == "2026-08-17T12:00:00"


def test_get_player_summary_unknown_player_returns_404(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/players/unknown/summary")

    assert response.status_code == 404


def test_get_player_summary_db_failure(client):
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.services.players_service.get_db_cursor", _broken_cursor):
        response = client.get("/v1/players/p1/summary")

    assert response.status_code == 500


def test_get_player_summary_no_news_returns_empty_list(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("p1", "Some Back", "RB", "KC", "ACTIVE", None, None)
    mock_cursor.fetchall.return_value = []

    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/players/p1/summary")

    assert response.status_code == 200
    assert response.json()["recent_news"] == []
    assert response.json()["injury_status"] is None
    # Unresolved espn_id serializes as null rather than failing response validation.
    assert response.json()["espn_id"] is None


def test_search_players_success(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("p1", "e1", "Some Back", "RB", "KC", "Questionable", 12.5, 15.2),
        ("p2", None, "Some Backup", "RB", "SF", None, None, None),
    ]

    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/players/search?q=Some")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    assert body[0]["player_id"] == "p1"
    assert body[0]["espn_id"] == "e1"
    assert body[0]["team"] == "KC"
    assert body[0]["injury_status"] == "Questionable"
    assert body[0]["vorp_score"] == 12.5
    assert body[0]["projected_points"] == 15.2
    assert body[1]["vorp_score"] is None
    # No crosswalk entry (every K and D/ST) - the UI must see this and disable manual pick
    # rather than fall back to the internal id, which would draft a different, real player.
    assert body[1]["espn_id"] is None


def test_search_players_short_query_returns_empty_without_hitting_db(client):
    with patch("src.services.players_service.get_db_cursor") as mock_get_cursor:
        response = client.get("/v1/players/search?q=a")

    assert response.status_code == 200
    assert response.json() == []
    mock_get_cursor.assert_not_called()


def test_search_players_db_error_returns_500(client):
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.services.players_service.get_db_cursor", _broken_cursor):
        response = client.get("/v1/players/search?q=Some")

    assert response.status_code == 500


def test_player_payloads_serialize_espn_ownership_and_null(client, monkeypatch):
    monkeypatch.setattr(players_service, "load_latest_espn_ownership", lambda cur, ids: {"p1": ESPN})
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("p1", "Some Back", "RB", "KC", "ACTIVE", None, None)
    mock_cursor.fetchall.return_value = []
    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        assert client.get("/v1/players/p1/summary").json()["espn"] == ESPN_JSON

    mock_cursor.fetchall.return_value = [
        ("p1", "e1", "Some Back", "RB", "KC", None, 1.0, 2.0),
        ("p2", None, "Some Backup", "RB", "SF", None, None, None),
    ]
    with patch("src.services.players_service.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        body = client.get("/v1/players/search?q=Some").json()
    assert body[0]["espn"] == ESPN_JSON
    assert body[1]["espn"] is None
