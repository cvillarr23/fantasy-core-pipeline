from contextlib import contextmanager
from unittest.mock import MagicMock, patch


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


def test_get_player_summary_success(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("p1", "Some Back", "RB", "KC", "ACTIVE", "Questionable")
    mock_cursor.fetchall.return_value = [
        ("rotowire", "Some Back is questionable for Sunday.", "https://example.com/1", "2026-08-17T12:00:00"),
    ]

    with patch("src.api.players.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/players/p1/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["player_id"] == "p1"
    assert body["name"] == "Some Back"
    assert body["injury_status"] == "Questionable"
    assert len(body["recent_news"]) == 1
    assert body["recent_news"][0]["source"] == "rotowire"


def test_get_player_summary_unknown_player_returns_404(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    with patch("src.api.players.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/players/unknown/summary")

    assert response.status_code == 404


def test_get_player_summary_db_failure(client):
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.api.players.get_db_cursor", _broken_cursor):
        response = client.get("/v1/players/p1/summary")

    assert response.status_code == 500


def test_get_player_summary_no_news_returns_empty_list(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("p1", "Some Back", "RB", "KC", "ACTIVE", None)
    mock_cursor.fetchall.return_value = []

    with patch("src.api.players.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/players/p1/summary")

    assert response.status_code == 200
    assert response.json()["recent_news"] == []
    assert response.json()["injury_status"] is None
