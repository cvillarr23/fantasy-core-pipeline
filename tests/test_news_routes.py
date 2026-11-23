from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


def _client_get(client, path, rows):
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    with patch("src.services.news_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        return client.get(path), cursor


def test_news_feed(client):
    rows = [(1, "p1", "espn_news", "ESPN", "Headline", "text", "https://e/1", datetime(2026, 11, 22, 12))]
    resp, _ = _client_get(client, "/v1/news?player_ids=p1,p2&limit=5", rows)
    assert resp.status_code == 200
    item = resp.json()["items"][0]
    assert item["source"] == "espn_news" and item["score"] is None and item["also_reported_by"] == []


def test_news_empty(client):
    resp, _ = _client_get(client, "/v1/news?player_ids=ghost", [])
    assert resp.json() == {"items": []}


def test_news_since_timezone_is_normalised_to_naive_utc(client):
    _, cursor = _client_get(client, "/v1/news?player_ids=p1&since=2026-11-22T12:00:00%2B02:00", [])
    assert cursor.execute.call_args[0][1][1] == datetime(2026, 11, 22, 10)


def test_news_validation(client):
    assert client.get("/v1/news").status_code == 422
    assert client.get("/v1/news?player_ids=,").status_code == 422
    assert client.get("/v1/news?player_ids=" + ",".join(f"p{i}" for i in range(51))).status_code == 422
    assert client.get("/v1/news?player_ids=p1&limit=0").status_code == 422


def test_recent(client):
    resp, _ = _client_get(client, "/v1/news/recent?player_ids=p1,ghost", [("p1",)])
    assert resp.json() == {"has_news_24h": {"p1": True, "ghost": False}}
