from contextlib import contextmanager
from unittest.mock import MagicMock, patch


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


def _payload(incoming, outgoing, season=2026, week=3):
    return {
        "incoming_player_ids": incoming,
        "outgoing_player_ids": outgoing,
        "season": season,
        "week": week,
    }


def test_evaluate_trade_success(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("p1", 10.0), ("p2", 4.0), ("p3", 6.0)]

    with patch("src.api.mcp_server.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.post("/v1/trade/evaluate", json=_payload(["p1"], ["p2", "p3"]))

    assert response.status_code == 200
    body = response.json()
    assert body["incoming_total"] == 10.0
    assert body["outgoing_total"] == 10.0
    assert body["delta_vorp"] == 0.0
    assert body["missing_player_ids"] == []


def test_evaluate_trade_reports_missing_players(client):
    mock_cursor = MagicMock()
    # p2 has no weekly_stats row for this season/week.
    mock_cursor.fetchall.return_value = [("p1", 10.0)]

    with patch("src.api.mcp_server.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.post("/v1/trade/evaluate", json=_payload(["p1"], ["p2"]))

    assert response.status_code == 200
    body = response.json()
    assert body["incoming_total"] == 10.0
    assert body["outgoing_total"] == 0.0
    assert body["delta_vorp"] == 10.0
    assert body["missing_player_ids"] == ["p2"]


def test_evaluate_trade_rejects_duplicate_incoming_ids(client):
    with patch("src.api.mcp_server.get_db_cursor") as mock_get_cursor:
        response = client.post("/v1/trade/evaluate", json=_payload(["p1", "p1"], ["p2"]))

    assert response.status_code == 400
    assert "duplicate" in response.json()["detail"]
    mock_get_cursor.assert_not_called()


def test_evaluate_trade_rejects_duplicate_outgoing_ids(client):
    with patch("src.api.mcp_server.get_db_cursor") as mock_get_cursor:
        response = client.post("/v1/trade/evaluate", json=_payload(["p1"], ["p2", "p2"]))

    assert response.status_code == 400
    assert "duplicate" in response.json()["detail"]
    mock_get_cursor.assert_not_called()


def test_evaluate_trade_rejects_player_on_both_sides(client):
    with patch("src.api.mcp_server.get_db_cursor") as mock_get_cursor:
        response = client.post("/v1/trade/evaluate", json=_payload(["p1", "p2"], ["p2", "p3"]))

    assert response.status_code == 400
    assert "both sides" in response.json()["detail"]
    mock_get_cursor.assert_not_called()


def test_evaluate_trade_db_failure_returns_500(client):
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.api.mcp_server.get_db_cursor", _broken_cursor):
        response = client.post("/v1/trade/evaluate", json=_payload(["p1"], ["p2"]))

    assert response.status_code == 500
