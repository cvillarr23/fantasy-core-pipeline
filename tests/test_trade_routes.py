from contextlib import contextmanager
from unittest.mock import MagicMock, patch


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


def test_evaluate_trade_success(client):
    cursor = MagicMock()
    cursor.fetchall.return_value = [("p1", 10.0), ("p2", 4.0)]

    with patch("src.services.trade_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        response = client.post(
            "/v1/trade/evaluate",
            json={
                "incoming_player_ids": ["p1"],
                "outgoing_player_ids": ["p2"],
                "season": 2026,
                "week": 3,
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["incoming_total"] == 10.0
    assert body["outgoing_total"] == 4.0
    assert body["delta_vorp"] == 6.0
    assert body["missing_player_ids"] == []


def test_evaluate_trade_rejects_overlapping_ids_with_422(client):
    response = client.post(
        "/v1/trade/evaluate",
        json={
            "incoming_player_ids": ["p1", "p2"],
            "outgoing_player_ids": ["p2", "p3"],
            "season": 2026,
            "week": 3,
        },
    )

    assert response.status_code == 422
    assert "both sides" in response.json()["detail"]


def test_evaluate_trade_db_failure_returns_500(client):
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.services.trade_service.get_db_cursor", _broken_cursor):
        response = client.post(
            "/v1/trade/evaluate",
            json={
                "incoming_player_ids": ["p1"],
                "outgoing_player_ids": ["p2"],
                "season": 2026,
                "week": 3,
            },
        )

    assert response.status_code == 500
