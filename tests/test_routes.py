from contextlib import contextmanager
from unittest.mock import MagicMock, patch


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


def _espn_id_ok_cursor():
    """Cursor whose first lookup (players.espn_id) hits, so the namespace guard passes."""
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    return cursor


def test_health_check_healthy(client):
    with patch("src.api.routes.get_db_cursor", _fake_db_cursor), \
         patch("src.api.routes.pika.BlockingConnection") as mock_connection:
        mock_connection.return_value = MagicMock()

        response = client.get("/v1/health")

    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "database": "connected", "broker": "connected"}


def test_health_check_database_unavailable(client):
    @contextmanager
    def _broken_db_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.api.routes.get_db_cursor", _broken_db_cursor):
        response = client.get("/v1/health")

    assert response.status_code == 503
    assert "database" in response.json()["detail"].lower()


def test_health_check_broker_unavailable(client):
    with patch("src.api.routes.get_db_cursor", _fake_db_cursor), \
         patch("src.api.routes.pika.BlockingConnection", side_effect=Exception("broker unreachable")):
        response = client.get("/v1/health")

    assert response.status_code == 503
    assert "broker" in response.json()["detail"].lower()


def test_trigger_lineup_swap_success(client):
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    payload = {
        "team_id": "1",
        "optimized_swaps": [
            {"drop_player_id": "4046", "add_player_id": "4983", "slot": "FLEX"}
        ],
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection):
        response = client.post("/v1/trigger/lineup", json=payload)

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["queue"] == "lineup_updates"
    assert body["team_id"] == "1"
    assert "tracking_id" in body and body["tracking_id"]

    mock_channel.queue_declare.assert_called_once_with(queue="lineup_updates", durable=True)
    publish_kwargs = mock_channel.basic_publish.call_args.kwargs
    assert publish_kwargs["routing_key"] == "lineup_updates"
    assert publish_kwargs["properties"].delivery_mode == 2
    assert body["tracking_id"] in publish_kwargs["body"]


def test_trigger_lineup_swap_broker_failure(client):
    payload = {
        "team_id": "1",
        "optimized_swaps": [
            {"drop_player_id": "4046", "add_player_id": "4983", "slot": "FLEX"}
        ],
    }

    with patch("src.api.routes.pika.BlockingConnection", side_effect=Exception("broker down")):
        response = client.post("/v1/trigger/lineup", json=payload)

    assert response.status_code == 500
    assert "background execution broker" in response.json()["detail"].lower()


def test_trigger_lineup_swap_validates_payload(client):
    response = client.post("/v1/trigger/lineup", json={"team_id": "1"})

    assert response.status_code == 422


def test_trigger_draft_pick_success(client):
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    payload = {"team_id": "7", "player_id": "4046", "name": "Some Back", "position": "RB", "pro_team": "KC"}

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["queue"] == "draft_targets"
    assert body["team_id"] == "7"
    assert "tracking_id" in body and body["tracking_id"]

    mock_channel.queue_declare.assert_called_once_with(queue="draft_targets", durable=True)
    publish_kwargs = mock_channel.basic_publish.call_args.kwargs
    assert publish_kwargs["routing_key"] == "draft_targets"
    assert publish_kwargs["properties"].delivery_mode == 2
    assert body["tracking_id"] in publish_kwargs["body"]


def test_trigger_draft_pick_broker_failure(client):
    payload = {"team_id": "7", "player_id": "4046", "name": "Some Back", "position": "RB", "pro_team": "KC"}

    with patch("src.api.routes.pika.BlockingConnection", side_effect=Exception("broker down")), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 500
    assert "background execution broker" in response.json()["detail"].lower()


def test_trigger_draft_pick_validates_payload(client):
    response = client.post("/v1/trigger/draft-pick", json={"team_id": "7"})

    assert response.status_code == 422


def test_trigger_draft_pick_rejects_internal_sleeper_id(client):
    """The exact bug this guard exists for: ESPN 12483 is Matthew Stafford, internal 12483 is
    Jack Bech. Sending the internal id drafts a different, real player, undetectably."""
    cursor = MagicMock()
    # Miss on players.espn_id, hit on players.player_id -> definitively the wrong namespace.
    cursor.fetchone.side_effect = [None, ("Jack Bech",)]

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection") as mock_connection:
        response = client.post("/v1/trigger/draft-pick", json={
            "team_id": "7", "player_id": "12483", "name": "Matthew Stafford",
            "position": "QB", "pro_team": "LAR",
        })

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "internal Sleeper id" in detail and "Jack Bech" in detail
    mock_connection.assert_not_called()


def test_trigger_draft_pick_allows_id_absent_from_both_columns(client):
    """The espn_id crosswalk covers ~82% of players and no K or D/ST, so an unknown id is not
    evidence of the wrong namespace - it must stay draftable."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [None, None]
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection):
        response = client.post("/v1/trigger/draft-pick", json={
            "team_id": "7", "player_id": "-16011", "name": "Ravens D/ST",
            "position": "DST", "pro_team": "BAL",
        })

    assert response.status_code == 202
    mock_channel.basic_publish.assert_called_once()


def test_trigger_draft_pick_fails_open_when_the_guard_lookup_errors(client):
    """The click bot is the draft-day safety valve; a database blip must not block a manual
    pick. The guard is a caller-bug check, not an authorization gate."""
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    with patch("src.api.routes.get_db_cursor", _broken_cursor), \
         patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection):
        response = client.post("/v1/trigger/draft-pick", json={
            "team_id": "7", "player_id": "4046", "name": "Some Back",
            "position": "RB", "pro_team": "KC",
        })

    assert response.status_code == 202
    mock_channel.basic_publish.assert_called_once()
