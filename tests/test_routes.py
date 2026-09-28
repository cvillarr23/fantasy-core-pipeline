import time

from src.api import routes
import json
from contextlib import contextmanager
from unittest.mock import MagicMock, patch


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


def _fake_redis():
    """Minimal Redis stand-in with real SET/SET NX/GET/DELETE semantics, so idempotency
    tests exercise the actual claim state machine rather than a scripted call sequence."""
    store: dict = {}
    hashes: dict = {}

    class _R:
        def hget(self, key, field):
            return hashes.get(key, {}).get(field)

        def set(self, key, value, nx=False, ex=None):
            if nx and key in store:
                return None
            store[key] = value
            return True

        def get(self, key):
            return store.get(key)

        def delete(self, key):
            return 1 if store.pop(key, None) is not None else 0

        def eval(self, _script, _numkeys, key, argv):
            # Mirrors _RELEASE_CLAIM_LUA: compare-and-delete on the caller's own token.
            if store.get(key) == argv:
                del store[key]
                return 1
            return 0

    client = _R()
    client.store = store
    client.hashes = hashes
    return client


def _espn_id_ok_cursor(name="Some Back"):
    """Cursor where the espn_id lookup returns the same player the payload names, so the
    namespace guard passes. The internal-id lookup misses."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(name,), None]
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

    mock_channel.queue_declare.assert_called_once_with(queue="lineup_updates", passive=True)
    publish_kwargs = mock_channel.basic_publish.call_args.kwargs
    assert publish_kwargs["routing_key"] == "lineup_updates"
    assert publish_kwargs["properties"].delivery_mode == 2
    assert body["tracking_id"] in publish_kwargs["body"]


def test_trigger_lineup_rejects_missing_or_wrong_api_key(client):
    payload = {
        "team_id": "1",
        "optimized_swaps": [
            {"drop_player_id": "4046", "add_player_id": "4983", "slot": "FLEX"}
        ],
    }

    with patch("src.api.routes.pika.BlockingConnection") as mock_conn:
        no_header_response = client.post(
            "/v1/trigger/lineup", json=payload, headers={"X-API-Key": ""}
        )
        wrong_key_response = client.post(
            "/v1/trigger/lineup", json=payload, headers={"X-API-Key": "not-the-key"}
        )

    assert no_header_response.status_code == 401
    assert wrong_key_response.status_code == 401
    mock_conn.assert_not_called()


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

    mock_channel.queue_declare.assert_called_once_with(queue="draft_targets", passive=True)
    publish_kwargs = mock_channel.basic_publish.call_args.kwargs
    assert publish_kwargs["routing_key"] == "draft_targets"
    assert publish_kwargs["properties"].delivery_mode == 2
    assert body["tracking_id"] in publish_kwargs["body"]


def test_trigger_draft_pick_idempotency_key_blocks_a_duplicate_enqueue(client):
    """A publish that fails ambiguously - read timeout, or a reset after the bytes went out -
    may already have committed a durable message. Without this the caller's retry queues a
    second draft action and the worker clicks twice."""
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_redis = _fake_redis()

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "idempotency_key": "L1:5:4046",
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        first = client.post("/v1/trigger/draft-pick", json=payload)
        second = client.post("/v1/trigger/draft-pick", json=payload)

    assert first.status_code == 202 and first.json()["status"] == "accepted"
    assert second.status_code == 202 and second.json()["status"] == "duplicate"
    assert mock_channel.basic_publish.call_count == 1
    assert mock_redis.store["draft:trigger:idempotency:L1:5:4046"] == "published"


def test_trigger_draft_pick_without_an_idempotency_key_never_touches_redis(client):
    mock_connection = MagicMock()
    mock_redis = MagicMock()
    payload = {"team_id": "7", "player_id": "4046", "name": "Some Back", "position": "RB", "pro_team": "KC"}

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 202
    mock_redis.set.assert_not_called()


def test_trigger_draft_pick_fails_open_when_redis_is_unavailable(client):
    """A duplicate SELECT for a player we already hold is rejected by ESPN; a missed pick
    hands the roster spot to autodraft. Given an unprovable claim, take the recoverable
    failure."""
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "idempotency_key": "L1:5:4046",
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", side_effect=Exception("redis down")), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    mock_channel.basic_publish.assert_called_once()


def test_trigger_draft_pick_closes_the_broker_connection_on_a_pre_publish_failure(client):
    """The optimizer retries this endpoint for the length of the pick clock. A failure after
    the TCP connection opened leaked a socket per attempt and could exhaust the broker."""
    mock_channel = MagicMock()
    mock_channel.queue_declare.side_effect = Exception("declare failed")
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_connection.is_open = True

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC",
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=_fake_redis()), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 500
    mock_connection.close.assert_called_once()


def test_trigger_draft_pick_closes_the_broker_connection_when_the_slot_is_superseded(client):
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_connection.is_open = True
    mock_redis = _fake_redis()
    mock_redis.hashes["draft:L1:board"] = {"last_processed_pick_number": "6"}

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "league_id": "L1", "pick_number": 6,
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 409
    mock_connection.close.assert_called_once()


def test_trigger_draft_pick_closes_the_broker_connection_exactly_once_on_success(client):
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_connection.is_open = True

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC",
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=_fake_redis()), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 202
    mock_connection.close.assert_called_once()


def test_trigger_draft_pick_discards_a_target_whose_slot_was_filled_mid_flight(client):
    """hold_and_publish's cancel_event cannot interrupt a request already inside
    requests.post. If the slot fills while the request is in flight, only the gateway can
    still stop it - otherwise the click lands on a later team's pick."""
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_redis = _fake_redis()
    mock_redis.hashes["draft:L1:board"] = {"last_processed_pick_number": "6"}

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "idempotency_key": "L1:6:4046",
        "league_id": "L1", "pick_number": 6,
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 409
    mock_channel.basic_publish.assert_not_called()
    assert "draft:trigger:idempotency:L1:6:4046" not in mock_redis.store


def test_trigger_draft_pick_publishes_when_the_slot_is_still_open(client):
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_redis = _fake_redis()
    mock_redis.hashes["draft:L1:board"] = {"last_processed_pick_number": "5"}

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "league_id": "L1", "pick_number": 6,
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 202
    body = json.loads(mock_channel.basic_publish.call_args.kwargs["body"])
    # league_id IS forwarded now (it wasn't before): the click bot needs it to know which
    # league's draft it's acting on rather than trusting its own pod's static ESPN_LEAGUE_ID
    # env var for every message regardless of which league the target is actually for.
    # pick_number IS forwarded too now (issue #108): the click bot echoes it back on a
    # confirmed ESPN rejection via draft_pick_rejected:{leagueId}, so hold_and_publish can
    # match the rejection to the pick it's currently holding.
    assert body["league_id"] == "L1"
    assert body["pick_number"] == 6


def test_trigger_draft_pick_releases_the_claim_on_a_pika_wrong_state_error(client):
    """ChannelWrongStateError is raised before any frame reaches the socket, so no message
    exists. Promoting the claim would hand the retry a 202 duplicate for nothing."""
    mock_channel = MagicMock()
    mock_channel.basic_publish.side_effect = routes.pika.exceptions.ChannelWrongStateError("closed")
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_redis = _fake_redis()

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "idempotency_key": "L1:5:4046",
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 500
    assert "draft:trigger:idempotency:L1:5:4046" not in mock_redis.store


def test_trigger_draft_pick_discards_a_pick_whose_deadline_has_passed(client):
    """The caller's HTTP timeout does not cancel this handler. Resuming from a blocked broker
    call and queueing anyway would click for a slot ESPN has already autodrafted."""
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_redis = _fake_redis()

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "idempotency_key": "L1:5:4046",
        "expires_at": time.time() - 1,
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 409
    mock_channel.basic_publish.assert_not_called()
    # Nothing was enqueued, so the claim must not block a caller that still has clock left.
    assert "draft:trigger:idempotency:L1:5:4046" not in mock_redis.store


def test_trigger_draft_pick_publishes_when_the_deadline_is_still_ahead(client):
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "expires_at": time.time() + 60,
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=_fake_redis()), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 202
    body = json.loads(mock_channel.basic_publish.call_args.kwargs["body"])
    assert "expires_at" not in body


def test_trigger_draft_pick_release_does_not_drop_a_retrys_fresh_claim(client):
    """A blind DELETE would drop the claim a retry already took, letting a third request
    enqueue the same action. The release is scoped to the releasing request's own token."""
    mock_redis = _fake_redis()
    mock_redis.store["draft:trigger:idempotency:L1:5:4046"] = "pending:someone-else"

    with patch("src.api.routes._redis_client", return_value=mock_redis):
        routes._release_idempotency_key("L1:5:4046", "my-token")

    assert mock_redis.store["draft:trigger:idempotency:L1:5:4046"] == "pending:someone-else"


def test_trigger_draft_pick_forwards_idempotency_key_and_pick_number_onto_the_queue(client):
    """idempotency_key and pick_number USED to be gateway-only state stripped before the queue
    (see git history) - issue #108 needs the click bot to echo both back on a confirmed ESPN
    rejection (draft_pick_rejected:{leagueId}) so hold_and_publish can match the rejection to
    the pick it's currently holding. expires_at remains gateway-only and is still stripped: the
    click bot has no use for the caller's HTTP deadline."""
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "idempotency_key": "L1:5:4046",
        "pick_number": 5, "expires_at": time.time() + 3600,
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=_fake_redis()), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 202
    body = json.loads(mock_channel.basic_publish.call_args.kwargs["body"])
    assert body["idempotency_key"] == "L1:5:4046"
    assert body["pick_number"] == 5
    assert "expires_at" not in body
    assert body["player_id"] == "4046"


def test_trigger_draft_pick_tells_the_caller_to_retry_while_an_earlier_request_is_in_flight(client):
    """A claim alone does not prove a message exists. If the first request claims the key and
    then hangs in BlockingConnection, answering "duplicate" stops the caller - and the first
    request may still fail pre-publish and release, leaving nothing enqueued at all."""
    mock_redis = _fake_redis()
    mock_redis.store["draft:trigger:idempotency:L1:5:4046"] = "pending"

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "idempotency_key": "L1:5:4046",
    }

    with patch("src.api.routes.pika.BlockingConnection") as mock_conn, \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    # 503 is in the optimizer's retryable set, so hold_and_publish tries again.
    assert response.status_code == 503
    mock_conn.assert_not_called()


def test_trigger_draft_pick_releases_the_claim_when_the_broker_fails_before_publishing(client):
    """A pre-publish broker failure that kept the claim turned a transient error into a
    permanent one: the retry got "duplicate", hold_and_publish read that as success and
    stopped, and the pick was ceded to autodraft by the guard meant to protect it."""
    mock_redis = _fake_redis()

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "idempotency_key": "L1:5:4046",
    }

    with patch("src.api.routes.pika.BlockingConnection", side_effect=Exception("broker down")), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 500
    assert "draft:trigger:idempotency:L1:5:4046" not in mock_redis.store


def test_trigger_draft_pick_keeps_the_claim_when_the_failure_is_ambiguous(client):
    """A failure at or after basic_publish may have committed the message. Releasing then
    would allow a genuine duplicate, so the claim stands."""
    mock_channel = MagicMock()
    mock_channel.basic_publish.side_effect = Exception("reset after send")
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_redis = _fake_redis()

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "idempotency_key": "L1:5:4046",
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 500
    # Ambiguous, so the claim is kept AND promoted: a retry must get a clean "duplicate"
    # rather than being told to keep retrying an outcome that can never be resolved.
    assert mock_redis.store["draft:trigger:idempotency:L1:5:4046"] == "published"


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


def test_trigger_draft_pick_rejects_the_real_stafford_bech_collision(client):
    """The exact bug this guard exists for, with both colliding rows modelled: 12483 is a real
    espn_id (Matthew Stafford) AND a real internal player_id (Jack Bech). A caller meaning Bech
    sends 12483, and a bare existence check on espn_id would wave it through and draft Stafford."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("Matthew Stafford",), ("Jack Bech",)]

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection") as mock_connection:
        response = client.post("/v1/trigger/draft-pick", json={
            "team_id": "7", "player_id": "12483", "name": "Jack Bech",
            "position": "WR", "pro_team": "NO",
        })

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "internal Sleeper id" in detail
    assert "Matthew Stafford" in detail, "the reply must name who would actually have been drafted"
    mock_connection.assert_not_called()


def test_trigger_draft_pick_allows_the_same_id_when_the_payload_means_the_espn_player(client):
    """Same colliding id, other direction: the caller names Stafford, so 12483 is correct."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("Matthew Stafford",), ("Jack Bech",)]
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection):
        response = client.post("/v1/trigger/draft-pick", json={
            "team_id": "7", "player_id": "12483", "name": "Matthew Stafford",
            "position": "QB", "pro_team": "LAR",
        })

    assert response.status_code == 202
    mock_channel.basic_publish.assert_called_once()


def test_trigger_draft_pick_rejects_internal_id_absent_from_the_crosswalk(client):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [None, ("Some Back",)]

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection") as mock_connection:
        response = client.post("/v1/trigger/draft-pick", json={
            "team_id": "7", "player_id": "4046", "name": "Some Back",
            "position": "RB", "pro_team": "KC",
        })

    assert response.status_code == 422
    mock_connection.assert_not_called()


def test_trigger_draft_pick_tolerates_name_punctuation_differences(client):
    """ESPN, Sleeper and DynastyProcess disagree on punctuation and suffixes; a formatting
    difference must not read as a namespace error and block a draft-day pick."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("A.J. Brown",), None]
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection):
        response = client.post("/v1/trigger/draft-pick", json={
            "team_id": "7", "player_id": "e-99", "name": "AJ Brown",
            "position": "WR", "pro_team": "PHI",
        })

    assert response.status_code == 202
    mock_channel.basic_publish.assert_called_once()


def test_trigger_draft_pick_allows_espn_id_whose_name_disagrees_with_no_internal_claim(client):
    """Crosswalk/name drift, not a namespace error: the payload's player does not hold the id
    internally either, so there is nothing decidable to reject and blocking a live pick on a
    name mismatch is worse than allowing it."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("Someone Else",), None]
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection):
        response = client.post("/v1/trigger/draft-pick", json={
            "team_id": "7", "player_id": "e-77", "name": "Some Back",
            "position": "RB", "pro_team": "KC",
        })

    assert response.status_code == 202
    mock_channel.basic_publish.assert_called_once()


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


def test_trigger_draft_pick_rejects_a_known_internal_id_even_when_the_name_disagrees(client):
    """A typo or alternate spelling does not turn a Sleeper id into an ESPN one. When the id is
    absent from players.espn_id entirely, the namespace is decidable without the name."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [None, ("Some Back",)]

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection") as mock_connection:
        response = client.post("/v1/trigger/draft-pick", json={
            "team_id": "7", "player_id": "4046", "name": "Sum Bakc",
            "position": "RB", "pro_team": "KC",
        })

    assert response.status_code == 422
    assert "Some Back" in response.json()["detail"], "must name the id's actual owner, not the typo"
    mock_connection.assert_not_called()


def test_trigger_draft_pick_rejects_a_colliding_id_named_by_its_generational_suffix(client):
    """Suffix drift must not let the dangerous case slip through: a caller sending Jack Bech's
    internal id while writing his name without a suffix still has to be rejected."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("Matthew Stafford",), ("Jack Bech III",)]

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection") as mock_connection:
        response = client.post("/v1/trigger/draft-pick", json={
            "team_id": "7", "player_id": "12483", "name": "Jack Bech",
            "position": "WR", "pro_team": "NO",
        })

    assert response.status_code == 422
    mock_connection.assert_not_called()


def test_normalized_name_drops_generational_suffixes_but_never_the_last_token():
    from src.api.routes import _normalized_name

    assert _normalized_name("Kenneth Walker III") == _normalized_name("Kenneth Walker")
    assert _normalized_name("A.J. Brown") == _normalized_name("AJ Brown")
    assert _normalized_name("Michael Pittman Jr.") == _normalized_name("Michael Pittman")
    # A bare suffix token is somebody's whole name; emptying it would match everything.
    assert _normalized_name("V") == "v"


def test_trigger_waiver_claim_success(client):
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    payload = {
        "team_id": "7", "player_id": "4046", "drop_player_id": "9999",
        "name": "Some Back", "team": "KC",
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/waiver", json=payload)

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["queue"] == "waiver_actions"
    assert body["team_id"] == "7"
    assert "tracking_id" in body and body["tracking_id"]

    mock_channel.queue_declare.assert_called_once_with(queue="waiver_actions", passive=True)
    publish_kwargs = mock_channel.basic_publish.call_args.kwargs
    assert publish_kwargs["routing_key"] == "waiver_actions"
    assert publish_kwargs["properties"].delivery_mode == 2
    assert body["tracking_id"] in publish_kwargs["body"]


def test_trigger_waiver_claim_allows_an_add_only_claim_with_no_drop_player(client):
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    payload = {"team_id": "7", "player_id": "4046", "name": "Some Back", "team": "KC"}

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/waiver", json=payload)

    assert response.status_code == 202
    mock_channel.basic_publish.assert_called_once()


def test_trigger_waiver_claim_broker_failure(client):
    payload = {"team_id": "7", "player_id": "4046", "name": "Some Back", "team": "KC"}

    with patch("src.api.routes.pika.BlockingConnection", side_effect=Exception("broker down")), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/waiver", json=payload)

    assert response.status_code == 500
    assert "background execution broker" in response.json()["detail"].lower()


def test_trigger_waiver_claim_validates_payload(client):
    response = client.post("/v1/trigger/waiver", json={"team_id": "7"})

    assert response.status_code == 422


def test_trigger_waiver_claim_rejects_the_real_stafford_bech_collision(client):
    """Same id-namespace danger as draft-pick: 12483 is a real espn_id (Stafford) and a real
    internal player_id (Bech). A caller meaning Bech must not silently claim Stafford."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("Matthew Stafford",), ("Jack Bech",)]

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection") as mock_connection:
        response = client.post("/v1/trigger/waiver", json={
            "team_id": "7", "player_id": "12483", "name": "Jack Bech", "team": "NO",
        })

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "internal Sleeper id" in detail
    assert "Matthew Stafford" in detail
    mock_connection.assert_not_called()


def test_trigger_waiver_claim_rejects_a_drop_player_id_absent_from_the_crosswalk(client):
    """drop_player_id carries no name to cross-check, so the guard can't disambiguate an
    id present in both namespaces - but an id present ONLY as an internal player_id is still
    decidably wrong and must still be rejected."""
    cursor = MagicMock()
    # add player_id (4046) resolves cleanly against espn_id; drop_player_id (9999) is a
    # bare internal id with no espn_id row at all.
    cursor.fetchone.side_effect = [("Some Back",), None, None, ("Some Bench Guy",)]

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection") as mock_connection:
        response = client.post("/v1/trigger/waiver", json={
            "team_id": "7", "player_id": "4046", "name": "Some Back", "team": "KC",
            "drop_player_id": "9999",
        })

    assert response.status_code == 422
    assert "internal Sleeper id" in response.json()["detail"]
    mock_connection.assert_not_called()


def test_trigger_lineup_swap_records_bot_decision_per_swap(client):
    """CORE-17 (#35): each swap in the payload carries its own decision_inputs (VORP for
    the added/dropped player), so a multi-swap request must produce one bot_decisions row
    per swap, not one row for the whole request."""
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    cursor = MagicMock()

    payload = {
        "team_id": "1",
        "optimized_swaps": [
            {
                "drop_player_id": "4046", "add_player_id": "4983", "slot": "FLEX",
                "decision_inputs": {"add_player_vorp_score": 12.5, "drop_player_vorp_score": 3.1},
            },
            {
                "drop_player_id": "5000", "add_player_id": "5001", "slot": "WR2",
                "decision_inputs": {"add_player_vorp_score": 9.0, "drop_player_vorp_score": 2.0},
            },
        ],
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        response = client.post("/v1/trigger/lineup", json=payload)

    assert response.status_code == 202
    insert_calls = [
        c for c in cursor.execute.call_args_list
        if c.args and "INSERT INTO bot_decisions" in c.args[0]
    ]
    assert len(insert_calls) == 2

    _, params = insert_calls[0].args
    assert params[1] == "lineup_bot"
    assert params[2] == "lineup_swap"
    assert params[3] == "1"
    assert params[7] == "4983"  # target_player_id (add)
    assert params[8] == "4046"  # drop_player_id
    assert json.loads(params[9])["add_player_vorp_score"] == 12.5

    _, params2 = insert_calls[1].args
    assert params2[7] == "5001"
    assert params2[8] == "5000"


def test_trigger_lineup_swap_still_succeeds_when_bot_decision_insert_fails(client):
    """A DB hiccup logging the audit trail must never block the live lineup action itself."""
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    @contextmanager
    def _broken_db_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    payload = {
        "team_id": "1",
        "optimized_swaps": [{"drop_player_id": "4046", "add_player_id": "4983", "slot": "FLEX"}],
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes.get_db_cursor", _broken_db_cursor):
        response = client.post("/v1/trigger/lineup", json=payload)

    assert response.status_code == 202
    mock_channel.basic_publish.assert_called_once()


def test_trigger_waiver_claim_records_bot_decision(client):
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("Some Back",), None, ("Some Bench Guy",), None]

    payload = {
        "team_id": "7", "player_id": "4046", "drop_player_id": "9999",
        "name": "Some Back", "team": "KC",
        "decision_inputs": {
            "add_player_vorp_score": 8.0, "drop_player_vorp_score": 1.0,
            "season": 2026, "week": 3,
        },
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        response = client.post("/v1/trigger/waiver", json=payload)

    assert response.status_code == 202
    insert_calls = [
        c for c in cursor.execute.call_args_list
        if c.args and "INSERT INTO bot_decisions" in c.args[0]
    ]
    assert len(insert_calls) == 1
    _, params = insert_calls[0].args
    assert params[1] == "waiver_sniper"
    assert params[2] == "waiver_claim"
    assert params[3] == "7"
    assert params[5] == 2026  # season
    assert params[6] == 3     # week
    assert params[7] == "4046"
    assert params[8] == "9999"
    assert json.loads(params[9])["add_player_vorp_score"] == 8.0


def test_trigger_waiver_claim_still_succeeds_when_bot_decision_insert_fails(client):
    call_count = {"n": 0}

    @contextmanager
    def _cursor_then_broken():
        call_count["n"] += 1
        if call_count["n"] == 1:
            yield _espn_id_ok_cursor()
        else:
            raise Exception("db down")

    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    payload = {"team_id": "7", "player_id": "4046", "name": "Some Back", "team": "KC"}

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes.get_db_cursor", _cursor_then_broken):
        response = client.post("/v1/trigger/waiver", json=payload)

    assert response.status_code == 202
    mock_channel.basic_publish.assert_called_once()


def test_trigger_draft_pick_records_bot_decision(client):
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    cursor = _espn_id_ok_cursor()

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "league_id": "L1",
        "decision_inputs": {"vorp_score": 15.2, "season": 2026},
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 202
    insert_calls = [
        c for c in cursor.execute.call_args_list
        if c.args and "INSERT INTO bot_decisions" in c.args[0]
    ]
    assert len(insert_calls) == 1
    _, params = insert_calls[0].args
    assert params[1] == "draft_clicker"
    assert params[2] == "draft_pick"
    assert params[4] == "L1"  # league_id
    assert params[5] == 2026  # season
    assert params[7] == "4046"  # target_player_id
    assert params[8] is None   # no drop side for a draft pick
    assert json.loads(params[9])["vorp_score"] == 15.2


def test_trigger_draft_pick_does_not_forward_decision_inputs_to_the_click_bot(client):
    """decision_inputs is gateway-only state for the audit trail - the click bot has no
    use for it and forwarding it would bloat every draft_targets message for nothing."""
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC",
        "decision_inputs": {"vorp_score": 15.2, "season": 2026},
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 202
    body = json.loads(mock_channel.basic_publish.call_args.kwargs["body"])
    assert "decision_inputs" not in body


def test_trigger_draft_pick_does_not_record_bot_decision_on_duplicate(client):
    """No new publish happened on the duplicate branch, so there's nothing new to record."""
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_redis = _fake_redis()
    cursor = _espn_id_ok_cursor()

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "idempotency_key": "L1:5:4046",
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        client.post("/v1/trigger/draft-pick", json=payload)
        cursor.fetchone.side_effect = [("Some Back",), None]
        second = client.post("/v1/trigger/draft-pick", json=payload)

    assert second.status_code == 202
    assert second.json()["status"] == "duplicate"
    insert_calls = [
        c for c in cursor.execute.call_args_list
        if c.args and "INSERT INTO bot_decisions" in c.args[0]
    ]
    assert len(insert_calls) == 1


def test_trigger_waiver_claim_allows_a_drop_player_id_known_to_espn(client):
    """Without a name to cross-check, any id that resolves in players.espn_id is accepted
    outright - there's nothing decidable to reject."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("Some Back",), None, ("Anyone",), None]
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection):
        response = client.post("/v1/trigger/waiver", json={
            "team_id": "7", "player_id": "4046", "name": "Some Back", "team": "KC",
            "drop_player_id": "9999",
        })

    assert response.status_code == 202
    mock_channel.basic_publish.assert_called_once()
