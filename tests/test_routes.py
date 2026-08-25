from contextlib import contextmanager
from unittest.mock import MagicMock, patch


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


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


def test_trigger_draft_pick_idempotency_key_blocks_a_duplicate_enqueue(client):
    """A publish that fails ambiguously - read timeout, or a reset after the bytes went out -
    may already have committed a durable message. Without this the caller's retry queues a
    second draft action and the worker clicks twice."""
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_redis = MagicMock()
    # First claim wins, second finds the key already held.
    mock_redis.set.side_effect = [True, None]

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
    assert mock_redis.set.call_args.kwargs["nx"] is True


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


def test_trigger_draft_pick_releases_the_claim_when_the_broker_fails_before_publishing(client):
    """A pre-publish broker failure that kept the claim turned a transient error into a
    permanent one: the retry got "duplicate", hold_and_publish read that as success and
    stopped, and the pick was ceded to autodraft by the guard meant to protect it."""
    mock_redis = MagicMock()
    mock_redis.set.return_value = True

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "idempotency_key": "L1:5:4046",
    }

    with patch("src.api.routes.pika.BlockingConnection", side_effect=Exception("broker down")), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 500
    mock_redis.delete.assert_called_once_with("draft:trigger:idempotency:L1:5:4046")


def test_trigger_draft_pick_keeps_the_claim_when_the_failure_is_ambiguous(client):
    """A failure at or after basic_publish may have committed the message. Releasing then
    would allow a genuine duplicate, so the claim stands."""
    mock_channel = MagicMock()
    mock_channel.basic_publish.side_effect = Exception("reset after send")
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel
    mock_redis = MagicMock()
    mock_redis.set.return_value = True

    payload = {
        "team_id": "7", "player_id": "4046", "name": "Some Back",
        "position": "RB", "pro_team": "KC", "idempotency_key": "L1:5:4046",
    }

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/draft-pick", json=payload)

    assert response.status_code == 500
    mock_redis.delete.assert_not_called()


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
