import pytest
import time

from src.api import routes
import json
from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


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




def test_trigger_lineup_swap_validates_payload(client):
    response = client.post("/v1/trigger/lineup", json={"team_id": "1"})

    assert response.status_code == 422
















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




def test_trigger_draft_pick_release_does_not_drop_a_retrys_fresh_claim(client):
    """A blind DELETE would drop the claim a retry already took, letting a third request
    enqueue the same action. The release is scoped to the releasing request's own token."""
    mock_redis = _fake_redis()
    mock_redis.store["draft:trigger:idempotency:L1:5:4046"] = "pending:someone-else"

    with patch("src.api.routes._redis_client", return_value=mock_redis):
        routes._release_idempotency_key("L1:5:4046", "my-token")

    assert mock_redis.store["draft:trigger:idempotency:L1:5:4046"] == "pending:someone-else"












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
    assert response.json()["tracking_id"] == "identity-test"


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
    assert response.json()["tracking_id"] == "identity-test"


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
    assert response.json()["tracking_id"] == "identity-test"


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
    assert response.json()["tracking_id"] == "identity-test"


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
    assert response.json()["tracking_id"] == "identity-test"


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




def test_trigger_waiver_claim_allows_an_add_only_claim_with_no_drop_player(client):
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    payload = {"team_id": "7", "player_id": "4046", "name": "Some Back", "team": "KC"}

    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_espn_id_ok_cursor())):
        response = client.post("/v1/trigger/waiver", json=payload)

    assert response.status_code == 202
    assert response.json()["tracking_id"] == "identity-test"




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


def test_trigger_lineup_swap_rejects_an_internal_sleeper_drop_id(client):
    """#187: the optimizer published players.player_id 2216 as drop_player_id. An id present
    only as an internal player_id is decidably the wrong namespace, name or no name."""
    cursor = MagicMock()
    # drop 2216: no espn_id row, but an internal player_id row.
    cursor.fetchone.side_effect = [None, ("Some Starter",)]

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection") as mock_connection:
        response = client.post("/v1/trigger/lineup", json={
            "team_id": "8",
            "optimized_swaps": [{"drop_player_id": "2216", "add_player_id": "4983", "slot": "FLEX"}],
        })

    assert response.status_code == 422
    assert "internal Sleeper id" in response.json()["detail"]
    mock_connection.assert_not_called()


def test_trigger_lineup_swap_rejects_the_real_stafford_bech_collision(client):
    """The overlap case: 12483 is Stafford's espn_id AND Bech's internal player_id. A swap
    meaning Bech would pass the worker's roster check if Stafford were rostered, so with the
    name supplied the gateway must reject it rather than bench/start the wrong player."""
    cursor = MagicMock()
    # drop 4046 resolves cleanly as an espn_id; add 12483 lives in both namespaces.
    cursor.fetchone.side_effect = [("Some Starter",), None, ("Matthew Stafford",), ("Jack Bech",)]

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection") as mock_connection:
        response = client.post("/v1/trigger/lineup", json={
            "team_id": "8",
            "optimized_swaps": [{
                "drop_player_id": "4046", "drop_player_name": "Some Starter",
                "add_player_id": "12483", "add_player_name": "Jack Bech", "slot": "WR2",
            }],
        })

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "internal Sleeper id" in detail
    assert "Matthew Stafford" in detail
    mock_connection.assert_not_called()


def test_trigger_lineup_swap_allows_the_colliding_id_when_the_name_is_the_espn_player(client):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("Some Starter",), None, ("Matthew Stafford",), ("Jack Bech",)]
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection):
        response = client.post("/v1/trigger/lineup", json={
            "team_id": "8",
            "optimized_swaps": [{
                "drop_player_id": "4046", "drop_player_name": "Some Starter",
                "add_player_id": "12483", "add_player_name": "Matthew Stafford", "slot": "QB",
            }],
        })

    assert response.status_code == 202
    assert response.json()["tracking_id"] == "identity-test"


def test_trigger_lineup_swap_allows_unnamed_espn_ids_from_the_web_ui(client):
    """The web UI's lineup editor sends ESPN ids without names; those must keep working."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("Some Starter",), None, ("Some Back",), None]
    mock_channel = MagicMock()
    mock_connection = MagicMock()
    mock_connection.channel.return_value = mock_channel

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection):
        response = client.post("/v1/trigger/lineup", json={
            "team_id": "8",
            "optimized_swaps": [{"drop_player_id": "4046", "add_player_id": "4983", "slot": "FLEX"}],
        })

    assert response.status_code == 202
    assert response.json()["tracking_id"] == "identity-test"










def test_record_bot_decision_commits_the_insert_through_the_real_cursor_helper():
    """#159: the table was empty in prod, so prove the row is actually committed - not just
    that an INSERT string was built. Goes through the real get_db_cursor/get_db_connection,
    faking only the pool, so a helper that forgot to commit would fail here."""
    conn = MagicMock()
    cursor = conn.cursor.return_value

    with patch("src.database.db_pool") as mock_pool:
        mock_pool.getconn.return_value = conn
        routes._record_bot_decision(
            tracking_id="t-1", bot_name="waiver_sniper", action="waiver_claim", team_id="7",
            target_player_id="4046", decision_inputs={"season": 2026, "week": 4},
        )

    sql, params = cursor.execute.call_args.args
    assert "INSERT INTO bot_decisions" in sql
    assert params[:3] == ("t-1", "waiver_sniper", "waiver_claim")
    assert (params[5], params[6]) == (2026, 4)
    conn.commit.assert_called_once()
    conn.rollback.assert_not_called()
    mock_pool.putconn.assert_called_once_with(conn)


def test_record_bot_decision_failure_is_logged_as_error_and_marked_on_the_span(caplog):
    """Fail-open is deliberate (the live action must not be blocked), but #159 showed a
    WARNING-only swallow is too easy to miss: the failure must surface at ERROR with the
    traceback and as a span event, so a dropped audit row is visible in logs and Tempo."""
    @contextmanager
    def _broken_db_cursor():
        raise Exception("relation \"bot_decisions\" does not exist")
        yield  # pragma: no cover

    # A local provider rather than the global one - see the dm.tracer note in
    # test_draft_matrix.py; get_current_span() resolves through context either way.
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    with patch("src.api.routes.get_db_cursor", _broken_db_cursor), \
         caplog.at_level("ERROR", logger="GatewayRoutes"), \
         provider.get_tracer("test").start_as_current_span("POST /v1/trigger/lineup"):
        routes._record_bot_decision(
            tracking_id="t-2", bot_name="lineup_bot", action="lineup_swap", team_id="8",
        )

    [record] = [r for r in caplog.records if "bot_decisions" in r.getMessage()]
    assert record.levelname == "ERROR"
    assert "t-2" in record.getMessage()
    assert record.exc_info is not None
    [span] = exporter.get_finished_spans()
    # Semantic-convention exception event (type + stack), so Tempo/Grafana index it...
    [event] = span.events
    assert event.name == "exception"
    assert event.attributes["exception.type"] == "Exception"
    assert "bot_decisions" in event.attributes["exception.message"]
    assert event.attributes["bot_name"] == "lineup_bot"
    assert event.attributes["tracking_id"] == "t-2"
    # ...plus a flat attribute to query the gap by name.
    assert span.attributes["bot_decision.write_failed"] is True


def test_record_bot_decision_stays_fail_open_when_telemetry_itself_raises():
    """Runs after the AMQP publish has committed: if the telemetry calls in the failure
    path raised, the route would 500 an already-published action and a retry would
    publish it twice. The failure report is best-effort; the helper must never raise."""
    @contextmanager
    def _broken_db_cursor():
        raise Exception("db down")
        yield  # pragma: no cover

    broken_span = MagicMock()
    # Keeps src/telemetry.py's TraceIdLogFilter (which also calls get_current_span) working.
    broken_span.get_span_context.return_value = routes.trace.INVALID_SPAN_CONTEXT
    broken_span.record_exception.side_effect = RuntimeError("exporter blew up")
    broken_span.set_attribute.side_effect = RuntimeError("exporter blew up")

    with patch("src.api.routes.get_db_cursor", _broken_db_cursor), \
         patch.object(routes.trace, "get_current_span", return_value=broken_span):
        routes._record_bot_decision(
            tracking_id="t-3", bot_name="waiver_sniper", action="waiver_claim", team_id="7",
        )








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
    assert response.json()["tracking_id"] == "identity-test"


# ---------------------------------------------------------------------------
# /trigger/lineup idempotency (#189)
# ---------------------------------------------------------------------------

LINEUP_KEYED_PAYLOAD = {
    "team_id": "8",
    "optimized_swaps": [{"drop_player_id": "16737", "add_player_id": "4360761", "slot": "WR"}],
    "idempotency_key": "lineup-8-abc",
}


def _namespace_ok_cursor():
    """Every id resolves as an espn_id and no name is sent, so the namespace guard passes."""
    cursor = MagicMock()
    cursor.fetchone.return_value = ("Somebody",)
    return cursor


def _post_lineup(client, mock_connection, mock_redis, payload=LINEUP_KEYED_PAYLOAD):
    with patch("src.api.routes.pika.BlockingConnection", return_value=mock_connection), \
         patch("src.api.routes._redis_client", return_value=mock_redis), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(_namespace_ok_cursor())):
        return client.post("/v1/trigger/lineup", json=payload)












@pytest.fixture(autouse=True)
def isolate_durable_storage_for_identity_tests(request):
    # These tests exercise namespace/auth guards; persistence and broker delivery have
    # dedicated transactional tests in test_dashboard_ux.py and test_queue_declarations.py.
    if any(word in request.node.name for word in ('allows', 'tolerates_name', 'guard_lookup_errors')):
        with patch('src.api.routes._accept_durable', return_value={'status':'accepted','tracking_id':'identity-test'}):
            yield
    else:
        yield


@pytest.mark.parametrize('queue,payload',[
    ('waiver_actions',routes.WaiverTriggerPayload(team_id='8',player_id='4046',name='Player',team='KC')),
    ('lineup_updates',routes.LineupTriggerPayload(team_id='8',optimized_swaps=[{'add_player_id':'4046','drop_player_id':'9999','slot':'RB'}])),
    ('draft_targets',routes.DraftPickTriggerPayload(team_id='8',player_id='4046',name='Player',position='RB',pro_team='KC')),
])
def test_acceptance_persists_before_delivery_and_returns_original_identity(queue,payload):
    order=[]
    def create(q,body,identity):
        order.append('persist');return 'original-action'
    def publish(identity):
        assert identity=='original-action';order.append('publish')
    with patch('src.services.activity_service.create_action',side_effect=create),patch('src.services.activity_runner.publish_one',side_effect=publish):
        result=routes._accept_durable(queue,payload)
    assert order==['persist','publish']
    assert result['tracking_id']=='original-action'


def test_durable_storage_failure_does_not_publish():
    from fastapi import HTTPException
    payload=routes.WaiverTriggerPayload(team_id='8',player_id='4046',name='Player',team='KC')
    with patch('src.services.activity_service.create_action',side_effect=RuntimeError('DB down')),patch('src.services.activity_runner.publish_one') as publish:
        with pytest.raises(HTTPException) as error: routes._accept_durable('waiver_actions',payload)
        assert error.value.status_code==503
        publish.assert_not_called()


def test_broker_failure_keeps_durable_action_accepted():
    payload=routes.WaiverTriggerPayload(team_id='8',player_id='4046',name='Player',team='KC')
    with patch('src.services.activity_service.create_action',return_value='saved'),patch('src.services.activity_runner.publish_one',side_effect=RuntimeError('broker down')):
        assert routes._accept_durable('waiver_actions',payload)['tracking_id']=='saved'


def test_new_actions_keep_decision_audit_and_duplicates_do_not_repeat_it():
    payload=routes.WaiverTriggerPayload(team_id='8',player_id='4046',name='Player',team='KC',decision_inputs={'week':5})
    with patch('src.services.activity_service.create_action',side_effect=lambda q,b,i:i),patch('src.services.activity_runner.publish_one'),patch('src.api.routes._record_bot_decision') as audit:
        routes._accept_durable('waiver_actions',payload)
        assert audit.call_args.kwargs['decision_inputs']=={'week':5}
    with patch('src.services.activity_service.create_action',return_value='previous'),patch('src.services.activity_runner.publish_one'),patch('src.api.routes._record_bot_decision') as audit:
        routes._accept_durable('waiver_actions',payload)
        audit.assert_not_called()
