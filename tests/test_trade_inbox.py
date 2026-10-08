"""Incoming trade contracts with transactional storage and broker boundaries mocked."""

import copy
import hashlib
import json
import re
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.services import trade_inbox_service as service

NOW = datetime(2026, 10, 7, 18, tzinfo=timezone.utc)
BASE = "/v1/trade/incoming/123"


@pytest.mark.parametrize(
    "fixture",
    json.loads(
        (Path(__file__).parent / "fixtures/trade_response_keys.json").read_text()
    ),
)
def test_actual_worker_hash_vectors(fixture):
    value = fixture["input"]
    assert (
        service.compute_decision_key(
            value["leagueId"],
            value["seasonId"],
            value["ourTeamId"],
            value["transactionId"],
            value["action"],
            value["fingerprint"],
            value["comment"],
        )
        == fixture["key"]
    )


def row(tx="123", status="open", final=4.0):
    return dict(
        id=7,
        status=status,
        league_id="456",
        season=2026,
        executed_at=NOW,
        outcome_details={},
        counter_offer=dict(
            espn_transaction_id=tx,
            related_transaction_id=None,
            thread_root_id=tx,
            proposing_team_id="3",
            our_team_id="8",
            espn_type="TRADE_PROPOSAL",
            espn_status="PENDING",
            send_espn_ids=["999"],
            receive_espn_ids=["888"],
            other_items=[],
            player_names={"999": "Roster name", "888": "Other player"},
            proposed_date=1791388800000,
            expiration_date=int((NOW + timedelta(hours=1)).timestamp() * 1000),
            comment="Their note",
            fingerprint="a" * 64,
            parse_issues=[],
            triage={"state": "new"},
            decision=None,
            shadow_policy=None,
            first_seen_at=NOW.isoformat(),
            last_seen_at=NOW.isoformat(),
            missed_reads=0,
            score=dict(
                state="scored",
                scoring_version="trade-score-v1",
                as_of_week=5,
                final_score=final,
                complete_horizon=True,
                unavailable_reasons=[],
                raw_delta_vorp=3.0,
                exceeds_roster_limit=False,
                unmapped_espn_ids=[],
                partial_reasons=[],
                players={
                    "999": {"player_id": "111", "name": "Send"},
                    "888": {"player_id": "222", "name": "Get"},
                },
                scored_fingerprint="a" * 64,
                scored_at=NOW.isoformat(),
                retry=False,
                stale=False,
                error=None,
            ),
        ),
    )


@pytest.fixture
def boundary(monkeypatch):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "456")
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    rows = [row()]
    lock = threading.Lock()
    cursor = MagicMock()

    @contextmanager
    def transaction():
        with lock:
            original = copy.deepcopy(rows)
            try:
                yield cursor
            except Exception:
                rows[:] = original
                raise

    def read(cur, league, tx):
        return copy.deepcopy(
            [
                r
                for r in rows
                if r["league_id"] == league
                and r["counter_offer"]["espn_transaction_id"] == tx
            ]
        )

    def save(cur, row_id, key, claim):
        target = next(r for r in rows if r["id"] == row_id)
        target["outcome_details"].setdefault("incoming_publications", {})[key] = (
            copy.deepcopy(claim)
        )

    def triage_save(cur, row_id, value):
        next(r for r in rows if r["id"] == row_id)["counter_offer"]["triage"] = (
            copy.deepcopy(value)
        )

    with (
        patch.object(service, "get_db_cursor", transaction),
        patch.object(service, "_read_locked", side_effect=read),
        patch.object(service, "_save_publication", side_effect=save),
        patch.object(service, "_save_triage", side_effect=triage_save),
        patch.object(service, "_worker_response_exists", return_value=False),
        patch.object(service, "_now", return_value=NOW),
        patch("src.api.routes._record_bot_decision") as audit,
        patch("src.api.routes.pika.BlockingConnection") as connection,
    ):
        yield rows, connection, audit, cursor


def token(boundary):
    """The review token of the offer as currently stored: what a fresh GET would show."""
    return service.compute_review_token(boundary[0][0])


def reviewed(boundary, payload):
    return {**payload, "review_token": token(boundary)}


@pytest.mark.parametrize(
    "comment,normalized", [(None, None), ("  ", None), ("  Yes ☃  ", "Yes ☃")]
)
def test_decision_key_worker_json(comment, normalized):
    canonical = [
        "trade-response-v1",
        "456",
        "2026",
        "8",
        "123",
        "accept",
        "a" * 64,
        normalized,
    ]
    expected = hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    assert (
        service.compute_decision_key(
            "456", 2026, "8", "123", "accept", "a" * 64, comment
        )
        == expected
    )


def test_decision_receipt_payload_and_duplicate(client, boundary):
    rows, connection, audit, cursor = boundary
    for code, approver in [(202, " human "), (200, "another")]:
        result = client.post(
            BASE + "/decision",
            json=reviewed(
                boundary,
                {"action": "accept", "approved_by": approver, "comment": " Yes "},
            ),
        )
        assert result.status_code == code, result.text
        receipt = result.json()
        assert receipt["status"] == "queued"
        assert receipt["approved_by"] == "human"
        assert receipt["duplicate"] == (code == 200)
    channel = connection.return_value.channel.return_value
    channel.basic_publish.assert_called_once()
    sent = json.loads(channel.basic_publish.call_args.kwargs["body"])
    assert sent == dict(
        tracking_id=receipt["tracking_id"],
        espn_transaction_id="123",
        action="accept",
        comment="Yes",
        approval=dict(decision_key=receipt["decision_key"], approved_by="human"),
        traceparent=sent["traceparent"],
    )
    assert channel.basic_publish.call_args.kwargs["routing_key"] == "trade_responses"
    channel.queue_declare.assert_called_once_with(queue="trade_responses", durable=True)
    channel.confirm_delivery.assert_called_once()
    assert channel.basic_publish.call_args.kwargs["mandatory"] is True
    assert rows[0]["status"] == "open"
    assert rows[0]["counter_offer"]["decision"] is None
    audit.assert_called_once()
    assert audit.call_args.kwargs["cursor"] is cursor
    assert any(
        call.args[1] == ("trade-inbox:456:123",)
        for call in cursor.execute.call_args_list
    )


@pytest.mark.parametrize(
    "status",
    [
        "unparsed",
        "responding",
        "accepted_pending_processing",
        "accepted",
        "declined",
        "countered",
        "vetoed",
        "expired",
        "withdrawn",
        "closed_unknown",
    ],
)
@pytest.mark.parametrize(
    "kind,payload",
    [
        ("decision", {"action": "decline", "approved_by": "human"}),
        ("counter", {"approved_by": "human", "week": 5}),
    ],
)
def test_only_open_actionable(client, boundary, status, kind, payload):
    rows, connection, _, _ = boundary
    rows[0]["status"] = status
    assert (
        client.post(BASE + "/" + kind, json=reviewed(boundary, payload)).status_code
        == 409
    )
    connection.assert_not_called()


@pytest.mark.parametrize(
    "change",
    [
        "expired",
        "margin",
        "bad_expiry",
        "parse",
        "scope",
        "fingerprint",
        "no_score",
        "old_score",
        "over_limit",
    ],
)
def test_unsafe_accept_refused(client, boundary, change):
    rows, connection, _, _ = boundary
    entry = rows[0]["counter_offer"]
    if change in ("expired", "margin"):
        entry["expiration_date"] = int(
            (NOW + timedelta(minutes=0 if change == "expired" else 5)).timestamp()
            * 1000
        )
    elif change == "bad_expiry":
        entry["expiration_date"] = "garbage"
    elif change == "parse":
        entry["parse_issues"] = ["unknown"]
    elif change == "scope":
        entry["our_team_id"] = "9"
    elif change == "fingerprint":
        entry["fingerprint"] = "invalid"
    elif change == "no_score":
        entry["score"] = None
    elif change == "old_score":
        entry["score"]["scored_fingerprint"] = "b" * 64
    elif change == "over_limit":
        entry["score"]["exceeds_roster_limit"] = True
    assert (
        client.post(
            BASE + "/decision",
            json=reviewed(boundary, {"action": "accept", "approved_by": "human"}),
        ).status_code
        == 409
    )
    connection.assert_not_called()


def test_decline_does_not_need_score_or_send_comment(client, boundary):
    rows, connection, _, _ = boundary
    rows[0]["counter_offer"]["score"] = {"nonsense": True}
    assert (
        client.post(
            BASE + "/decision",
            json=reviewed(
                boundary, {"action": "decline", "approved_by": "human", "comment": " "}
            ),
        ).status_code
        == 202
    )
    assert "comment" not in json.loads(
        connection.return_value.channel.return_value.basic_publish.call_args.kwargs[
            "body"
        ]
    )


def test_counter_uses_score_mapping_and_blocks_competitor(client, boundary):
    rows, connection, _, _ = boundary
    for code in (202, 200):
        response = client.post(
            BASE + "/counter",
            json=reviewed(boundary, {"approved_by": "human", "week": 5}),
        )
        assert response.status_code == code, response.text
    channel = connection.return_value.channel.return_value
    sent = json.loads(channel.basic_publish.call_args.kwargs["body"])
    assert sent == dict(
        tracking_id=response.json()["tracking_id"],
        season=2026,
        week=5,
        offering_team_id="3",
        incoming_player_ids=["222"],
        outgoing_player_ids=["111"],
        traceparent=sent["traceparent"],
    )
    assert channel.basic_publish.call_args.kwargs["routing_key"] == "trade_negotiations"
    assert rows[0]["status"] == "open"
    assert (
        client.post(
            BASE + "/decision",
            json=reviewed(boundary, {"action": "decline", "approved_by": "human"}),
        ).status_code
        == 409
    )
    channel.basic_publish.assert_called_once()


@pytest.mark.parametrize("mapping", [None, "", 999])
def test_unmapped_counter_cannot_guess_internal_id(client, boundary, mapping):
    rows, connection, _, _ = boundary
    rows[0]["counter_offer"]["score"]["players"]["999"]["player_id"] = mapping
    assert (
        client.post(
            BASE + "/counter",
            json=reviewed(boundary, {"approved_by": "human", "week": 5}),
        ).status_code
        == 409
    )
    connection.assert_not_called()


def test_confirmed_duplicate_after_worker_settles(client, boundary):
    rows, connection, _, _ = boundary
    payload = {"action": "decline", "approved_by": "human"}
    assert (
        client.post(BASE + "/decision", json=reviewed(boundary, payload)).status_code
        == 202
    )
    rows[0]["status"] = "declined"
    assert (
        client.post(BASE + "/decision", json=reviewed(boundary, payload)).status_code
        == 200
    )
    assert (
        client.post(
            BASE + "/decision", json=reviewed(boundary, {**payload, "action": "accept"})
        ).status_code
        == 409
    )
    connection.return_value.channel.return_value.basic_publish.assert_called_once()


@pytest.mark.parametrize("kind", ["decision", "counter"])
def test_unknown_publication_blocks_replay(client, boundary, kind):
    _, connection, _, _ = boundary
    connection.return_value.channel.return_value.basic_publish.side_effect = (
        TimeoutError("unknown")
    )
    payload = {
        "approved_by": "human",
        **({"action": "decline"} if kind == "decision" else {"week": 5}),
    }
    assert (
        client.post(BASE + "/" + kind, json=reviewed(boundary, payload)).status_code
        == 503
    )
    assert (
        client.post(BASE + "/" + kind, json=reviewed(boundary, payload)).status_code
        == 409
    )
    connection.return_value.channel.return_value.basic_publish.assert_called_once()


def test_pre_publish_failure_can_retry(client, boundary):
    _, connection, _, _ = boundary
    connection.side_effect = ConnectionError("before sending")
    payload = {"action": "decline", "approved_by": "human"}
    assert (
        client.post(BASE + "/decision", json=reviewed(boundary, payload)).status_code
        == 503
    )
    connection.side_effect = None
    assert (
        client.post(BASE + "/decision", json=reviewed(boundary, payload)).status_code
        == 202
    )


def test_audit_failure_rolls_back_claim(boundary):
    rows, connection, audit, _ = boundary
    audit.side_effect = RuntimeError("audit unavailable")
    with pytest.raises(RuntimeError):
        service.decide("123", "decline", "human", review_token=token(boundary))
    assert rows[0]["outcome_details"] == {}
    connection.assert_not_called()


def test_concurrent_publish_blocks_other_request(boundary):
    _, connection, _, _ = boundary
    entered, release = threading.Event(), threading.Event()

    def publish(**kwargs):
        entered.set()
        assert release.wait(5)

    connection.return_value.channel.return_value.basic_publish.side_effect = publish
    errors = []

    def run():
        try:
            service.decide("123", "decline", "human", review_token=token(boundary))
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert entered.wait(5)
        with pytest.raises(service.InboxConflict):
            service.decide("123", "accept", "other", review_token=token(boundary))
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive()
    assert not errors


def test_confirmation_preserves_worker_update(client, boundary):
    rows, connection, _, _ = boundary

    def publish(**kwargs):
        rows[0]["status"] = "declined"
        rows[0]["counter_offer"]["decision"] = {"outcome": "declined"}
        rows[0]["outcome_details"]["worker_extra"] = 1

    connection.return_value.channel.return_value.basic_publish.side_effect = publish
    assert (
        client.post(
            BASE + "/decision",
            json=reviewed(boundary, {"action": "decline", "approved_by": "human"}),
        ).status_code
        == 202
    )
    assert rows[0]["status"] == "declined"
    assert rows[0]["counter_offer"]["decision"] == {"outcome": "declined"}
    assert rows[0]["outcome_details"]["worker_extra"] == 1


def test_confirmation_save_failure_blocks_republish(client, boundary):
    rows, connection, _, _ = boundary
    save = service._save_publication.side_effect

    def fail_confirmation(cursor, row_id, key, claim):
        if claim["status"] == "queued":
            raise RuntimeError("db unavailable")
        save(cursor, row_id, key, claim)

    service._save_publication.side_effect = fail_confirmation
    with pytest.raises(RuntimeError):
        service.decide("123", "decline", "human", review_token=token(boundary))
    service._save_publication.side_effect = save
    assert (
        client.post(
            BASE + "/decision",
            json=reviewed(boundary, {"action": "decline", "approved_by": "human"}),
        ).status_code
        == 409
    )
    connection.return_value.channel.return_value.basic_publish.assert_called_once()


@pytest.mark.parametrize("status", ["open", "unparsed", "responding", "declined"])
def test_triage_preserves_state(client, boundary, status):
    rows, connection, audit, _ = boundary
    rows[0]["status"] = status
    response = client.post(
        BASE + "/triage",
        json={
            "state": "snoozed",
            "by": " human ",
            "snooze_until": (NOW + timedelta(hours=1)).isoformat(),
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == status
    assert rows[0]["counter_offer"]["triage"]["by"] == "human"
    connection.assert_not_called()
    audit.assert_not_called()


@pytest.mark.parametrize(
    "payload",
    [
        {"state": "snoozed", "by": "human"},
        {"state": "snoozed", "by": "human", "snooze_until": NOW.isoformat()},
        {"state": "seen", "by": " "},
    ],
)
def test_bad_triage(client, boundary, payload):
    assert client.post(BASE + "/triage", json=payload).status_code == 422


@pytest.mark.parametrize(
    "kind,payload",
    [
        ("triage", {"state": "seen", "by": "human"}),
        ("decision", {"action": "decline", "approved_by": "human"}),
        ("counter", {"approved_by": "human", "week": 5}),
    ],
)
def test_write_auth_and_not_found(client, boundary, kind, payload):
    rows, connection, _, _ = boundary
    if kind != "triage":
        payload = reviewed(boundary, payload)
    assert (
        client.post(
            BASE + "/" + kind, json=payload, headers={"X-API-Key": "wrong"}
        ).status_code
        == 401
    )
    rows.clear()
    assert client.post(BASE + "/" + kind, json=payload).status_code == 404
    connection.assert_not_called()


def test_sql_lock_lookup_and_json_updates():
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    service._read_locked(cursor, "456", "123")
    sql, params = cursor.execute.call_args.args
    assert "FOR UPDATE" in sql and "trade_inbox" in sql
    assert params == ("456", "123")
    assert "season =" not in sql
    service._save_triage(cursor, 7, {"state": "seen"})
    assert "jsonb_set" in cursor.execute.call_args.args[0]
    assert "status =" not in cursor.execute.call_args.args[0]
    service._save_publication(cursor, 7, "key", {"status": "pending"})
    assert "outcome_details" in cursor.execute.call_args.args[0]
    assert "counter_offer =" not in cursor.execute.call_args.args[0]


def test_list_rank_filter_and_paging(client, boundary):
    entries = [
        row("20", final=-1),
        row("10", final=0),
        row("2", final=0),
        row("30", final=None),
        row("40", status="declined", final=100),
    ]
    entries[2]["counter_offer"]["triage"] = {
        "state": "snoozed",
        "snooze_until": NOW.isoformat(),
    }
    entries[3]["counter_offer"]["triage"] = {"state": "dismissed"}
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [tuple(r[c] for c in service.ROW_COLUMNS) for r in entries],
        [("999", "Database name")],
        [],
    ]
    with patch.object(service, "get_db_cursor") as db:
        db.return_value.__enter__.return_value = cursor
        response = client.get("/v1/trade/incoming?limit=2&offset=1")
    assert response.status_code == 200, response.text
    offers = response.json()["offers"]
    assert [e["espn_transaction_id"] for e in offers] == ["2", "20"]
    assert offers[0]["send_players"] == [{"espn_id": "999", "name": "Database name"}]
    assert offers[0]["receive_players"] == [{"espn_id": "888", "name": "Other player"}]
    assert offers[0]["expiration_date"].endswith("Z")
    assert response.json()["limit"] == 2


@pytest.mark.parametrize(
    "bad_score",
    [{"scoring_version": "future"}, {"final_score": "99"}, {"final_score": True}],
)
def test_invalid_advisory_does_not_break_list(client, boundary, caplog, bad_score):
    rows, _, _, _ = boundary
    rows[0]["counter_offer"]["score"].update(bad_score)
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [tuple(rows[0][c] for c in service.ROW_COLUMNS)],
        [],
        [],
    ]
    with patch.object(service, "get_db_cursor") as db:
        db.return_value.__enter__.return_value = cursor
        response = client.get("/v1/trade/incoming")
    assert response.status_code == 200
    assert response.json()["offers"][0]["score"] is None
    assert "score" in caplog.text


@pytest.mark.parametrize(
    "query",
    [
        "status=bogus",
        "visibility=bogus",
        "limit=0",
        "limit=201",
        "offset=-1",
        "season=1",
    ],
)
def test_bad_list_filters(client, query):
    assert client.get("/v1/trade/incoming?" + query).status_code == 422


@pytest.mark.parametrize("kind", ["decision", "counter"])
def test_duplicate_inbox_rows_fail_closed(client, boundary, kind):
    rows, connection, _, _ = boundary
    rows.append({**copy.deepcopy(rows[0]), "id": 8})
    payload = {
        "approved_by": "human",
        **({"action": "decline"} if kind == "decision" else {"week": 5}),
    }
    assert (
        client.post(BASE + "/" + kind, json=reviewed(boundary, payload)).status_code
        == 409
    )
    connection.assert_not_called()


def test_existing_worker_response_blocks_new_action(client, boundary):
    _, connection, _, _ = boundary
    service._worker_response_exists.return_value = True
    assert (
        client.post(
            BASE + "/decision",
            json=reviewed(boundary, {"action": "decline", "approved_by": "human"}),
        ).status_code
        == 409
    )
    connection.assert_not_called()


def test_same_queued_key_never_republishes_blocked_response(client, boundary):
    rows, connection, _, _ = boundary
    payload = {"action": "decline", "approved_by": "human"}
    assert (
        client.post(BASE + "/decision", json=reviewed(boundary, payload)).status_code
        == 202
    )
    rows[0]["counter_offer"]["decision"] = {
        "outcome": "blocked",
        "closed_locks": ["not_armed"],
    }
    assert (
        client.post(BASE + "/decision", json=reviewed(boundary, payload)).status_code
        == 200
    )
    assert (
        client.post(
            BASE + "/decision",
            json=reviewed(boundary, {**payload, "comment": "changed"}),
        ).status_code
        == 409
    )
    connection.return_value.channel.return_value.basic_publish.assert_called_once()


def test_fingerprint_change_cannot_release_pending_send(client, boundary):
    rows, connection, _, _ = boundary
    connection.return_value.channel.return_value.basic_publish.side_effect = (
        TimeoutError("unknown")
    )
    payload = {"action": "decline", "approved_by": "human"}
    assert (
        client.post(BASE + "/decision", json=reviewed(boundary, payload)).status_code
        == 503
    )
    rows[0]["counter_offer"]["fingerprint"] = "b" * 64
    assert (
        client.post(BASE + "/decision", json=reviewed(boundary, payload)).status_code
        == 409
    )
    connection.return_value.channel.return_value.basic_publish.assert_called_once()


def test_finalization_rejects_replaced_claim(boundary):
    rows, connection, _, _ = boundary

    def publish(**kwargs):
        claim = next(iter(rows[0]["outcome_details"]["incoming_publications"].values()))
        claim["tracking_id"] = "changed"

    connection.return_value.channel.return_value.basic_publish.side_effect = publish
    with pytest.raises(service.InboxConflict, match="claim changed"):
        service.decide("123", "decline", "human", review_token=token(boundary))
    assert (
        next(iter(rows[0]["outcome_details"]["incoming_publications"].values()))[
            "status"
        ]
        == "pending"
    )


@pytest.mark.parametrize(
    "kind,payload",
    [
        ("decision", {"action": "accept", "approved_by": " "}),
        ("decision", {"action": "accept", "approved_by": "human", "score": {}}),
        ("decision", {"action": "accept"}),
        ("counter", {"approved_by": "human"}),
        ("counter", {"approved_by": "human", "week": 19}),
        (
            "counter",
            {"approved_by": "human", "week": 5, "incoming_player_ids": ["999"]},
        ),
    ],
)
def test_bad_action_requests(client, boundary, kind, payload):
    _, connection, _, _ = boundary
    assert (
        client.post(BASE + "/" + kind, json=reviewed(boundary, payload)).status_code
        == 422
    )
    connection.assert_not_called()


def test_missing_header_and_unconfigured_key(client, boundary, monkeypatch):
    from src.api import routes

    payload = {"action": "decline", "approved_by": "human"}
    key = client.headers.pop("X-API-Key")
    try:
        assert (
            client.post(
                BASE + "/decision", json=reviewed(boundary, payload)
            ).status_code
            == 422
        )
    finally:
        client.headers["X-API-Key"] = key
    monkeypatch.setattr(routes, "TRIGGER_API_KEY", None)
    assert (
        client.post(BASE + "/decision", json=reviewed(boundary, payload)).status_code
        == 500
    )
    boundary[1].assert_not_called()


def test_counter_rejects_overlapping_internal_mappings(client, boundary):
    rows, connection, _, _ = boundary
    rows[0]["counter_offer"]["score"]["players"]["888"]["player_id"] = "111"
    assert (
        client.post(
            BASE + "/counter",
            json=reviewed(boundary, {"approved_by": "human", "week": 5}),
        ).status_code
        == 409
    )
    connection.assert_not_called()


def test_triage_names_use_espn_lookup(client, boundary):
    _, _, _, cursor = boundary
    cursor.fetchall.side_effect = [[("999", "Database name")], []]
    response = client.post(BASE + "/triage", json={"state": "seen", "by": "human"})
    assert response.status_code == 200
    assert response.json()["send_players"][0]["name"] == "Database name"


@pytest.mark.parametrize(
    "status,visibility,expected",
    [
        ("live", "visible", ["2", "10"]),
        ("live", "all", ["3", "4", "2", "10"]),
        ("live", "hidden", ["3", "4"]),
        ("all", "all", ["6", "5", "3", "4", "2", "10"]),
        ("accepted_pending_processing", "all", ["5"]),
    ],
)
def test_visibility_and_history_filters(client, boundary, status, visibility, expected):
    rows = [
        row("10", final=-1),
        row("2", status="unparsed", final=0),
        row("3", final=1),
        row("4", final=1),
        row("5", status="accepted_pending_processing", final=2),
        row("6", status="accepted", final=3),
    ]
    rows[2]["counter_offer"]["triage"] = {"state": "dismissed"}
    rows[3]["counter_offer"]["triage"] = {
        "state": "snoozed",
        "snooze_until": (NOW + timedelta(seconds=1)).isoformat(),
    }
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [tuple(r[c] for c in service.ROW_COLUMNS) for r in rows],
        [],
        [],
    ]
    with patch.object(service, "get_db_cursor") as db:
        db.return_value.__enter__.return_value = cursor
        response = client.get(
            f"/v1/trade/incoming?status={status}&visibility={visibility}&league_id=456&season=2026"
        )
    assert response.status_code == 200, response.text
    assert [r["espn_transaction_id"] for r in response.json()["offers"]] == expected
    assert cursor.execute.call_args_list[0].args[1] == ["456", 2026]


def test_rank_incomplete_stale_scores_and_invalid_dates():
    rows = [
        row("10", final=None),
        row("2", final=2),
        row("3", final=2),
        row("4", final=True),
        row("5", final=-1),
    ]
    rows[1]["counter_offer"]["score"].update(stale=True, state="incomplete")
    rows[1]["counter_offer"]["expiration_date"] = None
    rows[2]["counter_offer"]["expiration_date"] = "bad"
    rows[1]["counter_offer"]["proposed_date"] += 1
    rows[3]["counter_offer"]["score"]["raw_delta_vorp"] = 99999
    assert [
        r["counter_offer"]["espn_transaction_id"]
        for r in sorted(rows, key=service._rank)
    ] == ["3", "2", "5", "10", "4"]


def test_listing_worker_outcome_and_claim_omits_publish_payload(client, boundary):
    rows, _, _, _ = boundary
    service.decide("123", "decline", "human", review_token=token(boundary))
    rows[0]["counter_offer"]["decision"] = {
        "outcome": "unknown",
        "verification": {"status": "unknown"},
    }
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [tuple(rows[0][c] for c in service.ROW_COLUMNS)],
        [],
        [],
    ]
    with patch.object(service, "get_db_cursor") as db:
        db.return_value.__enter__.return_value = cursor
        response = client.get("/v1/trade/incoming")
    assert response.status_code == 200, response.text
    offer = response.json()["offers"][0]
    assert offer["decision"]["outcome"] == "unknown"
    assert offer["publications"][0]["status"] == "queued"
    assert "payload" not in offer["publications"][0]


@pytest.mark.parametrize(
    "policy", [None, {"policy_version": "future", "reason": "private note"}]
)
def test_invalid_shadow_policy_does_not_break_list(boundary, caplog, policy):
    rows, _, _, _ = boundary
    rows[0]["counter_offer"]["shadow_policy"] = policy
    shaped = service._shape(rows[0])
    assert shaped["shadow_policy"] is None
    assert "private note" not in caplog.text


def test_sql_worker_claim_lookup():
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    assert service._worker_response_exists(cursor, "456", "123")
    sql, params = cursor.execute.call_args.args
    assert params == ("456", "123")
    assert "trade_response" in sql and "response_unknown" in sql and "rejected" in sql


def decline_key(comment=None, fingerprint="a" * 64):
    return service.compute_decision_key(
        "456", 2026, "8", "123", "decline", fingerprint, comment
    )


@pytest.fixture
def worker_responses(boundary):
    responses = [
        dict(
            id=41,
            status="rejected",
            decision_key=decline_key(),
            action="decline",
            comment=None,
        )
    ]
    with patch.object(
        service, "_worker_responses", side_effect=lambda *_: copy.deepcopy(responses)
    ):
        yield responses


def published(connection):
    channel = connection.return_value.channel.return_value
    return [json.loads(c.kwargs["body"]) for c in channel.basic_publish.call_args_list]


def test_retry_republishes_a_rejected_response_once(client, boundary, worker_responses):
    rows, connection, audit, _ = boundary
    original = service.decide("123", "decline", "human", review_token=token(boundary))
    for code in (202, 200):
        result = client.post(
            BASE + "/decision/retry",
            json=reviewed(boundary, {"approved_by": " retrier ", "of_claim_id": 41}),
        )
        assert result.status_code == code, result.text
        receipt = result.json()
        assert receipt["duplicate"] == (code == 200)
    assert receipt["retry_of"] == 41
    assert receipt["decision_key"] == decline_key()
    assert receipt["tracking_id"] != original["tracking_id"]
    sent = published(connection)
    assert len(sent) == 2
    assert sent[1] == dict(
        tracking_id=receipt["tracking_id"],
        espn_transaction_id="123",
        action="decline",
        approval=dict(decision_key=decline_key(), approved_by="retrier"),
        retry=dict(of_claim_id=41, approved_by="retrier", verified_not_answered=False),
        traceparent=sent[1]["traceparent"],
    )
    claims = rows[0]["outcome_details"]["incoming_publications"]
    assert claims[decline_key()]["tracking_id"] == original["tracking_id"]
    assert claims[f"{decline_key()}:retry:41"]["status"] == "queued"
    assert audit.call_args.kwargs["action"] == "trade_inbox_retry_decline"


def test_retry_keeps_the_approved_response_comment(client, boundary, worker_responses):
    _, connection, _, _ = boundary
    worker_responses[0].update(
        decision_key=decline_key("No thanks"), comment="No thanks"
    )
    result = client.post(
        BASE + "/decision/retry",
        json=reviewed(boundary, {"approved_by": "human", "of_claim_id": 41}),
    )
    assert result.status_code == 202, result.text
    assert published(connection)[0]["comment"] == "No thanks"
    assert published(connection)[0]["approval"]["decision_key"] == decline_key(
        "No thanks"
    )


def test_unknown_retry_needs_the_espn_check(client, boundary, worker_responses):
    rows, connection, _, _ = boundary
    worker_responses[0]["status"] = "response_unknown"
    rows[0]["status"] = "responding"
    rows[0]["counter_offer"]["decision"] = {
        "decision_key": decline_key(),
        "outcome": "unknown",
    }
    refused = client.post(
        BASE + "/decision/retry",
        json=reviewed(boundary, {"approved_by": "human", "of_claim_id": 41}),
    )
    assert refused.status_code == 409
    assert "checked ESPN" in refused.json()["detail"]
    assert published(connection) == []

    result = client.post(
        BASE + "/decision/retry",
        json=reviewed(
            boundary,
            {"approved_by": "human", "of_claim_id": 41, "verified_not_answered": True},
        ),
    )
    assert result.status_code == 202, result.text
    assert published(connection)[0]["retry"]["verified_not_answered"] is True


def test_unknown_retry_requires_its_own_responding_entry(
    client, boundary, worker_responses
):
    rows, connection, _, _ = boundary
    worker_responses[0]["status"] = "response_unknown"
    rows[0]["status"] = "responding"
    rows[0]["counter_offer"]["decision"] = {"decision_key": "b" * 64}
    result = client.post(
        BASE + "/decision/retry",
        json=reviewed(
            boundary,
            {"approved_by": "human", "of_claim_id": 41, "verified_not_answered": True},
        ),
    )
    assert result.status_code == 409
    assert published(connection) == []


@pytest.mark.parametrize("status", ["responding", "responded"])
def test_live_or_settled_responses_cannot_be_retried(
    client, boundary, worker_responses, status
):
    _, connection, _, _ = boundary
    worker_responses[0]["status"] = status
    result = client.post(
        BASE + "/decision/retry",
        json=reviewed(
            boundary,
            {"approved_by": "human", "of_claim_id": 41, "verified_not_answered": True},
        ),
    )
    assert result.status_code == 409
    assert published(connection) == []


def test_retry_refuses_unknown_or_stale_claim_ids(client, boundary, worker_responses):
    _, connection, _, _ = boundary
    missing = client.post(
        BASE + "/decision/retry",
        json=reviewed(boundary, {"approved_by": "human", "of_claim_id": 9}),
    )
    assert missing.status_code == 404
    worker_responses.append(
        dict(
            id=52,
            status="responding",
            decision_key=decline_key(),
            action="decline",
            comment=None,
        )
    )
    stale = client.post(
        BASE + "/decision/retry",
        json=reviewed(boundary, {"approved_by": "human", "of_claim_id": 41}),
    )
    assert stale.status_code == 409
    assert published(connection) == []


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r["counter_offer"].update(fingerprint="c" * 64),
        lambda r: r["counter_offer"].update(
            expiration_date=int((NOW + timedelta(minutes=4)).timestamp() * 1000)
        ),
        lambda r: r.update(status="declined"),
    ],
)
def test_retry_fails_closed_when_the_offer_changed(
    client, boundary, worker_responses, change
):
    rows, connection, _, _ = boundary
    change(rows[0])
    result = client.post(
        BASE + "/decision/retry",
        json=reviewed(boundary, {"approved_by": "human", "of_claim_id": 41}),
    )
    assert result.status_code == 409
    assert published(connection) == []


def test_retry_is_blocked_by_an_uncertain_publication(
    client, boundary, worker_responses
):
    rows, connection, _, _ = boundary
    rows[0]["outcome_details"]["incoming_publications"] = {
        "other": dict(
            kind="counter",
            status="pending",
            tracking_id="t",
            approved_by="h",
            approved_at=NOW.isoformat(),
            fingerprint="a" * 64,
        )
    }
    result = client.post(
        BASE + "/decision/retry",
        json=reviewed(boundary, {"approved_by": "human", "of_claim_id": 41}),
    )
    assert result.status_code == 409
    assert published(connection) == []


def test_accept_retry_rechecks_the_score(client, boundary, worker_responses):
    rows, connection, _, _ = boundary
    worker_responses[0].update(
        action="accept",
        decision_key=service.compute_decision_key(
            "456", 2026, "8", "123", "accept", "a" * 64
        ),
    )
    rows[0]["counter_offer"]["score"]["exceeds_roster_limit"] = True
    result = client.post(
        BASE + "/decision/retry",
        json=reviewed(boundary, {"approved_by": "human", "of_claim_id": 41}),
    )
    assert result.status_code == 409
    assert published(connection) == []


@pytest.mark.parametrize(
    "payload",
    [
        {"approved_by": "human"},
        {"approved_by": " ", "of_claim_id": 41},
        {"approved_by": "human", "of_claim_id": 0},
        {"approved_by": "human", "of_claim_id": 41, "extra": 1},
    ],
)
def test_bad_retry_requests(client, boundary, worker_responses, payload):
    assert (
        client.post(
            BASE + "/decision/retry", json=reviewed(boundary, payload)
        ).status_code
        == 422
    )


def test_retry_requires_the_api_key(client, boundary, worker_responses):
    result = client.post(
        BASE + "/decision/retry",
        json=reviewed(boundary, {"approved_by": "human", "of_claim_id": 41}),
        headers={"X-API-Key": "wrong"},
    )
    assert result.status_code == 401


def test_listing_exposes_the_latest_worker_response(client, boundary):
    rows, _, _, _ = boundary
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [tuple(rows[0][c] for c in service.ROW_COLUMNS)],
        [],
        [(41, "456", "123", "rejected", "decline", decline_key())],
    ]
    with patch.object(service, "get_db_cursor") as db:
        db.return_value.__enter__.return_value = cursor
        response = client.get("/v1/trade/incoming")
    assert response.status_code == 200, response.text
    assert response.json()["offers"][0]["response"] == dict(
        id=41, status="rejected", action="decline", decision_key=decline_key()
    )


def test_sql_worker_response_lookups():
    cursor = MagicMock()
    cursor.fetchall.return_value = [(41, "rejected", "k", "decline", None)]
    assert service._worker_responses(cursor, "456", "123") == [
        dict(id=41, status="rejected", decision_key="k", action="decline", comment=None)
    ]
    sql, params = cursor.execute.call_args.args
    assert params == ("456", "123")
    assert "trade_response" in sql and "rejected" in sql and "superseded" not in sql
    cursor.fetchall.return_value = [(41, "456", "123", "rejected", "decline", "k")]
    assert service._latest_responses(
        cursor, [dict(league_id="456", counter_offer={"espn_transaction_id": "123"})]
    ) == {
        ("456", "123"): dict(
            id=41, status="rejected", action="decline", decision_key="k"
        )
    }
    sql, params = cursor.execute.call_args.args
    assert "DISTINCT ON" in sql and params == (["123"],)


PUBLICATIONS = {
    "decision": ("/decision", {"action": "decline", "approved_by": "human"}),
    "counter": ("/counter", {"approved_by": "human", "week": 5}),
    "retry": ("/decision/retry", {"approved_by": "human", "of_claim_id": 41}),
}


def listed(client, rows):
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [tuple(r[c] for c in service.ROW_COLUMNS) for r in rows],
        [],
        [],
    ]
    with patch.object(service, "get_db_cursor") as db:
        db.return_value.__enter__.return_value = cursor
        response = client.get("/v1/trade/incoming")
    assert response.status_code == 200, response.text
    return response.json()["offers"]


def assert_nothing_claimed(boundary):
    rows, connection, audit, _ = boundary
    connection.assert_not_called()
    audit.assert_not_called()
    assert not rows[0]["outcome_details"].get("incoming_publications")


def test_get_exposes_review_token(client, boundary):
    rows, _, _, _ = boundary
    shown = listed(client, rows)[0]["review_token"]
    assert re.fullmatch(r"[0-9a-f]{64}", shown)
    assert shown == token(boundary)
    entry = rows[0]["counter_offer"]
    # Refresh bookkeeping, triage and rescoring aren't the offer the human reviewed.
    entry.update(last_seen_at=(NOW + timedelta(hours=1)).isoformat(), missed_reads=2)
    entry["score"].update(final_score=-9.0, as_of_week=6)
    entry["shadow_policy"] = None
    entry["player_names"] = {"999": "Renamed", "888": None}
    triaged = client.post(BASE + "/triage", json={"state": "seen", "by": "human"})
    assert triaged.status_code == 200, triaged.text
    assert triaged.json()["review_token"] == shown
    assert listed(client, rows)[0]["review_token"] == shown


@pytest.mark.parametrize("kind", PUBLICATIONS)
def test_fingerprint_change_between_get_and_post(
    client, boundary, worker_responses, kind
):
    rows, _, _, _ = boundary
    path, payload = PUBLICATIONS[kind]
    shown = listed(client, rows)[0]["review_token"]
    # A worker refresh replaces the offer after the human's read.
    rows[0]["counter_offer"].update(fingerprint="b" * 64, receive_espn_ids=["777"])
    rows[0]["counter_offer"]["score"]["scored_fingerprint"] = "b" * 64
    rows[0]["counter_offer"]["score"]["players"]["777"] = {
        "player_id": "333",
        "name": "Other",
    }
    worker_responses[0]["decision_key"] = decline_key(fingerprint="b" * 64)
    result = client.post(BASE + path, json={**payload, "review_token": shown})
    assert result.status_code == 409
    assert "changed since it was reviewed" in result.json()["detail"]
    assert shown not in result.text
    assert_nothing_claimed(boundary)


@pytest.mark.parametrize("kind", PUBLICATIONS)
def test_refresh_landing_while_waiting_for_lock(
    client, boundary, worker_responses, kind
):
    rows, _, _, _ = boundary
    path, payload = PUBLICATIONS[kind]
    shown = token(boundary)

    def worker_commits_first(*_):
        rows[0]["counter_offer"]["send_espn_ids"] = ["999", "555"]

    with patch.object(service, "_lock", side_effect=worker_commits_first):
        result = client.post(BASE + path, json={**payload, "review_token": shown})
    assert result.status_code == 409
    assert_nothing_claimed(boundary)


@pytest.mark.parametrize("kind", PUBLICATIONS)
@pytest.mark.parametrize(
    "before,after", [("Their note", "Different note"), ("x", "x "), (None, "")]
)
def test_note_only_change_is_rejected(
    client, boundary, worker_responses, kind, before, after
):
    rows, _, _, _ = boundary
    path, payload = PUBLICATIONS[kind]
    rows[0]["counter_offer"]["comment"] = before
    shown = token(boundary)
    rows[0]["counter_offer"]["comment"] = after
    # The note is outside the fingerprint, so the approval key alone can't see this.
    assert rows[0]["counter_offer"]["fingerprint"] == "a" * 64
    result = client.post(BASE + path, json={**payload, "review_token": shown})
    assert result.status_code == 409
    assert_nothing_claimed(boundary)


@pytest.mark.parametrize(
    "change",
    [
        lambda r: r["counter_offer"].update(send_espn_ids=["999", "555"]),
        lambda r: r["counter_offer"].update(receive_espn_ids=["777"]),
        lambda r: r["counter_offer"].update(
            other_items=[{"type": "DRAFT_PICK", "overallPickNumber": 3}]
        ),
        lambda r: r["counter_offer"].update(proposing_team_id="4"),
        lambda r: r["counter_offer"].update(fingerprint="b" * 64),
        lambda r: r["counter_offer"].update(comment=None),
        lambda r: r["counter_offer"].update(our_team_id="9"),
        lambda r: r["counter_offer"].update(espn_transaction_id="124"),
        lambda r: r.update(season=2027),
        lambda r: r.update(league_id="457"),
    ],
)
def test_each_reviewed_field_changes_the_token(change):
    original = row()
    changed = copy.deepcopy(original)
    change(changed)
    assert service.compute_review_token(changed) != service.compute_review_token(
        original
    )


def test_token_ignores_item_order_and_key_order():
    first, second = row(), row()
    first["counter_offer"].update(
        send_espn_ids=["1", "2"],
        other_items=[
            {"type": "DRAFT_PICK", "overallPickNumber": 3},
            {"type": "FAAB", "acquisitionBudget": 5},
        ],
    )
    second["counter_offer"].update(
        send_espn_ids=["2", "1"],
        other_items=[
            {"acquisitionBudget": 5, "type": "FAAB"},
            {"overallPickNumber": 3, "type": "DRAFT_PICK"},
        ],
    )
    assert service.compute_review_token(first) == service.compute_review_token(second)


def test_malformed_row_still_gets_a_token():
    broken = row()
    broken["counter_offer"].update(send_espn_ids="999", other_items=None)
    assert re.fullmatch(r"[0-9a-f]{64}", service.compute_review_token(broken))


@pytest.mark.parametrize("kind", PUBLICATIONS)
def test_stale_or_foreign_token(client, boundary, worker_responses, kind):
    path, payload = PUBLICATIONS[kind]
    for stale in (service.compute_review_token(row("124")), "f" * 64):
        result = client.post(BASE + path, json={**payload, "review_token": stale})
        assert result.status_code == 409
    for malformed in ("A" * 64, "a" * 63, "a" * 65, "", None):
        result = client.post(BASE + path, json={**payload, "review_token": malformed})
        assert result.status_code == 422
    assert client.post(BASE + path, json=payload).status_code == 422
    assert_nothing_claimed(boundary)


@pytest.mark.parametrize("kind", ["decision", "counter"])
def test_duplicate_receipt_preserved_with_token(client, boundary, kind):
    rows, connection, _, _ = boundary
    path, payload = PUBLICATIONS[kind]
    body = reviewed(boundary, payload)
    first = client.post(BASE + path, json=body)
    assert first.status_code == 202, first.text
    rows[0]["status"] = "declined" if kind == "decision" else "countered"
    rows[0]["counter_offer"]["last_seen_at"] = (NOW + timedelta(hours=1)).isoformat()
    again = client.post(BASE + path, json=body)
    assert again.status_code == 200, again.text
    assert again.json()["duplicate"] is True
    assert again.json()["tracking_id"] == first.json()["tracking_id"]
    connection.return_value.channel.return_value.basic_publish.assert_called_once()


def test_retry_duplicate_receipt_preserved_with_token(
    client, boundary, worker_responses
):
    _, connection, _, _ = boundary
    service.decide("123", "decline", "human", review_token=token(boundary))
    body = reviewed(boundary, PUBLICATIONS["retry"][1])
    assert client.post(BASE + "/decision/retry", json=body).status_code == 202
    again = client.post(BASE + "/decision/retry", json=body)
    assert again.status_code == 200 and again.json()["duplicate"] is True
    assert len(published(connection)) == 2


@pytest.mark.parametrize("kind", ["decision", "counter"])
def test_unresolved_block_preserved_with_token(client, boundary, kind):
    _, connection, _, _ = boundary
    channel = connection.return_value.channel.return_value
    channel.basic_publish.side_effect = TimeoutError("unknown")
    path, payload = PUBLICATIONS[kind]
    body = reviewed(boundary, payload)
    assert client.post(BASE + path, json=body).status_code == 503
    for attempt in (body, reviewed(boundary, {"action": "accept", "approved_by": "h"})):
        blocked = client.post(
            BASE + "/decision" if "action" in attempt else BASE + path, json=attempt
        )
        assert blocked.status_code == 409
        assert "outcome-unknown" in blocked.json()["detail"]
    channel.basic_publish.assert_called_once()


def test_mismatch_precedes_everything(client, boundary):
    rows, connection, audit, _ = boundary
    body = reviewed(boundary, PUBLICATIONS["decision"][1])
    assert client.post(BASE + "/decision", json=body).status_code == 202
    claims = copy.deepcopy(rows[0]["outcome_details"])
    rows[0]["counter_offer"]["comment"] = "Changed after the first send"
    service._save_publication.reset_mock()
    service._worker_response_exists.reset_mock()
    audit.reset_mock()
    replay = client.post(BASE + "/decision", json=body)
    # Not the duplicate receipt: the stored offer is no longer the one reviewed.
    assert replay.status_code == 409
    assert "changed since it was reviewed" in replay.json()["detail"]
    service._save_publication.assert_not_called()
    service._worker_response_exists.assert_not_called()
    audit.assert_not_called()
    assert rows[0]["outcome_details"] == claims
    connection.return_value.channel.return_value.basic_publish.assert_called_once()


@pytest.mark.parametrize("kind", ["decision", "counter"])
def test_claim_and_audit_record_review_token(client, boundary, kind):
    rows, connection, audit, _ = boundary
    path, payload = PUBLICATIONS[kind]
    shown = token(boundary)
    assert client.post(BASE + path, json=reviewed(boundary, payload)).status_code == 202
    claim = next(iter(rows[0]["outcome_details"]["incoming_publications"].values()))
    assert claim["review_token"] == shown
    assert audit.call_args.kwargs["decision_inputs"]["review_token"] == shown
    assert "review_token" not in json.dumps(published(connection))
    assert listed(client, rows)[0]["publications"][0]["review_token"] == shown


def test_retry_claim_records_review_token(client, boundary, worker_responses):
    rows, _, audit, _ = boundary
    shown = token(boundary)
    body = reviewed(boundary, PUBLICATIONS["retry"][1])
    assert client.post(BASE + "/decision/retry", json=body).status_code == 202
    claims = rows[0]["outcome_details"]["incoming_publications"]
    assert claims[f"{decline_key()}:retry:41"]["review_token"] == shown
    assert audit.call_args.kwargs["decision_inputs"]["review_token"] == shown
