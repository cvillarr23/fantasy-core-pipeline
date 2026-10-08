"""Incoming trade contracts with transactional storage and broker boundaries mocked."""

import copy
import hashlib
import json
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
            json={"action": "accept", "approved_by": approver, "comment": " Yes "},
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
    assert client.post(BASE + "/" + kind, json=payload).status_code == 409
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
            BASE + "/decision", json={"action": "accept", "approved_by": "human"}
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
            json={"action": "decline", "approved_by": "human", "comment": " "},
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
            BASE + "/counter", json={"approved_by": "human", "week": 5}
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
            BASE + "/decision", json={"action": "decline", "approved_by": "human"}
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
            BASE + "/counter", json={"approved_by": "human", "week": 5}
        ).status_code
        == 409
    )
    connection.assert_not_called()


def test_confirmed_duplicate_after_worker_settles(client, boundary):
    rows, connection, _, _ = boundary
    payload = {"action": "decline", "approved_by": "human"}
    assert client.post(BASE + "/decision", json=payload).status_code == 202
    rows[0]["status"] = "declined"
    assert client.post(BASE + "/decision", json=payload).status_code == 200
    assert (
        client.post(
            BASE + "/decision", json={**payload, "action": "accept"}
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
    assert client.post(BASE + "/" + kind, json=payload).status_code == 503
    assert client.post(BASE + "/" + kind, json=payload).status_code == 409
    connection.return_value.channel.return_value.basic_publish.assert_called_once()


def test_pre_publish_failure_can_retry(client, boundary):
    _, connection, _, _ = boundary
    connection.side_effect = ConnectionError("before sending")
    payload = {"action": "decline", "approved_by": "human"}
    assert client.post(BASE + "/decision", json=payload).status_code == 503
    connection.side_effect = None
    assert client.post(BASE + "/decision", json=payload).status_code == 202


def test_audit_failure_rolls_back_claim(boundary):
    rows, connection, audit, _ = boundary
    audit.side_effect = RuntimeError("audit unavailable")
    with pytest.raises(RuntimeError):
        service.decide("123", "decline", "human")
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
            service.decide("123", "decline", "human")
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=run)
    thread.start()
    try:
        assert entered.wait(5)
        with pytest.raises(service.InboxConflict):
            service.decide("123", "accept", "other")
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
            BASE + "/decision", json={"action": "decline", "approved_by": "human"}
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
        service.decide("123", "decline", "human")
    service._save_publication.side_effect = save
    assert (
        client.post(
            BASE + "/decision", json={"action": "decline", "approved_by": "human"}
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
    cursor.fetchall.side_effect = [[tuple(rows[0][c] for c in service.ROW_COLUMNS)], []]
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
    assert client.post(BASE + "/" + kind, json=payload).status_code == 409
    connection.assert_not_called()


def test_existing_worker_response_blocks_new_action(client, boundary):
    _, connection, _, _ = boundary
    service._worker_response_exists.return_value = True
    assert (
        client.post(
            BASE + "/decision", json={"action": "decline", "approved_by": "human"}
        ).status_code
        == 409
    )
    connection.assert_not_called()


def test_same_queued_key_never_republishes_blocked_response(client, boundary):
    rows, connection, _, _ = boundary
    payload = {"action": "decline", "approved_by": "human"}
    assert client.post(BASE + "/decision", json=payload).status_code == 202
    rows[0]["counter_offer"]["decision"] = {
        "outcome": "blocked",
        "closed_locks": ["not_armed"],
    }
    assert client.post(BASE + "/decision", json=payload).status_code == 200
    assert (
        client.post(
            BASE + "/decision", json={**payload, "comment": "changed"}
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
    assert client.post(BASE + "/decision", json=payload).status_code == 503
    rows[0]["counter_offer"]["fingerprint"] = "b" * 64
    assert client.post(BASE + "/decision", json=payload).status_code == 409
    connection.return_value.channel.return_value.basic_publish.assert_called_once()


def test_finalization_rejects_replaced_claim(boundary):
    rows, connection, _, _ = boundary

    def publish(**kwargs):
        claim = next(iter(rows[0]["outcome_details"]["incoming_publications"].values()))
        claim["tracking_id"] = "changed"

    connection.return_value.channel.return_value.basic_publish.side_effect = publish
    with pytest.raises(service.InboxConflict, match="claim changed"):
        service.decide("123", "decline", "human")
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
    assert client.post(BASE + "/" + kind, json=payload).status_code == 422
    connection.assert_not_called()


def test_missing_header_and_unconfigured_key(client, boundary, monkeypatch):
    from src.api import routes

    payload = {"action": "decline", "approved_by": "human"}
    key = client.headers.pop("X-API-Key")
    try:
        assert client.post(BASE + "/decision", json=payload).status_code == 422
    finally:
        client.headers["X-API-Key"] = key
    monkeypatch.setattr(routes, "TRIGGER_API_KEY", None)
    assert client.post(BASE + "/decision", json=payload).status_code == 500
    boundary[1].assert_not_called()


def test_counter_rejects_overlapping_internal_mappings(client, boundary):
    rows, connection, _, _ = boundary
    rows[0]["counter_offer"]["score"]["players"]["888"]["player_id"] = "111"
    assert (
        client.post(
            BASE + "/counter", json={"approved_by": "human", "week": 5}
        ).status_code
        == 409
    )
    connection.assert_not_called()


def test_triage_names_use_espn_lookup(client, boundary):
    _, _, _, cursor = boundary
    cursor.fetchall.return_value = [("999", "Database name")]
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
    service.decide("123", "decline", "human")
    rows[0]["counter_offer"]["decision"] = {
        "outcome": "unknown",
        "verification": {"status": "unknown"},
    }
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[tuple(rows[0][c] for c in service.ROW_COLUMNS)], []]
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
