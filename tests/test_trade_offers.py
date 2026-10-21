"""Trade approval contract. No broker, Redis, or live database connections."""

import copy
import json
import threading
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.schemas.trade_score import TradeScore
from src.services import trade_offer_service as service
from tests.test_trade_score_service import context  # noqa: F401
from tests.test_trade_score_service import score as shape_score

FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures/trade_offer_keys.json").read_text()
)


def staged(message=None, evaluation=None, row_id=1):
    fixture = next(f for f in FIXTURES if f["offer"]["message"] == message)
    o = fixture["offer"]
    row = dict(
        id=row_id,
        status="awaiting_approval",
        league_id=o["leagueId"],
        season=2026,
        executed_at="2026-10-03T12:00:00",
        outcome_details={},
        dry_run=True,
        counter_offer=dict(
            offer_key=fixture["key"],
            source="counter_offer",
            from_team_id="8",
            to_team_id="3",
            incoming_player_ids=o["incomingPlayerIds"],
            outgoing_player_ids=o["outgoingPlayerIds"],
            message=message,
            screenshot_path="/pod/local.png",
        ),
    )
    if evaluation is not None:
        row["counter_offer"].update(source="proposal", evaluation=evaluation)
    return row


def staged_counter(row_id=1, thread_root_id="tx-1"):
    """A staged counter whose key binds the incoming offer it answers."""
    fixture = next(f for f in FIXTURES if f["offer"].get("relatedTransactionId"))
    row = staged(row_id=row_id)
    row["counter_offer"].update(
        offer_key=fixture["key"],
        related_transaction_id=fixture["offer"]["relatedTransactionId"],
        thread_root_id=thread_root_id,
    )
    return row


def proposal_evaluation(trade_context, message):
    """The worker stores the /v1/trade/score response verbatim, so build it the same way."""
    return dict(
        version="proposal-evaluation-v1",
        evaluated_at="2026-10-07T18:00:00Z",
        request=dict(
            league_id="123",
            season=2026,
            week=5,
            horizon=2,
            our_team_id="1",
            counterparty_team_id="2",
            send_espn_ids=["103"],
            receive_espn_ids=["203"],
        ),
        score=TradeScore.model_validate(shape_score(trade_context)).model_dump(
            mode="json"
        ),
        target=dict(team_id="2", position="RB", deficit=1, severity=6.2),
        ranking=dict(
            basis="final_score",
            value_per_scored_week=3.1,
            rank=1,
            packages_proposed=5,
            packages_scored=4,
            packages_eligible=2,
        ),
        pitch=dict(
            message=message,
            evidence=[
                dict(
                    id="e1",
                    path="/roster_impact/counterparty/weeks/0/delta",
                    text="Your week 5 starting lineup changes by +1.0",
                )
            ],
            cited_evidence_ids=["e1"],
        ),
    )


@pytest.fixture
def boundary(monkeypatch):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "123456789")
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    rows = [staged()]
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

    def read(cur, key, **kwargs):
        return copy.deepcopy(
            [r for r in rows if r["counter_offer"]["offer_key"] == key]
        )

    def update(cur, row_id, status, approval):
        row = next(r for r in rows if r["id"] == row_id)
        row["status"] = status
        row["outcome_details"]["approval"] = copy.deepcopy(approval)

    def edit(cur, row, originals, offer):
        edited = {
            **copy.deepcopy(row),
            "id": max(r["id"] for r in rows) + 1,
            "counter_offer": copy.deepcopy(offer),
        }
        rows.append(edited)
        for original in originals:
            if original["status"] == "awaiting_approval":
                stored = next(r for r in rows if r["id"] == original["id"])
                stored["status"] = "superseded"
                stored["counter_offer"]["superseded_by"] = offer["offer_key"]
        return copy.deepcopy(edited)

    with (
        patch.object(service, "_save_edited_offer", side_effect=edit),
        patch.object(service, "get_db_cursor", transaction),
        patch.object(service, "_read_offers", side_effect=read),
        patch.object(service, "_save_approval", side_effect=update),
        patch("src.api.routes._record_bot_decision") as audit,
        patch("src.api.routes.pika.BlockingConnection") as connection,
    ):
        yield rows, connection.return_value.channel.return_value, audit


@pytest.mark.parametrize("fixture", FIXTURES)
def test_worker_hash_vectors(fixture):
    assert service.compute_offer_key(fixture["offer"]) == fixture["key"]


def test_approve_publishes_stored_offer(client, boundary):
    rows, channel, audit = boundary
    rows[0] = staged("  Fair deal?  ")
    key = rows[0]["counter_offer"]["offer_key"]
    response = client.post(
        f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
    )
    assert response.status_code == 202, response.text
    sent = json.loads(channel.basic_publish.call_args.kwargs["body"])
    assert sent["message"] == "  Fair deal?  "
    assert sent["approval"] == {"offer_key": key, "approved_by": "conor"}
    assert (
        sent["incoming_player_ids"] == rows[0]["counter_offer"]["incoming_player_ids"]
    )
    assert sent["season"] == 2026
    reconstructed = dict(
        leagueId="123456789",
        seasonId=str(sent["season"]),
        fromTeamId="8",
        toTeamId=sent["to_team_id"],
        incomingPlayerIds=sent["incoming_player_ids"],
        outgoingPlayerIds=sent["outgoing_player_ids"],
        message=sent.get("message"),
    )
    assert service.compute_offer_key(reconstructed) == key
    assert channel.basic_publish.call_args.kwargs["routing_key"] == "trade_submissions"
    channel.confirm_delivery.assert_called_once()
    channel.queue_declare.assert_called_once_with(
        queue="trade_submissions", durable=True
    )
    assert rows[0]["status"] == "approved"
    assert rows[0]["outcome_details"]["approval"]["approved_by"] == "conor"
    assert audit.call_args.kwargs["action"] == "trade_offer"
    assert audit.call_args.kwargs["cursor"] is not None


def test_double_approve_and_duplicate_staged_rows_publish_once(client, boundary):
    rows, channel, audit = boundary
    rows.append({**copy.deepcopy(rows[0]), "id": 2})
    key = rows[0]["counter_offer"]["offer_key"]
    for expected in [202, 200]:
        response = client.post(
            f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
        )
        assert response.status_code == expected, response.text
    channel.basic_publish.assert_called_once()
    audit.assert_called_once()
    assert "message" not in json.loads(channel.basic_publish.call_args.kwargs["body"])


@pytest.mark.parametrize(
    "field,value",
    [
        ("message", "edited"),
        ("to_team_id", "4"),
        ("incoming_player_ids", ["99"]),
        ("outgoing_player_ids", ["99"]),
        ("source", "invalid"),
        ("related_transaction_id", "tx-9"),
        ("thread_root_id", "tx-1"),
    ],
)
def test_edited_offer_rejected(client, boundary, field, value):
    rows, channel, _ = boundary
    key = rows[0]["counter_offer"]["offer_key"]
    rows[0]["counter_offer"][field] = value
    response = client.post(
        f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
    )
    assert response.status_code == 409
    channel.basic_publish.assert_not_called()


@pytest.mark.parametrize(
    "status",
    ["failure", "submitted", "submitting", "submit_blocked", "approval_pending"],
)
def test_non_awaiting_rejected(client, boundary, status):
    rows, channel, _ = boundary
    rows[0]["status"] = status
    key = rows[0]["counter_offer"]["offer_key"]
    assert (
        client.post(
            f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
        ).status_code
        == 409
    )
    channel.basic_publish.assert_not_called()


def test_request_cannot_override_player_terms(client, boundary):
    rows, channel, _ = boundary
    key = rows[0]["counter_offer"]["offer_key"]
    assert (
        client.post(
            f"/v1/trade/offers/{key}/approve",
            json={"approved_by": "conor", "to_team_id": "4"},
        ).status_code
        == 422
    )
    channel.basic_publish.assert_not_called()


def test_ambiguous_publish_never_retries(client, boundary):
    rows, channel, _ = boundary
    channel.basic_publish.side_effect = TimeoutError("ack lost")
    key = rows[0]["counter_offer"]["offer_key"]
    assert (
        client.post(
            f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
        ).status_code
        == 503
    )
    assert rows[0]["status"] == "approval_pending"
    assert (
        client.post(
            f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
        ).status_code
        == 409
    )
    channel.basic_publish.assert_called_once()


def test_pre_publish_failure_releases_claim(client, boundary):
    rows, channel, _ = boundary
    channel.queue_declare.side_effect = RuntimeError("not connected")
    key = rows[0]["counter_offer"]["offer_key"]
    assert (
        client.post(
            f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
        ).status_code
        == 503
    )
    assert rows[0]["status"] == "awaiting_approval"
    channel.basic_publish.assert_not_called()


@pytest.mark.parametrize("message", [None, "Edited pitch"])
def test_concurrent_approval_is_blocked(boundary, message):
    rows, channel, _ = boundary
    entered, release = threading.Event(), threading.Event()

    def publish(**kwargs):
        entered.set()
        assert release.wait(5)

    channel.basic_publish.side_effect = publish
    key = rows[0]["counter_offer"]["offer_key"]
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor() as pool:
        first = pool.submit(service.approve_offer, key, "conor", message)
        assert entered.wait(5)
        try:
            with pytest.raises(service.OfferConflict):
                service.approve_offer(key, "other", message)
        finally:
            release.set()
        first.result()
    channel.basic_publish.assert_called_once()


def test_list_filters_and_espn_names(client):
    cursor = MagicMock()
    row = staged()
    cursor.fetchall.side_effect = [
        [tuple(row[k] for k in service.ROW_COLUMNS)],
        [("4259545", "Player Name")],
    ]
    with patch.object(service, "get_db_cursor") as db:
        db.return_value.__enter__.return_value = cursor
        response = client.get(
            "/v1/trade/offers?status=awaiting_approval&league_id=123456789&season=2026&limit=10&offset=2"
        )
    assert response.status_code == 200, response.text
    sql, params = cursor.execute.call_args_list[0].args
    assert "transaction_type = 'trade_offer_submit'" in sql
    assert "status = %s" in sql and "league_id = %s" in sql and "season = %s" in sql
    assert params == ["awaiting_approval", "123456789", 2026, 10, 2]
    assert "espn_id" in cursor.execute.call_args_list[1].args[0]
    offer = response.json()["offers"][0]
    assert offer["incoming_players"][0] == {"espn_id": "4259545", "name": "Player Name"}
    assert offer["incoming_players"][1] == {"espn_id": "-16021", "name": None}
    assert "screenshot_path" not in offer
    assert "evaluation" in offer and offer["evaluation"] is None


def test_sql_claim_reads_every_matching_row_under_lock():
    cursor = MagicMock()
    service._read_offers(cursor, "key")
    sql, params = cursor.execute.call_args.args
    assert "transaction_type = 'trade_offer_submit'" in sql
    assert "counter_offer->>'offer_key' = %s" in sql
    assert "FOR UPDATE" in sql
    assert "LIMIT" not in sql
    assert params == ("key",)


def test_sql_approval_preserves_other_outcome_details():
    cursor = MagicMock()
    service._save_approval(cursor, 7, "approval_pending", {"approved_by": "conor"})
    sql, params = cursor.execute.call_args.args
    assert "COALESCE(outcome_details, '{}'::jsonb) ||" in sql
    assert params == ("approval_pending", '{"approved_by": "conor"}', 7)


@pytest.mark.parametrize("field,value", [("league_id", "wrong"), ("season", 2025)])
def test_edited_context_rejected(client, boundary, field, value):
    rows, channel, _ = boundary
    rows[0][field] = value
    key = rows[0]["counter_offer"]["offer_key"]
    assert (
        client.post(
            f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
        ).status_code
        == 409
    )
    channel.basic_publish.assert_not_called()


def test_worker_configuration_mismatch(client, boundary, monkeypatch):
    rows, channel, _ = boundary
    monkeypatch.setenv("ESPN_TEAM_ID", "9")
    key = rows[0]["counter_offer"]["offer_key"]
    assert (
        client.post(
            f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
        ).status_code
        == 409
    )
    channel.basic_publish.assert_not_called()


def test_audit_failure_prevents_publication(boundary):
    rows, channel, audit = boundary
    audit.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError):
        service.approve_offer(rows[0]["counter_offer"]["offer_key"], "conor")
    channel.basic_publish.assert_not_called()
    assert rows[0]["status"] == "awaiting_approval"


def test_missing_key(client, boundary):
    _, channel, _ = boundary
    assert (
        client.post(
            f'/v1/trade/offers/{"0"*64}/approve', json={"approved_by": "conor"}
        ).status_code
        == 404
    )
    channel.basic_publish.assert_not_called()


@pytest.mark.parametrize(
    "query", ["status=unknown", "limit=0", "limit=201", "offset=-1"]
)
def test_list_invalid_filters(client, query):
    assert client.get(f"/v1/trade/offers?{query}").status_code == 422


def test_transactional_audit_failure_propagates():
    from src.api.routes import _record_bot_decision

    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("audit failed")
    with pytest.raises(RuntimeError):
        _record_bot_decision(
            tracking_id="test",
            bot_name="trade_builder",
            action="trade_offer",
            team_id="8",
            cursor=cursor,
        )


def test_default_list_filters_only_staged_offers(client):
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    with patch.object(service, "get_db_cursor") as db:
        db.return_value.__enter__.return_value = cursor
        result = client.get("/v1/trade/offers")
    assert result.status_code == 200
    assert result.json() == {"offers": [], "limit": 50, "offset": 0}
    assert cursor.execute.call_args.args[1] == ["awaiting_approval", 50, 0]


def test_failure_to_save_confirmation_never_republishes(boundary):
    rows, channel, _ = boundary
    original = service._save_approval.side_effect

    def fail_confirmation(cursor, row_id, status, approval):
        if status == "approved":
            raise RuntimeError("commit failed")
        original(cursor, row_id, status, approval)

    service._save_approval.side_effect = fail_confirmation
    key = rows[0]["counter_offer"]["offer_key"]
    with pytest.raises(RuntimeError):
        service.approve_offer(key, "conor")
    with pytest.raises(service.OfferConflict):
        service.approve_offer(key, "conor")
    channel.basic_publish.assert_called_once()


def test_duplicate_does_not_replace_original_approver(client, boundary):
    rows, channel, audit = boundary
    key = rows[0]["counter_offer"]["offer_key"]
    first = client.post(
        f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
    )
    second = client.post(
        f"/v1/trade/offers/{key}/approve", json={"approved_by": "other"}
    )
    assert second.status_code == 200
    assert second.json()["approved_by"] == "conor"
    assert second.json()["tracking_id"] == first.json()["tracking_id"]
    channel.basic_publish.assert_called_once()
    audit.assert_called_once()


PITCH = "Fair deal?"


def test_approved_counter_keeps_its_link(client, boundary):
    rows, channel, _ = boundary
    rows[0] = staged_counter()
    key = rows[0]["counter_offer"]["offer_key"]
    response = client.post(
        f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
    )
    assert response.status_code == 202, response.text
    sent = json.loads(channel.basic_publish.call_args.kwargs["body"])
    assert sent["related_transaction_id"] == "tx-3"
    assert sent["thread_root_id"] == "tx-1"
    reconstructed = dict(
        leagueId="123456789",
        seasonId=str(sent["season"]),
        fromTeamId="8",
        toTeamId=sent["to_team_id"],
        incomingPlayerIds=sent["incoming_player_ids"],
        outgoingPlayerIds=sent["outgoing_player_ids"],
        message=sent.get("message"),
        relatedTransactionId=sent["related_transaction_id"],
    )
    assert service.compute_offer_key(reconstructed) == key


@pytest.mark.parametrize(
    "field,value",
    [("related_transaction_id", " tx-3"), ("thread_root_id", "")],
)
def test_malformed_counter_linkage_rejected(client, boundary, field, value):
    rows, channel, _ = boundary
    rows[0] = staged_counter()
    rows[0]["counter_offer"][field] = value
    key = rows[0]["counter_offer"]["offer_key"]
    response = client.post(
        f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
    )
    assert response.status_code == 409
    channel.basic_publish.assert_not_called()


def test_staged_copies_disagreeing_on_thread_root_are_rejected(client, boundary):
    rows, channel, _ = boundary
    rows[:] = [staged_counter(), staged_counter(row_id=2, thread_root_id="tx-0")]
    key = rows[0]["counter_offer"]["offer_key"]
    response = client.post(
        f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
    )
    assert response.status_code == 409
    channel.basic_publish.assert_not_called()


def list_offers_response(client, rows):
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [tuple(row[k] for k in service.ROW_COLUMNS) for row in rows],
        [],
    ]
    with patch.object(service, "get_db_cursor") as db:
        db.return_value.__enter__.return_value = cursor
        response = client.get("/v1/trade/offers")
    assert response.status_code == 200, response.text
    return {offer["id"]: offer for offer in response.json()["offers"]}


def test_list_passes_stored_evaluation_through(client, context):  # noqa: F811
    stored = proposal_evaluation(context, "  Fair deal?  ")
    offers = list_offers_response(client, [staged("  Fair deal?  ", stored)])
    assert offers[1]["evaluation"] == stored
    assert offers[1]["source"] == "proposal"
    assert offers[1]["message"] == "  Fair deal?  "


@pytest.mark.parametrize("explicit_null", [False, True])
def test_list_rows_without_evaluation_return_null(client, caplog, explicit_null):
    row = staged()
    if explicit_null:
        row["counter_offer"]["evaluation"] = None
    with caplog.at_level("WARNING"):
        offers = list_offers_response(client, [row])
    assert "evaluation" in offers[1] and offers[1]["evaluation"] is None
    assert not caplog.records


def _set(path, value):
    def mutate(evaluation):
        target = evaluation
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value
        return evaluation

    return mutate


def _drop(path):
    def mutate(evaluation):
        target = evaluation
        for key in path[:-1]:
            target = target[key]
        del target[path[-1]]
        return evaluation

    return mutate


@pytest.mark.parametrize(
    "corrupt",
    [
        _set(["version"], "proposal-evaluation-v2"),
        _drop(["version"]),
        lambda _: "x",
        lambda _: [],
        lambda _: 5,
        _set(["score", "scoring_version"], "trade-score-v2"),
        _drop(["score", "roster_impact"]),
        _set(["request", "extra"], True),
        _set(["ranking", "basis"], "other"),
        _set(["evaluated_at"], "yesterday"),
    ],
)
def test_invalid_evaluation_is_null_and_logged_without_breaking_list(
    client, context, caplog, corrupt  # noqa: F811
):
    good = proposal_evaluation(context, "  Fair deal?  ")
    bad = corrupt(proposal_evaluation(context, PITCH))
    rows = [staged("  Fair deal?  ", good), staged(None, bad, row_id=2)]
    with caplog.at_level("WARNING"):
        offers = list_offers_response(client, rows)
    assert offers[1]["evaluation"] == good
    assert "evaluation" in offers[2] and offers[2]["evaluation"] is None
    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "row 2" in warnings[0].getMessage()
    assert PITCH not in caplog.text


@pytest.mark.parametrize("evaluation", ["absent", "valid", "garbage"])
def test_approval_ignores_evaluation(
    client, boundary, context, evaluation
):  # noqa: F811
    rows, channel, audit = boundary
    stored = {
        "absent": None,
        "valid": proposal_evaluation(context, "  Fair deal?  "),
        "garbage": {"version": 7},
    }[evaluation]
    rows[0] = staged("  Fair deal?  ", stored)
    if evaluation == "absent":
        rows[0]["counter_offer"]["source"] = "proposal"
    key = rows[0]["counter_offer"]["offer_key"]
    response = client.post(
        f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
    )
    assert response.status_code == 202, response.text
    channel.basic_publish.assert_called_once()
    sent = json.loads(channel.basic_publish.call_args.kwargs["body"])
    assert sent.pop("tracking_id")
    sent.pop("traceparent")
    fixture = next(f for f in FIXTURES if f["offer"]["message"] == "  Fair deal?  ")
    assert sent == dict(
        season=2026,
        to_team_id="3",
        incoming_player_ids=fixture["offer"]["incomingPlayerIds"],
        outgoing_player_ids=fixture["offer"]["outgoingPlayerIds"],
        source="proposal",
        approval={"offer_key": key, "approved_by": "conor"},
        message="  Fair deal?  ",
    )
    assert service.compute_offer_key(fixture["offer"]) == key
    approval = rows[0]["outcome_details"]["approval"]
    assert "evaluation" not in approval["payload"]
    assert "evaluation" not in json.dumps(audit.call_args.kwargs["decision_inputs"])


def test_staged_copies_with_different_evaluations_approve_once(
    client, boundary, context  # noqa: F811
):
    rows, channel, _ = boundary
    rows[0] = staged("  Fair deal?  ", proposal_evaluation(context, "  Fair deal?  "))
    older = staged("  Fair deal?  ", row_id=2)
    older["counter_offer"]["source"] = "proposal"
    rows.append(older)
    key = rows[0]["counter_offer"]["offer_key"]
    for expected in [202, 200]:
        response = client.post(
            f"/v1/trade/offers/{key}/approve", json={"approved_by": "conor"}
        )
        assert response.status_code == expected, response.text
    channel.basic_publish.assert_called_once()


def test_list_shows_counter_linkage(client):
    offers = list_offers_response(client, [staged_counter(), staged(row_id=2)])
    assert offers[1]["related_transaction_id"] == "tx-3"
    assert offers[1]["thread_root_id"] == "tx-1"
    assert offers[2]["related_transaction_id"] is None
    assert offers[2]["thread_root_id"] is None


@pytest.mark.parametrize("counter", [False, True])
def test_approve_edited_message(client, boundary, context, counter):  # noqa: F811
    rows, channel, audit = boundary
    evaluation = proposal_evaluation(context, "  Fair deal?  ")
    rows[0] = staged_counter() if counter else staged("  Fair deal?  ", evaluation)
    original_message = rows[0]["counter_offer"]["message"]
    rows.append({**copy.deepcopy(rows[0]), "id": 2})
    original_key = rows[0]["counter_offer"]["offer_key"]
    response = client.post(
        f"/v1/trade/offers/{original_key}/approve",
        json={"approved_by": "conor", "message": "My own pitch"},
    )
    assert response.status_code == 202, response.text
    key = response.json()["offer_key"]
    assert key != original_key
    assert [r["status"] for r in rows] == ["superseded", "superseded", "approved"]
    edited = rows[-1]
    offer = edited["counter_offer"]
    assert service._validated_offer(edited, key) == offer
    assert offer["drafted_message"] == original_message
    assert offer["message_edited"] is True
    assert offer["edited_by"] == "conor"
    sent = json.loads(channel.basic_publish.call_args.kwargs["body"])
    assert sent["message"] == "My own pitch"
    assert sent["approval"] == {"offer_key": key, "approved_by": "conor"}
    if counter:
        assert sent["related_transaction_id"] == "tx-3"
        assert sent["thread_root_id"] == "tx-1"
    else:
        assert offer["evaluation"] == evaluation
        listed = list_offers_response(client, [edited])[edited["id"]]
        assert listed["evaluation"] == evaluation
        assert listed["drafted_message"] == original_message
        assert listed["message_edited"] is True
        assert listed["approval"]["approved_by"] == "conor"
    for retry_key in (original_key, key):
        for message in (None, "My own pitch"):
            retry = client.post(
                f"/v1/trade/offers/{retry_key}/approve",
                json={"approved_by": "other", "message": message},
            )
            assert retry.status_code == 200, retry.text
            assert retry.json() == {**response.json(), "duplicate": True}
        conflict = client.post(
            f"/v1/trade/offers/{retry_key}/approve",
            json={"approved_by": "other", "message": "A different pitch"},
        )
        assert conflict.status_code == 409
    channel.basic_publish.assert_called_once()
    audit.assert_called_once()


@pytest.mark.parametrize("message", [None, "  Fair deal?  "])
def test_explicit_unchanged_message(client, boundary, message):
    rows, channel, _ = boundary
    rows[0] = staged("  Fair deal?  ")
    key = rows[0]["counter_offer"]["offer_key"]
    for expected in (202, 200):
        response = client.post(
            f"/v1/trade/offers/{key}/approve",
            json={"approved_by": "conor", "message": message},
        )
        assert response.status_code == expected
        assert response.json()["offer_key"] == key
    assert len(rows) == 1
    assert not rows[0]["counter_offer"].get("message_edited", False)
    channel.basic_publish.assert_called_once()


@pytest.mark.parametrize("message", ["", " \n\t", "x" * 1001])
def test_invalid_approval_message(client, boundary, message):
    rows, channel, audit = boundary
    key = rows[0]["counter_offer"]["offer_key"]
    response = client.post(
        f"/v1/trade/offers/{key}/approve",
        json={"approved_by": "conor", "message": message},
    )
    assert response.status_code == 422
    assert rows[0]["status"] == "awaiting_approval"
    channel.basic_publish.assert_not_called()
    audit.assert_not_called()


def test_message_length_boundary(client, boundary):
    rows, _, _ = boundary
    key = rows[0]["counter_offer"]["offer_key"]
    assert (
        client.post(
            f"/v1/trade/offers/{key}/approve",
            json={"approved_by": "conor", "message": "x" * 1000},
        ).status_code
        == 202
    )


@pytest.mark.parametrize(
    "failure", ["audit", "before_publish", "ambiguous", "confirmation"]
)
def test_edited_approval_failures(client, boundary, failure):
    rows, channel, audit = boundary
    key = rows[0]["counter_offer"]["offer_key"]
    if failure == "audit":
        audit.side_effect = RuntimeError("audit unavailable")
    elif failure == "before_publish":
        channel.queue_declare.side_effect = RuntimeError("queue unavailable")
    elif failure == "ambiguous":
        channel.basic_publish.side_effect = TimeoutError("unknown send")
    else:
        save = service._save_approval.side_effect

        def fail_confirmation(cur, row_id, status, approval):
            if status == "approved":
                raise RuntimeError("confirmation unavailable")
            save(cur, row_id, status, approval)

        service._save_approval.side_effect = fail_confirmation
    if failure in ("audit", "confirmation"):
        with pytest.raises(RuntimeError):
            service.approve_offer(key, "conor", "Edited")
    else:
        with pytest.raises(service.PublishUnavailable):
            service.approve_offer(key, "conor", "Edited")
    if failure == "audit":
        assert len(rows) == 1 and rows[0]["status"] == "awaiting_approval"
        channel.basic_publish.assert_not_called()
    elif failure == "before_publish":
        assert rows[0]["status"] == "superseded"
        assert rows[1]["status"] == "awaiting_approval"
        channel.queue_declare.side_effect = None
        assert (
            service.approve_offer(key, "conor", "Edited")["offer_key"]
            == rows[1]["counter_offer"]["offer_key"]
        )
        channel.basic_publish.assert_called_once()
    else:
        assert rows[1]["status"] == "approval_pending"
        with pytest.raises(service.OfferConflict):
            service.approve_offer(key, "conor", "Edited")
        channel.basic_publish.assert_called_once()


def test_edit_after_unedited_approval_conflicts(client, boundary):
    rows, channel, _ = boundary
    key = rows[0]["counter_offer"]["offer_key"]
    service.approve_offer(key, "conor")
    response = client.post(
        f"/v1/trade/offers/{key}/approve",
        json={"approved_by": "conor", "message": "New pitch"},
    )
    assert response.status_code == 409
    channel.basic_publish.assert_called_once()


def test_edit_collision_fails_closed(boundary):
    rows, channel, _ = boundary
    original = rows[0]
    target = staged("  Fair deal?  ", row_id=2)
    rows.append(target)
    with pytest.raises(service.OfferConflict, match="already exists"):
        service.approve_offer(
            original["counter_offer"]["offer_key"], "conor", "  Fair deal?  "
        )
    assert all(row["status"] == "awaiting_approval" for row in rows)
    channel.basic_publish.assert_not_called()


def test_edited_key_contention_fails_closed(boundary):
    rows, channel, _ = boundary
    with patch.object(
        service, "_lock_edited_key", side_effect=service.OfferConflict("busy")
    ):
        with pytest.raises(service.OfferConflict):
            service.approve_offer(
                rows[0]["counter_offer"]["offer_key"], "conor", "Edit"
            )
    assert len(rows) == 1 and rows[0]["status"] == "awaiting_approval"
    channel.basic_publish.assert_not_called()


def test_unedited_list_defaults(client):
    offer = list_offers_response(client, [staged()])[1]
    assert offer["drafted_message"] is None
    assert offer["message_edited"] is False


@pytest.mark.parametrize("dry_run", [True, False, None])
def test_save_edited_offer_sql(dry_run):
    cursor = MagicMock()
    cursor.fetchone.return_value = (9,)
    original = staged()
    original["dry_run"] = dry_run
    offer = {**original["counter_offer"], "offer_key": "new", "message": "Edit"}
    edited = service._save_edited_offer(cursor, original, [original], offer)
    assert edited["id"] == 9 and edited["counter_offer"] == offer
    insert, supersede = cursor.execute.call_args_list
    assert "INSERT INTO execution_log" in insert.args[0]
    assert json.loads(insert.args[1][-2]) == offer
    assert insert.args[1][-1] is dry_run
    assert "status = 'superseded'" in supersede.args[0]
    assert supersede.args[1] == ("new", 1)


def test_edited_key_lock_is_nonblocking():
    cursor = MagicMock()
    cursor.fetchone.return_value = (False,)
    with pytest.raises(service.OfferConflict):
        service._lock_edited_key(cursor, "key")
    assert "pg_try_advisory_xact_lock" in cursor.execute.call_args.args[0]


@pytest.mark.parametrize("edit_again", [False, True])
def test_restaged_original_can_be_approved(client, boundary, edit_again):
    rows, channel, _ = boundary
    original = copy.deepcopy(rows[0])
    key = original["counter_offer"]["offer_key"]
    service.approve_offer(key, "editor", "First edit")
    rows.insert(0, {**original, "id": 3})
    message = "Second edit" if edit_again else None
    response = client.post(
        f"/v1/trade/offers/{key}/approve",
        json={"approved_by": "new approver", "message": message},
    )
    assert response.status_code == 202, response.text
    assert response.json()["approved_by"] == "new approver"
    assert rows[0]["status"] == ("superseded" if edit_again else "approved")
    if not edit_again:
        retry = client.post(
            f"/v1/trade/offers/{key}/approve", json={"approved_by": "retry"}
        )
        assert retry.status_code == 200
        assert retry.json() == {**response.json(), "duplicate": True}
    assert channel.basic_publish.call_count == 2


@pytest.mark.parametrize("long_draft", [False, True])
def test_original_draft_echo_returns_edited_receipt(client, boundary, long_draft):
    rows, channel, _ = boundary
    rows[0] = staged("  Fair deal?  ")
    if long_draft:
        rows[0]["counter_offer"]["message"] = "x" * 1001
        rows[0]["counter_offer"]["offer_key"] = service._edited_offer_key(
            rows[0], rows[0]["counter_offer"], "x" * 1001
        )
    key = rows[0]["counter_offer"]["offer_key"]
    draft = rows[0]["counter_offer"]["message"]
    first = client.post(
        f"/v1/trade/offers/{key}/approve",
        json={"approved_by": "editor", "message": "Edited pitch"},
    )
    assert first.status_code == 202
    response = client.post(
        f"/v1/trade/offers/{key}/approve",
        json={"approved_by": "other", "message": draft},
    )
    assert response.status_code == 200, response.text
    assert response.json() == {**first.json(), "duplicate": True}
    channel.basic_publish.assert_called_once()


def test_editor_survives_reapproval_by_another_person(client, boundary):
    rows, channel, _ = boundary
    key = rows[0]["counter_offer"]["offer_key"]
    channel.queue_declare.side_effect = RuntimeError("before send")
    with pytest.raises(service.PublishUnavailable):
        service.approve_offer(key, "editor", "Edited pitch")
    channel.queue_declare.side_effect = None
    service.approve_offer(key, "approver")
    listed = list_offers_response(client, [rows[-1]])[rows[-1]["id"]]
    assert listed["edited_by"] == "editor"
    assert listed["approval"]["approved_by"] == "approver"
    assert list_offers_response(client, [staged()])[1]["edited_by"] is None
    schema = client.get("/openapi.json").json()["components"]["schemas"]["TradeOffer"]
    assert "edited_by" in schema["properties"]


@pytest.mark.parametrize("counter", [False, True])
def test_validation_and_edit_share_key_fields(boundary, counter):
    row = staged_counter() if counter else staged("  Fair deal?  ")
    offer = row["counter_offer"]
    with patch.object(
        service, "_offer_key_fields", wraps=service._offer_key_fields
    ) as fields:
        service._validated_offer(row, offer["offer_key"])
        key = service._edited_offer_key(row, offer, offer.get("message"))
    assert key == offer["offer_key"]
    assert fields.call_count == 2
    assert fields.call_args_list[0] == fields.call_args_list[1]


def test_long_stored_draft_echo_is_unchanged(client, boundary):
    rows, channel, _ = boundary
    row = rows[0]
    row["counter_offer"]["message"] = "x" * 1001
    key = service._edited_offer_key(row, row["counter_offer"], "x" * 1001)
    row["counter_offer"]["offer_key"] = key
    for status in (202, 200):
        response = client.post(
            f"/v1/trade/offers/{key}/approve",
            json={"approved_by": "approver", "message": "x" * 1001},
        )
        assert response.status_code == status, response.text
        assert response.json()["offer_key"] == key
    channel.basic_publish.assert_called_once()


def test_confirmed_redirect_retry_does_not_lock_edited_key(boundary):
    rows, channel, _ = boundary
    key = rows[0]["counter_offer"]["offer_key"]
    first = service.approve_offer(key, "editor", "Edited pitch")
    with patch.object(
        service, "_lock_edited_key", side_effect=AssertionError("worker lock")
    ):
        assert service.approve_offer(key, "retry") == {**first, "duplicate": True}
    channel.basic_publish.assert_called_once()


@pytest.mark.parametrize("message", ["", " \n\t"])
def test_blank_messages_use_one_validation_error(client, boundary, message):
    rows, channel, _ = boundary
    key = rows[0]["counter_offer"]["offer_key"]
    response = client.post(
        f"/v1/trade/offers/{key}/approve",
        json={"approved_by": "approver", "message": message},
    )
    assert response.status_code == 422
    (error,) = response.json()["detail"]
    assert error["type"] == "value_error"
    assert error["msg"] == "Value error, message must not be blank"
    channel.basic_publish.assert_not_called()


def test_edited_offer_metadata_is_documented():
    docs = Path(__file__).parents[1] / "docs"
    model = (docs / "data-model.md").read_text()
    offer_contract = model.split("### `execution_log.counter_offer`", 1)[1].split(
        "### ", 1
    )[0]
    for field in ("drafted_message", "message_edited", "edited_by", "superseded_by"):
        assert f"`{field}`" in offer_contract
    assert "`awaiting_approval` → `superseded`" in model
    guide = (docs / "decision-pipelines/outgoing-trades.md").read_text()
    assert "`edited_by`, the self-reported label" in guide
    assert "`approval.approved_by` identifies the latest successful approver" in guide


def candidate_evaluation(trade_context, message):
    """Candidate-sourced proposals have no deficit target and carry extra ranking fields."""
    evaluation = proposal_evaluation(trade_context, message)
    evaluation["target"] = dict(team_id="2")
    evaluation["ranking"].update(source="candidates", candidate_rank=2)
    return evaluation


def test_list_returns_candidate_sourced_evaluation(client, context):  # noqa: F811
    stored = candidate_evaluation(context, "  Fair deal?  ")
    offers = list_offers_response(client, [staged("  Fair deal?  ", stored)])
    evaluation = offers[1]["evaluation"]
    assert evaluation is not None
    assert evaluation["target"]["team_id"] == "2"
    assert evaluation["target"]["position"] is None
    assert evaluation["target"]["deficit"] is None
    assert evaluation["target"]["severity"] is None


def test_stored_evaluation_accepts_candidate_target_and_unknown_ranking_fields(
    context, caplog  # noqa: F811
):
    stored = candidate_evaluation(context, PITCH)
    stored["ranking"]["future_field"] = "x"
    row = staged(None, stored)
    with caplog.at_level("WARNING"):
        result = service._stored_evaluation(row, row["counter_offer"])
    assert result is not None and result.target.team_id == "2"
    assert not caplog.records
