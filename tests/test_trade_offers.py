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

    def read(cur, key):
        return copy.deepcopy(
            [r for r in rows if r["counter_offer"]["offer_key"] == key]
        )

    def update(cur, row_id, status, approval):
        row = next(r for r in rows if r["id"] == row_id)
        row["status"] = status
        row["outcome_details"]["approval"] = copy.deepcopy(approval)

    with patch.object(service, "get_db_cursor", transaction), patch.object(
        service, "_read_offers_for_update", side_effect=read
    ), patch.object(service, "_save_approval", side_effect=update), patch(
        "src.api.routes._record_bot_decision"
    ) as audit, patch(
        "src.api.routes.pika.BlockingConnection"
    ) as connection:
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


def test_request_cannot_override_offer(client, boundary):
    rows, channel, _ = boundary
    key = rows[0]["counter_offer"]["offer_key"]
    assert (
        client.post(
            f"/v1/trade/offers/{key}/approve",
            json={"approved_by": "conor", "message": "edit"},
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


def test_concurrent_approval_is_blocked(boundary):
    rows, channel, _ = boundary
    entered, release = threading.Event(), threading.Event()

    def publish(**kwargs):
        entered.set()
        assert release.wait(5)

    channel.basic_publish.side_effect = publish
    key = rows[0]["counter_offer"]["offer_key"]
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor() as pool:
        first = pool.submit(service.approve_offer, key, "conor")
        assert entered.wait(5)
        try:
            with pytest.raises(service.OfferConflict):
                service.approve_offer(key, "other")
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
    service._read_offers_for_update(cursor, "key")
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
def test_approval_ignores_evaluation(client, boundary, context, evaluation):  # noqa: F811
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
