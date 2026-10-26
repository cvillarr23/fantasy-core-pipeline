"""Validate the maintained contract against real queue builders and publishers."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import jsonschema
import pytest
import yaml

from src.api import routes
from src.queue_contracts import PUBLISHED_QUEUES
from src.services import activity_service, activity_runner
from tests.test_trade_offers import boundary as offer_boundary, staged_counter
from tests.test_trade_inbox import (
    boundary,
    reviewed,
    worker_responses,
)

SPEC = yaml.safe_load((Path(__file__).parents[1] / "docs/asyncapi.yaml").read_text())


def validate(name, payload):
    schema = {**SPEC["components"]["schemas"][name]}
    jsonschema.Draft7Validator(
        schema, format_checker=jsonschema.FormatChecker()
    ).validate(payload)
    # Optional fields can disappear without violating JSON Schema. Keep their declared
    # shape in sync as well: representative fixtures populate every optional field.
    assert set(payload) == set(schema["properties"])


def test_channels_match_publisher_constants():
    assert set(SPEC["channels"]) == PUBLISHED_QUEUES
    assert {c["address"] for c in SPEC["channels"].values()} == PUBLISHED_QUEUES


def test_examples_validate():
    assert set(SPEC["components"]["schemas"]) == set(SPEC["components"]["messages"])
    for name, message in SPEC["components"]["messages"].items():
        assert message["payload"]["$ref"] == f"#/components/schemas/{name}"
        jsonschema.Draft7Validator(
            SPEC["components"]["schemas"][name],
            format_checker=jsonschema.FormatChecker(),
        ).validate(message["examples"][0]["payload"])


@pytest.mark.parametrize(
    "queue,model,values,name",
    [
        (
            "lineup_updates",
            routes.LineupTriggerPayload,
            dict(
                team_id="example-team",
                optimized_swaps=[
                    dict(
                        drop_player_id="espn-a",
                        add_player_id="espn-b",
                        slot="FLEX",
                        drop_player_name="Example A",
                        add_player_name="Example B",
                        matchup_multiplier=1.0,
                        decision_inputs={},
                    ),
                    dict(
                        kind="ir",
                        player_id="espn-c",
                        slot="IR",
                        player_name="Example C",
                        decision_inputs={},
                    ),
                    dict(
                        kind="activate_ir",
                        player_id="espn-d",
                        slot="BENCH",
                        player_name="Example D",
                    ),
                ],
            ),
            "LineupUpdate",
        ),
        (
            "waiver_actions",
            routes.WaiverTriggerPayload,
            dict(
                team_id="example-team",
                player_id="espn-a",
                name="Example A",
                team="XYZ",
                drop_player_id="espn-b",
                dry_run=True,
                decision_inputs={},
                after_tracking_id="parent",
            ),
            "WaiverClaim",
        ),
        (
            "draft_targets",
            routes.DraftPickTriggerPayload,
            dict(
                team_id="example-team",
                player_id="espn-a",
                name="Example A",
                position="RB",
                pro_team="XYZ",
                league_id="example-league",
                pick_number=42,
                expires_at=1893456000,
                mode="practice",
            ),
            "DraftTarget",
        ),
    ],
)
def test_trigger_outbox_payloads(queue, model, values, name, monkeypatch):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "example-league")
    cursor = MagicMock()
    cursor.fetchone.return_value = ("tracking",)
    # Run API serialization and durable outbox creation, then the actual broker send.
    with patch.object(activity_service, "get_db_cursor") as db, patch.object(
        activity_runner, "publish_one"
    ), patch.object(routes, "_record_bot_decision"), patch.object(
        routes, "_reject_internal_player_id"
    ), patch.object(
        routes, "_reject_player_rostered_elsewhere"
    ), patch.object(
        routes, "_ir_move_to_wait_for", return_value="parent"
    ), patch.object(
        routes, "_pick_slot_already_filled", return_value=False
    ):
        db.return_value.__enter__.return_value = cursor
        {
            "lineup_updates": routes.trigger_lineup_swap,
            "waiver_actions": routes.trigger_waiver_claim,
            "draft_targets": routes.trigger_draft_pick,
        }[queue](model(**values, idempotency_key="example-key"))
    insert = next(
        c
        for c in cursor.execute.call_args_list
        if "INSERT INTO activity_outbox" in c.args[0]
    )
    assert insert.args[1][1] == queue
    stored = json.loads(insert.args[1][2])
    cursor.fetchone.return_value = ("tracking", queue, stored, None)
    connection = MagicMock()
    with patch.object(activity_runner, "get_db_cursor") as db, patch.object(
        activity_runner.pika, "BlockingConnection", return_value=connection
    ), patch.object(routes, "_pick_slot_already_filled", return_value=False):
        db.return_value.__enter__.return_value = cursor
        assert activity_runner.publish_one("tracking")
    send = connection.channel.return_value.basic_publish.call_args.kwargs
    assert send["routing_key"] == queue
    validate(name, json.loads(send["body"]))
    for entry in stored.get("optimized_swaps", []):
        assert "decision_inputs" not in entry
        if "drop_player_id" in entry:
            assert "kind" not in entry
        schema = SPEC["components"]["schemas"][name]["properties"]["optimized_swaps"][
            "items"
        ]["oneOf"][int("kind" in entry)]
        assert set(entry) == set(schema["properties"])


def test_injury_alert_publisher():
    from src.ingestion import sync_players

    connection = MagicMock()
    with patch.object(sync_players.pika, "BlockingConnection", return_value=connection):
        sync_players.publish_waiver_alerts(
            [
                dict(
                    is_transition=True,
                    player_id="sleeper-example",
                    name="Example Player",
                    team="XYZ",
                    injury_status="Out",
                )
            ],
            {"sleeper-example"},
        )
    sent = connection.channel.return_value.basic_publish.call_args.kwargs
    assert sent["routing_key"] in SPEC["channels"]
    validate("InjuryAlert", json.loads(sent["body"]))


def test_trade_submission(client, offer_boundary):
    rows, channel, _ = offer_boundary
    rows[0] = staged_counter()
    rows[0]["counter_offer"]["message"] = "Example counter"
    from src.services import trade_offer_service

    offer = rows[0]["counter_offer"]
    key_input = dict(
        leagueId=rows[0]["league_id"],
        seasonId=str(rows[0]["season"]),
        fromTeamId=offer["from_team_id"],
        toTeamId=offer["to_team_id"],
        incomingPlayerIds=offer["incoming_player_ids"],
        outgoingPlayerIds=offer["outgoing_player_ids"],
        message=offer["message"],
        relatedTransactionId=offer["related_transaction_id"],
    )
    offer["offer_key"] = trade_offer_service.compute_offer_key(key_input)
    key = rows[0]["counter_offer"]["offer_key"]
    assert (
        client.post(
            f"/v1/trade/offers/{key}/approve", json={"approved_by": "example-reviewer"}
        ).status_code
        == 202
    )
    sent = channel.basic_publish.call_args.kwargs
    assert sent["routing_key"] == "trade_submissions"
    validate("TradeSubmission", json.loads(sent["body"]))


@pytest.mark.parametrize(
    "kind,name", [("decision", "TradeResponse"), ("counter", "TradeNegotiation")]
)
def test_inbox_publishers(client, boundary, kind, name):
    payload = (
        {"approved_by": "example-reviewer", "week": 5}
        if kind == "counter"
        else {
            "approved_by": "example-reviewer",
            "action": "decline",
            "comment": "Example comment",
        }
    )
    assert (
        client.post(
            f"/v1/trade/incoming/123/{kind}", json=reviewed(boundary, payload)
        ).status_code
        == 202
    )
    sent = boundary[1].return_value.channel.return_value.basic_publish.call_args.kwargs
    assert sent["routing_key"] == (
        "trade_responses" if kind == "decision" else "trade_negotiations"
    )
    body = json.loads(sent["body"])
    if kind == "decision":
        schema = SPEC["components"]["schemas"][name]
        jsonschema.Draft7Validator(schema).validate(body)
        assert set(body) == set(schema["properties"]) - {"retry"}
    else:
        validate(name, body)


def test_response_retry(client, boundary, worker_responses):
    worker_responses[0]["comment"] = "Example comment"
    from tests.test_trade_inbox import decline_key

    worker_responses[0]["decision_key"] = decline_key("Example comment")
    response = client.post(
        "/v1/trade/incoming/123/decision/retry",
        json=reviewed(boundary, {"approved_by": "example-reviewer", "of_claim_id": 41}),
    )
    assert response.status_code == 202, response.text
    sent = boundary[1].return_value.channel.return_value.basic_publish.call_args.kwargs
    assert sent["routing_key"] == "trade_responses"
    validate("TradeResponse", json.loads(sent["body"]))


def test_job_run_command():
    from datetime import datetime, timezone

    from src.launcher import publish
    from tests.test_launcher_k8s import CATALOG

    cursor = MagicMock()
    cursor.fetchone.return_value = ("run",)
    with patch.object(publish, "get_db_cursor") as db, patch.object(activity_runner, "publish_job_run"):
        db.return_value.__enter__.return_value = cursor
        publish.publish_run(
            CATALOG,
            "lineup-optimizer",
            trigger="chain",
            params={"week": 7, "dry_run": True},
            requested_by="example-user",
            dedupe_key="lineup-optimizer@root",
            parent_run_id="parent",
            chain_root_id="root",
            event_id="event",
            downstream=True,
            scheduled_for=datetime(2030, 10, 1, tzinfo=timezone.utc),
        )
    insert = next(c for c in cursor.execute.call_args_list if "INSERT INTO job_run_outbox" in c.args[0])
    stored = json.loads(insert.args[1][1])
    cursor.fetchone.return_value = ("run", stored)
    connection = MagicMock()
    with patch.object(activity_runner, "get_db_cursor") as db, patch.object(
        activity_runner.pika, "BlockingConnection", return_value=connection
    ):
        db.return_value.__enter__.return_value = cursor
        assert activity_runner.publish_job_run("run")
    send = connection.channel.return_value.basic_publish.call_args.kwargs
    assert send["routing_key"] == "job_runs"
    body = json.loads(send["body"])
    validate("RunCommand", body)
