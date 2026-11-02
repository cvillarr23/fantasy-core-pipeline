import json
import logging
import uuid
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pika
import pytest

from src.launcher.events import emit_event
from src.services import run_log_service


@pytest.fixture
def connection():
    connection = MagicMock()
    with patch(
        "src.launcher.events.pika.BlockingConnection", return_value=connection
    ) as opened:
        connection.opened = opened
        yield connection


def published(connection):
    return connection.channel.return_value.basic_publish.call_args.kwargs


def test_emit_publishes_the_contract_to_the_topic_exchange(connection):
    event_id = emit_event("rumor.signal_created", "player:1", {"signal_id": 7})

    message = published(connection)
    assert message["exchange"] == "job_events"
    assert message["routing_key"] == "rumor.signal_created"
    assert message["properties"].delivery_mode == 2
    assert message["properties"].content_type == "application/json"
    body = json.loads(message["body"])
    assert set(body) == {
        "event_id",
        "key",
        "subject",
        "payload",
        "emitted_by_run_id",
        "at",
    }
    assert body["event_id"] == event_id
    assert uuid.UUID(event_id)
    assert body["key"] == "rumor.signal_created"
    assert body["subject"] == "player:1"
    assert body["payload"] == {"signal_id": 7}
    assert body["emitted_by_run_id"] is None
    assert datetime.fromisoformat(body["at"]).tzinfo is not None


def test_emit_waits_for_the_broker_confirm_before_closing(connection):
    emit_event("rumor.signal_created", "player:1", {})
    channel = connection.channel.return_value
    calls = [call[0] for call in channel.mock_calls]
    assert calls.index("confirm_delivery") < calls.index("basic_publish")
    connection.close.assert_called_once()


def test_emit_names_the_adopted_run(connection):
    # run_scope pops RUN_ID from the environment, so only current_run still knows it.
    token = run_log_service.current_run.set(SimpleNamespace(id="run-9"))
    try:
        emit_event("rumor.signal_created", "player:1", {})
    finally:
        run_log_service.current_run.reset(token)
    assert json.loads(published(connection)["body"])["emitted_by_run_id"] == "run-9"


def test_emit_carries_the_trace_in_a_header_not_the_body(connection):
    def inject(carrier):
        carrier["traceparent"] = "00-abc-def-01"

    with patch("src.launcher.events.propagate.inject", side_effect=inject):
        emit_event("rumor.signal_created", "player:1", {})
    message = published(connection)
    assert message["properties"].headers == {"traceparent": "00-abc-def-01"}
    assert "traceparent" not in json.loads(message["body"])


@pytest.mark.parametrize("key", ["rumor.*", "rumor.#", "rumor", "Rumor.Created", ""])
def test_emit_rejects_a_key_that_is_not_an_exact_routing_key(connection, key):
    with pytest.raises(ValueError):
        emit_event(key, "player:1", {})
    connection.opened.assert_not_called()


def test_emit_rejects_an_unserializable_payload(connection):
    with pytest.raises(ValueError):
        emit_event("rumor.signal_created", "player:1", {"bad": object()})
    connection.opened.assert_not_called()


def test_unreachable_broker_is_logged_and_does_not_fail_the_job(caplog):
    with patch(
        "src.launcher.events.pika.BlockingConnection",
        side_effect=pika.exceptions.AMQPConnectionError("down"),
    ):
        with caplog.at_level(logging.ERROR):
            assert emit_event("rumor.signal_created", "player:1", {}) is None
    assert "rumor.signal_created" in caplog.text


def test_failed_publish_closes_the_connection_and_does_not_fail_the_job(connection):
    channel = connection.channel.return_value
    channel.basic_publish.side_effect = pika.exceptions.ChannelClosedByBroker(
        404, "no exchange"
    )
    assert emit_event("rumor.signal_created", "player:1", {}) is None
    connection.close.assert_called_once()
