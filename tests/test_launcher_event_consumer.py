import json
import logging
import threading
from unittest.mock import MagicMock, patch

import pika
import pytest
from opentelemetry import trace

from src.launcher import event_consumer
from src.launcher.event_consumer import EventConsumer, EventNotHandled
from src.launcher.events import JobEvent

BODY = {
    "event_id": "e1",
    "key": "rumor.signal_created",
    "subject": "player:1",
    "payload": {"signal_id": 7},
    "emitted_by_run_id": "run-1",
    "at": "2026-10-28T15:05:00+00:00",
}


def deliver(consumer, channel, body, headers=None):
    properties = MagicMock(headers=headers)
    consumer._on_message(channel, MagicMock(delivery_tag=5), properties, body)


def test_handled_event_is_parsed_and_acked():
    handled = []
    channel = MagicMock()
    deliver(EventConsumer(handled.append), channel, json.dumps(BODY))
    (event,) = handled
    assert isinstance(event, JobEvent)
    assert (event.event_id, event.key) == ("e1", "rumor.signal_created")
    channel.basic_ack.assert_called_once_with(5)


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"[1]",
        json.dumps({**BODY, "at": "not a time"}),
        json.dumps({k: v for k, v in BODY.items() if k != "event_id"}),
        json.dumps({**BODY, "at": "2026-10-28T15:05:00"}),
    ],
)
def test_malformed_event_is_acked_and_dropped(body, caplog):
    handled = []
    channel = MagicMock()
    with caplog.at_level(logging.ERROR):
        deliver(EventConsumer(handled.append), channel, body)
    assert handled == []
    channel.basic_ack.assert_called_once_with(5)
    channel.basic_nack.assert_not_called()
    assert caplog.records


def test_failed_handling_requeues_and_drops_the_connection():
    def handle(event):
        raise RuntimeError("database away")

    channel = MagicMock()
    with pytest.raises(EventNotHandled):
        deliver(EventConsumer(handle), channel, json.dumps(BODY))
    channel.basic_nack.assert_called_once_with(5, requeue=True)
    channel.basic_ack.assert_not_called()


def test_handler_runs_inside_the_emitters_trace():
    trace_id = "0af7651916cd43dd8448eb211c80319c"
    seen = []

    def handle(event):
        seen.append(trace.get_current_span().get_span_context().trace_id)

    deliver(
        EventConsumer(handle),
        MagicMock(),
        json.dumps(BODY),
        headers={"traceparent": f"00-{trace_id}-b7ad6b7169203331-01"},
    )
    assert seen == [int(trace_id, 16)]


def stop_after_first_poll(connection, stop):
    connection.process_data_events.side_effect = lambda **_: stop.set()


def test_run_binds_its_own_durable_queue_to_every_key():
    stop = threading.Event()
    connection = MagicMock()
    stop_after_first_poll(connection, stop)
    with patch(
        "src.launcher.event_consumer.pika.BlockingConnection", return_value=connection
    ):
        EventConsumer(MagicMock()).run(stop)
    channel = connection.channel.return_value
    channel.exchange_declare.assert_called_once_with(
        exchange="job_events", passive=True
    )
    channel.queue_declare.assert_called_once_with(
        queue="job_events.launcher", durable=True
    )
    channel.queue_bind.assert_called_once_with(
        queue="job_events.launcher", exchange="job_events", routing_key="#"
    )
    consume = channel.basic_consume.call_args.kwargs
    assert consume["queue"] == "job_events.launcher"
    connection.close.assert_called_once()


def test_broker_failure_reconnects_instead_of_ending_the_thread():
    stop = threading.Event()
    connection = MagicMock()
    stop_after_first_poll(connection, stop)
    with (
        patch.object(event_consumer, "RECONNECT_BACKOFF_SECONDS", (0,)),
        patch(
            "src.launcher.event_consumer.pika.BlockingConnection",
            side_effect=[pika.exceptions.AMQPConnectionError("down"), connection],
        ),
    ):
        EventConsumer(MagicMock()).run(stop)
    connection.channel.return_value.basic_consume.assert_called_once()


def test_stop_during_backoff_ends_the_thread():
    stop = threading.Event()
    stop.wait = MagicMock(side_effect=lambda delay: stop.set() or True)
    with patch(
        "src.launcher.event_consumer.pika.BlockingConnection",
        side_effect=pika.exceptions.AMQPConnectionError("down"),
    ) as opened:
        EventConsumer(MagicMock()).run(stop)
    opened.assert_called_once()


def test_launcher_consumes_events_into_the_scheduler():
    from src.launcher import __main__ as launcher

    with (
        patch.object(launcher, "configure_tracing"),
        patch.object(launcher, "load_catalog_from_env"),
        patch.object(launcher.k8s, "validate_catalog"),
        patch.object(launcher.signal, "signal"),
        patch.object(launcher, "InlineRunner"),
        patch.object(launcher, "PostgresSchedulerState"),
        patch.object(launcher, "Watcher"),
        patch.object(launcher, "Consumer"),
        patch.object(launcher.heartbeat, "run"),
        patch.object(launcher, "Scheduler") as scheduler,
        patch.object(launcher, "EventConsumer") as consumer,
        patch.object(launcher.threading, "Thread") as thread,
    ):
        assert launcher.main() == 0
    consumer.assert_called_once_with(scheduler.return_value.handle_event)
    targets = {c.kwargs["name"]: c.kwargs["target"] for c in thread.call_args_list}
    assert targets["launcher-events"] is consumer.return_value.run


def test_backoff_restarts_after_a_successful_subscribe():
    stop = threading.Event()
    delays = []

    def wait(delay):
        delays.append(delay)
        if len(delays) == 3:
            stop.set()
        return stop.is_set()

    stop.wait = wait
    subscribed = MagicMock()
    subscribed.process_data_events.side_effect = pika.exceptions.AMQPConnectionError()
    down = pika.exceptions.AMQPConnectionError("down")
    with patch(
        "src.launcher.event_consumer.pika.BlockingConnection",
        side_effect=[down, subscribed, down],
    ):
        EventConsumer(MagicMock()).run(stop)
    assert delays == [1, 1, 2]
