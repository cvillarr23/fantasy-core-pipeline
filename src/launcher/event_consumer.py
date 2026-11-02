"""Consumes `job_events` and hands each event to the scheduler (`python -m src.launcher`).

The launcher owns its subscription: a durable `job_events.launcher` queue, created here with
no arguments, so fantasy-infra can add it to the broker definitions later without a conflict.
It is bound with `#` because a binding outlives a catalog edit, which leaves the catalog alone
deciding which keys matter. The exchange is the broker's and is only checked, so a missing one
is logged and retried rather than created with the wrong settings.

A delivery is acked once its run is recorded. If recording fails the event goes back to the
queue, which is safe because the dedupe key makes a second delivery a no-op. A failing event
returns to the head of the queue and holds up the rest, so one still failing after
RETRY_LIMIT is rejected and logged: long enough to ride out a database restart, while the job
keeps its schedule. A malformed event would fail forever, so it is logged and dropped at once.

pika connections aren't thread-safe, so this thread keeps its own.
"""

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Callable

import pika
from opentelemetry import propagate, trace
from pydantic import ValidationError

from src.launcher.consumer import RECONNECT_BACKOFF_SECONDS
from src.launcher.events import JobEvent, close_quietly
from src.queue_contracts import AMQP_URL, JOB_EVENTS, JOB_EVENTS_LAUNCHER_QUEUE

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

POLL_SECONDS = 5
PREFETCH_COUNT = 20
RETRY_LIMIT = timedelta(minutes=15)


class EventNotHandled(RuntimeError):
    """An event couldn't be recorded; it went back to the queue."""


class EventConsumer:
    def __init__(self, handle: Callable[[JobEvent], None], clock=None):
        self.handle = handle
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.failures = 0
        # (event_id, first failure) of the event at the head of the queue.
        self.failing: tuple[str, datetime] | None = None

    def _on_message(self, channel, method, properties, body):
        event = _parse(body)
        if event is None:
            channel.basic_ack(method.delivery_tag)
            return
        try:
            self._handle_traced(event, properties)
        except Exception as exc:
            if self._past_retry_limit(event):
                logger.exception(
                    "Dropping event %s (%s, %s): still failing after %s",
                    event.event_id,
                    event.key,
                    event.subject,
                    RETRY_LIMIT,
                )
                channel.basic_nack(method.delivery_tag, requeue=False)
                return
            channel.basic_nack(method.delivery_tag, requeue=True)
            # Every prefetched delivery would fail the same way, so drop the connection
            # (returning them to the queue) and let run() back off before consuming again.
            raise EventNotHandled(
                f"event {event.event_id} returned to the queue"
            ) from exc
        self.failing = None
        channel.basic_ack(method.delivery_tag)

    def _past_retry_limit(self, event):
        """Records this failure; True once the event has been failing for RETRY_LIMIT. Only
        one event is tracked, because a failure drops the connection and it comes back first.
        """
        now = self.clock()
        if self.failing is None or self.failing[0] != event.event_id:
            self.failing = (event.event_id, now)
        if now - self.failing[1] < RETRY_LIMIT:
            return False
        self.failing = None
        return True

    def _handle_traced(self, event, properties):
        """Handles the event inside the emitter's trace, so its run links back to it."""
        with tracer.start_as_current_span(
            "launcher.event",
            context=propagate.extract(properties.headers or {}),
            attributes={"fantasy.event_id": event.event_id, "fantasy.event_key": event.key},
        ):
            self.handle(event)

    def _consume(self, stop):
        connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
        try:
            channel = connection.channel()
            channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            channel.exchange_declare(exchange=JOB_EVENTS, passive=True)
            channel.queue_declare(queue=JOB_EVENTS_LAUNCHER_QUEUE, durable=True)
            channel.queue_bind(
                queue=JOB_EVENTS_LAUNCHER_QUEUE, exchange=JOB_EVENTS, routing_key="#"
            )
            channel.basic_consume(
                queue=JOB_EVENTS_LAUNCHER_QUEUE, on_message_callback=self._on_message
            )
            logger.info("Consuming %s", JOB_EVENTS_LAUNCHER_QUEUE)
            self.failures = 0
            while not stop.is_set():
                connection.process_data_events(time_limit=POLL_SECONDS)
        finally:
            close_quietly(connection)

    def run(self, stop: threading.Event):
        """Consumes until `stop` is set, reconnecting with backoff. Nothing here may stop
        the launcher: without this thread, jobs still start from their schedules."""
        while not stop.is_set():
            try:
                self._consume(stop)
            except Exception:
                delay = RECONNECT_BACKOFF_SECONDS[
                    min(self.failures, len(RECONNECT_BACKOFF_SECONDS) - 1)
                ]
                self.failures += 1
                logger.exception(
                    "Consuming %s interrupted; reconnecting in %ss",
                    JOB_EVENTS_LAUNCHER_QUEUE,
                    delay,
                )
                stop.wait(delay)


def _parse(body) -> JobEvent | None:
    try:
        return JobEvent.model_validate_json(body)
    except ValidationError as exc:
        logger.error("Dropping invalid job event: %s", exc)
        return None
