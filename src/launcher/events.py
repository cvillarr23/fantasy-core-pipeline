"""The `job_events` contract: facts that can start runs, and `emit_event` to publish one.

`job_events` is a durable topic exchange and an event's routing key is its `key`. The launcher
maps keys to jobs through catalog `on_event` (`Scheduler.handle_event`); a fact nobody
subscribes to is simply ignored, so emitting one is always safe. The browser worker publishes
the same message from Node: see `docs/queue-publishing.md` and `docs/asyncapi.yaml`.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

import pika
from opentelemetry import propagate
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from src.queue_contracts import AMQP_URL, EVENT_KEY_PATTERN, JOB_EVENTS
from src.services import run_log_service

logger = logging.getLogger(__name__)

BROKER_TIMEOUT_SECONDS = 5


class JobEvent(BaseModel):
    """The `job_events` message body."""

    model_config = ConfigDict(extra="ignore")

    event_id: str
    key: str = Field(pattern=EVENT_KEY_PATTERN)
    subject: str
    payload: dict[str, Any]
    emitted_by_run_id: str | None
    at: AwareDatetime


def emit_event(key: str, subject: str, payload: dict[str, Any]) -> str | None:
    """Publishes one event, after the caller's own work has committed so a run it starts can
    read that work. Returns the `event_id`, or None when the broker didn't take it.

    A broker failure is logged and never raised: the caller's work is already done, a retry
    of the job wouldn't emit again because the fact is no longer new, and every subscriber
    has a schedule to fall back on. A key or payload that can't form an event is a bug, and
    raises ValueError.
    """
    event = JobEvent(
        event_id=str(uuid.uuid4()),
        key=key,
        subject=subject,
        payload=payload,
        emitted_by_run_id=run_log_service.current_run_id(),
        at=datetime.now(timezone.utc),
    )
    body = event.model_dump_json()
    try:
        _publish(event.key, body)
    except Exception:
        logger.exception("Event %s (%s) not published", event.event_id, event.key)
        return None
    return event.event_id


def _publish(key: str, body: str) -> None:
    params = pika.URLParameters(AMQP_URL)
    params.socket_timeout = BROKER_TIMEOUT_SECONDS
    params.blocked_connection_timeout = BROKER_TIMEOUT_SECONDS
    connection = pika.BlockingConnection(params)
    try:
        channel = connection.channel()
        channel.confirm_delivery()
        # Not mandatory: a topic with no subscriber bound yet is not a failure.
        channel.basic_publish(
            exchange=JOB_EVENTS,
            routing_key=key,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                headers=_trace_headers(),
            ),
        )
    finally:
        close_quietly(connection)


def _trace_headers() -> dict[str, str]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


def close_quietly(connection) -> None:
    if connection is None or not connection.is_open:
        return
    try:
        connection.close()
    except Exception:
        logger.warning("Could not close broker connection cleanly")
