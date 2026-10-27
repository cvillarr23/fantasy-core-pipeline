"""Launcher heartbeat: the `launcher_heartbeat` row behind `GET /v1/launcher/health`.

Every POLL_SECONDS it upserts the singleton row (`id=1`) with the time and the depths of
`job_runs` and its dead-letter queue. It runs on its own thread, so an unreachable broker
(up to BROKER_TIMEOUT_SECONDS per read) never delays the scheduler's tick, and a missing
heartbeat still means the launcher process itself has stalled or died.
"""

import json
import logging
import threading

import pika

from src.database import get_db_cursor
from src.queue_contracts import AMQP_URL, JOB_RUNS

logger = logging.getLogger(__name__)

POLL_SECONDS = 15
BROKER_TIMEOUT_SECONDS = 5
# `job_runs` dead-letters to `<queue>.dlq` (fantasy-infra's broker policy).
QUEUES = (JOB_RUNS, f"{JOB_RUNS}.dlq")


def read_queue_depths() -> dict[str, int | None]:
    """Read broker queue depths without creating queues or consuming messages."""
    connection = None
    depths: dict[str, int | None] = {queue: None for queue in QUEUES}
    try:
        params = pika.URLParameters(AMQP_URL)
        params.socket_timeout = BROKER_TIMEOUT_SECONDS
        params.blocked_connection_timeout = BROKER_TIMEOUT_SECONDS
        connection = pika.BlockingConnection(params)
        for queue in QUEUES:
            # A missing queue closes the channel, so each check gets its own.
            channel = connection.channel()
            try:
                result = channel.queue_declare(queue=queue, passive=True)
                depths[queue] = result.method.message_count
            except pika.exceptions.ChannelClosedByBroker:
                pass
            finally:
                if channel.is_open:
                    channel.close()
    except Exception:
        logger.warning("Launcher queue depths unavailable", exc_info=True)
    finally:
        if connection is not None and connection.is_open:
            try:
                connection.close()
            except Exception:
                logger.warning("Could not close RabbitMQ connection cleanly")
    return depths


def record_heartbeat(queue_depths: dict[str, int | None]) -> None:
    with get_db_cursor() as c:
        c.execute(
            """INSERT INTO launcher_heartbeat(id,at,queue_depths) VALUES (1,now(),%s)
            ON CONFLICT(id) DO UPDATE SET at=now(),queue_depths=EXCLUDED.queue_depths""",
            (json.dumps(queue_depths),),
        )


def run(stop: threading.Event, poll_seconds: float = POLL_SECONDS) -> None:
    """Beats until `stop` is set; a failed write (database away) is retried next poll."""
    while not stop.is_set():
        try:
            record_heartbeat(read_queue_depths())
        except Exception:
            logger.exception("Could not record launcher heartbeat")
        stop.wait(poll_seconds)
