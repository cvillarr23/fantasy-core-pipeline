import json
import threading
from contextlib import contextmanager
from unittest.mock import MagicMock

import pika

from src.launcher import heartbeat


class RecordingCursor:
    query = ""
    params = None

    def execute(self, query, params=None):
        self.query = query
        self.params = params


def test_heartbeat_upserts_the_singleton_row(monkeypatch):
    cursor = RecordingCursor()

    @contextmanager
    def cursor_context():
        yield cursor

    monkeypatch.setattr(heartbeat, "get_db_cursor", cursor_context)
    heartbeat.record_heartbeat({"job_runs": 3, "job_runs.dlq": 1})

    assert "INSERT INTO launcher_heartbeat" in cursor.query
    assert "ON CONFLICT(id) DO UPDATE" in cursor.query
    assert json.loads(cursor.params[0]) == {"job_runs": 3, "job_runs.dlq": 1}


def test_run_records_queue_depths_each_beat(monkeypatch):
    depths = {"job_runs": 3, "job_runs.dlq": 1}
    stop = threading.Event()
    beats = []

    def record(queue_depths):
        beats.append(queue_depths)
        stop.set()

    monkeypatch.setattr(heartbeat, "read_queue_depths", lambda: depths)
    monkeypatch.setattr(heartbeat, "record_heartbeat", record)
    heartbeat.run(stop, poll_seconds=0)

    assert beats == [depths]


def test_run_survives_a_failed_write(monkeypatch):
    stop = threading.Event()
    calls = []

    def record(queue_depths):
        calls.append(queue_depths)
        if len(calls) == 1:
            raise RuntimeError("database down")
        stop.set()

    monkeypatch.setattr(heartbeat, "read_queue_depths", dict)
    monkeypatch.setattr(heartbeat, "record_heartbeat", record)
    heartbeat.run(stop, poll_seconds=0)

    assert len(calls) == 2


def test_queue_depths_reads_job_runs_and_leaves_a_missing_queue_unknown(monkeypatch):
    connection = MagicMock()

    def declare(queue, passive):
        assert passive is True
        if queue == "job_runs.dlq":
            raise pika.exceptions.ChannelClosedByBroker(404, "NOT_FOUND")
        return MagicMock(method=MagicMock(message_count=4))

    connection.channel.return_value.queue_declare.side_effect = declare
    monkeypatch.setattr(
        heartbeat.pika, "BlockingConnection", MagicMock(return_value=connection)
    )

    assert heartbeat.read_queue_depths() == {"job_runs": 4, "job_runs.dlq": None}
    connection.close.assert_called_once()


def test_queue_depths_are_unknown_when_the_broker_is_away(monkeypatch):
    monkeypatch.setattr(
        heartbeat.pika,
        "BlockingConnection",
        MagicMock(side_effect=pika.exceptions.AMQPConnectionError("down")),
    )

    assert heartbeat.read_queue_depths() == {"job_runs": None, "job_runs.dlq": None}
