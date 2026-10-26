import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.launcher import publish
from src.launcher.catalog import UnknownJobError
from src.launcher.publish import RunCommand, publish_run
from tests.test_launcher_k8s import CATALOG


class FakeDb:
    """run_log + job_run_outbox with the unique dedupe_key index publish_run relies on."""

    def __init__(self):
        self.runs = {}
        self.outbox = {}
        self._result = None

    @contextmanager
    def cursor(self):
        yield self

    def execute(self, sql, params=()):
        if sql.lstrip().startswith("INSERT INTO run_log"):
            run_id, job, trigger, run_trigger, *_ = params
            dedupe_key = params[6]
            if any(r["dedupe_key"] == dedupe_key for r in self.runs.values()):
                self._result = None
                return
            self.runs[run_id] = {
                "job": job,
                "trigger_type": trigger,
                "trigger": json.loads(run_trigger),
                "dedupe_key": dedupe_key,
                "status": "queued",
                "params": params,
            }
            self._result = (run_id,)
        elif sql.startswith("SELECT id FROM run_log WHERE dedupe_key"):
            self._result = next(
                (i,) for i, r in self.runs.items() if r["dedupe_key"] == params[0]
            )
        elif sql.startswith("INSERT INTO job_run_outbox"):
            self.outbox[params[0]] = json.loads(params[1])
        else:
            raise AssertionError(sql)

    def fetchone(self):
        return self._result


@pytest.fixture
def db():
    db = FakeDb()
    with patch.object(publish, "get_db_cursor", db.cursor):
        yield db


@pytest.fixture
def deliver():
    with patch("src.services.activity_runner.publish_job_run") as deliver:
        yield deliver


def test_manual_run_is_queued_with_its_outbox_row_and_delivered(db, deliver):
    before = datetime.now(timezone.utc)
    result = publish_run(CATALOG, "lineup-optimizer", trigger="manual", params={"week": 7}, requested_by="ui")
    run_id = result["run_id"]
    assert result == {"run_id": run_id, "status": "queued"}
    deliver.assert_called_once_with(run_id)
    command = RunCommand.model_validate(db.outbox[run_id])
    assert command.job == "lineup-optimizer"
    assert command.params == {"week": 7}
    assert command.dedupe_key == run_id
    assert command.requested_by == "ui"
    assert before + timedelta(minutes=30) <= command.expires_at <= datetime.now(timezone.utc) + timedelta(minutes=30)
    row = db.runs[run_id]
    assert (row["job"], row["trigger_type"], row["status"]) == ("lineup-optimizer", "manual", "queued")
    assert row["trigger"]["params"] == {"week": 7}


def test_same_dedupe_key_twice_queues_one_run(db, deliver):
    key = "rumor-syncer@2026-10-27T19:30:00-05:00"
    first = publish_run(CATALOG, "rumor-syncer", trigger="schedule", dedupe_key=key)
    second = publish_run(CATALOG, "rumor-syncer", trigger="schedule", dedupe_key=key)
    assert second == {"run_id": first["run_id"], "status": "duplicate"}
    assert list(db.runs) == [first["run_id"]]
    assert list(db.outbox) == [first["run_id"]]
    deliver.assert_called_once_with(first["run_id"])


def test_scheduler_fields_pass_through(db, deliver):
    slot = datetime(2026, 10, 27, 21, 45, tzinfo=timezone.utc)
    expires = slot + timedelta(hours=1)
    result = publish_run(
        CATALOG,
        "rumor-action-trigger",
        trigger="chain",
        dedupe_key="rumor-action-trigger@root",
        parent_run_id="parent",
        chain_root_id="root",
        scheduled_for=slot,
        expires_at=expires,
        downstream=True,
    )
    command = RunCommand.model_validate(db.outbox[result["run_id"]])
    assert command.expires_at == expires
    assert command.scheduled_for == slot
    assert (command.parent_run_id, command.chain_root_id, command.downstream) == ("parent", "root", True)
    assert command.job_name_env() == "rumor-action-trigger@root"


def test_unknown_job_and_bad_params_write_nothing(db, deliver):
    with pytest.raises(UnknownJobError):
        publish_run(CATALOG, "no-such-job", trigger="manual")
    with pytest.raises(ValueError, match="unknown params"):
        publish_run(CATALOG, "rumor-syncer", trigger="manual", params={"week": 1})
    with pytest.raises(ValueError, match="must be int"):
        publish_run(CATALOG, "lineup-optimizer", trigger="manual", params={"week": "7"})
    assert db.runs == {}
    deliver.assert_not_called()


def test_broker_failure_still_returns_the_queued_run(db, deliver):
    deliver.side_effect = RuntimeError("broker down")
    result = publish_run(CATALOG, "rumor-syncer", trigger="cli")
    assert result["status"] == "queued"
    assert result["run_id"] in db.outbox
