import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from kubernetes.client.rest import ApiException

from src.launcher import consumer as consumer_module
from src.launcher.consumer import Consumer, overlap_refuses
from tests.test_launcher_k8s import CATALOG, LAUNCHER_ENV

NOW = datetime(2026, 10, 27, 19, 15, tzinfo=timezone.utc)


class FakeStore:
    """run_log rows in memory, with the same decisions RunStore's SQL makes."""

    def __init__(self):
        self.rows = {}
        self.bodies = []
        self.fail = False

    def add(self, run_id, job, status="queued", executor=None, attempt=1):
        self.rows[run_id] = {
            "job": job,
            "status": status,
            "executor": executor,
            "attempt": attempt,
            "end_reason": None,
        }

    @contextmanager
    def admission(self):
        yield self

    def status(self, run_id):
        if self.fail:
            raise RuntimeError("database unavailable")
        row = self.rows.get(run_id)
        return row["status"] if row else None

    def run_state(self, run_id):
        if self.fail:
            raise RuntimeError("database unavailable")
        row = self.rows.get(run_id)
        return (row["status"], row["attempt"]) if row else (None, None)

    def attempt(self, run_id):
        row = self.rows.get(run_id)
        return row["attempt"] if row else None

    def job_running(self, job, run_id):
        return any(
            r["job"] == job and r["status"] == "running" and i != run_id
            for i, r in self.rows.items()
        )

    def running_pods(self):
        return sum(
            r["status"] == "running" and r["executor"] == "pod"
            for r in self.rows.values()
        )

    def claim(self, run_id, executor, k8s_job=None):
        row = self.rows[run_id]
        if row["status"] != "queued":
            return False
        row.update(status="running", executor=executor, k8s_job=k8s_job)
        return True

    def end_queued(self, run_id, status, end_reason, error=None):
        row = self.rows[run_id]
        if row["status"] == "queued":
            row.update(status=status, end_reason=end_reason, error=error)

    def queued_commands(self):
        return [b for b in self.bodies if self.rows[b["run_id"]]["status"] == "queued"]

    def finish_running(self, run_id, status, end_reason, error=None, force=False):
        row = self.rows[run_id]
        if force or row["status"] == "running":
            row.update(status=status, end_reason=end_reason, error=error)


class Clock:
    def __init__(self):
        self.now = NOW

    def __call__(self):
        return self.now


@pytest.fixture(autouse=True)
def launcher_env(monkeypatch):
    for name, value in LAUNCHER_ENV.items():
        monkeypatch.setenv(name, value)


@pytest.fixture
def store():
    return FakeStore()


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def runner():
    runner = MagicMock()
    runner.busy = False
    return runner


@pytest.fixture
def launch_pod():
    return MagicMock()


def make_consumer(store, clock, runner, launch_pod, catalog=CATALOG):
    return Consumer(catalog, runner, store=store, clock=clock, launch_pod=launch_pod)


@pytest.fixture
def consumer(store, clock, runner, launch_pod):
    return make_consumer(store, clock, runner, launch_pod)


def body(run_id, job="rumor-syncer", ttl=timedelta(minutes=30), **fields):
    return {
        "run_id": run_id,
        "job": job,
        "params": {},
        "trigger": "manual",
        "dedupe_key": run_id,
        "expires_at": (NOW + ttl).isoformat(),
        **fields,
    }


def queue_run(store, consumer, run_id, job="rumor-syncer", **fields):
    store.add(run_id, job)
    return consumer.admit(body(run_id, job, **fields))


def test_same_command_delivered_twice_launches_once(store, consumer, launch_pod):
    assert queue_run(store, consumer, "r1")
    assert consumer.admit(body("r1"))
    assert consumer.drain() == ["r1"]
    # Redelivered after the launch: the row is no longer queued.
    assert consumer.admit(body("r1")) is False
    assert consumer.drain() == []
    launch_pod.assert_called_once()
    assert store.rows["r1"]["status"] == "running"
    assert store.rows["r1"]["k8s_job"] == launch_pod.call_args.args[0].metadata.name


def test_forbid_overlap_skips_the_new_run(store, consumer, launch_pod):
    store.add("earlier", "rumor-syncer", status="running", executor="pod")
    queue_run(store, consumer, "r1")
    assert consumer.drain() == []
    assert store.rows["r1"]["status"] == "skipped"
    assert store.rows["r1"]["end_reason"] == "overlap"
    assert consumer.pending == {}
    launch_pod.assert_not_called()


def test_queue_overlap_waits_for_the_running_run(store, clock, runner, launch_pod):
    catalog = CATALOG.model_copy(deep=True)
    catalog.jobs["rumor-syncer"].overlap = "queue"
    consumer = make_consumer(store, clock, runner, launch_pod, catalog)
    store.add("earlier", "rumor-syncer", status="running", executor="pod")
    queue_run(store, consumer, "r1")
    assert consumer.drain() == []
    assert store.rows["r1"]["status"] == "queued"
    store.rows["earlier"]["status"] = "success"
    assert consumer.drain() == ["r1"]


def test_full_cap_waits_then_launches_when_a_slot_frees(store, consumer, launch_pod):
    store.add("a", "trade-scan", status="running", executor="pod")
    store.add("b", "trade-inbox", status="running", executor="pod")
    queue_run(store, consumer, "r1")
    assert consumer.drain() == []
    assert "r1" in consumer.pending
    store.rows["a"]["status"] = "success"
    assert consumer.drain() == ["r1"]
    launch_pod.assert_called_once()


def test_full_cap_expires_the_run_at_its_queue_ttl(store, clock, consumer, launch_pod):
    store.add("a", "trade-scan", status="running", executor="pod")
    store.add("b", "trade-inbox", status="running", executor="pod")
    queue_run(store, consumer, "r1")
    clock.now = NOW + timedelta(minutes=29)
    assert consumer.drain() == []
    assert store.rows["r1"]["status"] == "queued"
    clock.now = NOW + timedelta(minutes=30)
    assert consumer.drain() == []
    assert store.rows["r1"]["status"] == "skipped"
    assert store.rows["r1"]["end_reason"] == "expired"
    assert consumer.pending == {}
    launch_pod.assert_not_called()


def test_retry_attempt_is_not_rejected_by_the_original_queue_expiry(
    store, consumer, launch_pod
):
    store.add("r2", "rumor-syncer", attempt=2)
    assert consumer.admit(body("r2", ttl=timedelta(minutes=-1), attempt=2))
    assert consumer.drain() == ["r2"]
    assert store.rows["r2"]["status"] == "running"
    launch_pod.assert_called_once()


def test_old_delivery_is_dropped_when_run_log_has_a_newer_attempt(store, consumer):
    store.add("r2", "rumor-syncer", attempt=2)
    assert not consumer.admit(body("r2", attempt=1))
    assert consumer.pending == {}


def test_old_duplicate_does_not_replace_newer_pending_attempt(store, consumer):
    store.add("r2", "rumor-syncer", attempt=2)
    assert consumer.admit(body("r2", attempt=2))
    assert not consumer.admit(body("r2", attempt=1))
    assert consumer.pending["r2"].attempt == 2


def test_new_retry_delivery_replaces_an_older_pending_attempt(store, consumer):
    store.add("r2", "rumor-syncer")
    assert consumer.admit(body("r2", attempt=1))
    store.rows["r2"].update(status="queued", attempt=2)
    assert consumer.admit(body("r2", attempt=2))
    assert consumer.pending["r2"].attempt == 2


def test_waiting_run_does_not_block_other_jobs(store, clock, runner, launch_pod):
    catalog = CATALOG.model_copy(deep=True)
    catalog.jobs["rumor-syncer"].overlap = "queue"
    consumer = make_consumer(store, clock, runner, launch_pod, catalog)
    store.add("earlier", "rumor-syncer", status="running", executor="pod")
    queue_run(store, consumer, "waits")
    queue_run(store, consumer, "goes", job="trade-scan")
    assert consumer.drain() == ["goes"]
    assert list(consumer.pending) == ["waits"]


def test_inline_runs_skip_the_pod_cap_and_wait_for_the_runner(
    store, clock, runner, launch_pod
):
    catalog = CATALOG.model_copy(deep=True)
    catalog.jobs["rumor-syncer"].executor = "inline"
    consumer = make_consumer(store, clock, runner, launch_pod, catalog)
    store.add("a", "trade-scan", status="running", executor="pod")
    store.add("b", "trade-inbox", status="running", executor="pod")
    queue_run(store, consumer, "r1", parent_run_id="parent")
    runner.busy = True
    assert consumer.drain() == []
    runner.busy = False
    assert consumer.drain() == ["r1"]
    launch_pod.assert_not_called()
    (run,) = runner.submit.call_args.args
    assert run.argv == ["python", "-m", "src.ingestion.sync_rumors"]
    assert run.deadline_seconds == 25 * 60
    assert run.env == {
        "RUN_ID": "r1",
        "RUN_TRIGGER": "manual",
        "JOB_NAME": "rumor-syncer@r1",
        "RUN_PARENT_ID": "parent",
    }
    assert store.rows["r1"]["executor"] == "inline"


def test_cancelled_while_pending_is_dropped(store, consumer, launch_pod):
    store.add("a", "trade-scan", status="running", executor="pod")
    store.add("b", "trade-inbox", status="running", executor="pod")
    queue_run(store, consumer, "r1")
    consumer.drain()
    store.rows["r1"]["status"] = "cancelled"
    store.rows["a"]["status"] = "success"
    assert consumer.drain() == []
    assert consumer.pending == {}
    launch_pod.assert_not_called()


def test_launch_failure_fails_the_run_once_the_job_is_known_absent(
    store, consumer, launch_pod, monkeypatch
):
    monkeypatch.setattr(consumer_module.k8s, "job_exists", lambda name: False)
    launch_pod.side_effect = RuntimeError("apiserver down")
    queue_run(store, consumer, "r1")
    consumer.drain()
    assert store.rows["r1"]["status"] == "failure"
    assert store.rows["r1"]["end_reason"] == "launch_failed"
    assert "apiserver down" in store.rows["r1"]["error"]


@pytest.mark.parametrize("exists", [True, None])
def test_timed_out_create_keeps_a_job_that_may_exist_running(
    store, consumer, launch_pod, monkeypatch, exists
):
    monkeypatch.setattr(consumer_module.k8s, "job_exists", lambda name: exists)
    launch_pod.side_effect = TimeoutError("read timed out")
    queue_run(store, consumer, "r1")
    consumer.drain()
    assert store.rows["r1"]["status"] == "running"
    assert store.rows["r1"]["end_reason"] is None


def test_refused_create_fails_without_a_lookup(
    store, consumer, launch_pod, monkeypatch
):
    lookup = MagicMock()
    monkeypatch.setattr(consumer_module.k8s, "job_exists", lookup)
    launch_pod.side_effect = ApiException(status=422)
    queue_run(store, consumer, "r1")
    consumer.drain()
    assert store.rows["r1"]["status"] == "failure"
    lookup.assert_not_called()


def test_unrecordable_launch_failure_does_not_stop_the_pass(
    store, consumer, launch_pod, monkeypatch
):
    monkeypatch.setattr(consumer_module.k8s, "job_exists", lambda name: False)
    launch_pod.side_effect = [RuntimeError("apiserver down"), None]
    store.finish_running = MagicMock(side_effect=RuntimeError("database down"))
    queue_run(store, consumer, "r1")
    queue_run(store, consumer, "r2", job="trade-scan")
    assert consumer.drain() == ["r1", "r2"]
    assert launch_pod.call_count == 2


def test_missing_launcher_config_fails_before_claiming(
    store, consumer, launch_pod, monkeypatch
):
    monkeypatch.delenv("CORE_IMAGE")
    queue_run(store, consumer, "r1")
    consumer.drain()
    assert store.rows["r1"]["status"] == "failure"
    assert store.rows["r1"]["end_reason"] == "launch_failed"
    launch_pod.assert_not_called()


def test_unknown_job_or_bad_params_fail_the_run(store, consumer, launch_pod):
    queue_run(store, consumer, "r1", job="no-such-job")
    queue_run(store, consumer, "r2", job="lineup-optimizer", params={"week": "seven"})
    consumer.drain()
    for run_id in ("r1", "r2"):
        assert store.rows[run_id]["status"] == "failure"
        assert store.rows[run_id]["end_reason"] == "invalid_command"
    launch_pod.assert_not_called()


def test_params_become_cli_flags(store, consumer, launch_pod):
    queue_run(
        store,
        consumer,
        "r1",
        job="lineup-optimizer",
        params={"week": 7, "dry_run": True},
    )
    consumer.drain()
    container = launch_pod.call_args.args[0].spec.template.spec.containers[0]
    assert container.command == [
        "python",
        "-m",
        "src.optimization.lineup_solver",
        "--week",
        "7",
        "--dry-run",
    ]


def test_admit_drops_invalid_and_no_longer_queued_commands(store, consumer):
    assert consumer.admit({"run_id": "x"}) is False
    store.add("done", "rumor-syncer", status="success")
    assert consumer.admit(body("done")) is False
    assert consumer.admit(body("missing")) is False
    assert consumer.pending == {}


def test_recover_rebuilds_pending_and_ignores_the_later_delivery(
    store, consumer, launch_pod
):
    store.add("r1", "rumor-syncer")
    store.add("r2", "trade-scan", status="cancelled")
    store.bodies = [body("r1"), body("r2", job="trade-scan")]
    consumer.recover()
    assert list(consumer.pending) == ["r1"]
    assert consumer.admit(body("r1"))
    assert consumer.drain() == ["r1"]
    launch_pod.assert_called_once()


def test_delivery_is_acked_once_admitted(store, consumer):
    store.add("r1", "rumor-syncer", status="running")
    store.add("r2", "rumor-syncer")
    channel = MagicMock()
    for tag, run_id in enumerate(("r1", "r2")):
        consumer._on_message(
            channel, MagicMock(delivery_tag=tag), None, json.dumps(body(run_id))
        )
    assert [c.args for c in channel.basic_ack.call_args_list] == [(0,), (1,)]
    assert list(consumer.pending) == ["r2"]


def test_non_json_delivery_is_acked_and_dropped(consumer):
    channel = MagicMock()
    consumer._on_message(channel, MagicMock(delivery_tag=7), None, b"not json")
    channel.basic_ack.assert_called_once_with(7)
    channel.basic_nack.assert_not_called()


def test_unreadable_run_log_returns_the_delivery_and_drops_the_connection(
    store, consumer
):
    store.fail = True
    channel = MagicMock()
    with pytest.raises(consumer_module.RunLogUnavailable):
        consumer._on_message(
            channel, MagicMock(delivery_tag=3), None, json.dumps(body("r1"))
        )
    channel.basic_nack.assert_called_once_with(3, requeue=True)
    channel.basic_ack.assert_not_called()


def test_overlap_refuses_only_forbid_jobs_with_a_running_run(store):
    store.add("earlier", "rumor-syncer", status="running", executor="pod")
    assert overlap_refuses(CATALOG, "rumor-syncer", store=store)
    assert not overlap_refuses(CATALOG, "trade-scan", store=store)
    catalog = CATALOG.model_copy(deep=True)
    catalog.jobs["rumor-syncer"].overlap = "queue"
    assert not overlap_refuses(catalog, "rumor-syncer", store=store)
