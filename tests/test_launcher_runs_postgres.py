"""Optional checks of every `Runs` transition, and the launch-detail backfill, against a
disposable local Postgres (see `tests/test_launcher_postgres.py` for the fixture)."""

import json
from datetime import datetime, timedelta, timezone

from src.launcher import runs as runs_module
from src.launcher.publish import publish_run
from src.launcher.runs import RunStore, Runs
from src.services import run_log_service as run_log
from tests.test_launcher_k8s import CATALOG
from tests.test_launcher_postgres import postgres, rows  # noqa: F401 (fixture)


def queue(job="rumor-syncer", **kwargs):
    return publish_run(CATALOG, job, trigger=kwargs.pop("trigger", "manual"), **kwargs)[
        "run_id"
    ]


def claimed(job="rumor-syncer", executor="pod", k8s_job=None, **kwargs):
    identity = queue(job, **kwargs)
    with RunStore().admission() as admission:
        assert admission.claim(identity, executor, k8s_job)
    return identity


def row(postgres, identity, columns="status,end_reason,attempt"):
    return rows(postgres, f"SELECT {columns} FROM run_log WHERE id=%s", (identity,))[0]


def chain_root(postgres):
    with runs_module.get_db_cursor() as c:
        values = Runs(c).insert_chain_root(
            chain="tuesday",
            trigger={"chain": "tuesday", "from_step": None},
            dedupe_key="tuesday@slot",
            scheduled_for=datetime.now(timezone.utc),
            requested_by=None,
            columns="id",
        )
    return values[0]


def test_queued_row_keeps_expiry_in_a_column_and_params_in_trigger(postgres):
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    identity = queue("lineup-optimizer", params={"week": 3}, expires_at=expires)
    stored_expiry, trigger, executor = row(postgres, identity, "expires_at,trigger,executor")
    assert stored_expiry == expires
    assert trigger == {"params": {"week": 3}, "downstream": False}
    assert executor is None


def test_claim_records_executor_and_the_counts_follow(postgres):
    pod = claimed(k8s_job="rumor-syncer-x-a1")
    claimed("trade-scan", executor="inline")
    queue("lineup-optimizer")
    assert row(postgres, pod, "executor,k8s_job") == ("pod", "rumor-syncer-x-a1")
    with RunStore().admission() as admission:
        assert admission.running_pods() == 1
        assert admission.launcher_counts() == (2, 1)


def test_finish_running_only_replaces_what_its_guard_allows(postgres):
    store = RunStore()
    identity = claimed()
    run_log.finish_run(identity, "success")
    store.finish_running(identity, "failure", "job_failed")
    assert row(postgres, identity)[:2] == ("success", None)
    store.finish_running(identity, "failure", "job_failed", force=True, expected_attempt=2)
    assert row(postgres, identity)[:2] == ("success", None)
    store.finish_running(identity, "failure", "job_failed", force=True, expected_attempt=1)
    assert row(postgres, identity)[:2] == ("failure", "job_failed")


def test_end_queued_leaves_a_claimed_run_alone(postgres):
    queued = queue()
    running = claimed("trade-scan")
    with RunStore().admission() as admission:
        admission.end_queued(queued, "skipped", "overlap")
        admission.end_queued(running, "skipped", "overlap")
    assert row(postgres, queued)[:2] == ("skipped", "overlap")
    assert row(postgres, running)[0] == "running"


def test_cancel_reports_what_it_did(postgres):
    identity = queue()
    finished = claimed("trade-scan")
    run_log.finish_run(finished, "success")
    with runs_module.get_db_cursor() as c:
        runs = Runs(c)
        assert runs.cancel(identity) is True
        assert runs.cancel(identity) == "cancelled"
        assert runs.cancel(finished) == "success"
        assert runs.cancel("missing") is None
    assert row(postgres, identity)[:2] == ("cancelled", "cancelled")


def test_cancelling_a_chain_root_cancels_its_active_steps_only(postgres):
    root = chain_root(postgres)
    queued = queue(trigger="chain", chain_root_id=root)
    running = claimed("trade-scan", trigger="chain", chain_root_id=root)
    done = claimed("lineup-optimizer", trigger="chain", chain_root_id=root)
    run_log.finish_run(done, "success")
    with runs_module.get_db_cursor() as c:
        assert Runs(c).cancel(root) is True
    assert [row(postgres, i)[0] for i in (root, queued, running, done)] == [
        "cancelled",
        "cancelled",
        "cancelled",
        "success",
    ]


def test_cancelled_run_is_watched_until_reconciled(postgres):
    store = RunStore()
    identity = claimed(k8s_job="job-1")
    with runs_module.get_db_cursor() as c:
        Runs(c).cancel(identity)
    assert [r.run_id for r in store.watched_runs()] == [identity]
    store.mark_cancel_reconciled(identity)
    assert store.watched_runs() == []
    assert row(postgres, identity, "cancel_reconciled_at IS NOT NULL")[0] is True


def test_mark_cancel_reconciled_ignores_runs_that_are_not_cancelled(postgres):
    identity = claimed()
    RunStore().mark_cancel_reconciled(identity)
    assert row(postgres, identity, "cancel_reconciled_at")[0] is None


def test_watched_runs_selects_retryable_failures_only_with_attempts_left(postgres):
    store = RunStore()
    retryable = claimed(executor="inline")
    exhausted = claimed("trade-scan", executor="inline")
    store.finish_running(retryable, "failure", "job_failed")
    store.finish_running(exhausted, "failure", "job_failed")
    limits = {"rumor-syncer": 2, "trade-scan": 1}
    assert [r.run_id for r in store.watched_runs(max_attempts=limits)] == [retryable]


def test_requeue_attempt_resets_the_row_and_its_outbox_command(postgres):
    store = RunStore()
    identity = claimed(executor="inline")
    store.finish_running(identity, "timeout", "deadline")
    assert not store.requeue_attempt(identity, 2, expected_attempt=5)
    assert store.requeue_attempt(identity, 2, expected_attempt=1)
    assert row(postgres, identity) == ("queued", None, 2)
    body = rows(postgres, "SELECT body FROM job_run_outbox WHERE run_id=%s", (identity,))
    assert body[0][0]["attempt"] == 2


def test_requeue_attempt_refuses_a_step_whose_chain_ended(postgres):
    root = chain_root(postgres)
    step = claimed(trigger="chain", chain_root_id=root)
    store = RunStore()
    store.finish_running(step, "failure", "job_failed")
    with runs_module.get_db_cursor() as c:
        Runs(c).finish_chain_root(root, "timeout", "max_duration")
    assert not store.requeue_attempt(step, 2)
    assert row(postgres, step)[0] == "failure"


def test_skipped_row_and_chain_root_reuse_dedupe_keys(postgres):
    slot = datetime.now(timezone.utc)
    with runs_module.get_db_cursor() as c:
        runs = Runs(c)
        args = dict(
            job="rumor-syncer",
            trigger_type="schedule",
            dedupe_key="rumor-syncer@slot",
            scheduled_for=slot,
            end_reason="paused",
        )
        assert runs.insert_skipped(**args)
        assert not runs.insert_skipped(**args)
        assert runs.run_id_for_dedupe_key("rumor-syncer@slot") is not None
    assert rows(postgres, "SELECT status,end_reason FROM run_log") == [
        ("skipped", "paused")
    ]


LEGACY = {
    "executor": "pod",
    "k8s_job": "rumor-syncer-x-a1",
    "expires_at": "2026-10-27T12:00:00+00:00",
    "cancel_reconciled": True,
    "params": {"week": 3},
    "downstream": False,
}


def legacy_row(postgres, identity, status, trigger):
    with postgres, postgres.cursor() as c:
        c.execute(
            """INSERT INTO run_log(id,job_name,trigger_type,trigger,status,finished_at)
            VALUES (%s,'rumor-syncer','manual',%s,%s,'2026-10-27T13:00:00+00:00')""",
            (identity, json.dumps(trigger), status),
        )


def migrate(postgres):
    with postgres, postgres.cursor() as c:
        c.execute(run_log.SCHEMA)


def test_migration_moves_launch_details_out_of_trigger(postgres):
    legacy_row(postgres, "old", "cancelled", LEGACY)
    legacy_row(postgres, "plain", "success", {"queue": "lineup", "tracking_id": "t"})
    migrate(postgres)
    columns = "executor,k8s_job,expires_at,cancel_reconciled_at,trigger"
    executor, k8s_job, expires_at, reconciled_at, trigger = row(postgres, "old", columns)
    assert (executor, k8s_job) == ("pod", "rumor-syncer-x-a1")
    assert expires_at == datetime(2026, 10, 27, 12, tzinfo=timezone.utc)
    assert reconciled_at == datetime(2026, 10, 27, 13, tzinfo=timezone.utc)
    assert trigger == {"params": {"week": 3}, "downstream": False}
    assert row(postgres, "plain", columns) == (
        None,
        None,
        None,
        None,
        {"queue": "lineup", "tracking_id": "t"},
    )


def test_migration_is_idempotent_and_keeps_the_watcher_selection(postgres):
    legacy_row(postgres, "live", "running", {"executor": "inline", "params": {}})
    migrate(postgres)
    migrate(postgres)
    assert row(postgres, "live", "executor,trigger") == ("inline", {"params": {}})
    assert [r.run_id for r in RunStore().watched_runs()] == ["live"]
