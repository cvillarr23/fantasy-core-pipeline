"""Optional checks of the launcher's run_log SQL against an explicitly supplied local disposable DB."""

import os
from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql

from src.launcher import heartbeat, publish, runs as store_module
from src.launcher.consumer import Consumer
from src.launcher.publish import publish_run
from src.launcher.scheduler import PostgresSchedulerState
from src.launcher.runs import RunStore
from src.services import (
    activity_service as activity,
    jobs_service,
    run_log_service as run_log,
)
from tests.test_launcher_k8s import CATALOG, LAUNCHER_ENV


@pytest.fixture
def postgres():
    dsn = os.getenv("ACTIVITY_TEST_DSN")
    if not dsn:
        pytest.skip("Set ACTIVITY_TEST_DSN to a disposable local Postgres")
    assert psycopg2.extensions.parse_dsn(dsn).get("host") in {"127.0.0.1", "localhost"}
    conn = psycopg2.connect(dsn)
    schema = "launcher_test_" + uuid4().hex
    with conn, conn.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {};").format(sql.Identifier(schema)))
        cursor.execute(sql.SQL("SET search_path TO {};").format(sql.Identifier(schema)))
        cursor.execute(activity.SCHEMA)
        cursor.execute(run_log.SCHEMA)

    @contextmanager
    def cursor_context():
        with conn, conn.cursor() as cursor:
            yield cursor

    with (
        patch.object(publish, "get_db_cursor", cursor_context),
        patch.object(store_module, "get_db_cursor", cursor_context),
        patch.object(run_log, "get_db_cursor", cursor_context),
        patch.object(jobs_service, "get_db_cursor", cursor_context),
        patch.object(heartbeat, "get_db_cursor", cursor_context),
        patch("src.services.activity_runner.publish_job_run"),
    ):
        try:
            yield conn
        finally:
            conn.rollback()
            with conn, conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE;").format(sql.Identifier(schema))
                )
            conn.close()


def rows(conn, query, params=()):
    with conn, conn.cursor() as c:
        c.execute(query, params)
        return c.fetchall()


def test_dedupe_key_conflict_returns_the_first_run(postgres):
    first = publish_run(
        CATALOG, "rumor-syncer", trigger="schedule", dedupe_key="rumor-syncer@slot"
    )
    second = publish_run(
        CATALOG, "rumor-syncer", trigger="schedule", dedupe_key="rumor-syncer@slot"
    )
    assert second == {"run_id": first["run_id"], "status": "duplicate"}
    assert rows(postgres, "SELECT id,status,job_name,trigger_type FROM run_log") == [
        (first["run_id"], "queued", "rumor-syncer", "schedule")
    ]
    assert rows(postgres, "SELECT run_id,published_at FROM job_run_outbox") == [
        (first["run_id"], None)
    ]


def test_admission_sql(postgres):
    store = RunStore()
    a = publish_run(CATALOG, "rumor-syncer", trigger="manual")["run_id"]
    b = publish_run(CATALOG, "rumor-syncer", trigger="manual")["run_id"]
    c = publish_run(CATALOG, "trade-scan", trigger="manual")["run_id"]
    assert [body["run_id"] for body in store.queued_commands()] == [a, b, c]
    with store.admission() as admission:
        assert admission.status(a) == "queued"
        assert not admission.job_running("rumor-syncer", b)
        assert admission.claim(a, "pod", "rumor-syncer-x-a1")
        assert not admission.claim(a, "pod", "again")
        assert admission.claim(c, "inline")
    with store.admission() as admission:
        assert admission.job_running("rumor-syncer", b)
        assert not admission.job_running("rumor-syncer", a)
        # Only pod-executor runs count toward the cap.
        assert admission.running_pods() == 1
        admission.end_queued(b, "skipped", "overlap")
        admission.end_queued(a, "skipped", "overlap")
    assert rows(
        postgres,
        "SELECT status,end_reason,k8s_job FROM run_log WHERE id=%s",
        (a,),
    ) == [("running", None, "rumor-syncer-x-a1")]
    assert rows(
        postgres, "SELECT status,end_reason FROM run_log WHERE id=%s", (b,)
    ) == [("skipped", "overlap")]
    assert [body["run_id"] for body in store.queued_commands()] == []
    store.finish_running(a, "success", "exit_code")
    store.finish_running(a, "failure", "exit_code")
    store.finish_running(c, "timeout", "deadline", "deadline exceeded", force=True)
    assert rows(
        postgres,
        "SELECT id,status,end_reason FROM run_log WHERE id IN (%s,%s) ORDER BY id=%s",
        (a, c, c),
    ) == [
        (a, "success", "exit_code"),
        (c, "timeout", "deadline"),
    ]


def test_cancelled_run_cannot_be_adopted_or_finalized_by_the_process(postgres):
    identity = publish_run(CATALOG, "rumor-syncer", trigger="manual")["run_id"]
    with RunStore().admission() as admission:
        assert admission.claim(identity, "pod", "rumor-syncer-r1-a1")
    with postgres, postgres.cursor() as cursor:
        cursor.execute(
            "UPDATE run_log SET status='cancelled',end_reason='cancelled' WHERE id=%s",
            (identity,),
        )
    run_log.adopt_run(identity, "sync_rumors", "manual")
    run_log.finish_run(identity, "failure", "cancelled pod stopped")
    RunStore().mark_cancel_reconciled(identity)
    assert rows(
        postgres, "SELECT status,end_reason FROM run_log WHERE id=%s", (identity,)
    ) == [("cancelled", "cancelled")]
    assert rows(
        postgres,
        "SELECT cancel_reconciled_at IS NOT NULL FROM run_log WHERE id=%s",
        (identity,),
    ) == [(True,)]


def test_old_process_cannot_adopt_or_finish_a_requeued_attempt(postgres):
    identity = publish_run(CATALOG, "lineup-optimizer", trigger="manual")["run_id"]
    store = RunStore()
    with store.admission() as admission:
        assert admission.claim(identity, "pod", "lineup-optimizer-r1-a1")
    assert store.requeue_attempt(identity, 2, expected_attempt=1)

    run_log.adopt_run(identity, "sync_rumors", "manual", expected_attempt=1)
    run_log.finish_run(identity, "failure", "late old process", expected_attempt=1)

    assert rows(
        postgres, "SELECT status,attempt,error FROM run_log WHERE id=%s", (identity,)
    ) == [("queued", 2, None)]


def test_process_exit_fallback_preserves_run_scope_outcome(postgres):
    identity = publish_run(CATALOG, "lineup-optimizer", trigger="manual")["run_id"]
    store = RunStore()
    with store.admission() as admission:
        assert admission.claim(identity, "inline")
    run_log.finish_run(identity, "failure", "worker error", end_reason="exit_code")

    store.finish_running(
        identity,
        "success",
        "exit_code",
        expected_attempt=1,
    )

    assert rows(
        postgres, "SELECT status,end_reason,error FROM run_log WHERE id=%s", (identity,)
    ) == [("failure", "exit_code", "worker error")]


def test_retry_requeues_the_same_run_and_updates_its_outbox_attempt(postgres):
    identity = publish_run(CATALOG, "lineup-optimizer", trigger="manual")["run_id"]
    store = RunStore()
    with store.admission() as admission:
        assert admission.claim(identity, "pod", "lineup-optimizer-r1-a1")
    assert not store.requeue_attempt(identity, 2, expected_attempt=9)
    assert store.requeue_attempt(identity, 2, expected_attempt=1)
    assert rows(
        postgres, "SELECT status,attempt FROM run_log WHERE id=%s", (identity,)
    ) == [("queued", 2)]
    commands = store.queued_commands()
    assert len(commands) == 1
    assert commands[0]["run_id"] == identity
    assert commands[0]["attempt"] == 2


def test_watcher_query_limits_terminal_rows_to_retryable_jobs(postgres):
    store = RunStore()
    active_pod = publish_run(CATALOG, "lineup-optimizer", trigger="manual")["run_id"]
    failed_pod = publish_run(CATALOG, "lineup-optimizer", trigger="manual")["run_id"]
    failed_inline = publish_run(CATALOG, "lineup-optimizer", trigger="manual")["run_id"]
    with store.admission() as admission:
        assert admission.claim(active_pod, "pod", "lineup-active-a1")
        assert admission.claim(failed_pod, "pod", "lineup-failed-a1")
        assert admission.claim(failed_inline, "inline")
    store.finish_running(failed_pod, "failure", "exit_code", force=True)
    store.finish_running(failed_inline, "failure", "exit_code", force=True)
    max_attempts = {
        "lineup-optimizer": CATALOG.resolve("lineup-optimizer").max_attempts
    }
    watched = {row.run_id for row in store.watched_runs([], max_attempts)}
    assert watched == {active_pod, failed_pod, failed_inline}
    with_visible_job = {
        row.run_id for row in store.watched_runs([failed_pod], max_attempts)
    }
    assert failed_pod in with_visible_job


def test_cancelling_a_chain_root_cancels_its_queued_and_running_steps(postgres):
    store = RunStore()
    state = PostgresSchedulerState(store_module.get_db_cursor)
    root = state.create_chain_root(
        chain="tuesday-weekly", dedupe_key="tuesday@slot", scheduled_for=None
    ).id
    other_root = state.create_chain_root(
        chain="tuesday-weekly", dedupe_key="tuesday@other", scheduled_for=None
    ).id

    def step(job, chain_root_id):
        return publish_run(
            CATALOG,
            job,
            trigger="chain",
            dedupe_key=f"{job}@{chain_root_id}",
            parent_run_id=chain_root_id,
            chain_root_id=chain_root_id,
        )["run_id"]

    queued = step("rumor-syncer", root)
    running = step("lineup-optimizer", root)
    finished = step("trade-scan", root)
    unrelated = step("lineup-optimizer", other_root)
    with store.admission() as admission:
        assert admission.claim(running, "pod", "lineup-optimizer-r1-a1")
        assert admission.claim(finished, "inline")
        assert admission.claim(unrelated, "pod", "lineup-optimizer-r2-a1")
    store.finish_running(finished, "success", "exit_code")

    assert jobs_service.cancel_run(root) == {"run_id": root, "status": "cancelled"}

    statuses = dict(rows(postgres, "SELECT id,status FROM run_log"))
    assert statuses == {
        root: "cancelled",
        queued: "cancelled",
        running: "cancelled",
        finished: "success",
        other_root: "running",
        unrelated: "running",
    }
    # The scheduler stops advancing the root and the watcher deletes the running step's
    # k8s Job. The queued step was never launched; the consumer drops it as not queued.
    assert [row.id for row in state.active_chain_roots()] == [other_root]
    cancelled = {
        row.run_id: row.k8s_job
        for row in store.watched_runs()
        if row.status == "cancelled"
    }
    assert cancelled == {running: "lineup-optimizer-r1-a1"}


def test_cancelling_a_chain_step_leaves_its_root_and_siblings_alone(postgres):
    state = PostgresSchedulerState(store_module.get_db_cursor)
    root = state.create_chain_root(
        chain="tuesday-weekly", dedupe_key="tuesday@slot", scheduled_for=None
    ).id
    first, second = (
        publish_run(
            CATALOG,
            job,
            trigger="chain",
            dedupe_key=f"{job}@{root}",
            parent_run_id=root,
            chain_root_id=root,
        )["run_id"]
        for job in ("rumor-syncer", "trade-scan")
    )

    jobs_service.cancel_run(first)

    assert dict(rows(postgres, "SELECT id,status FROM run_log")) == {
        root: "running",
        first: "cancelled",
        second: "queued",
    }


def test_heartbeat_upsert_keeps_one_row_with_the_latest_depths(postgres):
    heartbeat.record_heartbeat({"job_runs": 3, "job_runs.dlq": None})
    heartbeat.record_heartbeat({"job_runs": 0, "job_runs.dlq": 1})
    assert rows(postgres, "SELECT id,queue_depths FROM launcher_heartbeat") == [
        (1, {"job_runs": 0, "job_runs.dlq": 1})
    ]


def chain_step(job, chain_root_id):
    return publish_run(
        CATALOG,
        job,
        trigger="chain",
        dedupe_key=f"{job}@{chain_root_id}",
        parent_run_id=chain_root_id,
        chain_root_id=chain_root_id,
    )["run_id"]


def test_failed_step_is_not_requeued_once_its_chain_root_is_cancelled(postgres):
    store = RunStore()
    state = PostgresSchedulerState(store_module.get_db_cursor)
    root = state.create_chain_root(
        chain="tuesday-weekly", dedupe_key="tuesday@slot", scheduled_for=None
    ).id
    live_root = state.create_chain_root(
        chain="tuesday-weekly", dedupe_key="tuesday@live", scheduled_for=None
    ).id
    step = chain_step("lineup-optimizer", root)
    live_step = chain_step("lineup-optimizer", live_root)
    with store.admission() as admission:
        assert admission.claim(step, "pod", "lineup-optimizer-r1-a1")
        assert admission.claim(live_step, "pod", "lineup-optimizer-r2-a1")
    store.finish_running(step, "failure", "job_failed", force=True)
    store.finish_running(live_step, "failure", "job_failed", force=True)
    jobs_service.cancel_run(root)
    max_attempts = {
        "lineup-optimizer": CATALOG.resolve("lineup-optimizer").max_attempts
    }

    # The watcher no longer selects the ended chain's retryable failure, so it isn't
    # revisited every tick; the live chain's failure still is, and still requeues.
    watched = {row.run_id: row for row in store.watched_runs([], max_attempts)}
    assert set(watched) == {live_step}
    assert watched[live_step].chain_running is True
    assert not store.requeue_attempt(step, 2, expected_attempt=1)
    assert store.requeue_attempt(live_step, 2, expected_attempt=1)
    assert rows(
        postgres,
        "SELECT id,status,attempt FROM run_log WHERE id IN (%s,%s) ORDER BY id=%s",
        (step, live_step, live_step),
    ) == [(step, "failure", 1), (live_step, "queued", 2)]

    # A step whose k8s Job is still visible is selected so its Job is reaped, marked as
    # belonging to an ended chain so its failure is final.
    with_k8s_job = store.watched_runs([step], max_attempts)
    assert [(row.run_id, row.chain_running) for row in with_k8s_job] == [(step, False)]


def test_standalone_run_still_requeues(postgres):
    store = RunStore()
    identity = publish_run(CATALOG, "lineup-optimizer", trigger="manual")["run_id"]
    with store.admission() as admission:
        assert admission.claim(identity, "pod", "lineup-optimizer-r1-a1")
    store.finish_running(identity, "failure", "job_failed", force=True)
    max_attempts = {
        "lineup-optimizer": CATALOG.resolve("lineup-optimizer").max_attempts
    }
    assert [
        (row.run_id, row.chain_running) for row in store.watched_runs([], max_attempts)
    ] == [(identity, True)]
    assert store.requeue_attempt(identity, 2, expected_attempt=1)


def test_queued_step_of_an_ended_chain_is_skipped_at_admission(postgres, monkeypatch):
    for name, value in LAUNCHER_ENV.items():
        monkeypatch.setenv(name, value)
    state = PostgresSchedulerState(store_module.get_db_cursor)
    cancelled = state.create_chain_root(
        chain="tuesday-weekly", dedupe_key="tuesday@cancelled", scheduled_for=None
    ).id
    timed_out = state.create_chain_root(
        chain="tuesday-weekly", dedupe_key="tuesday@timeout", scheduled_for=None
    ).id
    running = state.create_chain_root(
        chain="tuesday-weekly", dedupe_key="tuesday@running", scheduled_for=None
    ).id
    jobs_service.cancel_run(cancelled)
    # A scheduler tick that read the root as running publishes after the cancel.
    raced = chain_step("rumor-syncer", cancelled)
    left_queued = chain_step("trade-scan", timed_out)
    state.finish_chain_root(timed_out, "timeout", "max_duration")
    live = chain_step("sync-league", running)

    runner = MagicMock(busy=False)
    consumer = Consumer(CATALOG, runner, store=RunStore(), launch_pod=MagicMock())
    consumer.recover()
    with patch.object(consumer, "_launch"):
        assert consumer.drain() == [live]

    assert dict(
        rows(
            postgres,
            "SELECT id,status||'/'||COALESCE(end_reason,'') FROM run_log WHERE id IN (%s,%s,%s)",
            (raced, left_queued, live),
        )
    ) == {
        raced: "skipped/chain_ended",
        left_queued: "skipped/chain_ended",
        live: "running/",
    }
