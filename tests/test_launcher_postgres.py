"""Optional checks of the launcher's run_log SQL against an explicitly supplied local disposable DB."""

import os
from contextlib import contextmanager
from unittest.mock import patch
from uuid import uuid4

import psycopg2
import pytest
from psycopg2 import sql

from src.launcher import publish, store as store_module
from src.launcher.publish import publish_run
from src.launcher.store import RunStore
from src.services import activity_service as activity, run_log_service as run_log
from tests.test_launcher_k8s import CATALOG


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
        "SELECT status,end_reason,trigger->>'k8s_job' FROM run_log WHERE id=%s",
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
    run_log.adopt_run(identity, "sync_rumors", "manual", {})
    run_log.finish_run(identity, "failure", "cancelled pod stopped")
    RunStore().mark_cancel_reconciled(identity)
    assert rows(
        postgres, "SELECT status,end_reason FROM run_log WHERE id=%s", (identity,)
    ) == [("cancelled", "cancelled")]
    assert rows(
        postgres,
        "SELECT trigger->>'cancel_reconciled' FROM run_log WHERE id=%s",
        (identity,),
    ) == [("true",)]


def test_old_process_cannot_adopt_or_finish_a_requeued_attempt(postgres):
    identity = publish_run(CATALOG, "lineup-optimizer", trigger="manual")["run_id"]
    store = RunStore()
    with store.admission() as admission:
        assert admission.claim(identity, "pod", "lineup-optimizer-r1-a1")
    assert store.retry_running(identity, 2, expected_attempt=1)

    run_log.adopt_run(identity, "sync_rumors", "manual", {}, expected_attempt=1)
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
    assert not store.retry_running(identity, 2, expected_attempt=9)
    assert store.retry_running(identity, 2, expected_attempt=1)
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
    watched = {row["run_id"] for row in store.watched_runs([], max_attempts)}
    assert watched == {active_pod, failed_pod, failed_inline}
    with_visible_job = {
        row["run_id"] for row in store.watched_runs([failed_pod], max_attempts)
    }
    assert failed_pod in with_visible_job
