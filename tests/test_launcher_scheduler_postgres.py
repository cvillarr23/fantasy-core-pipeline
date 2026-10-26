"""Optional checks of the scheduler's run_log SQL against an explicitly supplied local disposable DB."""

from contextlib import contextmanager
import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg2
from psycopg2 import sql
import pytest

from src.launcher.catalog import load_catalog
from src.launcher.scheduler import PostgresSchedulerState, Scheduler
from src.services import run_log_service as run_log

UTC = timezone.utc
TUESDAY = datetime(2026, 10, 27, 21, 0, 30, tzinfo=UTC)  # 16:00:30 America/Chicago


class AlwaysInSeason:
    def in_season(self, season, day):
        return True


@pytest.fixture
def conn():
    dsn = os.getenv("ACTIVITY_TEST_DSN")
    if not dsn:
        pytest.skip("Set ACTIVITY_TEST_DSN to a disposable local Postgres")
    assert psycopg2.extensions.parse_dsn(dsn).get("host") in {"127.0.0.1", "localhost"}
    conn = psycopg2.connect(dsn)
    schema = "scheduler_test_" + uuid4().hex
    with conn, conn.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {};").format(sql.Identifier(schema)))
        cursor.execute(sql.SQL("SET search_path TO {};").format(sql.Identifier(schema)))
        cursor.execute(run_log.SCHEMA)
    try:
        yield conn
    finally:
        conn.rollback()
        with conn, conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP SCHEMA {} CASCADE;").format(sql.Identifier(schema))
            )
        conn.close()


@pytest.fixture
def state(conn):
    @contextmanager
    def cursor_context():
        with conn, conn.cursor() as cursor:
            yield cursor

    return PostgresSchedulerState(cursor_context)


def publish_into(conn, published):
    """The queued run_log row the launcher's publish helper writes."""

    def publish(command):
        with conn, conn.cursor() as c:
            c.execute(
                """INSERT INTO run_log(id,job_name,trigger_type,status,dedupe_key,scheduled_for,
                parent_run_id,chain_root_id) VALUES (%s,%s,%s,'queued',%s,%s,%s,%s)
                ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING RETURNING id""",
                (
                    command["run_id"],
                    command["job"],
                    command["trigger"],
                    command["dedupe_key"],
                    command["scheduled_for"],
                    command["parent_run_id"],
                    command["chain_root_id"],
                ),
            )
            inserted = c.fetchone() is not None
        if inserted:
            published.append(command)
        return inserted

    return publish


def rows(conn, where="TRUE", params=()):
    with conn, conn.cursor() as c:
        c.execute(
            f"SELECT job_name,status,end_reason,chain_root_id FROM run_log WHERE {where} ORDER BY job_name",
            params,
        )
        return c.fetchall()


def set_status(conn, job, status):
    with conn, conn.cursor() as c:
        c.execute(
            "UPDATE run_log SET status=%s WHERE job_name=%s AND trigger_type='chain'",
            (status, job),
        )


def test_skipped_audit_row_is_written_once(conn, state):
    slot = datetime(2026, 10, 28, 15, 30, tzinfo=UTC)
    for _ in range(2):
        state.record_skipped(
            job_name="rumor-syncer",
            trigger_type="schedule",
            dedupe_key="rumor-syncer@slot",
            scheduled_for=slot,
            end_reason="paused",
        )
    assert rows(conn) == [("rumor-syncer", "skipped", "paused", None)]
    assert state.run_exists("rumor-syncer@slot")


def test_pause_window(conn, state):
    now = datetime(2026, 10, 28, tzinfo=UTC)
    with conn, conn.cursor() as c:
        c.execute(
            "INSERT INTO job_overrides(job_or_chain,paused_until) VALUES ('a',NULL),('b',%s),('c',%s)",
            (now + timedelta(hours=1), now - timedelta(hours=1)),
        )
    assert [state.paused(name, now) for name in ("a", "b", "c", "d")] == [
        True,
        True,
        False,
        False,
    ]


def test_chain_progresses_and_finalizes_through_run_log(conn, state):
    published: list[dict] = []
    scheduler = Scheduler(
        load_catalog("tests/fixtures/job-catalog.yaml"),
        state,
        publish_into(conn, published),
        seasons=AlwaysInSeason(),
    )
    scheduler.tick(TUESDAY)
    scheduler.tick(TUESDAY)
    (chain_root,) = state.active_chain_roots()
    assert chain_root.trigger == {"chain": "tuesday-weekly", "from_step": None}
    assert [c["job"] for c in published if c["trigger"] == "chain"] == ["sync-league"]

    set_status(conn, "sync-league", "success")
    set_status(conn, "metrics-extractor", "success")  # not started yet: a no-op
    scheduler.tick(TUESDAY + timedelta(minutes=5))
    assert set(state.chain_steps(chain_root.id)) == {"sync-league", "metrics-extractor"}

    with conn, conn.cursor() as c:
        c.execute(
            "UPDATE run_log SET status='failure', attempt=2 WHERE job_name='metrics-extractor'"
        )
    scheduler.tick(TUESDAY + timedelta(minutes=10))
    assert rows(conn, "job_name='sync-actual-scores'") == [
        ("sync-actual-scores", "skipped", "dependency_failed", chain_root.id)
    ]

    for job in (
        "projections-refresh",
        "lineup-optimizer",
        "waiver-trigger",
        "rumor-action-trigger",
    ):
        set_status(conn, job, "success")
        scheduler.tick(TUESDAY + timedelta(minutes=15))
    assert state.active_chain_roots() == []
    assert rows(conn, "id=chain_root_id") == [
        ("tuesday-weekly", "failure", None, chain_root.id)
    ]
    start = TUESDAY - timedelta(hours=1)
    assert [
        r.id
        for r in state.chain_roots_between(
            "tuesday-weekly", start, start + timedelta(days=1)
        )
    ] == [chain_root.id]
