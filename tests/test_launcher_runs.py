"""Unit tests for the launcher's run_log state machine, with a scripted cursor."""

from datetime import datetime, timezone

from src.launcher import runs
from src.launcher.runs import RunStore, Runs, WatchedRun
from src.services import run_log_service


class Cursor:
    """Records statements and answers each fetch from `results`, oldest first."""

    def __init__(self, *results):
        self.results = list(results)
        self.queries = []

    def execute(self, query, params=()):
        self.queries.append((" ".join(query.split()), params))

    def fetchone(self):
        return self.results.pop(0)

    def fetchall(self):
        return self.results.pop(0)


def test_trigger_types_keep_every_value_writers_record():
    assert set(run_log_service.LAUNCHER_TRIGGERS) == {
        "schedule",
        "chain",
        "event",
        "manual",
        "cli",
        "mcp",
    }
    assert set(run_log_service.TRIGGER_TYPES) == {
        "chain",
        "event",
        "cli",
        "mcp",
        "schedule",
        "queue",
        "http",
        "manual",
    }


def test_partial_indexes_match_the_watcher_predicates():
    retryable = runs._in(runs.RETRYABLE_STATUSES)
    index = next(
        line
        for line in run_log_service.SCHEMA.splitlines()
        if "run_log_retryable" in line
    )
    assert f"status IN {retryable}" in index and runs.LAUNCHER_RUN in index
    cursor = Cursor([])
    Runs(cursor).watched_runs()
    query = cursor.queries[0][0]
    assert f"r.status IN {retryable}" in query
    assert "r.cancel_reconciled_at IS NULL" in query
    cancel_index = next(
        line
        for line in run_log_service.SCHEMA.splitlines()
        if "run_log_cancel_unreconciled" in line
    )
    assert "cancel_reconciled_at IS NULL" in cancel_index


def test_status_sets_agree_with_the_run_log_statuses():
    every = set(run_log_service.STATUSES)
    for group in (
        runs.ACTIVE_STATUSES,
        runs.RETRYABLE_STATUSES,
        runs.FORCE_FINISHABLE_STATUSES,
        runs.REQUEUEABLE_STATUSES,
    ):
        assert set(group) <= every


def test_claim_sets_launch_columns_and_reports_a_lost_race():
    cursor = Cursor(("run-1",), None)
    assert Runs(cursor).claim("run-1", "pod", "job-1")
    assert not Runs(cursor).claim("run-1", "pod", "job-1")
    query, params = cursor.queries[0]
    assert "executor=%s, k8s_job=%s" in query and "status='queued'" in query
    assert params == ("pod", "job-1", "run-1")


def test_cancel_returns_true_the_current_status_or_none():
    assert Runs(Cursor(("root-x", ))).cancel("run-1") is True
    assert Runs(Cursor(None, ("success",))).cancel("run-1") == "success"
    assert Runs(Cursor(None, None)).cancel("run-1") is None


def test_cancelling_a_chain_root_also_cancels_its_steps():
    cursor = Cursor(("root-1",))
    assert Runs(cursor).cancel("root-1") is True
    assert "chain_root_id=%s AND id<>%s" in cursor.queries[1][0]


def test_cancelling_a_step_leaves_the_rest_of_the_chain():
    cursor = Cursor(("root-1",))
    assert Runs(cursor).cancel("step-1") is True
    assert len(cursor.queries) == 1


def test_finish_guard_depends_on_force_and_attempt():
    plain, plain_params = runs._finish_guard(False, None)
    assert plain == " AND status='running'" and plain_params == ()
    forced, params = runs._finish_guard(True, 3)
    assert "'running','success','failure','timeout'" in forced
    assert forced.endswith(" AND attempt=%s") and params == (3,)


def test_requeue_needs_an_outbox_row_and_a_row_still_in_a_requeueable_state():
    assert not Runs(Cursor(None)).requeue_attempt("run-1", 2)
    cursor = Cursor(({"attempt": 1},), None)
    assert not Runs(cursor).requeue_attempt("run-1", 2)
    assert "status IN ('running','failure','timeout')" in cursor.queries[1][0]
    assert len(cursor.queries) == 2


def test_requeue_rewrites_the_outbox_command_with_the_new_attempt():
    cursor = Cursor(('{"attempt": 1}',), ("run-1",))
    assert Runs(cursor).requeue_attempt("run-1", 2, expected_attempt=1)
    assert cursor.queries[-1][1][0] == '{"attempt": 2}'


def test_watched_runs_maps_columns_to_launch_details():
    started = datetime(2026, 10, 27, tzinfo=timezone.utc)
    row = ("r", "job", "running", 1, started, None, "pod", "k8s-1", True)
    cursor = Cursor([row])
    assert Runs(cursor).watched_runs({"r"}, {"job": 2}) == [
        WatchedRun("r", "job", "running", 1, started, None, "pod", "k8s-1", True)
    ]
    assert "r.cancel_reconciled_at IS NULL" in cursor.queries[0][0]
    assert "r.executor IS NOT NULL" in cursor.queries[0][0]


def test_run_store_admission_takes_the_advisory_lock_first(monkeypatch):
    cursor = Cursor()

    class Context:
        def __enter__(self):
            return cursor

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(runs, "get_db_cursor", lambda: Context())
    with RunStore().admission() as admission:
        assert isinstance(admission, Runs)
    assert cursor.queries == [
        ("SELECT pg_advisory_xact_lock(%s)", (runs.ADMISSION_LOCK_KEY,))
    ]
