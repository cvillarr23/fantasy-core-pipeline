"""Optional checks of approval runs against a disposable local Postgres: both ways of opening
one, every way it ends, its `then` run, and that it holds no capacity (see
`tests/test_launcher_postgres.py` for the fixture)."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.launcher import runs as runs_module
from src.launcher.runs import Runs, RunStore
from src.launcher.scheduler import PostgresSchedulerState
from src.services import approval_service, jobs_service
from tests.test_launcher_k8s import CATALOG
from tests.test_launcher_postgres import postgres, rows  # noqa: F401 (fixture)
from tests.test_launcher_runs_postgres import chain_root, claimed, row

THEN = {"job": "lineup-optimizer", "params": {"week": 3}}


@pytest.fixture
def db(postgres):  # noqa: F811
    @contextmanager
    def cursor_context():
        with postgres, postgres.cursor() as cursor:
            yield cursor

    with (
        patch.object(approval_service, "get_db_cursor", cursor_context),
        patch.object(jobs_service, "_catalog", lambda: CATALOG),
    ):
        yield postgres, cursor_context


def open_one(requester, key="offer-1", then=THEN, **kwargs):
    return approval_service.open_approval(
        requester, key=key, title="Send offer?", then=then, link="/trades", **kwargs
    )


def waiting_approval(cursor_context, expires_at, chain_root_id=None, job="approval"):
    with cursor_context() as c:
        return Runs(c).insert_approval(
            job=job,
            trigger_type="approval",
            trigger={"title": "t", "then": THEN},
            parent_run_id=chain_root_id,
            chain_root_id=chain_root_id,
            dedupe_key=f"approval@{expires_at.isoformat()}#{job}",
            expires_at=expires_at,
        )


def test_a_running_run_opens_a_waiting_approval_once_per_key(db):
    conn, _ = db
    scan = claimed("trade-scan")
    opened = open_one(scan)
    assert opened["status"] == "waiting"
    columns = "job_name,status,executor,trigger_type,parent_run_id,dedupe_key,trigger"
    assert row(conn, opened["run_id"], columns) == (
        "approval",
        "waiting",
        "approval",
        "approval",
        scan,
        f"approval@{scan}#offer-1",
        {"key": "offer-1", "title": "Send offer?", "link": "/trades", "then": THEN},
    )
    assert open_one(scan) == {"run_id": opened["run_id"], "status": "duplicate"}
    assert open_one(scan, key="offer-2")["run_id"] != opened["run_id"]


def test_opening_needs_a_running_requester_and_a_valid_then(db):
    with pytest.raises(jobs_service.RunNotFoundError):
        open_one("no-such-run")
    scan = claimed("trade-scan")
    with RunStore().admission() as admission:
        admission.finish_running(scan, "success", None)
    with pytest.raises(approval_service.ApprovalRequesterNotRunningError):
        open_one(scan)
    running = claimed("trade-scan", dedupe_key="scan-2")
    with pytest.raises(jobs_service.JobNotFoundError):
        open_one(running, then={"job": "nope"})
    with pytest.raises(ValueError):
        open_one(running, then={"job": "lineup-optimizer", "params": {"bogus": 1}})
    with pytest.raises(ValueError, match="future"):
        open_one(running, expires_at=datetime.now(timezone.utc) - timedelta(minutes=1))
    with pytest.raises(ValueError, match="within 7 days"):
        open_one(running, expires_at=datetime.now(timezone.utc) + timedelta(days=8))


def test_approving_ends_it_success_and_starts_its_then_run_once(db):
    conn, _ = db
    approval = open_one(claimed("trade-scan"))["run_id"]
    decided = approval_service.approve(approval, "conor")
    assert decided["status"] == "success"
    assert row(conn, approval, "status,end_reason,requested_by") == (
        "success",
        "approved",
        "conor",
    )
    then_run = decided["then_run_id"]
    assert row(
        conn, then_run, "job_name,status,parent_run_id,requested_by,trigger"
    ) == (
        "lineup-optimizer",
        "queued",
        approval,
        "conor",
        {"params": {"week": 3}, "downstream": False},
    )
    assert row(conn, then_run, "dedupe_key,chain_root_id,trigger_type") == (
        f"lineup-optimizer@{approval}",
        None,
        "manual",
    )
    assert approval_service.start_approved_run(approval)["status"] == "duplicate"
    with pytest.raises(approval_service.ApprovalResolvedError) as again:
        approval_service.approve(approval, "conor")
    assert again.value.status == "success"


@pytest.mark.parametrize("outcome", ["dismissed", "superseded"])
def test_dismissing_or_superseding_skips_it_and_starts_nothing(db, outcome):
    conn, cursor_context = db
    approval = open_one(claimed("trade-scan"))["run_id"]
    with cursor_context() as c:
        approval_service.resolve_approval(c, approval, outcome, "conor")
    assert row(conn, approval, "status,end_reason,requested_by") == (
        "skipped",
        outcome,
        "conor",
    )
    assert approval_service.start_approved_run(approval) is None
    assert (
        rows(conn, "SELECT id FROM run_log WHERE parent_run_id=%s", (approval,)) == []
    )


def test_an_expired_approval_cannot_be_approved_but_is_swept(db):
    conn, cursor_context = db
    now = datetime.now(timezone.utc)
    approval = waiting_approval(cursor_context, now - timedelta(seconds=1))
    with pytest.raises(approval_service.ApprovalResolvedError) as refused:
        approval_service.approve(approval, "conor")
    assert refused.value.status == "expired"
    assert row(conn, approval, "status")[0] == "waiting"

    later = waiting_approval(cursor_context, now + timedelta(hours=1))
    state = PostgresSchedulerState(cursor_context)
    assert state.expire_approvals(now) == [approval]
    assert row(conn, approval) == ("skipped", "expired", 1)
    assert row(conn, later, "status")[0] == "waiting"


def test_resolving_an_unknown_or_ordinary_run_is_not_found(db):
    with pytest.raises(approval_service.ApprovalNotFoundError):
        approval_service.dismiss("no-such-run", "conor")
    with pytest.raises(approval_service.ApprovalNotFoundError):
        approval_service.dismiss(claimed("trade-scan"), "conor")


def test_a_waiting_approval_holds_no_capacity_and_needs_no_reconcile(db):
    conn, cursor_context = db
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    approval = waiting_approval(cursor_context, future)
    with RunStore().admission() as admission:
        assert admission.running_pods() == 0
        assert admission.running_in_jobs(["approval"]) == 0
        assert admission.launcher_counts() == (0, 0)
        assert not admission.job_running("approval", "")
        assert admission.cancel(approval) is True
    assert row(conn, approval, "status")[0] == "cancelled"
    store = RunStore()
    assert not store.has_unreconciled_cancels()
    assert approval not in {run.run_id for run in store.watched_runs()}


def test_cancelling_a_chain_root_cancels_its_waiting_approval_step(db):
    conn, cursor_context = db
    root = chain_root(conn)
    state = PostgresSchedulerState(cursor_context)
    step = state.open_step_approval(
        step="confirm",
        title="Go?",
        root_id=root,
        dedupe_key=f"confirm@{root}",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    assert state.chain_steps(root)["confirm"].status == "waiting"
    assert (
        state.open_step_approval(
            step="confirm",
            title="Go?",
            root_id=root,
            dedupe_key=f"confirm@{root}",
            expires_at=datetime.now(timezone.utc),
        )
        is None
    )
    assert RunStore().cancel(root) is True
    assert row(conn, step.id, "status")[0] == "cancelled"


def test_approved_without_then_finds_only_unstarted_recent_approvals(db):
    _, cursor_context = db
    state = PostgresSchedulerState(cursor_context)
    since = datetime.now(timezone.utc) - timedelta(minutes=30)
    pending = open_one(claimed("trade-scan"))["run_id"]
    started = open_one(claimed("trade-scan", dedupe_key="scan-2"))["run_id"]
    with cursor_context() as c:
        approval_service.resolve_approval(c, pending, "approved", "conor")
    approval_service.approve(started, "conor")
    found = state.approved_without_then(since)
    assert [approval.run_id for approval in found] == [pending]
    assert found[0].then == THEN
    assert found[0].requested_by == "conor"
    assert state.approved_without_then(datetime.now(timezone.utc)) == []
    assert runs_module.then_dedupe_key("lineup-optimizer", pending) == (
        f"lineup-optimizer@{pending}"
    )
