from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.launcher import watcher as watcher_module
from src.launcher.watcher import Watcher
from tests.test_launcher_k8s import CATALOG

NOW = datetime(2026, 10, 27, 19, 15, tzinfo=timezone.utc)


def run(**overrides):
    values = {
        "run_id": "r1",
        "job_name": "lineup-optimizer",
        "status": "running",
        "attempt": 1,
        "started_at": NOW - timedelta(minutes=2),
        "chain_root_id": None,
        "executor": "pod",
        "k8s_job": "job-r1-a1",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def k8s_job(
    run_id="r1",
    *,
    name="job-r1-a1",
    active=1,
    succeeded=0,
    failed=0,
    reason=None,
    start_time=None,
):
    return SimpleNamespace(
        metadata=SimpleNamespace(labels={"fantasy.run-id": run_id}, name=name),
        status=SimpleNamespace(
            active=active,
            succeeded=succeeded,
            failed=failed,
            start_time=start_time,
            conditions=(
                [SimpleNamespace(type="Failed", reason=reason, status="True")]
                if reason
                else []
            ),
        ),
    )


class Store:
    def __init__(self, runs, retry_result=True):
        self.runs = runs
        self.finished = []
        self.retried = []
        self.retry_result = retry_result
        self.cancelled_reconciled = []

    def watched_runs(self, job_run_ids=(), max_attempts=None):
        return self.runs

    def finish_running(
        self, run_id, status, end_reason, error=None, force=False, expected_attempt=None
    ):
        self.finished.append((run_id, status, end_reason, error, expected_attempt))

    def retry_running(self, run_id, attempt, expected_attempt=None):
        self.retried.append((run_id, attempt, expected_attempt))
        return self.retry_result

    def mark_cancel_reconciled(self, run_id):
        self.cancelled_reconciled.append(run_id)


def make_watcher(runs, *, catalog=CATALOG, now=NOW, api=None, inline=None):
    store = Store(runs)
    api = api or MagicMock()
    inline = inline or MagicMock()
    watcher = Watcher(
        catalog, store=store, inline_runner=inline, api=api, clock=lambda: now
    )
    return watcher, store, api, inline


def test_succeeded_job_finalizes_run_and_reaps_job():
    watcher, store, api, _ = make_watcher([run()], api=MagicMock())
    watcher.reconcile([k8s_job(active=0, succeeded=1)])
    assert store.finished == [("r1", "success", "completed", None, 1)]
    api.delete_namespaced_job.assert_called_once()


def test_succeeded_job_is_reaped_when_run_scope_already_finished():
    watcher, store, api, _ = make_watcher([run(status="success")], api=MagicMock())
    watcher.reconcile([k8s_job(active=0, succeeded=1)])
    assert store.finished == [("r1", "success", "completed", None, 1)]
    api.delete_namespaced_job.assert_called_once()


def test_failed_job_retries_same_run_while_attempts_remain():
    watcher, store, _, _ = make_watcher([run()], api=MagicMock())
    watcher.reconcile([k8s_job(active=0, failed=1)])
    assert store.retried == [("r1", 2, 1)]
    assert store.finished == []


def test_process_recorded_failure_is_retried_after_job_becomes_terminal():
    watcher, store, _, _ = make_watcher([run(status="failure")], api=MagicMock())
    watcher.reconcile([k8s_job(active=0, failed=1)])
    assert store.retried == [("r1", 2, 1)]
    assert store.finished == []


def test_visible_failed_job_from_prior_attempt_cannot_retry_current_attempt():
    watcher, store, api, _ = make_watcher(
        [run(attempt=2, k8s_job="job-r1-a2")], api=MagicMock()
    )
    current = k8s_job(name="job-r1-a2", active=1)
    old = k8s_job(name="job-r1-a1", active=0, failed=1)
    watcher.reconcile([current, old])
    assert store.retried == []
    assert store.finished == []
    api.delete_namespaced_job.assert_called_once_with(
        "job-r1-a1",
        watcher_module.K8S_NAMESPACE,
        propagation_policy="Foreground",
        _request_timeout=watcher_module.K8S_REQUEST_TIMEOUT_SECONDS,
    )


def test_lost_retry_transition_does_not_finalize_a_newer_attempt():
    watcher, store, _, _ = make_watcher([run()], api=MagicMock())
    store.retry_result = False
    watcher.reconcile([k8s_job(active=0, failed=1)])
    assert store.retried == [("r1", 2, 1)]
    assert store.finished == []


def test_failed_job_finalizes_after_max_attempts():
    watcher, store, _, _ = make_watcher([run(attempt=3)], api=MagicMock())
    watcher.reconcile([k8s_job(active=0, failed=1)])
    assert store.finished == [("r1", "failure", "job_failed", None, 3)]


def test_deadline_exceeded_job_finalizes_as_timeout():
    watcher, store, _, _ = make_watcher([run(attempt=3)], api=MagicMock())
    watcher.reconcile([k8s_job(active=0, failed=1, reason="DeadlineExceeded")])
    assert store.finished == [("r1", "timeout", "deadline", None, 3)]


def test_failed_terminal_condition_is_honored_without_failed_counter():
    watcher, store, _, _ = make_watcher([run(attempt=3)], api=MagicMock())
    job = k8s_job(active=0, failed=0, reason="DeadlineExceeded", start_time=NOW)
    watcher.reconcile([job])
    assert store.finished == [("r1", "timeout", "deadline", None, 3)]


def test_removed_catalog_job_does_not_abort_later_run_reconciliation():
    watcher, store, _, _ = make_watcher(
        [
            run(job_name="removed-job", status="failure"),
            run(run_id="r2", job_name="lineup-optimizer", k8s_job="job-r2-a1"),
        ],
        api=MagicMock(),
    )
    watcher.reconcile([k8s_job(run_id="r2", name="job-r2-a1", active=0, succeeded=1)])
    assert store.finished == [("r2", "success", "completed", None, 1)]


def test_pending_start_timeout_deletes_job_and_retries():
    old = NOW - CATALOG.global_settings.start_timeout - timedelta(seconds=1)
    watcher, store, api, _ = make_watcher([run(started_at=old)], api=MagicMock())
    watcher.reconcile([k8s_job(active=0, start_time=None)])
    assert store.retried == [("r1", 2, 1)]
    api.delete_namespaced_job.assert_called_once()


def test_orphaned_run_times_out_and_chain_root_is_ignored():
    old = NOW - CATALOG.resolve("lineup-optimizer").deadline - timedelta(minutes=11)
    watcher, store, _, _ = make_watcher(
        [run(started_at=old, attempt=3), run(run_id="root", chain_root_id="root")],
        api=MagicMock(),
    )
    watcher.reconcile([])
    assert store.finished == [("r1", "timeout", "orphaned", None, 3)]


def test_cancelled_pod_deletes_job_and_inline_cancellation_kills_process():
    watcher, _, api, inline = make_watcher(
        [
            run(status="cancelled"),
            run(run_id="inline", status="cancelled", executor="inline", k8s_job=None),
        ],
        api=MagicMock(),
    )
    watcher.reconcile([k8s_job()])
    api.delete_namespaced_job.assert_called_once()
    inline.cancel.assert_called_once_with("inline")
