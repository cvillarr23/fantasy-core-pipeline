"""Reconciles launcher run rows with Kubernetes Job outcomes.

The run_log row is the source of truth for overlap and admission. This watcher turns
Kubernetes terminal states, cancellation, and lost Jobs into durable row transitions so no
stale run can hold those limits indefinitely.
"""

import logging
import threading
from datetime import datetime, timezone

from kubernetes.client.rest import ApiException

from src.k8s_client import (
    K8S_NAMESPACE,
    K8S_REQUEST_TIMEOUT_SECONDS,
    batch_v1_api,
    core_v1_api,
)
from src.launcher.catalog import UnknownJobError
from src.launcher.runs import RETRYABLE_STATUSES, RunStore

logger = logging.getLogger(__name__)

POLL_SECONDS = 60
ORPHAN_GRACE_SECONDS = 10 * 60


class Watcher:
    def __init__(self, catalog, store=None, inline_runner=None, api=None, clock=None):
        self.catalog = catalog
        self.store = store or RunStore()
        self.inline_runner = inline_runner
        self.api = api or batch_v1_api()
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def run(self, stop: threading.Event):
        while not stop.is_set():
            try:
                k8s_jobs = self.api.list_namespaced_job(
                    K8S_NAMESPACE,
                    label_selector="fantasy.run-id",
                    _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS,
                ).items
                pods = (
                    core_v1_api()
                    .list_namespaced_pod(
                        K8S_NAMESPACE,
                        label_selector="fantasy.run-id",
                        _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS,
                    )
                    .items
                )
                self.reconcile(k8s_jobs, pods)
            except Exception:
                logger.exception("Launcher run reconciliation failed; will retry")
            stop.wait(POLL_SECONDS)

    def reconcile(self, k8s_jobs, pods=None):
        """Apply one snapshot of launcher k8s Jobs to current active/cancelled run rows."""
        by_run = _k8s_jobs_by_run(k8s_jobs)
        pending_k8s_job_names = _pending_k8s_job_names(pods or [])
        for run in self.store.watched_runs(set(by_run), self._max_attempts()):
            run_k8s_jobs = by_run.get(run.run_id, [])
            if run.status == "cancelled":
                if self._cancel(run, run_k8s_jobs):
                    self.store.mark_cancel_reconciled(run.run_id)
                continue
            # A chain root has no k8s Job or process; the scheduler ends it itself.
            if run.chain_root_id == run.run_id:
                continue
            if run.executor == "inline":
                self._reconcile_inline(run)
            else:
                self._reconcile_pod(run, run_k8s_jobs, pending_k8s_job_names)

    def _max_attempts(self):
        return {
            name: self.catalog.resolve(name).max_attempts for name in self.catalog.jobs
        }

    def _reconcile_inline(self, run):
        if (
            run.status == "running"
            and self.inline_runner
            and self.inline_runner.is_running(run.run_id)
        ):
            return
        self._reconcile_without_workload(run, "exit_code")

    def _reconcile_pod(self, run, k8s_jobs, pending_k8s_job_names):
        self._reap_previous_attempts(run, k8s_jobs)
        current = self._current_k8s_job(run, k8s_jobs)
        if current is None and k8s_jobs:
            self._reap_leftover_k8s_jobs(run, k8s_jobs)
            return
        if current is None:
            self._reconcile_without_workload(run, "job_failed")
            return
        outcome = _k8s_job_outcome(current)
        if run.status in {"success", "failure", "timeout"} and outcome is None:
            return
        self._reconcile_job(
            run,
            current,
            outcome,
            pending=current.metadata.name in pending_k8s_job_names,
        )

    def _reap_previous_attempts(self, run, k8s_jobs):
        """Deletes finished k8s Jobs of the run's earlier attempts."""
        if run.k8s_job is None:
            return
        for previous in k8s_jobs:
            if (
                previous.metadata.name != run.k8s_job
                and _k8s_job_outcome(previous) is not None
            ):
                self._delete(previous)

    @staticmethod
    def _current_k8s_job(run, k8s_jobs):
        """The k8s Job of the run's current attempt: the one named in the row, or the only
        one when the row names none."""
        current = next(
            (k8s_job for k8s_job in k8s_jobs if k8s_job.metadata.name == run.k8s_job),
            None,
        )
        if run.k8s_job is None and len(k8s_jobs) == 1:
            current = k8s_jobs[0]
        return current

    def _reap_leftover_k8s_jobs(self, run, k8s_jobs):
        """Deletes k8s Jobs that aren't the current attempt's, without applying their
        outcome to this attempt, and fails a `running` attempt past its start timeout.

        A prior attempt's terminal k8s Job may remain visible while Kubernetes processes
        its deletion.
        """
        removed = all(self._delete(previous) for previous in k8s_jobs)
        if removed and run.status == "running" and self._start_timed_out(run):
            self._finish_or_retry(run, "failure", "start_timeout")

    def _reconcile_without_workload(self, run, failure_reason):
        """A run with no live k8s Job or inline process: a `running` row is timed out once
        orphaned, and a recorded failure or timeout with attempts left is retried."""
        if run.status == "running":
            self._orphan(run)
        elif run.status in {"failure", "timeout"} and self._has_attempt_left(run):
            self._finish_or_retry(
                run,
                run.status,
                "deadline" if run.status == "timeout" else failure_reason,
            )

    def _reconcile_job(self, run, k8s_job, outcome, pending=False):
        if outcome is not None:
            self._finish_or_retry(run, *outcome)
            self._delete(k8s_job)
            return
        status = getattr(k8s_job, "status", None)
        started_at = getattr(status, "start_time", None)
        active = getattr(status, "active", None) or 0
        is_pending = pending or (not started_at and not active)
        if is_pending and self._start_timed_out(run):
            if not self._delete(k8s_job):
                return
            self._finish_or_retry(run, "failure", "start_timeout")

    def _finish_or_retry(self, run, outcome, reason):
        try:
            job = self.catalog.resolve(run.job_name)
        except UnknownJobError:
            logger.warning(
                "Run %s belongs to removed catalog job %s", run.run_id, run.job_name
            )
            job = None
        if job is not None and self._should_retry(run, outcome, job):
            self._requeue(run, job)
            return
        self.store.finish_running(
            run.run_id, outcome, reason, force=True, expected_attempt=run.attempt
        )

    @staticmethod
    def _should_retry(run, outcome, job):
        """Whether a failure or timeout gets another attempt. A step of a chain that was
        cancelled or ran past max_duration doesn't: its outcome is final."""
        if outcome not in RETRYABLE_STATUSES or run.attempt >= job.max_attempts:
            return False
        if not run.chain_running:
            logger.info("Not retrying run %s: its chain has ended", run.run_id)
            return False
        return True

    def _requeue(self, run, job):
        """Queues the next attempt. A refusal means another writer moved the row on, or the
        chain ended since it was read; either way the next tick sees the row's new state.
        """
        if self.store.requeue_attempt(
            run.run_id, run.attempt + 1, expected_attempt=run.attempt
        ):
            logger.info(
                "Retrying run %s of %s at attempt %s",
                run.run_id,
                job.name,
                run.attempt + 1,
            )

    def _has_attempt_left(self, run):
        try:
            job = self.catalog.resolve(run.job_name)
        except UnknownJobError:
            return False
        return run.chain_running and run.attempt < job.max_attempts

    def _orphan(self, run):
        try:
            deadline = self.catalog.resolve(run.job_name).deadline
        except UnknownJobError:
            deadline = self.catalog.global_settings.max_deadline
        age_limit = deadline.total_seconds() + ORPHAN_GRACE_SECONDS
        if self._age(run) > age_limit:
            self._finish_or_retry(run, "timeout", "orphaned")

    def _cancel(self, run, k8s_jobs):
        if run.executor == "inline":
            if not self.inline_runner:
                return False
            self.inline_runner.cancel(run.run_id)
            return True
        return all(self._delete(k8s_job) for k8s_job in k8s_jobs)

    def _delete(self, k8s_job):
        name = k8s_job.metadata.name
        try:
            self.api.delete_namespaced_job(
                name,
                K8S_NAMESPACE,
                propagation_policy="Foreground",
                _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS,
            )
            return True
        except ApiException as exc:
            if exc.status == 404:
                return True
            logger.warning("Could not delete launcher k8s Job %s: %s", name, exc)
        except Exception:
            logger.exception("Could not delete launcher k8s Job %s", name)
        return False

    def _start_timed_out(self, run):
        return (
            self._age(run) > self.catalog.global_settings.start_timeout.total_seconds()
        )

    def _age(self, run):
        return (self.clock() - run.started_at).total_seconds()


def _k8s_job_outcome(k8s_job):
    """The run outcome a finished k8s Job reports, as `(status, end_reason)`, or None while
    it is still running. A `Complete` condition or a succeeded pod wins over a failure.
    """
    status = getattr(k8s_job, "status", None)
    conditions = [
        condition
        for condition in getattr(status, "conditions", None) or []
        if getattr(condition, "status", None) == "True"
    ]
    if (getattr(status, "succeeded", None) or 0) > 0 or any(
        condition.type == "Complete" for condition in conditions
    ):
        return "success", "completed"
    failed = next(
        (condition for condition in conditions if condition.type == "Failed"), None
    )
    if (getattr(status, "failed", None) or 0) > 0 or failed is not None:
        if failed is not None and failed.reason == "DeadlineExceeded":
            return "timeout", "deadline"
        return "failure", "job_failed"
    return None


def _k8s_jobs_by_run(k8s_jobs):
    """Launcher k8s Jobs grouped by their `fantasy.run-id` label."""
    by_run = {}
    for k8s_job in k8s_jobs:
        labels = getattr(k8s_job.metadata, "labels", None) or {}
        run_id = labels.get("fantasy.run-id")
        if run_id:
            by_run.setdefault(run_id, []).append(k8s_job)
    return by_run


def _pending_k8s_job_names(pods):
    """Names of the k8s Jobs that own a `Pending` pod."""
    names = set()
    for pod in pods:
        labels = getattr(pod.metadata, "labels", None) or {}
        name = labels.get("batch.kubernetes.io/job-name") or labels.get("job-name")
        if name and getattr(pod.status, "phase", None) == "Pending":
            names.add(name)
    return names
