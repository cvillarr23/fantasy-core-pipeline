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
from src.launcher.store import RunStore

logger = logging.getLogger(__name__)

POLL_SECONDS = 60
ORPHAN_GRACE_SECONDS = 10 * 60


def _value(row, name):
    return row.get(name) if isinstance(row, dict) else getattr(row, name)


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
                jobs = self.api.list_namespaced_job(
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
                self.reconcile(jobs, pods)
            except Exception:
                logger.exception("Launcher run reconciliation failed; will retry")
            stop.wait(POLL_SECONDS)

    def reconcile(self, jobs, pods=None):
        """Apply one snapshot of launcher Jobs to current active/cancelled run rows."""
        by_run = {}
        for job in jobs:
            labels = getattr(job.metadata, "labels", None) or {}
            run_id = labels.get("fantasy.run-id")
            if run_id:
                by_run.setdefault(run_id, []).append(job)
        pending_jobs = set()
        for pod in pods or []:
            labels = getattr(pod.metadata, "labels", None) or {}
            job_name = labels.get("batch.kubernetes.io/job-name") or labels.get(
                "job-name"
            )
            if job_name and getattr(pod.status, "phase", None) == "Pending":
                pending_jobs.add(job_name)

        max_attempts = {
            name: self.catalog.resolve(name).max_attempts for name in self.catalog.jobs
        }
        for run in self.store.watched_runs(set(by_run), max_attempts):
            run_id = _value(run, "run_id")
            run_jobs = by_run.get(run_id, [])
            status = _value(run, "status")
            if status == "cancelled":
                if self._cancel(run, run_jobs):
                    self.store.mark_cancel_reconciled(run_id)
                continue
            if _value(run, "chain_root_id") == run_id:
                continue
            if _value(run, "executor") == "inline":
                if status == "running":
                    self._orphan_inline(run)
                elif status in {"failure", "timeout"} and self._has_attempt_left(run):
                    self._finish_or_retry(
                        run, status, "deadline" if status == "timeout" else "exit_code"
                    )
                continue
            expected_name = _value(run, "k8s_job")
            previous_jobs = (
                [job for job in run_jobs if job.metadata.name != expected_name]
                if expected_name is not None
                else []
            )
            for previous in previous_jobs:
                if self._job_terminal(previous):
                    self._reap(previous)
            current_job = next(
                (job for job in run_jobs if job.metadata.name == expected_name), None
            )
            if expected_name is None and len(run_jobs) == 1:
                current_job = run_jobs[0]
            if current_job is None and run_jobs:
                # A prior attempt's terminal Job may remain visible while Kubernetes
                # processes its deletion. Reap it without applying its outcome to this try.
                removed = all(self._delete(previous) for previous in run_jobs)
                if (
                    removed
                    and status == "running"
                    and self._age(run)
                    > self.catalog.global_settings.start_timeout.total_seconds()
                ):
                    self._finish_or_retry(run, "failure", "start_timeout")
                continue
            if current_job is None:
                if status == "running":
                    self._orphan(run)
                elif status in {"failure", "timeout"} and self._has_attempt_left(run):
                    self._finish_or_retry(
                        run, status, "deadline" if status == "timeout" else "job_failed"
                    )
                continue
            if status in {"success", "failure", "timeout"} and not self._job_terminal(
                current_job
            ):
                continue
            self._reconcile_job(
                run,
                current_job,
                pending=current_job.metadata.name in pending_jobs,
            )

    @staticmethod
    def _job_terminal(job):
        status = getattr(job, "status", None)
        conditions = getattr(status, "conditions", None) or []
        complete = any(
            condition.type == "Complete"
            and getattr(condition, "status", None) == "True"
            for condition in conditions
        )
        if (getattr(status, "succeeded", None) or 0) > 0 or complete:
            return True
        failed = any(
            condition.type == "Failed" and getattr(condition, "status", None) == "True"
            for condition in conditions
        )
        if (getattr(status, "failed", None) or 0) > 0 or failed:
            return True
        return False

    def _reconcile_job(self, run, job, pending=False):
        status = getattr(job, "status", None)
        conditions = getattr(status, "conditions", None) or []
        complete = any(
            condition.type == "Complete"
            and getattr(condition, "status", None) == "True"
            for condition in conditions
        )
        if (getattr(status, "succeeded", None) or 0) > 0 or complete:
            self._finish_or_retry(run, "success", "completed")
            self._reap(job)
            return
        failed_condition = next(
            (
                condition
                for condition in conditions
                if condition.type == "Failed"
                and getattr(condition, "status", None) == "True"
            ),
            None,
        )
        failed = (
            getattr(status, "failed", None) or 0
        ) > 0 or failed_condition is not None
        reason = failed_condition.reason if failed_condition else None
        if failed:
            if reason == "DeadlineExceeded":
                outcome, end_reason = "timeout", "deadline"
            else:
                outcome, end_reason = "failure", "job_failed"
            self._finish_or_retry(run, outcome, end_reason)
            self._reap(job)
            return

        started_at = getattr(status, "start_time", None)
        active = getattr(status, "active", None) or 0
        is_pending = pending or (not started_at and not active)
        if (
            is_pending
            and self._age(run)
            > self.catalog.global_settings.start_timeout.total_seconds()
        ):
            if not self._delete(job):
                return
            self._finish_or_retry(run, "failure", "start_timeout")

    def _finish_or_retry(self, run, outcome, reason):
        run_id = _value(run, "run_id")
        job_name = _value(run, "job_name")
        attempt = _value(run, "attempt")
        try:
            job = self.catalog.resolve(job_name)
        except UnknownJobError:
            logger.warning("Run %s belongs to removed catalog job %s", run_id, job_name)
            self.store.finish_running(
                run_id, outcome, reason, force=True, expected_attempt=attempt
            )
            return
        if outcome in {"failure", "timeout"} and attempt < job.max_attempts:
            if self.store.retry_running(run_id, attempt + 1, expected_attempt=attempt):
                logger.info(
                    "Retrying run %s of %s at attempt %s", run_id, job.name, attempt + 1
                )
            return
        self.store.finish_running(
            run_id, outcome, reason, force=True, expected_attempt=attempt
        )

    def _has_attempt_left(self, run):
        try:
            job = self.catalog.resolve(_value(run, "job_name"))
        except UnknownJobError:
            return False
        return _value(run, "attempt") < job.max_attempts

    def _orphan(self, run):
        if _value(run, "chain_root_id") == _value(run, "run_id"):
            return
        try:
            job = self.catalog.resolve(_value(run, "job_name"))
            deadline = job.deadline
        except UnknownJobError:
            deadline = self.catalog.global_settings.max_deadline
        age_limit = deadline.total_seconds() + ORPHAN_GRACE_SECONDS
        if self._age(run) > age_limit:
            self._finish_or_retry(run, "timeout", "orphaned")

    def _orphan_inline(self, run):
        if self.inline_runner and self.inline_runner.is_running(_value(run, "run_id")):
            return
        self._orphan(run)

    def _cancel(self, run, jobs):
        run_id = _value(run, "run_id")
        executor = _value(run, "executor")
        if executor == "inline":
            if not self.inline_runner:
                return False
            self.inline_runner.cancel(run_id)
            return True
        return all(self._delete(job) for job in jobs)

    def _delete(self, job):
        try:
            self.api.delete_namespaced_job(
                job.metadata.name,
                K8S_NAMESPACE,
                propagation_policy="Foreground",
                _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS,
            )
            return True
        except ApiException as exc:
            if exc.status == 404:
                return True
            logger.warning(
                "Could not delete launcher Job %s: %s", job.metadata.name, exc
            )
        except Exception:
            logger.exception("Could not delete launcher Job %s", job.metadata.name)
        return False

    def _reap(self, job):
        self._delete(job)

    def _age(self, run):
        return (self.clock() - _value(run, "started_at")).total_seconds()


def run(catalog, stop, **kwargs):
    Watcher(catalog, **kwargs).run(stop)
