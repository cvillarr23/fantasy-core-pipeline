"""Draft-day runs through the job launcher.

The draft endpoints (`src/api/draft.py`) used to build and delete k8s Jobs themselves. They now
request catalog runs and cancel them, and the launcher creates, watches and stops the pods:

- `draft-listener` / `practice-draft-listener`: draft_matrix.py's pick-stream listener.
- `draft-connector`: the browser worker's draft_sync.js for the live league.
- `practice-draft-connector`: draft_sync.js pointed at a manually joined practice room.

All four are `class: service` catalog jobs: exempt from the pod cap and capped by
`max_service_deadline`. The practice ones declare the connector's `draft:{league_id}:health`
heartbeat, so the watcher retires an abandoned practice room (formerly the stale-draft sweeper's
job). A run's `league_id` param is how the endpoints find it again; the connectors pass it as an
env var their entrypoint ignores (the live one reads ESPN_LEAGUE_ID, the practice one learns its
league from the room).

The API only reads k8s (jobs and pods `get`/`list`): to wait for a cancelled run's pod to go,
to read a run's Job phase, and to find its pod for logs.
"""

import logging
import time
from types import SimpleNamespace
from typing import Optional

from src.k8s_client import (
    K8S_NAMESPACE,
    K8S_REQUEST_TIMEOUT_SECONDS,
    batch_v1_api,
    core_v1_api,
)
from src.launcher import k8s
from src.launcher.catalog import UnknownJobError
from src.launcher.publish import publish_run
from src.launcher.runs import JobRun, RunStore
from src.services import jobs_service

logger = logging.getLogger(__name__)

LISTENER_JOBS = {"live": "draft-listener", "practice": "practice-draft-listener"}
LIVE_CONNECTOR_JOB = "draft-connector"
PRACTICE_CONNECTOR_JOB = "practice-draft-connector"
LISTENER_JOB_NAMES = tuple(LISTENER_JOBS.values())

ACTIVE_STATUSES = ("queued", "running")
REQUESTED_BY = "draft-api"

# Waiting for a cancelled run's pod: the watcher notices a cancel within 5s, deletes the Job
# (Foreground) and the pod gets a 5s termination grace. 25 polls of 2s leaves headroom.
TERMINATION_MAX_POLLS = 25
TERMINATION_POLL_INTERVAL_SECONDS = 2

# A run row's state when its k8s Job can't be seen (not created yet, or already reaped).
_ROW_JOB_STATES = {
    "queued": "Active",
    "running": "Active",
    "success": "Succeeded",
    "failure": "Failed",
    "timeout": "Failed",
}


class DraftLauncherUnavailableError(RuntimeError):
    """The launcher can't take the run: no catalog mounted, or it has no such draft job."""


def _store() -> RunStore:
    return RunStore()


# ---- starting runs ----------------------------------------------------------------------


def start_listener(
    mode: str,
    *,
    league_id: str,
    team_id: str,
    season: int,
    week: int,
    pick_clock_seconds: int,
    auto_draft_delay_seconds: int,
) -> dict:
    """Requests the mode's listener run. Returns `{"run_id", "job_name"}`."""
    return _start(
        LISTENER_JOBS[mode],
        {
            "league_id": str(league_id),
            "team_id": str(team_id),
            "season": int(season),
            "week": int(week),
            "mode": mode,
            "pick_clock_seconds": int(pick_clock_seconds),
            "auto_draft_delay_seconds": int(auto_draft_delay_seconds),
        },
    )


def start_live_connector(league_id: str) -> dict:
    return _start(LIVE_CONNECTOR_JOB, {"league_id": str(league_id)})


def start_practice_connector(room_url: str, league_id: Optional[str]) -> dict:
    params = {"room_url": room_url}
    if league_id is not None:
        params["league_id"] = str(league_id)
    return _start(PRACTICE_CONNECTOR_JOB, params)


def _start(job: str, params: dict) -> dict:
    """Queues one run. `job_name` is the k8s Job the launcher will create for its first
    attempt (deterministic from the run id), which the web UI shows."""
    try:
        catalog = jobs_service._catalog()
        result = publish_run(
            catalog, job, trigger="manual", params=params, requested_by=REQUESTED_BY
        )
    except (jobs_service.CatalogUnavailableError, UnknownJobError) as exc:
        raise DraftLauncherUnavailableError(f"{job}: {exc}") from exc
    run_id = result["run_id"]
    return {"run_id": run_id, "job_name": k8s_job_name(job, run_id)}


def k8s_job_name(job: str, run_id: str, attempt: int = 1) -> str:
    return k8s.job_name(SimpleNamespace(job=job, run_id=run_id, attempt=attempt))


# ---- finding runs -----------------------------------------------------------------------


def active_listeners() -> list[JobRun]:
    """Queued and running listener runs of both modes, newest first."""
    return _store().active_runs(LISTENER_JOB_NAMES)


def latest_listener(league_id: str) -> Optional[JobRun]:
    """The league's newest listener run in any status, or None."""
    return _store().latest_run(LISTENER_JOB_NAMES, "league_id", league_id)


def active_league_runs(league_id: str) -> tuple[list[JobRun], list[JobRun]]:
    """(the league's active listener runs, its active connector runs, both modes)."""
    store = _store()
    listeners = store.active_runs(LISTENER_JOB_NAMES, "league_id", league_id)
    connectors = store.active_runs(
        (LIVE_CONNECTOR_JOB, PRACTICE_CONNECTOR_JOB), "league_id", league_id
    )
    return listeners, connectors


def active_live_connectors(league_id: str) -> list[JobRun]:
    return _store().active_runs((LIVE_CONNECTOR_JOB,), "league_id", league_id)


def active_practice_connectors() -> list[JobRun]:
    return _store().active_runs((PRACTICE_CONNECTOR_JOB,))


def active_practice_listeners() -> list[JobRun]:
    return _store().active_runs((LISTENER_JOBS["practice"],))


def latest_practice_connector() -> Optional[JobRun]:
    return _store().latest_run((PRACTICE_CONNECTOR_JOB,))


def is_active(run: Optional[JobRun]) -> bool:
    return run is not None and run.status in ACTIVE_STATUSES


def ended_without_outcome(run: Optional[JobRun]) -> bool:
    """Whether no listener describes the board: none ever ran, or it was stopped (cancelled,
    skipped, or retired for a lapsed heartbeat) rather than finishing on its own. The old
    k8s-only check read the same thing as "no Job exists"."""
    return (
        run is None
        or run.status in ("cancelled", "skipped")
        or run.end_reason == "unhealthy"
    )


# ---- k8s reads --------------------------------------------------------------------------


def _job_phase(job_status) -> str:
    if job_status.active:
        return "Active"
    if job_status.succeeded:
        return "Succeeded"
    if job_status.failed:
        return "Failed"
    return "Unknown"


def _k8s_jobs(run_ids) -> list:
    if not run_ids:
        return []
    selector = f"fantasy.run-id in ({','.join(run_ids)})"
    jobs = batch_v1_api().list_namespaced_job(
        K8S_NAMESPACE, label_selector=selector, _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS
    )
    return list(getattr(jobs, "items", []) or [])


def run_job_state(run: Optional[JobRun]) -> str:
    """The run's k8s Job phase (Active/Succeeded/Failed), or the row's when its Job can't be
    seen. An unreadable apiserver falls back to the row too: run_log is the source of truth."""
    if run is None:
        return "Unknown"
    try:
        k8s_jobs = _k8s_jobs([run.run_id])
    except Exception as exc:
        logger.warning("Could not read the k8s Job of run %s: %s", run.run_id, exc)
        k8s_jobs = []
    if k8s_jobs:
        newest = max(k8s_jobs, key=lambda j: j.metadata.creation_timestamp)
        return _job_phase(newest.status)
    return _ROW_JOB_STATES.get(run.status, "Unknown")


def run_pods(run_id: str) -> list:
    """The run's pods, for its logs. Raises when the apiserver can't be read."""
    pods = core_v1_api().list_namespaced_pod(
        K8S_NAMESPACE,
        label_selector=f"fantasy.run-id={run_id}",
        _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS,
    )
    return list(getattr(pods, "items", []) or [])


def read_pod_log(pod_name: str, tail: int) -> str:
    return core_v1_api().read_namespaced_pod_log(
        pod_name,
        K8S_NAMESPACE,
        tail_lines=tail,
        timestamps=True,
        _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS,
    )


# ---- stopping runs ----------------------------------------------------------------------


def cancel_and_wait(run_ids) -> bool:
    """Cancels each run, then waits for their k8s Jobs to be gone. Returns False when one
    outlived the wait; callers must not treat that as stopped. Raises when the database or
    apiserver can't be read, which must not read as "stopped" either."""
    run_ids = list(run_ids)
    if not run_ids:
        return True
    store = _store()
    for run_id in run_ids:
        outcome = store.cancel(run_id)
        if outcome is not True:
            logger.info("Run %s was already %s; not cancelling it", run_id, outcome)
    return wait_for_runs_gone(run_ids)


def wait_for_runs_gone(run_ids) -> bool:
    for _ in range(TERMINATION_MAX_POLLS):
        if not _k8s_jobs(run_ids):
            return True
        time.sleep(TERMINATION_POLL_INTERVAL_SECONDS)
    return False
