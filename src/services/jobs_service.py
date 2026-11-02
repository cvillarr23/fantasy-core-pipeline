"""Reusable launcher job operations for REST, CLI, and MCP callers."""

import json
from datetime import datetime, timezone
from typing import Any, Literal
from functools import cache

from src.database import get_db_cursor
from src.launcher.catalog import (
    Catalog,
    Chain,
    ResolvedJob,
    UnknownJobError,
    load_catalog_from_env,
    validate_run_params,
)
from src.launcher.consumer import overlap_refuses
from src.launcher.publish import publish_run
from src.launcher.runs import Runs
from src.schemas.jobs import JobsResponse, LauncherHealthResponse
from src.services import run_log_service


class CatalogUnavailableError(Exception):
    """The launcher catalog ConfigMap is not mounted in this API process."""


class JobNotFoundError(Exception):
    """A requested job or chain is not present in the catalog."""


class RunOverlapError(Exception):
    """The catalog's forbid rule refuses a second concurrent run."""


class RunNotFoundError(Exception):
    """No run with that identity exists."""


class RunNotCancellableError(Exception):
    """A run has already finished and cannot be cancelled."""


class PauseNotFoundError(Exception):
    """No pause override exists for the requested name."""


class LauncherHealthUnavailableError(Exception):
    """The launcher has not recorded a heartbeat yet."""


@cache
def _season_calendar():
    """One calendar per process, so polling GET /v1/jobs keeps its cached NFL schedule."""
    from src.launcher.scheduler import NflSeasonCalendar

    return NflSeasonCalendar()


def _scheduler(catalog: Catalog):
    from src.launcher.scheduler import PostgresSchedulerState, Scheduler

    return Scheduler(catalog, PostgresSchedulerState(), seasons=_season_calendar())


def _catalog() -> Catalog:
    try:
        return load_catalog_from_env()
    except FileNotFoundError as exc:
        raise CatalogUnavailableError("Job catalog is not mounted") from exc


def list_jobs() -> JobsResponse:
    try:
        catalog = _catalog()
    except CatalogUnavailableError:
        return JobsResponse(jobs=[], chains=[])

    last_runs = _last_runs(list(catalog.jobs))
    overrides = _pause_overrides()
    scheduler = _scheduler(catalog)
    jobs = [
        _job_info(
            catalog.resolve(name),
            last_runs.get(name),
            overrides.get(name),
            scheduler.next_job_run(name),
        )
        for name in catalog.jobs
    ]
    chains = [
        _chain_info(name, chain, last_runs, overrides, scheduler.next_chain_run(name))
        for name, chain in catalog.chains.items()
    ]
    return JobsResponse(jobs=jobs, chains=chains)


def _last_runs(names: list[str]) -> dict[str, dict[str, Any]]:
    """Each job's most recent run_log row, keyed by job name."""
    if not names:
        return {}
    with get_db_cursor() as cursor:
        cursor.execute(
            """SELECT DISTINCT ON (job_name) job_name,id,status,started_at,finished_at,trigger_type
            FROM run_log WHERE job_name=ANY(%s) ORDER BY job_name,started_at DESC""",
            (names,),
        )
        keys = ("job_name", "id", "status", "started_at", "finished_at", "trigger_type")
        return {row[0]: dict(zip(keys, row)) for row in cursor.fetchall()}


def _pause_overrides() -> dict[str, dict[str, Any]]:
    """Every job_overrides row, keyed by job or chain name."""
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT job_or_chain,paused_until,reason,set_by,set_at FROM job_overrides"
        )
        keys = ("name", "paused_until", "reason", "set_by", "set_at")
        return {row[0]: dict(zip(keys, row)) for row in cursor.fetchall()}


def _job_info(
    resolved: ResolvedJob,
    last_run: dict | None,
    pause: dict | None,
    next_run_at: datetime | None,
) -> dict[str, Any]:
    """A catalog job with its effective settings (defaults applied), runs and pause."""
    job = resolved.job
    return {
        "name": resolved.name,
        "executor": resolved.executor,
        "overlap": resolved.overlap,
        "max_attempts": resolved.max_attempts,
        "deadline_seconds": int(resolved.deadline.total_seconds()),
        "alert": resolved.alert,
        "acts": job.acts,
        "params": job.params,
        "profile": job.profile,
        "command": job.command,
        "schedules": [schedule.model_dump(mode="json") for schedule in job.schedules],
        "on_event": [event.model_dump(mode="json") for event in job.on_event],
        "last_run": last_run,
        "next_run_at": next_run_at,
        "pause": pause,
    }


def _chain_info(
    name: str,
    chain: Chain,
    last_runs: dict[str, dict[str, Any]],
    overrides: dict[str, dict[str, Any]],
    next_run_at: datetime | None,
) -> dict[str, Any]:
    """A catalog chain with its steps' edges and last runs, next slot and pause."""
    return {
        "name": name,
        "schedule": chain.schedule.model_dump(mode="json") if chain.schedule else None,
        "max_duration_seconds": int(chain.max_duration.total_seconds()),
        "steps": {
            step: {
                "after": [dependency.model_dump() for dependency in config.after],
                "latest": config.latest,
                "last_run": last_runs.get(step),
            }
            for step, config in chain.steps.items()
        },
        "next_run_at": next_run_at,
        "pause": overrides.get(name),
    }


def launcher_health() -> LauncherHealthResponse:
    with get_db_cursor() as cursor:
        cursor.execute("SELECT at,queue_depths FROM launcher_heartbeat WHERE id=1")
        heartbeat = cursor.fetchone()
        if heartbeat is None:
            raise LauncherHealthUnavailableError("Launcher heartbeat is unavailable")
        running_count, queued_count = Runs(cursor).launcher_counts()

    heartbeat_at, queue_depths = heartbeat
    if isinstance(queue_depths, str):
        queue_depths = json.loads(queue_depths)
    age = max(0.0, (datetime.now(timezone.utc) - heartbeat_at).total_seconds())
    return LauncherHealthResponse(
        heartbeat_at=heartbeat_at,
        heartbeat_age_seconds=age,
        queue_depths=queue_depths,
        running_count=running_count,
        queued_count=queued_count,
    )


def start_run(
    name: str,
    *,
    params: dict[str, Any] | None = None,
    requested_by: str | None = None,
    downstream: bool = False,
    trigger: Literal["manual", "cli", "mcp"] = "manual",
) -> dict[str, str]:
    catalog = _catalog()
    try:
        job = catalog.resolve(name).job
    except UnknownJobError as exc:
        raise JobNotFoundError(str(exc)) from exc

    # Validate before overlap admission so invalid caller input remains a 422 even when
    # another run happens to be active.
    validate_run_params(job, params)
    if overlap_refuses(catalog, name):
        raise RunOverlapError(f"A run for {name!r} is already running")

    if downstream:
        chain_root_id, run_id = _scheduler(catalog).start_chain(
            name, params=params, requested_by=requested_by, trigger=trigger
        )
        return {
            "run_id": run_id,
            "chain_root_id": chain_root_id,
            "status": "queued",
        }

    return publish_run(
        catalog,
        name,
        trigger=trigger,
        params=params,
        requested_by=requested_by,
        downstream=False,
    )


def cancel_run(identity: str) -> dict[str, str]:
    """Cancels a queued or running run; the watcher then stops its k8s Job or process.

    Cancelling a chain root also cancels the chain's queued and running steps in the same
    transaction. The scheduler only advances `running` roots, so no further step starts.
    """
    with get_db_cursor() as cursor:
        outcome = Runs(cursor).cancel(identity)
    if outcome is True:
        return {"run_id": identity, "status": "cancelled"}
    if outcome is None:
        raise RunNotFoundError(identity)
    raise RunNotCancellableError(outcome)


def pause_job(
    name: str,
    *,
    until: datetime | None,
    reason: str,
    set_by: str,
) -> dict[str, Any]:
    catalog = _catalog()
    if name not in catalog.jobs and name not in catalog.chains:
        raise JobNotFoundError(name)
    if until is not None:
        if until.tzinfo is None:
            raise ValueError("until must include a timezone")
        if until <= datetime.now(timezone.utc):
            raise ValueError("until must be in the future")
    with get_db_cursor() as cursor:
        cursor.execute(
            """INSERT INTO job_overrides(job_or_chain,paused_until,reason,set_by,set_at)
            VALUES (%s,%s,%s,%s,now()) ON CONFLICT(job_or_chain) DO UPDATE SET
            paused_until=EXCLUDED.paused_until,reason=EXCLUDED.reason,
            set_by=EXCLUDED.set_by,set_at=now()""",
            (name, until, reason, set_by),
        )
    return {"name": name, "paused_until": until, "reason": reason}


def unpause_job(name: str) -> dict[str, str]:
    with get_db_cursor() as cursor:
        cursor.execute(
            "DELETE FROM job_overrides WHERE job_or_chain=%s RETURNING job_or_chain",
            (name,),
        )
        if not cursor.fetchone():
            raise PauseNotFoundError(name)
    return {"name": name, "status": "resumed"}


def get_run(identity: str):
    try:
        return run_log_service.run_detail(identity)
    except run_log_service.RunNotFoundError as exc:
        raise RunNotFoundError(identity) from exc
