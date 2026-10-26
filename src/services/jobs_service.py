"""Reusable launcher job operations for REST, CLI, and MCP callers."""

import json
from datetime import datetime, timezone
from typing import Any

from src.database import get_db_cursor
from src.launcher.catalog import Catalog, UnknownJobError, load_catalog_from_env, validate_run_params
from src.launcher.consumer import overlap_refuses
from src.launcher.publish import publish_run
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


def _scheduler(catalog: Catalog):
    from src.launcher.scheduler import PostgresSchedulerState, Scheduler

    return Scheduler(catalog, PostgresSchedulerState())


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

    names = list(catalog.jobs)
    last_runs: dict[str, dict[str, Any]] = {}
    if names:
        with get_db_cursor() as cursor:
            cursor.execute(
                """SELECT DISTINCT ON (job_name) job_name,id,status,started_at,finished_at,trigger_type
                FROM run_log WHERE job_name=ANY(%s) ORDER BY job_name,started_at DESC""",
                (names,),
            )
            keys = ("job_name", "id", "status", "started_at", "finished_at", "trigger_type")
            last_runs = {row[0]: dict(zip(keys, row)) for row in cursor.fetchall()}

    overrides: dict[str, dict[str, Any]] = {}
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT job_or_chain,paused_until,reason,set_by,set_at FROM job_overrides"
        )
        keys = ("name", "paused_until", "reason", "set_by", "set_at")
        overrides = {row[0]: dict(zip(keys, row)) for row in cursor.fetchall()}

    defaults = catalog.defaults
    jobs = [
        {
            "name": name,
            "executor": job.executor or defaults.executor,
            "overlap": job.overlap or defaults.overlap,
            "max_attempts": job.max_attempts or defaults.max_attempts,
            "deadline_seconds": int((job.deadline or defaults.deadline).total_seconds()),
            "alert": job.alert or defaults.alert,
            "acts": job.acts,
            "params": job.params,
            "profile": job.profile,
            "command": job.command,
            "schedules": [schedule.model_dump(mode="json") for schedule in job.schedules],
            "on_event": [event.model_dump(mode="json") for event in job.on_event],
            "last_run": last_runs.get(name),
            "pause": overrides.get(name),
        }
        for name, job in catalog.jobs.items()
    ]
    chains = [
        {
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
            "pause": overrides.get(name),
        }
        for name, chain in catalog.chains.items()
    ]
    return JobsResponse(jobs=jobs, chains=chains)


def launcher_health() -> LauncherHealthResponse:
    with get_db_cursor() as cursor:
        cursor.execute("SELECT at,queue_depths FROM launcher_heartbeat WHERE id=1")
        heartbeat = cursor.fetchone()
        if heartbeat is None:
            raise LauncherHealthUnavailableError("Launcher heartbeat is unavailable")
        cursor.execute(
            """SELECT count(*) FILTER (WHERE status='running'),
            count(*) FILTER (WHERE status='queued') FROM run_log"""
        )
        running_count, queued_count = cursor.fetchone()

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
            name, params=params, requested_by=requested_by
        )
        return {
            "run_id": run_id,
            "chain_root_id": chain_root_id,
            "status": "queued",
        }

    try:
        return publish_run(
            catalog,
            name,
            trigger="manual",
            params=params,
            requested_by=requested_by,
            downstream=False,
        )
    except UnknownJobError as exc:
        raise JobNotFoundError(str(exc)) from exc


def cancel_run(identity: str) -> dict[str, str]:
    with get_db_cursor() as cursor:
        cursor.execute(
            """UPDATE run_log SET status='cancelled',finished_at=now(),end_reason='cancelled'
            WHERE id=%s AND status IN ('queued','running') RETURNING status""",
            (identity,),
        )
        if cursor.fetchone():
            return {"run_id": identity, "status": "cancelled"}
        cursor.execute("SELECT status FROM run_log WHERE id=%s", (identity,))
        row = cursor.fetchone()
        if row is None:
            raise RunNotFoundError(identity)
        raise RunNotCancellableError(row[0])


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
