"""HTTP API for catalog jobs, run commands, and pause overrides."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from src.api.routes import require_trigger_api_key
from src.database import get_db_cursor
from src.launcher.catalog import (
    Catalog,
    UnknownJobError,
    load_catalog_from_env,
    validate_run_params,
)
from src.launcher.consumer import overlap_refuses
from src.launcher.publish import publish_run
from src.services import run_log_service

jobs_router = APIRouter()


class StartRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    params: dict[str, Any] = Field(default_factory=dict)
    downstream: bool = False


class PauseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    until: datetime | None = None
    reason: str = Field(min_length=1, max_length=500)


def _user(request: Request) -> str:
    return (
        request.headers.get("x-auth-request-user")
        or request.headers.get("x-forwarded-user")
        or "league-operator"
    )


def _effective(job, catalog):
    defaults = catalog.defaults
    return {
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
    }


def _last_runs(catalog: Catalog) -> dict[str, dict[str, Any]]:
    names = list(catalog.jobs)
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


def _overrides() -> dict[str, dict[str, Any]]:
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT job_or_chain,paused_until,reason,set_by,set_at FROM job_overrides"
        )
        keys = ("name", "paused_until", "reason", "set_by", "set_at")
        return {row[0]: dict(zip(keys, row)) for row in cursor.fetchall()}


def _list_catalog(catalog: Catalog) -> dict:
    last_runs = _last_runs(catalog)
    overrides = _overrides()
    return {
        "jobs": [
            {
                "name": name,
                **_effective(job, catalog),
                "last_run": last_runs.get(name),
                "pause": overrides.get(name),
            }
            for name, job in catalog.jobs.items()
        ],
        "chains": [
            {
                "name": name,
                "schedule": chain.schedule.model_dump(mode="json")
                if chain.schedule
                else None,
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
        ],
    }


@jobs_router.get("/jobs")
def get_jobs():
    try:
        catalog = load_catalog_from_env()
    except FileNotFoundError:
        return {"jobs": [], "chains": []}
    return _list_catalog(catalog)


@jobs_router.post(
    "/jobs/{name}/runs",
    status_code=202,
    dependencies=[Depends(require_trigger_api_key)],
)
def start_job(name: str, payload: StartRunRequest, request: Request):
    try:
        catalog = load_catalog_from_env()
    except FileNotFoundError as exc:
        raise HTTPException(503, "Job catalog is not mounted") from exc
    try:
        catalog.resolve(name)
    except UnknownJobError as exc:
        raise HTTPException(404, str(exc)) from exc
    if payload.downstream:
        raise HTTPException(422, "Downstream chain runs are not available yet")
    try:
        validate_run_params(catalog.jobs[name], payload.params)
        if overlap_refuses(catalog, name):
            raise HTTPException(409, "A run for this job is already running")
        return publish_run(
            catalog,
            name,
            trigger="manual",
            params=payload.params,
            requested_by=_user(request),
            downstream=payload.downstream,
        )
    except UnknownJobError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@jobs_router.post(
    "/runs/{identity}/cancel", dependencies=[Depends(require_trigger_api_key)]
)
def cancel_run(identity: str):
    """Mark a run cancelled; the launcher watcher deletes its Job or stops its inline process."""
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
            raise HTTPException(404, "Run not found")
        raise HTTPException(409, f"Run cannot be cancelled from status {row[0]}")


@jobs_router.put(
    "/jobs/{name}/pause", dependencies=[Depends(require_trigger_api_key)]
)
def pause_job(name: str, payload: PauseRequest, request: Request):
    try:
        catalog = load_catalog_from_env()
    except FileNotFoundError as exc:
        raise HTTPException(503, "Job catalog is not mounted") from exc
    if name not in catalog.jobs and name not in catalog.chains:
        raise HTTPException(404, "Unknown job or chain")
    until = payload.until
    if until is not None:
        if until.tzinfo is None:
            raise HTTPException(422, "until must include a timezone")
        if until <= datetime.now(timezone.utc):
            raise HTTPException(422, "until must be in the future")
    with get_db_cursor() as cursor:
        cursor.execute(
            """INSERT INTO job_overrides(job_or_chain,paused_until,reason,set_by,set_at)
            VALUES (%s,%s,%s,%s,now()) ON CONFLICT(job_or_chain) DO UPDATE SET
            paused_until=EXCLUDED.paused_until,reason=EXCLUDED.reason,
            set_by=EXCLUDED.set_by,set_at=now()""",
            (name, until, payload.reason, _user(request)),
        )
    return {"name": name, "paused_until": until, "reason": payload.reason}


@jobs_router.delete(
    "/jobs/{name}/pause", dependencies=[Depends(require_trigger_api_key)]
)
def unpause_job(name: str):
    with get_db_cursor() as cursor:
        cursor.execute(
            "DELETE FROM job_overrides WHERE job_or_chain=%s RETURNING job_or_chain",
            (name,),
        )
        if not cursor.fetchone():
            raise HTTPException(404, "Pause not found")
    return {"name": name, "status": "resumed"}


@jobs_router.get("/runs/{identity}")
def get_run(identity: str):
    try:
        return run_log_service.run_detail(identity)
    except run_log_service.RunNotFoundError as exc:
        raise HTTPException(404, "Run not found") from exc
