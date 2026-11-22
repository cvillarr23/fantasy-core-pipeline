"""Thin REST controller for unified launcher jobs and runs."""

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from src.api.routes import require_trigger_api_key
from src.schemas.jobs import (
    ApprovalDecisionResponse,
    ApprovalResponse,
    CancelRunResponse,
    JobsResponse,
    LauncherHealthResponse,
    OpenApprovalRequest,
    PauseRequest,
    PauseResponse,
    ResourceSuggestionsResponse,
    RunStartResponse,
    StartRunRequest,
    UnpauseResponse,
)
from src.schemas.run_log import RunDetail
from src.services import approval_service, jobs_service

jobs_router = APIRouter()


def _raise_http(error: Exception):
    if isinstance(error, jobs_service.LauncherHealthUnavailableError):
        raise HTTPException(503, str(error)) from error
    if isinstance(error, jobs_service.CatalogUnavailableError):
        raise HTTPException(503, str(error)) from error
    if isinstance(
        error,
        (
            jobs_service.JobNotFoundError,
            jobs_service.RunNotFoundError,
            jobs_service.PauseNotFoundError,
            approval_service.ApprovalNotFoundError,
        ),
    ):
        raise HTTPException(404, str(error)) from error
    if isinstance(
        error,
        (
            jobs_service.RunOverlapError,
            jobs_service.RunNotCancellableError,
            approval_service.ApprovalRequesterNotRunningError,
            approval_service.ApprovalResolvedError,
            approval_service.ApprovalDecidedElsewhereError,
        ),
    ):
        raise HTTPException(409, str(error)) from error
    if isinstance(error, ValueError):
        raise HTTPException(422, str(error)) from error
    raise error


def _user(request: Request) -> str:
    return (
        request.headers.get("x-auth-request-user")
        or request.headers.get("x-forwarded-user")
        or "league-operator"
    )


@jobs_router.get("/jobs", response_model=JobsResponse)
def get_jobs():
    return jobs_service.list_jobs()


@jobs_router.get("/jobs/resource-suggestions", response_model=ResourceSuggestionsResponse)
def get_resource_suggestions(
    days: int = Query(30, ge=1, le=90), min_runs: int = Query(5, ge=1, le=1000)
):
    try:
        return jobs_service.resource_suggestions(days, min_runs)
    except Exception as error:
        _raise_http(error)


@jobs_router.get("/launcher/health", response_model=LauncherHealthResponse)
def get_launcher_health():
    try:
        return jobs_service.launcher_health()
    except Exception as error:
        _raise_http(error)


@jobs_router.post(
    "/jobs/{name}/runs",
    response_model=RunStartResponse,
    status_code=202,
    dependencies=[Depends(require_trigger_api_key)],
)
def start_run_endpoint(name: str, payload: StartRunRequest, request: Request):
    try:
        return jobs_service.start_run(
            name,
            params=payload.params,
            requested_by=_user(request),
            downstream=payload.downstream,
            trigger=payload.trigger,
        )
    except Exception as error:
        _raise_http(error)


@jobs_router.post(
    "/runs/{identity}/cancel",
    response_model=CancelRunResponse,
    dependencies=[Depends(require_trigger_api_key)],
)
def cancel_run(identity: str):
    """The watcher deletes the k8s Job or stops the inline process for a cancelled row."""
    try:
        return jobs_service.cancel_run(identity)
    except Exception as error:
        _raise_http(error)


@jobs_router.post(
    "/runs/{run_id}/approvals",
    response_model=ApprovalResponse,
    status_code=202,
    dependencies=[Depends(require_trigger_api_key)],
)
def open_approval(run_id: str, payload: OpenApprovalRequest):
    """A running process asks a person to approve `then` (or just to confirm)."""
    try:
        return approval_service.open_approval(
            run_id,
            key=payload.key,
            title=payload.title,
            then=payload.then.model_dump() if payload.then else None,
            link=payload.link,
            expires_at=payload.expires_at,
        )
    except Exception as error:
        _raise_http(error)


@jobs_router.post(
    "/runs/{identity}/approve",
    response_model=ApprovalDecisionResponse,
    dependencies=[Depends(require_trigger_api_key)],
)
def approve_run(identity: str, request: Request):
    try:
        return approval_service.approve(identity, _user(request))
    except Exception as error:
        _raise_http(error)


@jobs_router.post(
    "/runs/{identity}/dismiss",
    response_model=ApprovalDecisionResponse,
    dependencies=[Depends(require_trigger_api_key)],
)
def dismiss_run(identity: str, request: Request):
    try:
        return approval_service.dismiss(identity, _user(request))
    except Exception as error:
        _raise_http(error)


@jobs_router.put(
    "/jobs/{name}/pause",
    response_model=PauseResponse,
    dependencies=[Depends(require_trigger_api_key)],
)
def pause_job(name: str, payload: PauseRequest, request: Request):
    try:
        return jobs_service.pause_job(
            name,
            until=payload.until,
            reason=payload.reason,
            set_by=_user(request),
        )
    except Exception as error:
        _raise_http(error)


@jobs_router.delete(
    "/jobs/{name}/pause",
    response_model=UnpauseResponse,
    dependencies=[Depends(require_trigger_api_key)],
)
def unpause_job(name: str):
    try:
        return jobs_service.unpause_job(name)
    except Exception as error:
        _raise_http(error)


@jobs_router.get("/runs/{identity}", response_model=RunDetail)
def get_run(identity: str):
    try:
        return jobs_service.get_run(identity)
    except Exception as error:
        _raise_http(error)
