from fastapi import APIRouter, HTTPException, Query, Request
from src.schemas.run_log import JobHealthList, RunDetail, RunList, Trace
from src.services import activity_service as service
from src.services import run_log_service

activity_router = APIRouter()


def user(request):
    # oauth2-proxy strips/replaces the trusted identity header at the ingress.
    return (
        request.headers.get("x-auth-request-user")
        or request.headers.get("x-forwarded-user")
        or "league-operator"
    )


@activity_router.get("/actions")
def actions(
    request: Request,
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    attention: bool = False,
):
    return {"items": service.list_actions(user(request), limit, offset, attention)}


@activity_router.get("/actions/{identity}")
def action(identity: str):
    item = service.action_detail(identity)
    if not item:
        raise HTTPException(404, "Submission not found")
    return item


@activity_router.post("/actions/{identity}/check")
def check(identity: str):
    if not service.action_detail(identity):
        raise HTTPException(404, "Submission not found")
    from src.services.activity_runner import reconcile_action

    reconcile_action(identity)
    return service.action_detail(identity)


@activity_router.get("/sessions")
def sessions(limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0)):
    return {"items": service.sessions(limit=limit, offset=offset)}


@activity_router.get("/sessions/{identity}")
def session(identity: str):
    rows = service.sessions(identity)
    if not rows:
        raise HTTPException(404, "Session not found")
    return rows[0]


@activity_router.post("/read/{identity}")
def read(identity: str, request: Request):
    if not service.mark_read(identity, user(request)):
        raise HTTPException(404, "Activity not found")
    return {"status": "read"}


@activity_router.get("/bots")
def bots():
    return {"items": service.bot_summary()}


@activity_router.get("/runs", response_model=RunList)
def runs(
    job: str | None = None,
    status: str | None = Query(None, pattern="^(running|success|failure)$"),
    trigger: str | None = Query(None, pattern="^(schedule|queue|http|manual)$"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    return {"items": run_log_service.list_runs(job, status, trigger, limit, offset)}


@activity_router.get("/runs/{identity}", response_model=RunDetail)
def run(identity: str):
    try:
        return run_log_service.run_detail(identity)
    except run_log_service.RunNotFoundError:
        raise HTTPException(404, "Run not found")


@activity_router.get("/jobs", response_model=JobHealthList)
def jobs():
    return {
        "items": run_log_service.job_health(),
        "dead_letters": run_log_service.dead_letter_depths(),
    }


@activity_router.get("/trace", response_model=Trace)
def trace(
    ref_type: str = Query(..., pattern="^(raw_signal|rumor_signal|activity_action)$"),
    ref_id: str = Query(..., min_length=1),
):
    try:
        return run_log_service.trace(ref_type, ref_id)
    except run_log_service.RunNotFoundError:
        raise HTTPException(404, "Nothing found to trace")
