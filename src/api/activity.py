from fastapi import APIRouter, HTTPException, Query, Request
from src.services import activity_service as service

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
