"""Approval runs: a run_log row (executor `approval`, status `waiting`) that waits for a person.

A running process opens one (`open_approval`, `POST /v1/runs/{run_id}/approvals`), naming in
`then` the run to start once approved. An `approval:` chain step opens its own through the
scheduler. Either way it holds no pod, group or overlap slot, and it ends:

| Outcome | Status / end_reason | Then |
|---|---|---|
| approved | `success` / `approved` | `then` starts, parented to the approval |
| dismissed | `skipped` / `dismissed` | nothing |
| superseded by a newer one | `skipped` / `superseded` | nothing |
| past `expires_at` | `skipped` / `expired` (the scheduler's sweep) | nothing |

`resolve_approval` runs on the caller's cursor, so a route that owns more state (trade
approval) can resolve the approval in its own transaction. It never publishes: the caller
calls `start_approved_run` after committing, and the scheduler starts any `then` run that
didn't start.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from src.database import get_db_cursor
from src.launcher.approvals import start_then
from src.launcher.catalog import UnknownJobError, validate_run_params
from src.launcher.publish import publish_run
from src.launcher.runs import APPROVAL, APPROVAL_OUTCOMES, WAITING, Runs
from src.services import jobs_service

logger = logging.getLogger(__name__)

DEFAULT_APPROVAL_TTL = timedelta(hours=24)
MAX_APPROVAL_TTL = timedelta(days=7)
# The job_name of an approval a process opened. A chain step's approval uses the step name.
APPROVAL_JOB_NAME = APPROVAL
# Approvals whose `then` job only a domain route may decide, mapped to that route's page. A
# trade offer's approve route edits the message, recomputes the offer key and writes the
# bot_decisions audit in the same transaction, so `/v1/runs/{id}/approve` must not skip it.
DOMAIN_DECIDED_JOBS = {"trade-submit": "the Trades page"}


class ApprovalNotFoundError(Exception):
    """No approval run with that id."""


class ApprovalResolvedError(Exception):
    """The approval is no longer waiting, or has expired and can't be approved."""

    def __init__(self, run_id: str, status: str):
        self.status = status
        super().__init__(f"Approval {run_id} is {status}, not waiting")


class ApprovalDecidedElsewhereError(Exception):
    """The approval is decided through its domain's own route, not the generic one."""


class ApprovalRequesterNotRunningError(Exception):
    """Only a running run can ask for an approval."""


def approval_dedupe_key(run_id: str, key: str) -> str:
    return f"approval@{run_id}#{key}"


def open_approval(
    run_id: str,
    *,
    key: str,
    title: str,
    then: dict[str, Any] | None = None,
    link: str | None = None,
    expires_at: datetime | None = None,
) -> dict[str, str]:
    """Opens an approval run asked for by run `run_id`. Returns `{"run_id", "status"}` with
    status `waiting`, or `duplicate` with the existing approval's id when this run already
    asked under `key`.

    Raises jobs_service.RunNotFoundError for an unknown run,
    ApprovalRequesterNotRunningError for one not running, jobs_service.JobNotFoundError for an
    unknown `then.job`, and ValueError for bad `then.params` or `expires_at`.
    """
    checked_then = _checked_then(then)
    expiry = _checked_expiry(expires_at, datetime.now(timezone.utc))
    with get_db_cursor() as cursor:
        runs = Runs(cursor)
        _require_running(runs, run_id)
        dedupe_key = approval_dedupe_key(run_id, key)
        approval_id = runs.insert_approval(
            job=APPROVAL_JOB_NAME,
            trigger_type="approval",
            trigger=_approval_trigger(key, title, link, checked_then),
            parent_run_id=run_id,
            dedupe_key=dedupe_key,
            expires_at=expiry,
        )
        if approval_id is None:
            return {
                "run_id": runs.run_id_for_dedupe_key(dedupe_key),
                "status": "duplicate",
            }
    logger.info("Run %s opened approval %s (%s)", run_id, approval_id, key)
    return {"run_id": approval_id, "status": WAITING}


def resolve_approval(cursor, run_id: str, outcome: str, decided_by: str) -> None:
    """Ends a waiting approval on the caller's cursor (`outcome`: approved, dismissed or
    superseded). Raises ApprovalNotFoundError or ApprovalResolvedError. An approved one's
    `then` run is not started here: call `start_approved_run` after committing."""
    if outcome not in APPROVAL_OUTCOMES:
        raise ValueError(f"unknown approval outcome {outcome!r}")
    resolved = Runs(cursor).resolve_approval(run_id, outcome, decided_by)
    if resolved is None:
        raise ApprovalNotFoundError(run_id)
    if resolved is not True:
        raise ApprovalResolvedError(run_id, _blocking_status(resolved))


def start_approved_run(run_id: str) -> dict[str, str] | None:
    """Starts an approved approval's `then` run, once however often it is called. Returns
    publish_run's result, or None when it gates nothing or isn't approved."""
    with get_db_cursor() as cursor:
        approval = Runs(cursor).approval(run_id)
    if approval is None or approval.status != "success":
        return None
    return start_then(jobs_service.catalog(), approval, publish_run)


def approve(run_id: str, decided_by: str) -> dict[str, Any]:
    return _decide(run_id, "approved", decided_by)


def dismiss(run_id: str, decided_by: str) -> dict[str, Any]:
    return _decide(run_id, "dismissed", decided_by)


def _decide(run_id: str, outcome: str, decided_by: str) -> dict[str, Any]:
    """Resolves the approval in its own transaction, then starts its `then` run. A failed
    start is logged and left to the scheduler, which retries it."""
    with get_db_cursor() as cursor:
        _require_generic(Runs(cursor).approval(run_id))
        resolve_approval(cursor, run_id, outcome, decided_by)
    response: dict[str, Any] = {
        "run_id": run_id,
        "status": APPROVAL_OUTCOMES[outcome],
        "end_reason": outcome,
        "then_run_id": None,
    }
    if outcome == "approved":
        response["then_run_id"] = _start_then_logged(run_id)
    return response


def _require_generic(approval) -> None:
    """Refuses an approval whose `then` job a domain route decides (`DOMAIN_DECIDED_JOBS`)."""
    then = approval.then if approval else None
    page = DOMAIN_DECIDED_JOBS.get(then["job"]) if then else None
    if page:
        raise ApprovalDecidedElsewhereError(
            f"Approval {approval.run_id} is decided on {page}"
        )


def _start_then_logged(run_id: str) -> str | None:
    try:
        started = start_approved_run(run_id)
    except Exception:
        logger.exception(
            "Approval %s approved; the scheduler will start its run", run_id
        )
        return None
    return started["run_id"] if started else None


def _checked_then(then: dict[str, Any] | None) -> dict[str, Any] | None:
    """`then` with params validated against the catalog now, so an approval can't be
    approved into a run the launcher would refuse."""
    if then is None:
        return None
    catalog = jobs_service.catalog()
    try:
        job = catalog.resolve(then["job"]).job
    except UnknownJobError as exc:
        raise jobs_service.JobNotFoundError(str(exc)) from exc
    return {"job": then["job"], "params": validate_run_params(job, then.get("params"))}


def _checked_expiry(expires_at: datetime | None, now: datetime) -> datetime:
    if expires_at is None:
        return now + DEFAULT_APPROVAL_TTL
    if expires_at.tzinfo is None:
        raise ValueError("expires_at must include a timezone")
    if expires_at <= now:
        raise ValueError("expires_at must be in the future")
    if expires_at > now + MAX_APPROVAL_TTL:
        raise ValueError(f"expires_at must be within {MAX_APPROVAL_TTL.days} days")
    return expires_at


def _require_running(runs: Runs, run_id: str) -> None:
    status = runs.status(run_id)
    if status is None:
        raise jobs_service.RunNotFoundError(run_id)
    if status != "running":
        raise ApprovalRequesterNotRunningError(
            f"Run {run_id} is {status}; only a running run can ask for an approval"
        )


def _approval_trigger(
    key: str, title: str, link: str | None, then: dict[str, Any] | None
) -> dict[str, Any]:
    trigger: dict[str, Any] = {"key": key, "title": title}
    if link:
        trigger["link"] = link
    if then:
        trigger["then"] = then
    return trigger


def _blocking_status(status: str) -> str:
    """`expired` for an approval still waiting past its expiry, which only an approve
    refuses; the sweep will record it as such."""
    return "expired" if status == WAITING else status
