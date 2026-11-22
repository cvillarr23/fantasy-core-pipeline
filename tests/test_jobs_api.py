import json
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException, Request

from src.api import jobs as jobs_api
from src.launcher.catalog import Catalog, Schedule
from src.schemas.jobs import LauncherHealthResponse, PauseRequest, StartRunRequest
from src.services import jobs_service


@pytest.fixture
def catalog():
    return Catalog.model_validate(
        {
            "version": 1,
            "global": {
                "max_concurrent_pods": 2,
                "max_deadline": "2h",
                "start_timeout": "10m",
                "manual_queue_ttl": "30m",
            },
            "profiles": {"core": {"image": "core:test"}},
            "groups": {"espn-session": {"limit": 1}},
            "jobs": {
                "sample-job": {
                    "profile": "core",
                    "command": ["python", "-m", "sample"],
                    "params": {"count": "int", "dry_run": "bool?"},
                    "group": "espn-session",
                }
            },
            "chains": {
                "sample-chain": {
                    "max_duration": "1h",
                    "steps": {"sample-job": {}},
                }
            },
        }
    )


def request():
    return Request(
        {
            "type": "http",
            "headers": [(b"x-auth-request-user", b"operator")],
        }
    )


class Cursor:
    def __init__(
        self, update_status=None, select_status=None, rows=(), chain_root_id=None
    ):
        self.update_status = update_status
        self.select_status = select_status
        self.chain_root_id = chain_root_id
        self.rows = list(rows)
        self.sql = []
        self.params = []
        self.last_query = ""

    def execute(self, query, params=None):
        self.sql.append(query)
        self.params.append(params)
        self.last_query = query

    def fetchone(self):
        if "RETURNING chain_root_id" in self.last_query:
            if self.update_status in ("queued", "running"):
                self.update_status = "cancelled"
                return (self.chain_root_id, "pod")
            return None
        if "SELECT status FROM run_log" in self.last_query:
            return (self.select_status,) if self.select_status is not None else None
        if "INSERT INTO run_log" in self.last_query:
            return ("run-123",)
        if "DELETE FROM job_overrides" in self.last_query:
            return ("sample-job",) if self.select_status == "paused" else None
        return None

    def fetchall(self):
        if "FROM job_overrides" in self.last_query:
            return []
        return self.rows


@contextmanager
def db_cursor(cursor):
    yield cursor


def test_start_run_maps_unknown_bad_and_missing_params(catalog, monkeypatch):
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    overlap = Mock(return_value=False)
    publish = Mock(return_value={"run_id": "x", "status": "queued"})
    monkeypatch.setattr(jobs_service, "overlap_refuses", overlap)
    monkeypatch.setattr(jobs_service, "publish_run", publish)

    with pytest.raises(HTTPException) as unknown:
        jobs_api.start_run_endpoint("missing", StartRunRequest(), request())
    with pytest.raises(HTTPException) as bad:
        jobs_api.start_run_endpoint(
            "sample-job", StartRunRequest(params={"other": 3}), request()
        )
    with pytest.raises(HTTPException) as missing:
        jobs_api.start_run_endpoint(
            "sample-job", StartRunRequest(params={"dry_run": True}), request()
        )

    assert unknown.value.status_code == 404
    assert bad.value.status_code == 422
    assert missing.value.status_code == 422
    publish.assert_not_called()
    overlap.assert_not_called()


def test_start_run_refuses_overlap(catalog, monkeypatch):
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    publish = Mock(return_value={"run_id": "x", "status": "queued"})
    monkeypatch.setattr(jobs_service, "publish_run", publish)
    monkeypatch.setattr(jobs_service, "overlap_refuses", lambda *_: True)
    with pytest.raises(HTTPException) as response:
        jobs_api.start_run_endpoint(
            "sample-job", StartRunRequest(params={"count": 4}), request()
        )
    assert response.value.status_code == 409
    publish.assert_not_called()


def test_start_run_persists_queued_row_and_outbox(catalog, monkeypatch):
    cursor = Cursor()
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    monkeypatch.setattr(jobs_service, "overlap_refuses", lambda *_: False)
    monkeypatch.setattr(jobs_service, "get_db_cursor", lambda: db_cursor(cursor))
    monkeypatch.setattr("src.launcher.publish.get_db_cursor", lambda: db_cursor(cursor))
    with patch("src.services.activity_runner.publish_job_run"):
        result = jobs_api.start_run_endpoint(
            "sample-job", StartRunRequest(params={"count": 4}), request()
        )

    assert result["status"] == "queued"
    assert any(
        "INSERT INTO run_log" in query and "'queued'" in query for query in cursor.sql
    )
    outbox = next(
        params
        for query, params in zip(cursor.sql, cursor.params)
        if "INSERT INTO job_run_outbox" in query
    )
    command = json.loads(outbox[1])
    assert command["run_id"] == result["run_id"]
    assert command["job"] == "sample-job"
    assert command["params"] == {"count": 4}
    assert command["requested_by"] == "operator"
    assert command["downstream"] is False


@pytest.mark.parametrize("trigger", ["cli", "mcp"])
def test_start_run_records_the_requested_trigger(catalog, monkeypatch, trigger):
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    monkeypatch.setattr(jobs_service, "overlap_refuses", lambda *_: False)
    published = Mock(return_value={"run_id": "r1", "status": "queued"})
    monkeypatch.setattr(jobs_service, "publish_run", published)

    jobs_api.start_run_endpoint("sample-job", StartRunRequest(params={"count": 1}, trigger=trigger), request())

    assert published.call_args.kwargs["trigger"] == trigger


def test_start_run_passes_parent_and_dedupe_key(catalog, monkeypatch):
    """Trade approval parents its send to the scan and keys it on the offer."""
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    monkeypatch.setattr(jobs_service, "overlap_refuses", lambda *_: False)
    published = Mock(return_value={"run_id": "r1", "status": "duplicate"})
    monkeypatch.setattr(jobs_service, "publish_run", published)

    result = jobs_service.start_run(
        "sample-job",
        params={"count": 1},
        requested_by="example-reviewer",
        parent_run_id="scan-run",
        dedupe_key="sample-job@offer",
    )

    assert result == {"run_id": "r1", "status": "duplicate"}
    assert published.call_args.kwargs["parent_run_id"] == "scan-run"
    assert published.call_args.kwargs["dedupe_key"] == "sample-job@offer"


def test_downstream_start_passes_the_requested_trigger(catalog, monkeypatch):
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    monkeypatch.setattr(jobs_service, "overlap_refuses", lambda *_: False)
    scheduler = Mock()
    scheduler.start_chain.return_value = ("root", "run")
    monkeypatch.setattr(jobs_service, "_scheduler", lambda _catalog: scheduler)

    jobs_api.start_run_endpoint(
        "sample-job",
        StartRunRequest(params={"count": 1}, downstream=True, trigger="mcp"),
        request(),
    )

    assert scheduler.start_chain.call_args.kwargs["trigger"] == "mcp"


def test_start_run_request_rejects_triggers_a_caller_may_not_claim():
    with pytest.raises(ValueError):
        StartRunRequest(trigger="schedule")


def test_downstream_start_returns_chain_root_and_run_id(catalog, monkeypatch):
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    monkeypatch.setattr(jobs_service, "overlap_refuses", lambda *_: False)

    class FakeScheduler:
        def __init__(self, *_):
            pass

        def start_chain(self, name, *, params, requested_by, trigger):
            assert name == "sample-job"
            assert params == {"count": 2}
            assert requested_by == "operator"
            assert trigger == "manual"
            return "chain-root", "run-456"

    monkeypatch.setattr(jobs_service, "_scheduler", lambda _catalog: FakeScheduler())
    result = jobs_api.start_run_endpoint(
        "sample-job",
        StartRunRequest(params={"count": 2}, downstream=True),
        request(),
    )
    assert result == {
        "run_id": "run-456",
        "chain_root_id": "chain-root",
        "status": "queued",
    }


def test_downstream_start_maps_overlap_and_chain_ambiguity(catalog, monkeypatch):
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    monkeypatch.setattr(jobs_service, "_scheduler", Mock())
    monkeypatch.setattr(jobs_service, "overlap_refuses", lambda *_: True)
    with pytest.raises(HTTPException) as overlap:
        jobs_api.start_run_endpoint(
            "sample-job",
            StartRunRequest(params={"count": 2}, downstream=True),
            request(),
        )
    assert overlap.value.status_code == 409

    monkeypatch.setattr(jobs_service, "overlap_refuses", lambda *_: False)

    class AmbiguousScheduler:
        def __init__(self, *_):
            pass

        def start_chain(self, *_args, **_kwargs):
            raise ValueError("step belongs to several chains")

    monkeypatch.setattr(
        jobs_service, "_scheduler", lambda _catalog: AmbiguousScheduler()
    )
    with pytest.raises(HTTPException) as ambiguous:
        jobs_api.start_run_endpoint(
            "sample-job",
            StartRunRequest(params={"count": 2}, downstream=True),
            request(),
        )
    assert ambiguous.value.status_code == 422


@pytest.mark.parametrize(
    ("state", "expected"), [("queued", 200), ("success", 409), (None, 404)]
)
def test_cancel_run_statuses(monkeypatch, state, expected):
    cursor = Cursor(update_status=state, select_status=state)
    monkeypatch.setattr(jobs_service, "get_db_cursor", lambda: db_cursor(cursor))
    if expected == 200:
        assert jobs_api.cancel_run("run-123") == {
            "run_id": "run-123",
            "status": "cancelled",
        }
    else:
        with pytest.raises(HTTPException) as response:
            jobs_api.cancel_run("run-123")
        assert response.value.status_code == expected


@pytest.mark.parametrize(
    ("chain_root_id", "cancels_steps"),
    [("run-123", True), ("other-root", False), (None, False)],
)
def test_cancel_run_cancels_steps_only_for_a_chain_root(
    monkeypatch, chain_root_id, cancels_steps
):
    cursor = Cursor(update_status="running", chain_root_id=chain_root_id)
    monkeypatch.setattr(jobs_service, "get_db_cursor", lambda: db_cursor(cursor))
    assert jobs_api.cancel_run("run-123") == {
        "run_id": "run-123",
        "status": "cancelled",
    }
    steps = [
        params
        for query, params in zip(cursor.sql, cursor.params)
        if "WHERE chain_root_id=%s AND id<>%s" in query
    ]
    assert steps == ([("run-123", "run-123")] if cancels_steps else [])


def test_pause_validates_and_upserts_indefinite_pause(catalog, monkeypatch):
    cursor = Cursor()
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    monkeypatch.setattr(jobs_service, "get_db_cursor", lambda: db_cursor(cursor))
    with pytest.raises(HTTPException) as naive:
        jobs_api.pause_job(
            "sample-job",
            PauseRequest(until="2030-01-01T00:00:00", reason="ops"),
            request(),
        )
    with pytest.raises(HTTPException) as past:
        jobs_api.pause_job(
            "sample-job",
            PauseRequest(until="2000-01-01T00:00:00Z", reason="ops"),
            request(),
        )
    assert naive.value.status_code == 422
    assert past.value.status_code == 422

    response = jobs_api.pause_job(
        "sample-job", PauseRequest(reason="maintenance"), request()
    )
    assert response["paused_until"] is None
    assert any("ON CONFLICT(job_or_chain) DO UPDATE" in query for query in cursor.sql)


def test_unpause_missing_pause_returns_404(monkeypatch):
    cursor = Cursor()
    monkeypatch.setattr(jobs_service, "get_db_cursor", lambda: db_cursor(cursor))
    with pytest.raises(HTTPException) as response:
        jobs_api.unpause_job("sample-job")
    assert response.value.status_code == 404


def test_unpause_existing_pause_deletes_override(monkeypatch):
    cursor = Cursor(select_status="paused")
    monkeypatch.setattr(jobs_service, "get_db_cursor", lambda: db_cursor(cursor))
    assert jobs_api.unpause_job("sample-job") == {
        "name": "sample-job",
        "status": "resumed",
    }
    assert "DELETE FROM job_overrides" in cursor.sql[0]


def test_list_jobs_returns_jobs_chains_and_reuses_last_run_map(
    catalog, monkeypatch
):
    run = ("sample-job", "run-1", "success", datetime.now(timezone.utc), None, "manual")
    cursor = Cursor(rows=[run])
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    monkeypatch.setattr(jobs_service, "get_db_cursor", lambda: db_cursor(cursor))
    payload = jobs_api.get_jobs()
    assert payload.jobs[0].name == "sample-job"
    resolved = catalog.resolve("sample-job")
    assert (
        payload.jobs[0].executor,
        payload.jobs[0].max_attempts,
        payload.jobs[0].deadline_seconds,
        payload.jobs[0].group,
    ) == (
        resolved.executor,
        resolved.max_attempts,
        int(resolved.deadline.total_seconds()),
        "espn-session",
    )
    assert payload.jobs[0].last_run.id == "run-1"
    step = payload.chains[0].steps["sample-job"]
    assert step.after == []
    assert step.last_run.id == "run-1"
    assert sum("DISTINCT ON (job_name)" in query for query in cursor.sql) == 1


def test_list_jobs_reports_next_run_for_jobs_and_chains(catalog, monkeypatch):
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    monkeypatch.setattr(jobs_service, "get_db_cursor", lambda: db_cursor(Cursor()))
    catalog.jobs["sample-job"].schedules = [
        Schedule(cron="0 12 * * *", tz="America/Chicago")
    ]
    catalog.chains["sample-chain"].schedule = Schedule(
        cron="0 16 * * 2", tz="America/Chicago"
    )
    payload = jobs_api.get_jobs()
    assert payload.jobs[0].next_run_at > datetime.now(timezone.utc)
    assert payload.chains[0].next_run_at > datetime.now(timezone.utc)


def test_list_jobs_has_no_next_run_without_a_schedule(catalog, monkeypatch):
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    monkeypatch.setattr(jobs_service, "get_db_cursor", lambda: db_cursor(Cursor()))
    payload = jobs_api.get_jobs()
    assert payload.jobs[0].next_run_at is None
    assert payload.chains[0].next_run_at is None


def test_season_calendar_survives_between_requests(catalog):
    first = jobs_service._scheduler(catalog)
    second = jobs_service._scheduler(catalog)
    assert first.seasons is second.seasons


def test_missing_catalog_returns_empty_jobs_and_503_for_write(monkeypatch):
    def missing_catalog():
        raise jobs_service.CatalogUnavailableError("catalog is not mounted")

    monkeypatch.setattr(jobs_service, "_catalog", missing_catalog)
    assert jobs_api.get_jobs().jobs == []
    with pytest.raises(HTTPException) as response:
        jobs_api.start_run_endpoint("sample-job", StartRunRequest(), request())
    assert response.value.status_code == 503


def test_mutating_routes_require_api_key_dependency():
    protected_paths = {
        ("POST", "/jobs/{name}/runs"),
        ("POST", "/runs/{identity}/cancel"),
        ("PUT", "/jobs/{name}/pause"),
        ("DELETE", "/jobs/{name}/pause"),
        ("POST", "/runs/{run_id}/approvals"),
        ("POST", "/runs/{identity}/approve"),
        ("POST", "/runs/{identity}/dismiss"),
    }
    protected = {
        (method, route.path)
        for route in jobs_api.jobs_router.routes
        for method in route.methods
        if any(
            dependency.dependency is jobs_api.require_trigger_api_key
            for dependency in route.dependencies
        )
    }
    assert protected_paths <= protected


def test_launcher_health_returns_heartbeat_and_run_counts(monkeypatch):
    health = {
        "heartbeat_at": datetime(2026, 10, 26, 12, 0, tzinfo=timezone.utc),
        "heartbeat_age_seconds": 12.5,
        "queue_depths": {"job_runs": 3, "job_runs.dlq": 1},
        "running_count": 2,
        "queued_count": 4,
    }
    monkeypatch.setattr(
        jobs_service,
        "launcher_health",
        lambda: LauncherHealthResponse(**health),
    )

    response = jobs_api.get_launcher_health()

    assert response.model_dump() == health


def test_launcher_health_is_unavailable_before_first_heartbeat(monkeypatch):
    def unavailable():
        raise jobs_service.LauncherHealthUnavailableError("no heartbeat")

    monkeypatch.setattr(jobs_service, "launcher_health", unavailable)

    with pytest.raises(HTTPException) as response:
        jobs_api.get_launcher_health()

    assert response.value.status_code == 503


def test_launcher_health_counts_only_runs_with_executor_trigger(monkeypatch):
    class HealthCursor:
        queries = []
        reads = 0

        def execute(self, query, params=None):
            self.queries.append(query)

        def fetchone(self):
            self.reads += 1
            if self.reads == 1:
                return (datetime.now(timezone.utc), {"job_runs": 0, "job_runs.dlq": 0})
            return (1, 2)

    cursor = HealthCursor()
    monkeypatch.setattr(jobs_service, "get_db_cursor", lambda: db_cursor(cursor))

    health = jobs_service.launcher_health()

    assert health.running_count == 1
    assert health.queued_count == 2
    assert "status='running' AND executor IS NOT NULL" in cursor.queries[1]


# Approval routes: the service is covered against Postgres in tests/test_approval_postgres.py.


def test_open_approval_passes_the_request_through(monkeypatch):
    from src.schemas.jobs import OpenApprovalRequest
    from src.services import approval_service

    opened = Mock(return_value={"run_id": "approval-1", "status": "waiting"})
    monkeypatch.setattr(approval_service, "open_approval", opened)
    payload = OpenApprovalRequest(
        key="offer-1",
        title="Send offer?",
        link="/trades?offer=offer-1",
        then={"job": "trade-submit", "params": {"offer_key": "offer-1"}},
    )
    assert jobs_api.open_approval("scan-1", payload) == {
        "run_id": "approval-1",
        "status": "waiting",
    }
    opened.assert_called_once_with(
        "scan-1",
        key="offer-1",
        title="Send offer?",
        then={"job": "trade-submit", "params": {"offer_key": "offer-1"}},
        link="/trades?offer=offer-1",
        expires_at=None,
    )


def test_open_approval_request_needs_an_aware_expiry_and_no_extra_fields():
    from pydantic import ValidationError

    from src.schemas.jobs import OpenApprovalRequest

    with pytest.raises(ValidationError):
        OpenApprovalRequest(key="k", title="t", expires_at=datetime(2026, 11, 22))
    with pytest.raises(ValidationError):
        OpenApprovalRequest(key="k", title="t", approve=True)
    with pytest.raises(ValidationError):
        OpenApprovalRequest(key="k", title="t", then={"job": "j", "extra": 1})


@pytest.mark.parametrize(
    ("error", "status"),
    [
        ("ApprovalNotFoundError", 404),
        ("ApprovalRequesterNotRunningError", 409),
        ("ApprovalResolvedError", 409),
        ("ApprovalDecidedElsewhereError", 409),
    ],
)
def test_approval_errors_map_to_http_statuses(error, status):
    from src.services import approval_service

    cls = getattr(approval_service, error)
    exc = cls("run-1", "success") if error == "ApprovalResolvedError" else cls("x")
    with pytest.raises(HTTPException) as raised:
        jobs_api._raise_http(exc)
    assert raised.value.status_code == status


@pytest.mark.parametrize(
    ("route", "service"), [("approve_run", "approve"), ("dismiss_run", "dismiss")]
)
def test_approve_and_dismiss_record_the_user(monkeypatch, route, service):
    from src.services import approval_service

    decided = Mock(
        return_value={"run_id": "a-1", "status": "success", "end_reason": "approved"}
    )
    monkeypatch.setattr(approval_service, service, decided)
    assert getattr(jobs_api, route)("a-1", request())["run_id"] == "a-1"
    decided.assert_called_once_with("a-1", "operator")


def test_approving_logs_and_leaves_a_failed_start_to_the_scheduler(monkeypatch):
    from src.services import approval_service

    resolve = Mock()
    monkeypatch.setattr(approval_service, "resolve_approval", resolve)
    monkeypatch.setattr(approval_service, "get_db_cursor", lambda: db_cursor(Cursor()))
    monkeypatch.setattr(
        approval_service, "start_approved_run", Mock(side_effect=RuntimeError("down"))
    )
    assert approval_service.approve("a-1", "operator") == {
        "run_id": "a-1",
        "status": "success",
        "end_reason": "approved",
        "then_run_id": None,
    }
    assert resolve.call_args[0][1:] == ("a-1", "approved", "operator")


def test_chain_info_exposes_an_approval_step(monkeypatch):
    catalog = Catalog.model_validate(
        {
            "version": 1,
            "global": {
                "max_concurrent_pods": 2,
                "max_deadline": "2h",
                "start_timeout": "10m",
                "manual_queue_ttl": "30m",
            },
            "profiles": {"core": {"image": "core:test"}},
            "jobs": {"act": {"profile": "core", "command": ["python", "-m", "act"]}},
            "chains": {
                "nightly": {
                    "max_duration": "6h",
                    "steps": {
                        "confirm": {"approval": {"timeout": "2h", "title": "Go?"}},
                        "act": {"after": [{"step": "confirm"}]},
                    },
                }
            },
        }
    )
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    monkeypatch.setattr(jobs_service, "_last_runs", Mock(return_value={}))
    monkeypatch.setattr(jobs_service, "_pause_overrides", Mock(return_value={}))
    response = jobs_service.list_jobs()
    (chain,) = response.chains
    assert chain.steps["confirm"].approval.model_dump() == {
        "title": "Go?",
        "timeout_seconds": 7200,
    }
    assert chain.steps["act"].approval is None
    assert jobs_service._last_runs.call_args[0][0] == ["act", "confirm"]
