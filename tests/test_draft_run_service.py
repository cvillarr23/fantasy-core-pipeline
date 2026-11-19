from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.launcher.catalog import UnknownJobError
from src.launcher.runs import JobRun
from src.services import draft_run_service as service
from src.services import jobs_service
from tests.test_launcher_k8s import CATALOG


def _run(status="running", end_reason=None, job="draft-listener"):
    return JobRun("6f1c2a9e-1b2c-4d5e-8f90-123456789abc", job, status, {"league_id": "L1"}, datetime(2026, 8, 20), end_reason)


def _k8s_job(active=0, succeeded=0, failed=0, minute=0):
    return SimpleNamespace(
        metadata=SimpleNamespace(creation_timestamp=datetime(2026, 8, 20, 12, minute)),
        status=SimpleNamespace(active=active, succeeded=succeeded, failed=failed),
    )


@pytest.fixture
def publish():
    with patch("src.services.draft_run_service.jobs_service._catalog", return_value=CATALOG), \
         patch("src.services.draft_run_service.publish_run", return_value={"run_id": "6f1c2a9e-1b2c-4d5e-8f90-123456789abc", "status": "queued"}) as mock:
        yield mock


def test_start_listener_requests_a_manual_run_of_the_modes_listener(publish):
    started = service.start_listener(
        "practice", league_id=42, team_id=7, season=2026, week=1,
        pick_clock_seconds=30, auto_draft_delay_seconds=15,
    )

    catalog, job = publish.call_args.args
    assert catalog is CATALOG and job == "practice-draft-listener"
    assert publish.call_args.kwargs == {
        "trigger": "manual",
        "params": {
            "league_id": "42", "team_id": "7", "season": 2026, "week": 1, "mode": "practice",
            "pick_clock_seconds": 30, "auto_draft_delay_seconds": 15,
        },
        "requested_by": "draft-api",
    }
    # The fixture catalog accepts exactly these params.
    from src.launcher.catalog import validate_run_params

    validate_run_params(CATALOG.jobs[job], publish.call_args.kwargs["params"])
    # The launcher names the first attempt's k8s Job from the run id.
    assert started == {
        "run_id": "6f1c2a9e-1b2c-4d5e-8f90-123456789abc",
        "job_name": "practice-draft-listener-6f1c2a9e1b2c-a1",
    }


def test_connectors_pass_their_league_and_room(publish):
    service.start_live_connector("L1")
    assert publish.call_args.args[1:] == ("draft-connector",)
    assert publish.call_args.kwargs["params"] == {"league_id": "L1"}

    service.start_practice_connector("https://espn/room", None)
    assert publish.call_args.kwargs["params"] == {"room_url": "https://espn/room"}
    service.start_practice_connector("https://espn/room", "555")
    assert publish.call_args.kwargs["params"] == {"room_url": "https://espn/room", "league_id": "555"}


@pytest.mark.parametrize(
    "error",
    [jobs_service.CatalogUnavailableError("not mounted"), UnknownJobError("unknown job 'draft-listener'")],
)
def test_a_launcher_without_the_draft_jobs_is_unavailable(error):
    with patch("src.services.draft_run_service.jobs_service._catalog", side_effect=error):
        with pytest.raises(service.DraftLauncherUnavailableError):
            service.start_live_connector("L1")


@pytest.mark.parametrize(
    ("status", "end_reason", "absent"),
    [
        ("running", None, False),
        ("success", "completed", False),
        ("failure", "job_failed", False),
        ("timeout", "deadline", False),
        ("timeout", "unhealthy", True),
        ("cancelled", "cancelled", True),
        ("skipped", "overlap", True),
    ],
)
def test_a_stopped_listener_no_longer_describes_the_board(status, end_reason, absent):
    assert service.ended_without_outcome(_run(status, end_reason)) is absent
    assert service.ended_without_outcome(None) is True


@pytest.fixture
def batch_api():
    api = MagicMock()
    with patch("src.services.draft_run_service.batch_v1_api", return_value=api):
        yield api


def test_job_state_reads_the_newest_k8s_job_of_the_run(batch_api):
    batch_api.list_namespaced_job.return_value.items = [_k8s_job(failed=1, minute=1), _k8s_job(active=1, minute=2)]

    assert service.run_job_state(_run()) == "Active"
    kwargs = batch_api.list_namespaced_job.call_args.kwargs
    assert kwargs["label_selector"] == "fantasy.run-id in (6f1c2a9e-1b2c-4d5e-8f90-123456789abc)"
    assert kwargs["_request_timeout"] == service.K8S_REQUEST_TIMEOUT_SECONDS


@pytest.mark.parametrize(
    ("status", "state"),
    [("queued", "Active"), ("running", "Active"), ("success", "Succeeded"),
     ("failure", "Failed"), ("timeout", "Failed"), ("cancelled", "Unknown")],
)
def test_job_state_falls_back_to_the_row_without_a_k8s_job(batch_api, status, state):
    batch_api.list_namespaced_job.return_value.items = []
    assert service.run_job_state(_run(status)) == state


def test_job_state_falls_back_to_the_row_when_the_apiserver_fails(batch_api):
    batch_api.list_namespaced_job.side_effect = RuntimeError("apiserver down")
    assert service.run_job_state(_run("success")) == "Succeeded"
    assert service.run_job_state(None) == "Unknown"


def test_cancel_and_wait_cancels_each_run_then_waits_for_their_jobs(batch_api):
    store = MagicMock()
    store.cancel.side_effect = [True, "success"]
    batch_api.list_namespaced_job.return_value.items = [_k8s_job(active=1)]
    gone = MagicMock(items=[])
    batch_api.list_namespaced_job.side_effect = [
        MagicMock(items=[_k8s_job(active=1)]), gone,
    ]

    with patch("src.services.draft_run_service._store", return_value=store), \
         patch("src.services.draft_run_service.time.sleep") as sleep:
        assert service.cancel_and_wait(["a", "b"]) is True

    assert [c.args for c in store.cancel.call_args_list] == [("a",), ("b",)]
    assert batch_api.list_namespaced_job.call_args.kwargs["label_selector"] == "fantasy.run-id in (a,b)"
    sleep.assert_called_once_with(service.TERMINATION_POLL_INTERVAL_SECONDS)


def test_cancel_and_wait_reports_a_job_that_outlives_the_wait(batch_api):
    batch_api.list_namespaced_job.return_value.items = [_k8s_job(active=1)]
    with patch("src.services.draft_run_service._store", return_value=MagicMock()), \
         patch("src.services.draft_run_service.time.sleep"):
        assert service.cancel_and_wait(["a"]) is False
    assert batch_api.list_namespaced_job.call_count == service.TERMINATION_MAX_POLLS


def test_cancel_and_wait_with_nothing_to_stop_touches_nothing(batch_api):
    with patch("src.services.draft_run_service._store") as store:
        assert service.cancel_and_wait([]) is True
    store.assert_not_called()
    batch_api.list_namespaced_job.assert_not_called()


def test_pod_reads_are_scoped_to_the_run_and_bounded():
    core = MagicMock()
    core.list_namespaced_pod.return_value.items = ["pod"]
    with patch("src.services.draft_run_service.core_v1_api", return_value=core):
        assert service.run_pods("r1") == ["pod"]
        service.read_pod_log("pod-1", 50)
    assert core.list_namespaced_pod.call_args.kwargs["label_selector"] == "fantasy.run-id=r1"
    assert core.list_namespaced_pod.call_args.kwargs["_request_timeout"] == service.K8S_REQUEST_TIMEOUT_SECONDS
    assert core.read_namespaced_pod_log.call_args.kwargs["tail_lines"] == 50
    assert core.read_namespaced_pod_log.call_args.kwargs["_request_timeout"] == service.K8S_REQUEST_TIMEOUT_SECONDS


def test_lookups_query_the_draft_jobs_by_league():
    store = MagicMock()
    with patch("src.services.draft_run_service._store", return_value=store):
        service.latest_listener("L1")
        service.active_league_runs("L1")
        service.active_practice_listeners()
    assert store.latest_run.call_args.args == (service.LISTENER_JOB_NAMES, "league_id", "L1")
    assert [c.args for c in store.active_runs.call_args_list] == [
        (service.LISTENER_JOB_NAMES, "league_id", "L1"),
        (("draft-connector", "practice-draft-connector"), "league_id", "L1"),
        (("practice-draft-listener",),),
    ]


def test_catalog_draft_jobs_match_the_service():
    for job in [*service.LISTENER_JOB_NAMES, service.LIVE_CONNECTOR_JOB, service.PRACTICE_CONNECTOR_JOB]:
        assert CATALOG.resolve(job).job_class == "service"
        assert "league_id" in CATALOG.jobs[job].params
    assert CATALOG.jobs["practice-draft-listener"].health.key == "draft:{league_id}:health"
    assert CATALOG.jobs["draft-listener"].health is None
