"""The draft routes that start and stop launcher runs: /start, /stop, /practice/sync-job and
/logs. draft_run_service is replaced by an in-memory fake of run_log; its own SQL and k8s
reads are tested in test_draft_run_service.py and test_launcher_runs_postgres.py."""

from contextlib import contextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.launcher.runs import JobRun
from src.services import draft_run_service as service

LIVE = "draft-listener"
PRACTICE = "practice-draft-listener"
ROOM_URL = "https://fantasy.espn.com/football/draft?leagueId=555&draftRoomId=room-9"


class FakeRuns:
    """run_log rows for the draft jobs, answering the service calls draft.py makes."""

    def __init__(self):
        self.rows = []
        self.started = []
        self.cancelled = []
        # cancel_and_wait results in call order; True once exhausted.
        self.stop_results = []
        self.fail_lookups = False
        self.pods = {}
        self._clock = datetime(2026, 8, 20, 12, 0)

    def add(self, job, status="running", **params):
        self._clock += timedelta(minutes=1)
        run = JobRun(f"run-{len(self.rows) + 1}", job, status, params, self._clock)
        self.rows.append(run)
        return run

    def _active(self, jobs, **match):
        self._check()
        return sorted(
            (
                r for r in self.rows
                if r.job_name in jobs
                and r.status in service.ACTIVE_STATUSES
                and all(r.params.get(k) == v for k, v in match.items())
            ),
            key=lambda r: r.started_at,
            reverse=True,
        )

    def _latest(self, jobs, **match):
        self._check()
        runs = [
            r for r in self.rows
            if r.job_name in jobs and all(r.params.get(k) == v for k, v in match.items())
        ]
        return max(runs, key=lambda r: r.started_at) if runs else None

    def _check(self):
        if self.fail_lookups:
            raise RuntimeError("run_log unavailable")

    def _start(self, job, params):
        self.started.append((job, params))
        run = self.add(job, status="queued", **params)
        return {"run_id": run.run_id, "job_name": f"{job}-{run.run_id}-a1"}

    # ---- service API ----

    def active_listeners(self):
        return self._active(service.LISTENER_JOB_NAMES)

    def latest_listener(self, league_id):
        return self._latest(service.LISTENER_JOB_NAMES, league_id=league_id)

    def active_league_runs(self, league_id):
        connectors = (service.LIVE_CONNECTOR_JOB, service.PRACTICE_CONNECTOR_JOB)
        return (
            self._active(service.LISTENER_JOB_NAMES, league_id=league_id),
            self._active(connectors, league_id=league_id),
        )

    def active_live_connectors(self, league_id):
        return self._active((service.LIVE_CONNECTOR_JOB,), league_id=league_id)

    def active_practice_connectors(self):
        return self._active((service.PRACTICE_CONNECTOR_JOB,))

    def active_practice_listeners(self):
        return self._active((PRACTICE,))

    def latest_practice_connector(self):
        return self._latest((service.PRACTICE_CONNECTOR_JOB,))

    def start_listener(self, mode, **params):
        return self._start(service.LISTENER_JOBS[mode], {**params, "mode": mode})

    def start_live_connector(self, league_id):
        return self._start(service.LIVE_CONNECTOR_JOB, {"league_id": league_id})

    def start_practice_connector(self, room_url, league_id):
        params = {"room_url": room_url}
        if league_id is not None:
            params["league_id"] = league_id
        return self._start(service.PRACTICE_CONNECTOR_JOB, params)

    def cancel_and_wait(self, run_ids):
        run_ids = list(run_ids)
        self.cancelled.append(run_ids)
        stopped = self.stop_results.pop(0) if self.stop_results else True
        if isinstance(stopped, Exception):
            raise stopped
        self.rows = [
            JobRun(r.run_id, r.job_name, "cancelled", r.params, r.started_at, "cancelled")
            if r.run_id in run_ids else r
            for r in self.rows
        ]
        return stopped

    def run_job_state(self, run):
        return service._ROW_JOB_STATES.get(run.status, "Unknown") if run else "Unknown"

    def run_pods(self, run_id):
        return self.pods.get(run_id, [])

    def read_pod_log(self, pod_name, tail):
        return "2026-08-20T12:00:00Z line one\n2026-08-20T12:00:01Z espn_s2=secret-cookie"


SERVICE_CALLS = [
    "active_listeners", "latest_listener", "active_league_runs", "active_live_connectors",
    "active_practice_connectors", "active_practice_listeners", "latest_practice_connector",
    "start_listener", "start_live_connector", "start_practice_connector", "cancel_and_wait",
    "run_job_state", "run_pods", "read_pod_log",
]


@pytest.fixture
def runs():
    fake = FakeRuns()
    with patch.multiple(
        "src.api.draft.draft_run_service", **{name: getattr(fake, name) for name in SERVICE_CALLS}
    ):
        yield fake


@pytest.fixture
def cleared():
    """Calls to _clear_league_draft_state, as (league_id, include_pick_stream, raise_on_error)."""
    calls = []

    def clear(league_id, include_pick_stream=False, raise_on_error=False):
        calls.append((league_id, include_pick_stream, raise_on_error))
        return 3

    with patch("src.api.draft._clear_league_draft_state", side_effect=clear) as mock:
        mock.calls = calls
        yield calls


@pytest.fixture
def lock_redis():
    """A Redis for the practice start lock that always grants it."""
    r_client = MagicMock()
    r_client.set.return_value = True
    r_client.eval.return_value = 1
    with patch("src.api.draft._redis_client", return_value=r_client):
        yield r_client


@contextmanager
def _config(row):
    cursor = MagicMock()
    cursor.fetchone.return_value = row

    @contextmanager
    def fake_cursor():
        yield cursor

    with patch("src.api.draft.get_db_cursor", fake_cursor):
        yield cursor


def _start(client, row=("L1", "7", 2026, "live"), **params):
    with _config(row) as cursor:
        response = client.post("/v1/draft/start", params=params)
    return response, cursor


def _listener_params(league_id="L1", mode="live", team_id="7"):
    return {"league_id": league_id, "team_id": team_id, "season": 2026, "mode": mode}


# ---------------------------------------------------------------------------
# POST /v1/draft/start
# ---------------------------------------------------------------------------
def test_start_requests_the_live_listener_with_the_live_clock(client, runs, cleared):
    response, _ = _start(client)

    assert response.status_code == 202
    body = response.json()
    assert body["already_running"] is False
    assert body["run_id"] == "run-1"
    assert body["job_name"] == "draft-listener-run-1-a1"
    assert body["publisher_run_id"] is None
    assert runs.started == [(LIVE, {
        "league_id": "L1", "team_id": "7", "season": 2026, "week": 1, "mode": "live",
        "pick_clock_seconds": 90, "auto_draft_delay_seconds": 45,
    })]
    # The previous run's board is cleared before the run is requested, never its pick stream.
    assert cleared == [("L1", False, False)]


def test_start_requests_the_practice_listener_with_the_practice_clock(client, runs, cleared):
    response, _ = _start(client, row=("MOCK1", "3", 2026, "practice"))

    assert response.status_code == 202
    ((job, params),) = runs.started
    assert job == PRACTICE
    assert (params["mode"], params["pick_clock_seconds"], params["auto_draft_delay_seconds"]) == ("practice", 30, 15)


def test_start_reads_the_named_config_or_else_the_newest(client, runs, cleared):
    _, cursor = _start(client, league_id="L1")
    query, args = cursor.execute.call_args.args
    assert "WHERE league_id = %s" in query and args == ("L1",)

    _, cursor = _start(client, row=("L2", "7", 2026, "live"))
    assert "ORDER BY updated_at DESC LIMIT 1" in cursor.execute.call_args.args[0]


def test_start_requires_a_stored_config(client, runs, cleared):
    response, _ = _start(client, row=None)
    assert response.status_code == 400
    assert runs.started == []


def test_start_reports_an_active_listener_for_the_league_instead_of_a_second_one(client, runs, cleared):
    existing = runs.add(LIVE, **_listener_params())

    response, _ = _start(client)

    assert response.status_code == 202
    assert response.json()["already_running"] is True
    assert response.json()["run_id"] == existing.run_id
    assert runs.started == []
    # The running listener's board is left alone.
    assert cleared == []


def test_start_restarts_a_league_whose_listener_finished(client, runs, cleared):
    runs.add(LIVE, status="success", **_listener_params())

    response, _ = _start(client)

    assert response.json()["already_running"] is False
    assert len(runs.started) == 1


def test_start_refuses_while_another_league_has_a_listener(client, runs, cleared):
    runs.add(LIVE, **_listener_params("OTHER"))

    response, _ = _start(client)

    assert response.status_code == 409
    assert "OTHER" in response.json()["detail"]
    assert runs.started == [] and runs.cancelled == []


def test_practice_start_reclaims_a_stale_practice_listener(client, runs, cleared):
    stale = runs.add(PRACTICE, **_listener_params("OLDMOCK", "practice"))

    response, _ = _start(client, row=("MOCK1", "3", 2026, "practice"))

    assert response.status_code == 202
    assert runs.cancelled == [[stale.run_id]]
    assert cleared == [("OLDMOCK", False, False), ("MOCK1", False, False)]
    assert [job for job, _ in runs.started] == [PRACTICE]


def test_practice_start_never_reclaims_a_live_listener(client, runs, cleared):
    runs.add(PRACTICE, **_listener_params("OLDMOCK", "practice"))
    runs.add(LIVE, **_listener_params("L1"))

    response, _ = _start(client, row=("MOCK1", "3", 2026, "practice"))

    assert response.status_code == 409
    assert "L1" in response.json()["detail"]
    assert runs.cancelled == [] and runs.started == []


def test_practice_start_aborts_when_the_stale_listener_will_not_stop(client, runs, cleared):
    runs.add(PRACTICE, **_listener_params("OLDMOCK", "practice"))
    runs.stop_results = [False]

    response, _ = _start(client, row=("MOCK1", "3", 2026, "practice"))

    assert response.status_code == 500
    assert "two listeners" in response.json()["detail"]
    assert runs.started == []


@pytest.mark.parametrize("unreadable", ["latest_listener", "active_listeners"])
def test_start_refuses_when_listener_runs_cannot_be_read(client, runs, cleared, unreadable):
    with patch(f"src.api.draft.draft_run_service.{unreadable}", side_effect=RuntimeError("db down")):
        response, _ = _start(client)

    assert response.status_code == 500
    assert runs.started == []


def test_start_is_503_when_the_launcher_cannot_take_the_run(client, runs, cleared):
    with patch(
        "src.api.draft.draft_run_service.start_listener",
        side_effect=service.DraftLauncherUnavailableError("draft-listener: unknown job"),
    ):
        response, _ = _start(client)

    assert response.status_code == 503
    assert "unknown job" in response.json()["detail"]


def test_start_with_publisher_requests_the_live_connector(client, runs, cleared):
    with patch("src.api.draft.ESPN_LEAGUE_ID", "L1"):
        response, _ = _start(client, with_publisher=True)

    assert response.status_code == 202
    body = response.json()
    assert body["publisher_run_id"] == "run-2"
    assert body["publisher_job_name"] == "draft-connector-run-2-a1"
    assert runs.started[1] == (service.LIVE_CONNECTOR_JOB, {"league_id": "L1"})


def test_start_with_publisher_is_live_only(client, runs, cleared):
    response, _ = _start(client, row=("MOCK1", "3", 2026, "practice"), with_publisher=True)
    assert response.status_code == 400
    assert "/v1/draft/practice/sync-job" in response.json()["detail"]
    assert runs.started == []


def test_start_with_publisher_only_serves_espn_league_id(client, runs, cleared):
    with patch("src.api.draft.ESPN_LEAGUE_ID", "REAL"):
        response, _ = _start(client, with_publisher=True)
    assert response.status_code == 400
    assert "REAL" in response.json()["detail"]
    assert runs.started == []


def test_start_with_publisher_skips_a_connector_already_active(client, runs, cleared):
    runs.add(service.LIVE_CONNECTOR_JOB, league_id="L1")

    with patch("src.api.draft.ESPN_LEAGUE_ID", "L1"):
        response, _ = _start(client, with_publisher=True)

    assert response.status_code == 202
    assert response.json()["publisher_run_id"] is None
    assert [job for job, _ in runs.started] == [LIVE]


def test_start_with_publisher_surfaces_a_failed_connector_request(client, runs, cleared):
    with patch("src.api.draft.ESPN_LEAGUE_ID", "L1"), \
         patch("src.api.draft.draft_run_service.start_live_connector", side_effect=RuntimeError("broker down")):
        response, _ = _start(client, with_publisher=True)

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert "run-1" in detail and "broker down" in detail


# ---------------------------------------------------------------------------
# POST /v1/draft/stop
# ---------------------------------------------------------------------------
def test_stop_cancels_the_listener_then_the_connector_and_clears_the_pick_stream(client, runs, cleared):
    listener = runs.add(LIVE, **_listener_params())
    connector = runs.add(service.LIVE_CONNECTOR_JOB, league_id="L1")
    runs.add(LIVE, **_listener_params("OTHER"))

    response = client.post("/v1/draft/stop", params={"league_id": "L1"})

    assert response.status_code == 200
    assert response.json() == {
        "status": "stopped",
        "league_id": "L1",
        "job_deleted": True,
        "publisher_job_deleted": True,
        "cancelled_run_ids": [listener.run_id, connector.run_id],
        "redis_keys_cleared": 3,
        "pick_stream_cleared": True,
    }
    assert runs.cancelled == [[listener.run_id], [connector.run_id]]
    assert cleared == [("L1", True, True)]


def test_stop_without_a_connector_keeps_the_pick_stream_unless_confirmed(client, runs, cleared):
    runs.add(LIVE, **_listener_params())

    response = client.post("/v1/draft/stop", params={"league_id": "L1"})
    assert response.json()["pick_stream_cleared"] is False
    assert cleared == [("L1", False, True)]

    response = client.post("/v1/draft/stop", params={"league_id": "L1", "confirm_publisher_stopped": True})
    assert response.json()["pick_stream_cleared"] is True
    assert response.json()["job_deleted"] is False
    assert cleared[-1] == ("L1", True, True)


def test_stop_with_nothing_running_still_clears_the_board(client, runs, cleared):
    response = client.post("/v1/draft/stop", params={"league_id": "L1"})
    assert response.status_code == 200
    assert runs.cancelled == []
    assert cleared == [("L1", False, True)]


def test_stop_is_504_and_leaves_redis_when_the_listener_will_not_stop(client, runs, cleared):
    runs.add(LIVE, **_listener_params())
    runs.add(service.LIVE_CONNECTOR_JOB, league_id="L1")
    runs.stop_results = [False]

    response = client.post("/v1/draft/stop", params={"league_id": "L1", "confirm_publisher_stopped": True})

    assert response.status_code == 504
    assert "NOT cleared" in response.json()["detail"]
    assert len(runs.cancelled) == 1
    assert cleared == []


def test_stop_is_504_when_the_connector_will_not_stop_even_if_confirmed(client, runs, cleared):
    runs.add(LIVE, **_listener_params())
    runs.add(service.LIVE_CONNECTOR_JOB, league_id="L1")
    runs.stop_results = [True, False]

    response = client.post("/v1/draft/stop", params={"league_id": "L1", "confirm_publisher_stopped": True})

    assert response.status_code == 504
    assert "connector" in response.json()["detail"]
    assert cleared == []


def test_stop_is_500_when_runs_cannot_be_read(client, runs, cleared):
    runs.fail_lookups = True
    response = client.post("/v1/draft/stop", params={"league_id": "L1"})
    assert response.status_code == 500
    assert cleared == []


def test_stop_is_500_when_redis_cannot_be_cleared(client, runs):
    with patch("src.api.draft._clear_league_draft_state", side_effect=RuntimeError("redis down")):
        response = client.post("/v1/draft/stop", params={"league_id": "L1"})
    assert response.status_code == 500
    assert "redis down" in response.json()["detail"]


# ---------------------------------------------------------------------------
# POST/GET/DELETE /v1/draft/practice/sync-job
# ---------------------------------------------------------------------------
def test_practice_start_requests_the_connector_and_clears_the_room_league(client, runs, cleared, lock_redis):
    response = client.post("/v1/draft/practice/sync-job", json={"room_url": ROOM_URL})

    assert response.status_code == 202
    assert response.json() == {"status": "accepted", "run_id": "run-1", "job_name": "practice-draft-connector-run-1-a1"}
    assert runs.started == [(service.PRACTICE_CONNECTOR_JOB, {"room_url": ROOM_URL, "league_id": "555"})]
    # No connector runs yet, so the previous run's ledger and Stream must go first, fail-closed.
    assert cleared == [("555", True, True)]
    lock_redis.eval.assert_called()  # released


def test_practice_start_without_a_league_in_the_url_clears_nothing(client, runs, cleared, lock_redis):
    url = "https://fantasy.espn.com/football/draft#room-xyz"
    response = client.post("/v1/draft/practice/sync-job", json={"room_url": url})

    assert response.status_code == 202
    assert runs.started == [(service.PRACTICE_CONNECTOR_JOB, {"room_url": url})]
    assert cleared == []


def test_practice_start_keeps_the_pick_stream_a_listener_still_consumes(client, runs, cleared, lock_redis):
    runs.add(PRACTICE, **_listener_params("555", "practice"))

    response = client.post("/v1/draft/practice/sync-job", json={"room_url": ROOM_URL})

    assert response.status_code == 202
    assert cleared == []


def test_practice_start_refuses_while_a_connector_is_active(client, runs, cleared, lock_redis):
    runs.add(service.PRACTICE_CONNECTOR_JOB, room_url=ROOM_URL, league_id="555")

    response = client.post("/v1/draft/practice/sync-job", json={"room_url": ROOM_URL})

    assert response.status_code == 409
    assert "force=true" in response.json()["detail"]
    assert runs.started == []


def test_practice_start_fails_closed_when_the_room_league_cannot_be_cleared(client, runs, lock_redis):
    with patch("src.api.draft._clear_league_draft_state", side_effect=RuntimeError("redis down")):
        response = client.post("/v1/draft/practice/sync-job", json={"room_url": ROOM_URL})

    assert response.status_code == 500
    assert "pick ledger" in response.json()["detail"]
    assert runs.started == []


def test_practice_start_is_503_when_the_launcher_cannot_take_the_run(client, runs, cleared, lock_redis):
    with patch(
        "src.api.draft.draft_run_service.start_practice_connector",
        side_effect=service.DraftLauncherUnavailableError("no catalog"),
    ):
        response = client.post("/v1/draft/practice/sync-job", json={"room_url": ROOM_URL})
    assert response.status_code == 503


def test_practice_force_start_retires_the_old_run_first(client, runs, cleared, lock_redis):
    old_connector = runs.add(service.PRACTICE_CONNECTOR_JOB, room_url=ROOM_URL, league_id="444")
    old_listener = runs.add(PRACTICE, **_listener_params("444", "practice"))

    response = client.post("/v1/draft/practice/sync-job", params={"force": True}, json={"room_url": ROOM_URL})

    assert response.status_code == 202
    assert runs.cancelled == [[old_connector.run_id], [old_listener.run_id]]
    # The retired league's state goes strictly (the new connector must not inherit it).
    assert ("444", True, True) in cleared
    assert runs.started[-1][0] == service.PRACTICE_CONNECTOR_JOB


def test_practice_force_start_refuses_while_the_old_connector_is_still_up(client, runs, cleared, lock_redis):
    runs.add(service.PRACTICE_CONNECTOR_JOB, room_url=ROOM_URL, league_id="444")
    runs.stop_results = [False]

    response = client.post("/v1/draft/practice/sync-job", params={"force": True}, json={"room_url": ROOM_URL})

    assert response.status_code == 409
    assert "has not stopped yet" in response.json()["detail"]
    assert runs.started == []


def test_practice_force_start_surfaces_a_failed_teardown(client, runs, cleared, lock_redis):
    runs.add(service.PRACTICE_CONNECTOR_JOB, room_url=ROOM_URL, league_id="444")
    runs.stop_results = [RuntimeError("db down")]

    response = client.post("/v1/draft/practice/sync-job", params={"force": True}, json={"room_url": ROOM_URL})

    assert response.status_code == 500
    assert runs.started == []


def test_practice_force_start_refuses_when_the_old_run_state_cannot_be_cleared(client, runs, lock_redis):
    runs.add(service.PRACTICE_CONNECTOR_JOB, room_url=ROOM_URL, league_id="444")

    def clear(league_id, include_pick_stream=False, raise_on_error=False):
        if raise_on_error:
            raise RuntimeError("redis down")
        return 0

    with patch("src.api.draft._clear_league_draft_state", side_effect=clear):
        response = client.post("/v1/draft/practice/sync-job", params={"force": True}, json={"room_url": ROOM_URL})

    assert response.status_code == 500
    assert "seed from the previous run" in response.json()["detail"]
    assert runs.started == []


def test_practice_start_refuses_a_concurrent_start(client, runs, cleared, lock_redis):
    lock_redis.set.return_value = False

    response = client.post("/v1/draft/practice/sync-job", json={"room_url": ROOM_URL})

    assert response.status_code == 409
    assert "already in progress" in response.json()["detail"]
    assert runs.started == []


def test_practice_start_proceeds_when_the_lock_is_unavailable(client, runs, cleared, lock_redis):
    lock_redis.set.side_effect = ConnectionError("redis down")

    response = client.post("/v1/draft/practice/sync-job", json={"room_url": ROOM_URL})

    assert response.status_code == 202


def test_practice_start_releases_the_lock_on_failure(client, runs, cleared, lock_redis):
    with patch("src.api.draft.draft_run_service.start_practice_connector", side_effect=RuntimeError("boom")):
        response = client.post("/v1/draft/practice/sync-job", json={"room_url": ROOM_URL})

    assert response.status_code == 500
    release = lock_redis.eval.call_args_list[-1].args
    assert "del" in release[0] and release[2] == "draft:practice:start-lock"


def test_practice_start_lock_ttl_covers_two_full_termination_waits():
    from src.api.draft import K8S_REQUEST_TIMEOUT_SECONDS, PRACTICE_START_LOCK_TTL_SECONDS

    one_wait = service.TERMINATION_MAX_POLLS * (
        K8S_REQUEST_TIMEOUT_SECONDS + service.TERMINATION_POLL_INTERVAL_SECONDS
    )
    assert PRACTICE_START_LOCK_TTL_SECONDS >= 2 * one_wait


def test_practice_status_reports_the_newest_connector_and_its_room(client, runs):
    runs.add(service.PRACTICE_CONNECTOR_JOB, status="cancelled", room_url="https://x", league_id="1")
    current = runs.add(service.PRACTICE_CONNECTOR_JOB, room_url=ROOM_URL, league_id="555")
    r_client = MagicMock()
    r_client.exists.return_value = 1

    with patch("src.api.draft._redis_client", return_value=r_client):
        response = client.get("/v1/draft/practice/sync-job/status")

    assert response.json() == {
        "job_state": "Active",
        "pod_active": True,
        "room_connected": True,
        "room_handshake_complete": True,
        "active_league_id": "555",
        "room_identifier": "room-9",
        "run_id": current.run_id,
    }
    r_client.exists.assert_called_once_with("draft:555:connected")


def test_practice_status_room_not_connected_and_fragment_identifier(client, runs):
    runs.add(service.PRACTICE_CONNECTOR_JOB, room_url="https://fantasy.espn.com/football/draft#room-xyz")
    with patch("src.api.draft._redis_client") as r_client:
        body = client.get("/v1/draft/practice/sync-job/status").json()
    assert (body["room_connected"], body["room_identifier"], body["active_league_id"]) == (False, "room-xyz", None)
    r_client.assert_not_called()  # no league to check


def test_practice_status_of_an_ended_run_skips_redis(client, runs):
    runs.add(service.PRACTICE_CONNECTOR_JOB, status="cancelled", room_url=ROOM_URL, league_id="555")
    with patch("src.api.draft._redis_client") as r_client:
        body = client.get("/v1/draft/practice/sync-job/status").json()
    assert (body["job_state"], body["pod_active"], body["room_connected"]) == ("Unknown", False, False)
    r_client.assert_not_called()


def test_practice_status_with_no_run_and_unreadable_run_log(client, runs):
    assert client.get("/v1/draft/practice/sync-job/status").json()["job_state"] == "Unknown"
    runs.fail_lookups = True
    assert client.get("/v1/draft/practice/sync-job/status").json()["run_id"] is None


def test_end_practice_run_cancels_every_connector_and_listener_and_clears_their_leagues(client, runs, cleared):
    older = runs.add(service.PRACTICE_CONNECTOR_JOB, room_url=ROOM_URL, league_id="444")
    newest = runs.add(service.PRACTICE_CONNECTOR_JOB, room_url=ROOM_URL, league_id="555")
    listener = runs.add(PRACTICE, **_listener_params("555", "practice"))
    live = runs.add(LIVE, **_listener_params("L1"))

    response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 200
    assert response.json() == {
        "status": "deleted",
        "job_name": f"practice-draft-connector-{newest.run_id.replace('-', '')[:12]}-a1",
        "connector_run_ids": [newest.run_id, older.run_id],
        "listener_runs_cancelled": [listener.run_id],
        "connector_gone": True,
    }
    assert runs.cancelled == [[newest.run_id, older.run_id], [listener.run_id]]
    assert live.run_id not in sum(runs.cancelled, [])
    assert sorted(cleared) == [("444", True, False), ("555", True, False), ("555", True, False)]


def test_end_practice_run_keeps_the_pick_stream_when_the_connector_will_not_stop(client, runs, cleared):
    runs.add(service.PRACTICE_CONNECTOR_JOB, room_url=ROOM_URL, league_id="555")
    listener = runs.add(PRACTICE, **_listener_params("555", "practice"))
    runs.stop_results = [False]

    response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 200
    assert response.json()["connector_gone"] is False
    # The listener is still retired, but only its board is cleared.
    assert runs.cancelled[-1] == [listener.run_id]
    assert cleared == [("555", False, False)]


def test_end_practice_run_retires_a_listener_left_without_a_connector(client, runs, cleared):
    listener = runs.add(PRACTICE, **_listener_params("555", "practice"))

    response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 200
    assert response.json()["job_name"] is None
    assert response.json()["listener_runs_cancelled"] == [listener.run_id]
    assert cleared == [("555", True, False)]


def test_end_practice_run_is_404_when_nothing_runs(client, runs, cleared):
    assert client.delete("/v1/draft/practice/sync-job").status_code == 404


def test_end_practice_run_is_500_when_the_listener_will_not_stop(client, runs, cleared):
    runs.add(service.PRACTICE_CONNECTOR_JOB, room_url=ROOM_URL, league_id="555")
    runs.add(PRACTICE, **_listener_params("555", "practice"))
    runs.stop_results = [True, False]

    response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 500
    assert "could not be" in response.json()["detail"]


def test_end_practice_run_is_500_when_runs_cannot_be_read(client, runs, cleared):
    runs.fail_lookups = True
    assert client.delete("/v1/draft/practice/sync-job").status_code == 500


# ---------------------------------------------------------------------------
# GET /v1/draft/logs (#68)
# ---------------------------------------------------------------------------
def _pod(name, minute, exit_code=None, reason=None):
    terminated = SimpleNamespace(exit_code=exit_code, reason=reason) if exit_code is not None else None
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, creation_timestamp=datetime(2026, 8, 20, 12, minute)),
        status=SimpleNamespace(
            phase="Failed" if terminated else "Running",
            container_statuses=[SimpleNamespace(state=SimpleNamespace(terminated=terminated))],
        ),
    )


def test_listener_logs_read_the_newest_pod_of_the_leagues_newest_run(client, runs):
    runs.add(LIVE, status="failure", **_listener_params())
    current = runs.add(LIVE, status="failure", **_listener_params())
    runs.pods[current.run_id] = [_pod("old", 1), _pod("new", 2, exit_code=1, reason="Error")]

    response = client.get("/v1/draft/logs", params={"league_id": "L1", "job": "listener"})

    assert response.status_code == 200
    body = response.json()
    assert (body["pod_name"], body["pod_phase"], body["container_exit_code"], body["container_reason"]) == (
        "new", "Failed", 1, "Error",
    )
    assert body["lines"][0].endswith("line one")
    assert "secret-cookie" not in body["lines"][1]


def test_room_sync_logs_use_the_newest_connector_whatever_the_league(client, runs):
    connector = runs.add(service.PRACTICE_CONNECTOR_JOB, room_url=ROOM_URL)
    runs.pods[connector.run_id] = [_pod("room", 1)]

    response = client.get("/v1/draft/logs", params={"league_id": "anything", "job": "room-sync"})

    assert response.json()["pod_name"] == "room"


def test_logs_are_404_without_a_run_or_a_pod(client, runs):
    assert client.get("/v1/draft/logs", params={"league_id": "L1", "job": "listener"}).status_code == 404
    runs.add(LIVE, **_listener_params())
    assert client.get("/v1/draft/logs", params={"league_id": "L1", "job": "listener"}).status_code == 404


def test_logs_are_500_when_pods_cannot_be_listed(client, runs):
    runs.add(LIVE, **_listener_params())
    with patch("src.api.draft.draft_run_service.run_pods", side_effect=RuntimeError("apiserver down")):
        response = client.get("/v1/draft/logs", params={"league_id": "L1", "job": "listener"})
    assert response.status_code == 500


def test_logs_tail_is_clamped_to_the_max(client, runs):
    run = runs.add(LIVE, **_listener_params())
    runs.pods[run.run_id] = [_pod("p", 1)]
    with patch("src.api.draft.draft_run_service.read_pod_log", return_value="x") as read:
        client.get("/v1/draft/logs", params={"league_id": "L1", "job": "listener", "tail": 99999})
    assert read.call_args.args == ("p", 1000)
