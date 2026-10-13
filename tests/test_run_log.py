from unittest.mock import MagicMock, patch

import pytest

from src import trigger_client
from src.api import routes
from src.services import (
    activity_runner as runner,
    activity_service as activity,
    llm_history,
)
from src.services import run_log_service as run_log


@pytest.fixture
def recorded():
    """Captures run_log writes instead of hitting Postgres."""
    calls = {"start": [], "finish": [], "links": []}

    def start(job_name, trigger_type=None, trigger=None, parent_run_id=None):
        calls["start"].append((job_name, trigger_type, trigger, parent_run_id))
        return f"run-{len(calls['start'])}"

    with (
        patch.object(run_log, "start_run", side_effect=start),
        patch.object(
            run_log, "finish_run", side_effect=lambda *a: calls["finish"].append(a)
        ),
        patch.object(
            run_log, "_insert_links", side_effect=lambda *a: calls["links"].append(a)
        ),
    ):
        yield calls


@pytest.mark.parametrize(
    "job,expected",
    [
        ("fantasy-rumor-syncer-29865000", "schedule"),
        ("fantasy-rumor-syncer-manual-202610131500", "manual"),
        ("something-else-1", "manual"),
    ],
)
def test_trigger_type_comes_from_the_job_name(monkeypatch, job, expected):
    monkeypatch.setenv("RUN_CRONJOB", "fantasy-rumor-syncer")
    monkeypatch.setenv("RUN_K8S_JOB", job)
    assert run_log.trigger_from_env() == (
        expected,
        {"cronjob": "fantasy-rumor-syncer", "k8s_job": job},
    )


def test_no_cronjob_env_is_manual(monkeypatch):
    monkeypatch.delenv("RUN_CRONJOB", raising=False)
    monkeypatch.delenv("RUN_K8S_JOB", raising=False)
    monkeypatch.setenv("HOSTNAME", "laptop")
    assert run_log.trigger_from_env() == ("manual", {"host": "laptop"})


def test_success_records_summary_and_links(recorded):
    with run_log.run_scope("sync_rumors", "manual") as run:
        run.summary["fetched"] = 3
        run_log.link("raw_signal", [1, 2], "output")
    assert recorded["links"] == [("run-1", "raw_signal", [1, 2], "output")]
    assert recorded["finish"] == [("run-1", "success", None, {"fetched": 3})]
    assert run_log.current_run_id() is None


def test_exception_marks_failure_and_propagates(recorded):
    with pytest.raises(ValueError):
        with run_log.run_scope("sync_rumors", "manual"):
            raise ValueError("feed down")
    assert recorded["finish"][0][:3] == ("run-1", "failure", "ValueError: feed down")


@pytest.mark.parametrize(
    "code,status", [(0, "success"), (None, "success"), (1, "failure")]
)
def test_run_main_maps_exit_codes(recorded, code, status):
    if code:
        with pytest.raises(SystemExit):
            run_log.run_main("lineup_optimizer", lambda: code)
    else:
        assert run_log.run_main("lineup_optimizer", lambda: code) == code
    assert recorded["finish"][0][1] == status


def test_nested_scope_is_a_child_and_ensure_run_joins(recorded):
    with run_log.run_scope("outer", "manual") as outer:
        with run_log.ensure_run("rumor_action_trigger", "http") as joined:
            assert joined is outer
        with run_log.run_scope("inner", "manual"):
            pass
    assert recorded["start"][1] == ("inner", "manual", None, "run-1")


def test_link_outside_a_run_is_a_noop():
    with patch.object(run_log, "get_db_cursor") as db:
        run_log.link("raw_signal", [1], "output")
    db.assert_not_called()


def test_recording_failure_never_fails_the_job():
    with patch.object(run_log, "get_db_cursor", side_effect=RuntimeError("db down")):
        with run_log.run_scope("sync_rumors", "manual") as run:
            run.link("raw_signal", [1], "output")
        assert run.id is None


def test_trigger_client_forwards_the_run_id(recorded):
    response = MagicMock(status_code=202)
    with patch.object(trigger_client.requests, "post", return_value=response) as post:
        with run_log.run_scope("lineup_optimizer", "manual"):
            trigger_client.post_trigger("lineup", {})
        trigger_client.post_trigger("lineup", {})
    assert post.call_args_list[0].kwargs["headers"][run_log.RUN_ID_HEADER] == "run-1"
    assert run_log.RUN_ID_HEADER not in post.call_args_list[1].kwargs["headers"]


def test_accepted_action_is_linked_to_the_calling_run():
    payload = routes.WaiverTriggerPayload(
        team_id="8", player_id="1", name="Player", team="KC"
    )
    with (
        patch.object(
            activity, "create_action", side_effect=lambda q, b, proposed, **k: proposed
        ),
        patch.object(runner, "publish_one"),
        patch.object(routes, "_record_bot_decision"),
        patch.object(run_log, "_insert_links") as links,
    ):
        result = routes._accept_durable("waiver_actions", payload, run_id="caller")
    links.assert_called_once_with(
        "caller", "activity_action", [result["tracking_id"]], "output"
    )


def test_duplicate_action_is_not_relinked():
    payload = routes.WaiverTriggerPayload(
        team_id="8", player_id="1", name="Player", team="KC"
    )
    with (
        patch.object(activity, "create_action", return_value="original"),
        patch.object(runner, "publish_one"),
        patch.object(run_log, "_insert_links") as links,
    ):
        routes._accept_durable("waiver_actions", payload, run_id="caller")
    links.assert_not_called()


def test_llm_session_is_linked_to_the_active_run(recorded):
    result = MagicMock(text="[]", provider="groq", model="m", latency_ms=1)
    with (
        patch.object(llm_history, "session_start", return_value="session-1"),
        patch.object(llm_history, "session_event"),
    ):
        with run_log.run_scope("rumor_digest", "manual"):
            llm_history.record_call(lambda prompt, **kw: result)(
                "hi", call_site="rumor_digest"
            )
    assert ("run-1", "llm_session", ["session-1"], "output") in recorded["links"]
