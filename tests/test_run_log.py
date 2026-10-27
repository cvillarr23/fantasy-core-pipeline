import logging
import os
import subprocess
import sys
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

    def finish(*args, **kwargs):
        calls["finish"].append(args + (kwargs.get("expected_attempt"),))

    with (
        patch.object(run_log, "start_run", side_effect=start),
        patch.object(run_log, "finish_run", side_effect=finish),
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


@pytest.mark.parametrize("trigger", ["chain", "event", "cli", "mcp"])
def test_explicit_trigger_env_takes_precedence(monkeypatch, trigger):
    monkeypatch.setenv("RUN_TRIGGER", trigger)
    monkeypatch.setenv("RUN_CRONJOB", "fantasy-rumor-syncer")
    monkeypatch.setenv("RUN_K8S_JOB", "fantasy-rumor-syncer-29865000")
    assert run_log.trigger_from_env()[0] == trigger


def test_outer_scope_adopts_launcher_run_id_once(recorded, monkeypatch):
    monkeypatch.setenv("RUN_ID", "launcher-run")
    monkeypatch.setenv("RUN_ATTEMPT", "2")
    monkeypatch.setenv("RUN_TRIGGER", "schedule")
    monkeypatch.delenv("RUN_PARENT_ID", raising=False)
    adopted = []
    with patch.object(
        run_log, "adopt_run", side_effect=lambda *args: adopted.append(args)
    ):
        with run_log.run_scope("sync_rumors") as outer:
            assert outer.id == "launcher-run"
            assert os.getenv("RUN_ID") is None
            assert os.getenv("RUN_PARENT_ID") == "launcher-run"
            with run_log.run_scope("child") as child:
                assert child.id == "run-1"
    assert adopted == [("launcher-run", "sync_rumors", "schedule", None, 2)]
    assert recorded["start"][0][0] == "child"
    assert recorded["start"][0][3] == "launcher-run"
    assert recorded["finish"][1][4] == 2
    assert os.getenv("RUN_PARENT_ID") is None


def test_scope_without_launcher_run_leaves_process_env_alone(recorded, monkeypatch):
    """The API runs overlapping scopes in one process, so only adopted runs may export an id."""
    monkeypatch.delenv("RUN_ID", raising=False)
    monkeypatch.delenv("RUN_PARENT_ID", raising=False)
    with run_log.run_scope("http_job", "http"):
        assert os.getenv("RUN_PARENT_ID") is None
        with run_log.run_scope("http_child", "http"):
            assert os.getenv("RUN_PARENT_ID") is None
    assert recorded["start"][1][3] == "run-1"


def test_subprocess_in_run_scope_inherits_parent_run_id(recorded, monkeypatch):
    monkeypatch.setenv("RUN_ID", "launcher-run")
    with patch.object(run_log, "adopt_run"):
        with run_log.run_scope("refresher"):
            result = subprocess.run(
                [sys.executable, "-c", "import os; print(os.getenv('RUN_PARENT_ID'))"],
                check=True,
                capture_output=True,
                text=True,
            )
    assert result.stdout.strip() == "launcher-run"


def test_success_records_summary_and_links(recorded):
    with run_log.run_scope("sync_rumors", "manual") as run:
        run.summary["fetched"] = 3
        run_log.link("raw_signal", [1, 2], "output")
    assert recorded["links"] == [("run-1", "raw_signal", [1, 2], "output")]
    assert recorded["finish"][0][:3] == ("run-1", "success", None)
    assert recorded["finish"][0][3]["fetched"] == 3
    assert run_log.current_run_id() is None


def test_run_keeps_its_own_log_lines_as_the_outcome(recorded):
    job_logger = logging.getLogger("RumorSignalSync")
    job_logger.setLevel(logging.INFO)
    with run_log.run_scope("sync_rumors", "manual"):
        job_logger.warning("Reddit fetch failed")
        logging.getLogger("pika.connection").info("Closing connection")
        job_logger.info("Persisted 0 new raw signal row(s) of 5 fetched")
        run_log.add_summary(headline="5 fetched, 0 new")
    job_logger.info("after the run")
    summary = recorded["finish"][0][3]
    assert summary["log_tail"] == [
        "WARNING Reddit fetch failed",
        "INFO Persisted 0 new raw signal row(s) of 5 fetched",
    ]
    assert (summary["warnings"], summary["errors"]) == (1, 0)
    assert summary["last_message"] == "Persisted 0 new raw signal row(s) of 5 fetched"
    assert summary["headline"] == "5 fetched, 0 new"
    assert not any(
        isinstance(h, run_log._RunLogCapture) for h in logging.getLogger().handlers
    )


def test_log_lines_from_outside_the_run_are_not_captured(recorded):
    job_logger = logging.getLogger("RumorSignalSync")
    job_logger.setLevel(logging.INFO)
    with run_log.run_scope("sync_rumors", "manual") as run:
        token = run_log.current_run.set(None)
        job_logger.info("another request's line")
        run_log.current_run.reset(token)
    assert recorded["finish"][0][3]["log_tail"] == []


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


def test_dead_letter_depths_skip_missing_queues():
    import pika

    connection = MagicMock(is_open=True)
    present = MagicMock()
    present.queue_declare.return_value.method.message_count = 2
    missing = MagicMock()
    missing.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(
        404, "NOT_FOUND"
    )
    connection.channel.side_effect = [present, missing, present]
    with patch("pika.BlockingConnection", return_value=connection):
        depths = run_log.dead_letter_depths()
    assert depths == [
        {"queue": "lineup_updates.dlq", "messages": 2},
        {"queue": "draft_targets.dlq", "messages": 2},
    ]
    connection.close.assert_called_once()


def test_dead_letter_depths_are_none_when_the_broker_is_down():
    with patch("pika.BlockingConnection", side_effect=OSError("refused")):
        assert run_log.dead_letter_depths() is None
