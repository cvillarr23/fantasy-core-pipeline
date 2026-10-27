import signal
import sys
import time

import pytest

from src.launcher import inline
from src.launcher.inline import InlineRun, InlineRunner, expand_command


class Recorder:
    def __init__(self):
        self.finished = []
        self.statuses = {"r1": "running"}

    def status(self, run_id):
        return self.statuses.get(run_id)

    def finish_running(
        self, run_id, status, end_reason, error=None, force=False, expected_attempt=None
    ):
        self.finished.append((run_id, status, end_reason, force))


@pytest.fixture
def store():
    return Recorder()


@pytest.fixture
def runner(store):
    runner = InlineRunner(store=store)
    runner.start()
    yield runner
    runner.stop(grace=5)


def python(code, deadline=30.0, env=None):
    return InlineRun(
        run_id="r1",
        job="tiny-job",
        argv=["python", "-c", code],
        env=env or {"RUN_ID": "r1"},
        deadline_seconds=deadline,
    )


def wait_idle(runner, timeout=10):
    end = time.monotonic() + timeout
    while runner.busy:
        assert time.monotonic() < end, "inline run did not finish"
        time.sleep(0.02)


def test_deadline_terminates_the_run_as_timeout(runner, store):
    started = time.monotonic()
    runner.submit(python("import time; time.sleep(60)", deadline=0.5))
    wait_idle(runner)
    assert time.monotonic() - started < 5
    assert store.finished == [("r1", "timeout", "deadline", True)]


def test_sigkill_follows_an_ignored_sigterm(runner, store, monkeypatch, tmp_path):
    monkeypatch.setattr(inline, "KILL_GRACE_SECONDS", 0.5)
    sent = []
    real_signal_group = inline._signal_group
    monkeypatch.setattr(
        inline,
        "_signal_group",
        lambda p, sig: (sent.append(sig), real_signal_group(p, sig)),
    )
    ready = tmp_path / "ready"
    code = (
        "import pathlib, signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f"pathlib.Path({str(ready)!r}).touch(); time.sleep(60)"
    )
    runner.submit(python(code, deadline=3))
    wait_idle(runner)
    assert ready.exists()
    assert sent == [signal.SIGTERM, signal.SIGKILL]
    assert store.finished == [("r1", "timeout", "deadline", True)]


@pytest.mark.parametrize(
    ("code", "status"),
    [("raise SystemExit(0)", "success"), ("raise SystemExit(3)", "failure")],
)
def test_exit_code_fills_in_a_row_the_child_left_running(runner, store, code, status):
    runner.submit(python(code))
    wait_idle(runner)
    assert store.finished == [("r1", status, "exit_code", False)]


def test_child_gets_run_identity_and_launcher_env(runner, store, tmp_path, monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    out = tmp_path / "env"
    code = (
        "import os, pathlib, sys; "
        f"pathlib.Path({str(out)!r}).write_text(os.environ['RUN_ID'] + ' ' + sys.argv[1])"
    )
    run = InlineRun(
        "r1",
        "tiny-job",
        ["python", "-c", code, "$(CURRENT_YEAR)"],
        {"RUN_ID": "r1"},
        30,
    )
    runner.submit(run)
    wait_idle(runner)
    assert out.read_text() == "r1 2026"


def test_runner_is_busy_until_the_run_finishes(runner):
    runner.submit(python("import time; time.sleep(0.3)"))
    assert runner.busy
    wait_idle(runner)
    assert not runner.busy


def test_stop_terminates_the_running_child(store):
    runner = InlineRunner(store=store)
    runner.start()
    runner.submit(python("import time; time.sleep(60)"))
    time.sleep(0.3)
    started = time.monotonic()
    runner.stop(grace=10)
    assert time.monotonic() - started < 5
    assert store.finished == [("r1", "failure", "launcher_shutdown", False)]


def test_cancel_terminates_only_the_matching_inline_run(runner, store):
    runner.submit(python("import time; time.sleep(60)"))
    deadline = time.monotonic() + 5
    while not runner.is_running("r1"):
        assert time.monotonic() < deadline, "inline run did not start"
        time.sleep(0.01)
    assert runner.cancel("other-run") is False
    assert runner.cancel("r1") is True
    wait_idle(runner)
    assert store.finished == [("r1", "cancelled", "cancelled", True)]


def test_cancel_before_inline_submit_prevents_the_process_from_starting(
    runner, store, tmp_path
):
    ran = tmp_path / "ran"
    store.statuses["r1"] = "cancelled"
    assert runner.cancel("r1") is False
    runner.submit(python(f"import pathlib; pathlib.Path({str(ran)!r}).touch()"))
    wait_idle(runner)
    assert not ran.exists()
    assert store.finished == [("r1", "cancelled", "cancelled", True)]


def test_stop_kills_a_child_that_ignores_sigterm(store, monkeypatch, tmp_path):
    monkeypatch.setattr(inline, "KILL_GRACE_SECONDS", 0.5)
    runner = InlineRunner(store=store)
    runner.start()
    ready = tmp_path / "ready"
    runner.submit(
        python(
            "import pathlib, signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            f"pathlib.Path({str(ready)!r}).touch(); time.sleep(60)"
        )
    )
    while not ready.exists():
        time.sleep(0.02)
    started = time.monotonic()
    runner.stop(grace=0.5)
    assert time.monotonic() - started < 5
    assert not runner._thread.is_alive()
    assert store.finished == [("r1", "failure", "launcher_shutdown", False)]


def test_runner_survives_an_unrecordable_outcome(store, monkeypatch):
    calls = []

    def flaky(
        run_id, status, end_reason, error=None, force=False, expected_attempt=None
    ):
        calls.append(status)
        if len(calls) <= 2:
            raise RuntimeError("database down")

    store.finish_running = flaky
    runner = InlineRunner(store=store)
    runner.start()
    runner.submit(python("pass"))
    wait_idle(runner)
    runner.submit(python("pass"))
    wait_idle(runner)
    runner.stop(grace=5)
    # First run: exit-code write and the runner_error write both fail; the second still runs.
    assert calls == ["success", "failure", "success"]


def test_expand_command_follows_kubernetes_rules():
    env = {"CURRENT_YEAR": "2026", "ESPN_TEAM_ID": "7"}
    assert expand_command(
        [
            "python",
            "-m",
            "x",
            "--season",
            "$(CURRENT_YEAR)",
            "--team-id",
            "$(ESPN_TEAM_ID)",
            "$(UNSET)",
            "$$(CURRENT_YEAR)",
        ],
        env,
    ) == [
        sys.executable,
        "-m",
        "x",
        "--season",
        "2026",
        "--team-id",
        "7",
        "$(UNSET)",
        "$(CURRENT_YEAR)",
    ]
    assert expand_command(["node", "bot.js"], env) == ["node", "bot.js"]
