"""Runs `executor: inline` jobs as subprocesses of the launcher, one at a time.

Inline runs are for tiny, frequent jobs where a pod's startup costs more than the job. They
inherit the launcher's environment (which already holds the database and broker settings)
plus the run identity, and get the same deadline a pod would: SIGTERM to the whole process
group, then SIGKILL after KILL_GRACE_SECONDS. Unlike pods, nothing else watches them, so the
runner records their final status itself.
"""

import logging
import os
import queue
import re
import signal
import subprocess
import sys
import threading
from dataclasses import dataclass

from src.launcher.store import RunStore

logger = logging.getLogger(__name__)

KILL_GRACE_SECONDS = 30

_ENV_REFERENCE = re.compile(r"\$\$|\$\(([A-Za-z_][A-Za-z0-9_]*)\)")


def expand_command(argv, env):
    """Kubernetes' `$(VAR)` expansion, which a pod gets for free: a defined variable is
    substituted, an undefined reference is left as written, and `$$` escapes a `$`."""

    def substitute(match):
        if match.group(0) == "$$":
            return "$"
        return env.get(match.group(1), match.group(0))

    expanded = [_ENV_REFERENCE.sub(substitute, arg) for arg in argv]
    if expanded and expanded[0] == "python":
        expanded[0] = sys.executable
    return expanded


@dataclass(frozen=True)
class InlineRun:
    run_id: str
    job: str
    argv: list
    env: dict
    deadline_seconds: float
    attempt: int = 1


class InlineRunner:
    def __init__(self, store=None):
        self.store = store or RunStore()
        self._queue = queue.Queue()
        self._busy = threading.Event()
        self._stopping = threading.Event()
        self._lock = threading.Lock()
        self._process = None
        self._current_run_id = None
        self._cancelled = set()
        self._thread = threading.Thread(
            target=self._loop, name="inline-runner", daemon=True
        )

    @property
    def busy(self):
        return self._busy.is_set()

    def start(self):
        self._thread.start()

    def submit(self, run):
        """Called only when not busy; busy is set here, before the hand-off, so the consumer
        never admits a second inline run while this one waits to start."""
        with self._lock:
            self._busy.set()
            self._queue.put(run)

    def is_running(self, run_id):
        with self._lock:
            return self._current_run_id == run_id

    def cancel(self, run_id):
        """Stop the selected inline process group. Its row is already `cancelled`, so the
        run thread only has to end without recording an outcome."""
        with self._lock:
            if self._current_run_id != run_id:
                if self.store.status(run_id) == "cancelled":
                    self._cancelled.add(run_id)
                return False
            self._cancelled.add(run_id)
            process = self._process
        if process is not None:
            _terminate(process)
        return True

    def stop(self, grace=KILL_GRACE_SECONDS):
        """SIGTERM a running child, SIGKILL it if it outlives `grace`, and wait for the
        runner to record the outcome. The child has its own session, so it would otherwise
        outlive the launcher and leave its run `running`."""
        self._stopping.set()
        self._queue.put(None)
        self._signal_current(signal.SIGTERM)
        self._thread.join(grace)
        if self._thread.is_alive():
            self._signal_current(signal.SIGKILL)
            self._thread.join(5)

    def _signal_current(self, sig):
        with self._lock:
            process = self._process
        if process is not None:
            _signal_group(process, sig)

    def _loop(self):
        while True:
            run = self._queue.get()
            if run is None:
                return
            # Nothing may end this loop but stop(): a dead runner thread would leave `busy`
            # set by the next submit() forever, and no inline run would start again.
            try:
                with self._lock:
                    self._current_run_id = run.run_id
                self._execute(run)
            except Exception:
                logger.exception(
                    "Inline run %s of %s could not be completed", run.run_id, run.job
                )
                try:
                    self.store.finish_running(
                        run.run_id,
                        "failure",
                        "runner_error",
                        expected_attempt=run.attempt,
                    )
                except Exception:
                    logger.exception(
                        "Inline run %s outcome could not be recorded", run.run_id
                    )
            finally:
                with self._lock:
                    self._current_run_id = None
                    self._cancelled.discard(run.run_id)
                self._busy.clear()

    def _execute(self, run):
        env = {**os.environ, **run.env}
        argv = expand_command(run.argv, env)
        logger.info("Starting inline run run_id=%s job=%s", run.run_id, run.job)
        process = self._start_process(run, argv, env)
        if process is None:
            return
        try:
            try:
                code = process.wait(timeout=run.deadline_seconds)
            except subprocess.TimeoutExpired:
                self._end_timed_out(run, process)
                return
            self._end_exited(run, process, code)
        finally:
            with self._lock:
                self._process = None

    def _start_process(self, run, argv, env):
        """Starts the run's process, or returns None when the launcher is stopping or the
        run was cancelled before it started. Checked under the lock so a cancel() can't
        slip in between the check and the start."""
        with self._lock:
            if self._stopping.is_set():
                self._record_shutdown(run)
                return None
            if self._was_cancelled(run):
                return None
            process = self._process = subprocess.Popen(
                argv, env=env, start_new_session=True
            )
            return process

    def _end_timed_out(self, run, process):
        _terminate(process)
        if self._cancelled_since_start(run):
            return
        logger.error(
            "Inline run run_id=%s job=%s exceeded its %ss deadline",
            run.run_id,
            run.job,
            run.deadline_seconds,
        )
        self.store.finish_running(
            run.run_id,
            "timeout",
            "deadline",
            f"deadline of {run.deadline_seconds}s exceeded",
            force=True,
            expected_attempt=run.attempt,
        )

    def _end_exited(self, run, process, code):
        if self._stopping.is_set() and code != 0:
            # stop() sent SIGTERM; give it the grace period, then make sure it's gone.
            _terminate(process)
            self._record_shutdown(run)
            return
        if self._cancelled_since_start(run):
            return
        logger.info(
            "Inline run run_id=%s job=%s exited with %s", run.run_id, run.job, code
        )
        self.store.finish_running(
            run.run_id,
            "success" if code == 0 else "failure",
            "exit_code",
            None if code == 0 else f"exit code {code}",
            expected_attempt=run.attempt,
        )

    def _record_shutdown(self, run):
        self.store.finish_running(
            run.run_id, "failure", "launcher_shutdown", expected_attempt=run.attempt
        )

    def _cancelled_since_start(self, run):
        with self._lock:
            return self._was_cancelled(run)

    def _was_cancelled(self, run):
        """Whether `run` was cancelled; call with `_lock` held. The API wrote `cancelled`
        before the watcher called cancel(), so there is nothing left to record, and the run
        must end without writing an exit code or timeout over that row."""
        if run.run_id not in self._cancelled:
            return False
        logger.info("Inline run run_id=%s job=%s was cancelled", run.run_id, run.job)
        return True


def _signal_group(process, sig):
    try:
        os.killpg(process.pid, sig)
    except ProcessLookupError:
        pass


def _terminate(process):
    """SIGTERM the process group (draft-prep style jobs spawn their own children), then
    SIGKILL whatever is left once KILL_GRACE_SECONDS pass."""
    _signal_group(process, signal.SIGTERM)
    try:
        process.wait(timeout=KILL_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        pass
    # Also reaps children that outlived their parent's exit.
    _signal_group(process, signal.SIGKILL)
    process.wait()
