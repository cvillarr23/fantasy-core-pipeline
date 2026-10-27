"""Consumes `job_runs` and starts each run as a k8s Job or an inline subprocess.

A delivery is acked as soon as it is admitted to the in-memory pending list, not when the
run starts. A run can wait for the pod cap for as long as its expiry allows (a schedule's
catch_up is up to 2h), and RabbitMQ closes the channel on any delivery left unacked past
its 30-minute consumer_timeout. The `queued` run_log row, written before the command was
published, is the durable record: after a restart the pending list is rebuilt from it.

Each pass over the pending list, oldest first, decides every run on its own, so a run
waiting for its job's previous run doesn't hold up other jobs:

- no longer `queued` (cancelled, or already handled) -> dropped
- a chain step whose root is no longer `running` (cancelled, or past max_duration) ->
  `skipped (chain_ended)`
- past `expires_at` -> `skipped (expired)`
- another run of the job is running -> `skipped (overlap)` under `forbid`, waits under `queue`
- pod cap full, or the inline runner busy -> waits
- otherwise `running`, then the k8s Job is created or the inline runner takes it

A run still marked `running` after its pod died holds its overlap and cap slot until the
watcher times it out as orphaned.
"""

import json
import logging
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone

import pika
from kubernetes.client.rest import ApiException
from opentelemetry import propagate, trace
from pydantic import ValidationError

from src.launcher import k8s
from src.launcher.catalog import UnknownJobError, params_to_args, validate_run_params
from src.launcher.inline import InlineRun
from src.launcher.publish import RunCommand
from src.launcher.runs import RunStore
from src.queue_contracts import AMQP_URL, JOB_RUNS

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

POLL_SECONDS = 5
PREFETCH_COUNT = 20
RECONNECT_BACKOFF_SECONDS = (1, 2, 5, 10, 30)


class RunLogUnavailable(RuntimeError):
    """A delivery couldn't be checked against run_log."""


def overlap_refuses(catalog, job, store=None):
    """Whether a new run of `job` would be refused right now (`forbid` with one running), for
    callers that answer 409 instead of queueing a run that will be skipped."""
    if catalog.resolve(job).overlap != "forbid":
        return False
    with (store or RunStore()).admission() as admission:
        return admission.job_running(job, "")


class Consumer:
    def __init__(self, catalog, inline_runner, store=None, clock=None, launch_pod=None):
        self.catalog = catalog
        self.inline_runner = inline_runner
        self.store = store or RunStore()
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.launch_pod = launch_pod or k8s.create_job
        self.pending: OrderedDict[str, RunCommand] = OrderedDict()

    # ---- admission ------------------------------------------------------------------

    def admit(self, body):
        """Adds a delivered command to the pending list. Returns False for one to drop (not
        a valid command, or its run is no longer queued); raises when run_log is unreadable.

        A delivery at another attempt than the pending one replaces it only when run_log
        says it is the current attempt; attempts only grow, so an older one never is.
        """
        try:
            command = RunCommand.model_validate(body)
        except ValidationError as exc:
            logger.error("Dropping invalid run command: %s", exc)
            return False
        pending = self.pending.get(command.run_id)
        if pending is not None and pending.attempt == command.attempt:
            return True
        if not self._is_current_attempt(command, *self.store.run_state(command.run_id)):
            return False
        self.pending[command.run_id] = command
        return True

    def recover(self):
        """Rebuilds the pending list from queued runs. A delivery of the same run later is
        recognised by run_id and ignored."""
        for body in self.store.queued_commands():
            try:
                command = RunCommand.model_validate(body)
            except ValidationError as exc:
                logger.error("Skipping unreadable queued run command: %s", exc)
                continue
            self.pending.setdefault(command.run_id, command)
        if self.pending:
            logger.info("Recovered %d queued runs", len(self.pending))

    def drain(self):
        """One pass over the pending list; returns the run_ids launched."""
        launched = []
        for run_id, command in list(self.pending.items()):
            try:
                outcome = self._decide(command)
            except Exception:
                logger.exception("Admission of run %s deferred", run_id)
                continue
            if outcome == "wait":
                continue
            del self.pending[run_id]
            if outcome is not None:
                try:
                    self._launch(command, *outcome)
                except Exception:
                    logger.exception("Launch of run %s did not complete", run_id)
                launched.append(run_id)
        return launched

    def _decide(self, command):
        """Returns "wait", None (finished with: dropped or ended), or the launch arguments."""
        try:
            resolved = self.catalog.resolve(command.job)
            params = validate_run_params(resolved.job, command.params)
        except (UnknownJobError, ValueError) as exc:
            with self.store.admission() as admission:
                self._fail_queued(admission, command, "invalid_command", exc)
            return None
        argv = [*resolved.job.command, *params_to_args(params)]
        with self.store.admission() as admission:
            if not self._is_current_attempt(
                command, *admission.run_state(command.run_id)
            ):
                return None
            if self._chain_ended(admission, command):
                self._skip(
                    admission,
                    command,
                    "chain_ended",
                    f"chain run {command.chain_root_id} has ended",
                )
                return None
            if self._expired(command):
                self._skip(
                    admission,
                    command,
                    "expired",
                    "expired while queued",
                    level=logging.WARNING,
                )
                return None
            overlap = self._overlap_outcome(admission, command, resolved)
            if overlap == "skip":
                self._skip(
                    admission, command, "overlap", "another run is still running"
                )
                return None
            if overlap == "wait" or self._capacity_full(admission, resolved):
                return "wait"
            k8s_job = None
            if resolved.executor != "inline":
                k8s_job = self._build_k8s_job_or_fail(
                    admission, command, resolved, argv
                )
                if k8s_job is None:
                    return None
            if not admission.claim(
                command.run_id,
                resolved.executor,
                k8s_job.metadata.name if k8s_job else None,
            ):
                return None
        return resolved, argv, k8s_job

    @staticmethod
    def _is_current_attempt(command, status, attempt):
        """Whether the command is for a still-queued run at run_log's current attempt."""
        if status != "queued":
            logger.info(
                "Ignoring run command run_id=%s job=%s: run is %s",
                command.run_id,
                command.job,
                status or "missing",
            )
            return False
        if command.attempt != attempt:
            logger.info(
                "Ignoring stale run command run_id=%s attempt=%s; current attempt is %s",
                command.run_id,
                command.attempt,
                attempt,
            )
            return False
        return True

    @staticmethod
    def _chain_ended(admission, command):
        """Whether the command is a chain step whose root is no longer `running`.

        A scheduler tick that raced its root's cancel can publish a step after the cancel,
        and a step can still be queued when its root hits max_duration; neither may start
        an ended chain.
        """
        return bool(
            command.chain_root_id
            and command.chain_root_id != command.run_id
            and not admission.chain_running(command.chain_root_id)
        )

    def _expired(self, command):
        """Whether a first attempt sat queued past its `expires_at`.

        `expires_at` decides whether the original request is still worth starting. Once an
        attempt has run, its configured retries must not inherit that stale queue deadline
        or the first late failure would silently suppress them.
        """
        return command.attempt == 1 and self.clock() >= command.expires_at

    @staticmethod
    def _overlap_outcome(admission, command, resolved):
        """While another run of the job is running: "skip" under `forbid`, "wait" under
        `queue`. None when no other run is running."""
        if not admission.job_running(command.job, command.run_id):
            return None
        return "skip" if resolved.overlap == "forbid" else "wait"

    def _capacity_full(self, admission, resolved):
        """Whether the run's executor has no room: the inline runner is busy, or the pod
        cap is reached."""
        if resolved.executor == "inline":
            return self.inline_runner.busy
        return (
            admission.running_pods() >= self.catalog.global_settings.max_concurrent_pods
        )

    def _build_k8s_job_or_fail(self, admission, command, resolved, argv):
        """The run's k8s Job, or None after failing the run when the launcher's own
        config can't build it."""
        try:
            return k8s.build_job(command, resolved, argv)
        except k8s.LaunchConfigError as exc:
            self._fail_queued(admission, command, "launch_failed", exc)
            return None

    @staticmethod
    def _skip(admission, command, end_reason, why, level=logging.INFO):
        admission.end_queued(command.run_id, "skipped", end_reason)
        logger.log(level, "Skipped run %s of %s: %s", command.run_id, command.job, why)

    @staticmethod
    def _fail_queued(admission, command, end_reason, exc):
        admission.end_queued(command.run_id, "failure", end_reason, str(exc))
        logger.error("Run %s of %s failed: %s", command.run_id, command.job, exc)

    def _launch(self, command, resolved, argv, k8s_job):
        context = propagate.extract(
            {"traceparent": command.traceparent} if command.traceparent else {}
        )
        with tracer.start_as_current_span(
            "launcher.launch",
            context=context,
            attributes={"fantasy.job": command.job, "fantasy.run_id": command.run_id},
        ):
            try:
                if k8s_job is None:
                    self._start_inline(command, resolved, argv)
                else:
                    self._start_pod(k8s_job)
            except Exception as exc:
                logger.exception(
                    "Run %s of %s could not be launched", command.run_id, command.job
                )
                if not self._launch_failed(command, k8s_job, exc):
                    return
        logger.info(
            "Launched run run_id=%s job=%s executor=%s attempt=%s",
            command.run_id,
            command.job,
            resolved.executor,
            command.attempt,
        )

    def _start_inline(self, command, resolved, argv):
        self.inline_runner.submit(
            InlineRun(
                run_id=command.run_id,
                job=command.job,
                argv=argv,
                env=command.launch_env(),
                deadline_seconds=resolved.deadline.total_seconds(),
                attempt=command.attempt,
            )
        )

    def _start_pod(self, k8s_job):
        self.launch_pod(k8s_job)

    def _launch_failed(self, command, k8s_job, exc):
        """Settles a launch that raised. Returns True when the k8s Job turned out to exist.

        A timeout or 5xx doesn't say whether the apiserver created the k8s Job, and failing a
        run whose pod then starts would free its overlap slot while it acts. So unless the
        apiserver refused outright (4xx), the k8s Job is looked up first; if that can't be
        answered either, the run stays `running` for the watcher's start timeout to settle.
        """
        refused = isinstance(exc, ApiException) and 400 <= (exc.status or 0) < 500
        if k8s_job is not None and not refused:
            exists = k8s.job_exists(k8s_job.metadata.name)
            if exists:
                logger.warning(
                    "k8s Job %s exists despite the error; run launched",
                    k8s_job.metadata.name,
                )
                return True
            if exists is None:
                logger.error(
                    "Run %s of %s: k8s Job %s may exist; leaving the run for the watcher",
                    command.run_id,
                    command.job,
                    k8s_job.metadata.name,
                )
                return False
        try:
            self.store.finish_running(
                command.run_id,
                "failure",
                "launch_failed",
                f"{type(exc).__name__}: {exc}",
                expected_attempt=command.attempt,
            )
        except Exception:
            logger.exception(
                "Run %s launch failure could not be recorded", command.run_id
            )
        return False

    # ---- broker -----------------------------------------------------------------------

    def _on_message(self, channel, method, properties, body):
        try:
            payload = json.loads(body)
        except ValueError:
            logger.error("Dropping run command that is not JSON: %r", body[:200])
            channel.basic_ack(method.delivery_tag)
            return
        try:
            self.admit(payload)
        except Exception as exc:
            channel.basic_nack(method.delivery_tag, requeue=True)
            # Every prefetched delivery would fail the same way, so drop the connection
            # (returning them to the queue) and let run() back off before consuming again.
            raise RunLogUnavailable(
                "run command not admitted; returned to the queue"
            ) from exc
        channel.basic_ack(method.delivery_tag)

    def _tick(self):
        from src.services.activity_runner import drain_job_runs

        try:
            self.drain()
        except Exception:
            logger.exception("Pending run admission deferred")
        drain_job_runs()

    def run(self, stop: threading.Event):
        """Consumes until `stop` is set, reconnecting to the broker with backoff. Pending
        runs keep being admitted while the broker is away: they're already acked."""
        while True:
            try:
                self.recover()
                break
            except Exception:
                logger.exception("Queued runs could not be recovered; retrying")
                if stop.wait(POLL_SECONDS):
                    return
        failures = 0
        while not stop.is_set():
            connection = None
            try:
                params = pika.URLParameters(AMQP_URL)
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.basic_qos(prefetch_count=PREFETCH_COUNT)
                channel.queue_declare(queue=JOB_RUNS, passive=True)
                channel.basic_consume(
                    queue=JOB_RUNS, on_message_callback=self._on_message
                )
                logger.info("Consuming %s", JOB_RUNS)
                failures = 0
                while not stop.is_set():
                    connection.process_data_events(time_limit=POLL_SECONDS)
                    self._tick()
            except Exception:
                delay = RECONNECT_BACKOFF_SECONDS[
                    min(failures, len(RECONNECT_BACKOFF_SECONDS) - 1)
                ]
                failures += 1
                logger.exception(
                    "Consuming %s interrupted; reconnecting in %ss", JOB_RUNS, delay
                )
                deadline = time.monotonic() + delay
                while not stop.is_set() and time.monotonic() < deadline:
                    self._tick()
                    stop.wait(min(POLL_SECONDS, max(0, deadline - time.monotonic())))
            finally:
                if connection is not None and connection.is_open:
                    try:
                        connection.close()
                    except Exception:
                        logger.warning("Could not close broker connection cleanly")
