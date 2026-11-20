"""`python -m src.launcher`: the unified job launcher.

Loads the catalog from JOB_CATALOG_PATH and checks every profile can be turned into a pod
spec, then waits as a standby until it holds the `fantasy-job-launcher` Lease
(`src/launcher/leader.py`). Two replicas run; only the leader consumes `job_runs` until
SIGTERM, with the scheduler, the event consumer, the watcher, the heartbeat and the Lease
renewal each on its own thread, so a slow season lookup or broker read never delays admitting
runs.

Taking over, it first fails every inline run still `running`: inline runs are children of the
launcher that started them, so that launcher is gone (`launcher_lost`, retried by the watcher).
Everything else a previous leader left is already in Postgres and k8s.

Shutdown stops consuming and gives a running inline job KILL_GRACE_SECONDS to exit, so the
Deployment's terminationGracePeriodSeconds should exceed that, and then releases the Lease so
the standby takes over at once. A lost Lease kills inline work within
LOST_LEASE_KILL_GRACE_SECONDS, before the standby can take the Lease, and exits 1 so the
container restarts as a standby.
"""

import logging
import signal
import sys
import threading

from src.launcher import heartbeat, k8s, leader
from src.launcher.catalog import load_catalog_from_env
from src.launcher.consumer import Consumer
from src.launcher.event_consumer import EventConsumer
from src.launcher.inline import KILL_GRACE_SECONDS, InlineRunner
from src.launcher.leader import LeaseElector
from src.launcher.runs import RunStore
from src.launcher.scheduler import PostgresSchedulerState, Scheduler
from src.launcher.watcher import Watcher
from src.telemetry import configure_tracing

logger = logging.getLogger("JobLauncher")

LOST_LEASE_KILL_GRACE_SECONDS = 1
TAKEOVER_RETRY_SECONDS = 5
INLINE_TAKEOVER_END_REASON = "launcher_lost"


def main() -> int:
    configure_tracing("job-launcher")
    catalog = _usable_catalog()
    if catalog is None:
        return 1
    stop = _stop_on_signals()
    elector = LeaseElector()
    if not elector.acquire(stop):
        logger.info("Stopped while standing by")
        return 0
    renewer = _start_thread(elector.hold, stop, "launcher-lease")
    try:
        _lead(catalog, stop, elector.lost)
    finally:
        stop.set()
        renewer.join(leader.RETRY_SECONDS + 2 * leader.REQUEST_TIMEOUT_SECONDS)
        if not elector.lost.is_set():
            elector.release()
    return 1 if elector.lost.is_set() else 0


def _usable_catalog():
    try:
        catalog = load_catalog_from_env()
        k8s.validate_catalog(catalog)
    except Exception:
        logger.exception("Job catalog is not usable; refusing to start")
        return None
    return catalog


def _stop_on_signals():
    stop = threading.Event()

    def request_stop(signum, frame):
        logger.info("Received signal %s; stopping", signum)
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    return stop


def _start_thread(target, stop, name):
    thread = threading.Thread(target=target, args=(stop,), name=name, daemon=True)
    thread.start()
    return thread


def _lead(catalog, stop, lost):
    """Runs every leader-only part until `stop` is set."""
    if not _fail_previous_inline_runs(stop):
        return
    inline_runner = InlineRunner()
    inline_runner.start()
    scheduler = Scheduler(catalog, PostgresSchedulerState())
    _start_thread(scheduler.run, stop, "launcher-scheduler")
    _start_thread(EventConsumer(scheduler.handle_event).run, stop, "launcher-events")
    watcher = Watcher(catalog, inline_runner=inline_runner)
    _start_thread(watcher.run, stop, "launcher-watcher")
    _start_thread(heartbeat.run, stop, "launcher-heartbeat")
    logger.info("Launcher started with %d jobs", len(catalog.jobs))
    try:
        Consumer(catalog, inline_runner).run(stop)
    finally:
        inline_runner.stop(_inline_grace(lost))


def _fail_previous_inline_runs(stop):
    """Fails inline rows a previous leader left `running`, retrying while the database is
    away. False when stopped first."""
    while not stop.is_set():
        try:
            failed = RunStore().fail_running_inline(INLINE_TAKEOVER_END_REASON)
        except Exception:
            logger.exception("Could not end a previous leader's inline runs; retrying")
            stop.wait(TAKEOVER_RETRY_SECONDS)
            continue
        if failed:
            logger.warning(
                "Failed inline runs a previous leader left running: %s", failed
            )
        return True
    return False


def _inline_grace(lost):
    """A lost Lease leaves only the gap until the standby can take it over."""
    return LOST_LEASE_KILL_GRACE_SECONDS if lost.is_set() else KILL_GRACE_SECONDS


if __name__ == "__main__":
    sys.exit(main())
