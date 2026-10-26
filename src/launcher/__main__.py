"""`python -m src.launcher`: the unified job launcher (one replica, Recreate strategy).

Loads the catalog from JOB_CATALOG_PATH, checks every profile can be turned into a pod spec,
then consumes `job_runs` until SIGTERM. Shutdown stops consuming and gives a running inline
job KILL_GRACE_SECONDS to exit, so the Deployment's terminationGracePeriodSeconds should
exceed that.
"""

import logging
import signal
import sys
import threading

from src.launcher import k8s
from src.launcher.catalog import load_catalog_from_env
from src.launcher.consumer import Consumer
from src.launcher.inline import InlineRunner
from src.telemetry import configure_tracing

logger = logging.getLogger("JobLauncher")


def main() -> int:
    configure_tracing("job-launcher")
    try:
        catalog = load_catalog_from_env()
        k8s.validate_catalog(catalog)
    except Exception:
        logger.exception("Job catalog is not usable; refusing to start")
        return 1
    stop = threading.Event()

    def request_stop(signum, frame):
        logger.info("Received signal %s; stopping", signum)
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    inline_runner = InlineRunner()
    inline_runner.start()
    logger.info("Launcher started with %d jobs", len(catalog.jobs))
    try:
        Consumer(catalog, inline_runner).run(stop)
    finally:
        inline_runner.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
