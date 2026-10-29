"""Refresh projections, kicker/DST projections, and VORP for the upcoming weeks.

Run as ``python -m src.jobs.draft_prep_refresh``. Each week is processed in order with a
shared 600-second budget; a failed target week stops the run, while a later failed week
skips its remaining stages and allows the rest of the horizon to refresh.
"""
import logging
import os
import subprocess
import sys
import time

from src.nfl_schedule import resolve_upcoming_week
from src.services import run_log_service
from src.telemetry import configure_tracing

logger = logging.getLogger("DraftPrepRefresher")
WEEK_BUDGET_SECONDS = 600
STAGES = (
    "src.ingestion.sync_projections",
    "src.ingestion.sync_kicker_dst",
    "src.optimization.vorp_matrix",
)


def main() -> int:
    try:
        season = int(os.environ["CURRENT_YEAR"])
        first_week = resolve_upcoming_week(season)
        if first_week is not None and not 1 <= int(first_week) <= 18:
            raise ValueError(f"Target week {first_week} is outside regular-season weeks 1-18")
    except Exception:
        logger.exception("Unable to resolve refresh season/week")
        return 1

    if first_week is None:
        logger.info("Offseason: the season=%s regular season is over; no weeks to refresh", season)
        return 0

    first_week = int(first_week)
    last_week = min(first_week + 3, 18)
    completed, failed = [], []
    logger.info("Refreshing season=%s weeks=%s..%s", season, first_week, last_week)
    for week in range(first_week, last_week + 1):
        week_started = time.monotonic()
        for stage in STAGES:
            stage_started = time.monotonic()
            remaining = WEEK_BUDGET_SECONDS - (stage_started - week_started)
            logger.info(
                "Starting season=%s week=%s stage=%s budget_remaining=%.1fs",
                season, week, stage, remaining,
            )
            try:
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(stage, WEEK_BUDGET_SECONDS)
                subprocess.run(
                    [sys.executable, "-u", "-m", stage, "--season", str(season), "--week", str(week)],
                    check=True,
                    timeout=remaining,
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
                logger.error(
                    "Failed season=%s week=%s stage=%s elapsed=%.1fs: %s",
                    season, week, stage, time.monotonic() - stage_started, exc,
                )
                failed.append(week)
                break
            logger.info(
                "Completed season=%s week=%s stage=%s elapsed=%.1fs",
                season, week, stage, time.monotonic() - stage_started,
            )
        else:
            completed.append(week)
            logger.info(
                "Week complete season=%s week=%s elapsed=%.1fs",
                season, week, time.monotonic() - week_started,
            )

        if week == first_week and failed:
            break

    logger.info(
        "Refresh summary season=%s target_week=%s completed_weeks=%s failed_weeks=%s",
        season, first_week, completed, failed,
    )
    return 1 if failed else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    configure_tracing("draft-prep-refresher")
    # The outermost run scope adopts the launcher's RUN_ID, so each stage subprocess records
    # a child run instead of adopting (and ending) this one.
    sys.exit(run_log_service.run_main("draft_prep_refresher", main))
