"""Refresh projections, kicker/DST projections, and VORP for the upcoming weeks.

Run as ``python -m src.jobs.draft_prep_refresh [--week-offset N]``. Each week's stages run in
order with a shared 600-second budget.

- With ``--week-offset N`` it refreshes only the upcoming week plus ``N``: one fan-out child
  of the launcher's ``projections-refresh`` job, which runs one child per offset in parallel.
  A week past the regular season is a no-op (exit 0).
- Without it, every week of the horizon (the upcoming week and up to three more) is processed
  in turn: a failed target week stops the run, while a later failed week skips its remaining
  stages and allows the rest of the horizon to refresh.
"""
import argparse
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
LAST_REGULAR_SEASON_WEEK = 18
HORIZON_EXTRA_WEEKS = 3
STAGES = (
    "src.ingestion.sync_projections",
    "src.ingestion.sync_kicker_dst",
    "src.optimization.vorp_matrix",
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or [])
    try:
        season = int(os.environ["CURRENT_YEAR"])
        first_week = resolve_upcoming_week(season)
        if first_week is not None and not 1 <= int(first_week) <= LAST_REGULAR_SEASON_WEEK:
            raise ValueError(f"Target week {first_week} is outside regular-season weeks 1-18")
    except Exception:
        logger.exception("Unable to resolve refresh season/week")
        return 1

    if first_week is None:
        logger.info("Offseason: the season=%s regular season is over; no weeks to refresh", season)
        return 0
    if args.week_offset is not None:
        return _refresh_one_week(season, int(first_week) + args.week_offset)
    return _refresh_horizon(season, int(first_week))


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--week-offset",
        type=_non_negative_int,
        help="refresh only the upcoming week plus this many weeks",
    )
    return parser.parse_args(argv)


def _non_negative_int(value: str) -> int:
    offset = int(value)
    if offset < 0:
        raise argparse.ArgumentTypeError("week offset must be 0 or more")
    return offset


def _refresh_one_week(season: int, week: int) -> int:
    if week > LAST_REGULAR_SEASON_WEEK:
        logger.info("No week to refresh: season=%s week=%s is past the regular season", season, week)
        return 0
    return 0 if _refresh_week(season, week) else 1


def _refresh_horizon(season: int, first_week: int) -> int:
    """Refreshes the target week and up to three more, stopping if the target week fails."""
    last_week = min(first_week + HORIZON_EXTRA_WEEKS, LAST_REGULAR_SEASON_WEEK)
    completed, failed = [], []
    logger.info("Refreshing season=%s weeks=%s..%s", season, first_week, last_week)
    for week in range(first_week, last_week + 1):
        (completed if _refresh_week(season, week) else failed).append(week)
        if week == first_week and failed:
            break

    logger.info(
        "Refresh summary season=%s target_week=%s completed_weeks=%s failed_weeks=%s",
        season, first_week, completed, failed,
    )
    return 1 if failed else 0


def _refresh_week(season: int, week: int) -> bool:
    """Runs each stage for one week within the shared budget; False once a stage fails."""
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
            return False
        logger.info(
            "Completed season=%s week=%s stage=%s elapsed=%.1fs",
            season, week, stage, time.monotonic() - stage_started,
        )
    logger.info(
        "Week complete season=%s week=%s elapsed=%.1fs",
        season, week, time.monotonic() - week_started,
    )
    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    configure_tracing("draft-prep-refresher")
    # The outermost run scope adopts the launcher's RUN_ID, so each stage subprocess records
    # a child run instead of adopting (and ending) this one.
    sys.exit(run_log_service.run_main("draft_prep_refresher", lambda: main(sys.argv[1:])))
