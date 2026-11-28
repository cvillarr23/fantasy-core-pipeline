"""Refresh Sleeper and ESPN projections (skill players and K/D-ST), and VORP, for the upcoming weeks.

Run as ``python -m src.jobs.draft_prep_refresh [--week-offset N]``. Each week's stages run in
order with a shared 600-second budget.

- With ``--week-offset N`` it refreshes only the upcoming week plus ``N``: one fan-out child
  of the launcher's ``projections-refresh`` job, which runs one child per offset in parallel.
  A week past the regular season is a no-op (exit 0).
- Without it, every week of the horizon (the upcoming week and up to three more) is processed
  in turn: a failed target week stops the run, while a later failed week skips its remaining
  stages and allows the rest of the horizon to refresh.
- A failed ESPN skill-projection stage (``OPTIONAL_STAGES``) is logged and skipped, so the week
  still runs vorp_matrix on the Sleeper fallback.
- With ``--rest-of-season`` it refreshes the weeks after that horizon through the league's last
  fantasy playoff week (``league_settings.playoff_settings``, synced by sync_league), in turn,
  for the trade score's rest-of-season term (#328). A failed week doesn't stop the later ones.
  No synced playoff settings, or no weeks left past the horizon, is a no-op (exit 0).
"""
import argparse
import logging
import os
import subprocess
import sys
import time

from src.database import get_db_cursor
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
    # After sync_projections (it only updates the rows Sleeper wrote) and before vorp_matrix,
    # which reads the ESPN+Sleeper consensus projected_points (#462).
    "src.ingestion.sync_espn_projections",
    "src.optimization.vorp_matrix",
)
# Stages whose failure is logged and skipped rather than failing the week. The consensus
# falls back to Sleeper when ESPN's half is missing or stale, and vorp_matrix must still run
# after sync_projections, or projected_points would be newer than vorp_score. Each gets at
# most OPTIONAL_STAGE_BUDGET_SECONDS, so it can't eat vorp_matrix's share of the week budget.
OPTIONAL_STAGES = frozenset({"src.ingestion.sync_espn_projections"})
OPTIONAL_STAGE_BUDGET_SECONDS = 180


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
    if args.rest_of_season:
        return _refresh_rest_of_season(season, int(first_week))
    return _refresh_horizon(season, int(first_week))


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--rest-of-season",
        action="store_true",
        help="refresh the weeks after the horizon through the last fantasy playoff week",
    )
    mode.add_argument(
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


def load_last_playoff_week(league_id: str, season: int) -> int | None:
    """The configured league's last fantasy playoff week for this season, or None if not
    synced. Filtered by league: league_settings also holds other leagues (a practice
    draft league), whose playoff settings must never stand in for ours."""
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT playoff_settings FROM league_settings WHERE league_id = %s AND season = %s;",
            (league_id, season),
        )
        row = cursor.fetchone()
    weeks = ((row[0] if row else None) or {}).get("playoff_weeks") or []
    return max(weeks) if weeks else None


def _refresh_rest_of_season(season: int, first_week: int) -> int:
    """Refreshes each week after the horizon through the last playoff week, in turn."""
    try:
        last_week = load_last_playoff_week(os.environ["ESPN_LEAGUE_ID"], season)
    except Exception:
        logger.exception("Unable to read the league's playoff settings")
        return 1
    if last_week is None:
        logger.info("No synced playoff settings for season=%s; nothing to refresh", season)
        return 0
    weeks = range(first_week + HORIZON_EXTRA_WEEKS + 1, min(last_week, LAST_REGULAR_SEASON_WEEK) + 1)
    if not weeks:
        logger.info("No rest-of-season weeks past the horizon for season=%s week=%s", season, first_week)
        return 0
    completed, failed = [], []
    for week in weeks:
        (completed if _refresh_week(season, week) else failed).append(week)
    logger.info(
        "Rest-of-season refresh summary season=%s completed_weeks=%s failed_weeks=%s",
        season, completed, failed,
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
        optional = stage in OPTIONAL_STAGES
        try:
            if remaining <= 0:
                raise subprocess.TimeoutExpired(stage, WEEK_BUDGET_SECONDS)
            subprocess.run(
                [sys.executable, "-u", "-m", stage, "--season", str(season), "--week", str(week)],
                check=True,
                timeout=min(remaining, OPTIONAL_STAGE_BUDGET_SECONDS) if optional else remaining,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
            if optional and remaining > 0:
                logger.warning(
                    "Skipped optional season=%s week=%s stage=%s elapsed=%.1fs: %s",
                    season, week, stage, time.monotonic() - stage_started, exc,
                )
                continue
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
