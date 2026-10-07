"""
Which NFL week the scheduled jobs should act on, read from nflverse's schedule.

Every scheduled entrypoint that defaults its week resolves it here, so a failed schedule
download fails the Job instead of quietly acting on some other week. nflverse briefly
404s `schedules/games.parquet` while it replaces the release asset, and GitHub downloads
occasionally drop, so the read is retried with backoff before giving up.
"""
import logging
import time
from typing import Callable

import nflreadpy as nfl
import polars as pl

logger = logging.getLogger("NflSchedule")

SCHEDULE_LOAD_ATTEMPTS = 6
SCHEDULE_BACKOFF_SECONDS = 5.0
SCHEDULE_MAX_BACKOFF_SECONDS = 30.0


class WeekResolutionError(RuntimeError):
    """The target week can't be determined. Callers must fail, never substitute a week."""


class RegularSeasonOverError(Exception):
    """Every regular-season game of the season is final; there is no upcoming week."""


def load_schedule(
    season: int,
    *,
    attempts: int = SCHEDULE_LOAD_ATTEMPTS,
    backoff_seconds: float = SCHEDULE_BACKOFF_SECONDS,
    max_backoff_seconds: float = SCHEDULE_MAX_BACKOFF_SECONDS,
    sleep: Callable[[float], None] | None = None,
) -> pl.DataFrame:
    """nflverse's schedule for `season`, retrying any download failure. Five sleeps of
    5-30s outlast the asset swap; HTTP callers pass attempts=1 so a request never waits."""
    sleep = sleep or time.sleep
    for attempt in range(1, attempts + 1):
        try:
            return nfl.load_schedules(season)
        except Exception as exc:
            if attempt == attempts:
                raise WeekResolutionError(
                    f"Could not load the {season} NFL schedule after {attempts} attempt(s): {exc}"
                ) from exc
            delay = min(backoff_seconds * 2 ** (attempt - 1), max_backoff_seconds)
            logger.warning(
                f"Loading the {season} NFL schedule failed (attempt {attempt}/{attempts}): {exc}; "
                f"retrying in {delay:.0f}s."
            )
            sleep(delay)


def _week_completion(df_schedules: pl.DataFrame) -> pl.DataFrame:
    """One row per regular-season week: whether every game in it has a final score."""
    return (
        df_schedules.filter(pl.col("game_type") == "REG")
        .group_by("week")
        .agg((pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null()).all().alias("complete"))
    )


def last_completed_week(df_schedules: pl.DataFrame) -> int | None:
    """Latest regular-season week whose games all have final scores, or None before any
    week has finished. A week with only Thursday's game final doesn't count - ingesting
    it would write a partial week."""
    if df_schedules.is_empty():
        return None
    weeks = _week_completion(df_schedules).filter(pl.col("complete"))
    if weeks.is_empty():
        return None
    return int(weeks["week"].max())


def upcoming_week(df_schedules: pl.DataFrame) -> int | None:
    """Earliest regular-season week with a game not yet final - the week lineups and
    projections are for. Same answer nflreadpy's get_current_week() gives, minus the
    postseason. None once the whole regular season is final."""
    weeks = _week_completion(df_schedules).filter(~pl.col("complete"))
    if weeks.is_empty():
        return None
    return int(weeks["week"].min())


def resolve_upcoming_week(season: int, **load_kwargs) -> int | None:
    """The upcoming regular-season week of `season`: week 1 before kickoff, None once the
    regular season is over. Raises WeekResolutionError when the schedule can't be read,
    has no regular-season games for the season, or says the season is over while a later
    season has already started (a stale CURRENT_YEAR would otherwise no-op every run)."""
    df = load_schedule(season, **load_kwargs)
    regular_season = df.filter((pl.col("season") == season) & (pl.col("game_type") == "REG"))
    if regular_season.is_empty():
        raise WeekResolutionError(f"The nflverse schedule has no {season} regular-season games.")

    week = upcoming_week(regular_season)
    if week is not None:
        return week

    current_season = int(nfl.get_current_season())
    if current_season > season:
        raise WeekResolutionError(
            f"The {season} regular season is over but the {current_season} season has started; "
            "is CURRENT_YEAR stale?"
        )
    logger.info(f"The {season} regular season is over; there is no upcoming week.")
    return None


def require_upcoming_week(season: int, **load_kwargs) -> int:
    """resolve_upcoming_week for callers that need a week, e.g. an HTTP route mapping
    RegularSeasonOverError to its own status."""
    week = resolve_upcoming_week(season, **load_kwargs)
    if week is None:
        raise RegularSeasonOverError(f"The {season} regular season is over; pass a week explicitly.")
    return week
