"""
NFL bye weeks per team, from the same nflverse schedule lineup_solver.load_teams_playing
reads. No table stores a bye; recommenders derive it here so a player payload can say
"bye in Week 9" ahead of time instead of only noticing a missing weekly_stats row.

Separate from src.nfl_schedule because metrics_extractor (which owns team-code
normalization) already imports that module.
"""
import logging
from functools import lru_cache

import polars as pl

from src.ingestion.metrics_extractor import normalize_team
from src.nfl_schedule import load_schedule

logger = logging.getLogger("NflByes")

# ESPN's pro-team codes that differ from the nflverse schedule's.
ESPN_TO_NFLVERSE_TEAM = {"WSH": "WAS"}


def to_nflverse_team(team):
    """An ESPN, Sleeper or legacy team code mapped onto the nflverse schedule's code."""
    if team is None:
        return None
    return normalize_team(ESPN_TO_NFLVERSE_TEAM.get(team, team))


def bye_weeks_from_schedule(df_schedules: pl.DataFrame) -> dict:
    """Pure. Team -> its regular-season bye week: the week inside the team's own season
    span in which it has no game. Postseason weeks never count."""
    regular = df_schedules.filter(pl.col("game_type") == "REG")
    if regular.is_empty():
        return {}
    weeks_by_team: dict = {}
    for week, home, away in regular.select(["week", "home_team", "away_team"]).iter_rows():
        for team in (home, away):
            weeks_by_team.setdefault(normalize_team(team), set()).add(int(week))
    all_weeks = set(range(1, max(max(weeks) for weeks in weeks_by_team.values()) + 1))
    byes = {}
    for team, weeks in weeks_by_team.items():
        idle = sorted(all_weeks - weeks)
        if idle:
            byes[team] = idle[0]
    return byes


@lru_cache(maxsize=4)
def load_bye_weeks(season: int) -> dict:
    """Cached per season; byes don't move once the schedule is published. A failed read
    raises and is not cached, so the next call retries."""
    return bye_weeks_from_schedule(load_schedule(season, attempts=1))


def bye_weeks_or_empty(season: int) -> dict:
    """load_bye_weeks for display-only callers: an unreadable schedule leaves bye_week
    unknown (null) rather than failing the whole response."""
    try:
        return load_bye_weeks(season)
    except Exception as exc:
        logger.warning(f"NFL bye weeks unavailable for {season}: {exc}")
        return {}


def bye_week_for(team, bye_weeks: dict):
    """The bye week of `team` (any code), or None when unknown."""
    return bye_weeks.get(to_nflverse_team(team))


def teams_playing_from_byes(bye_weeks: dict, week: int):
    """The nflverse teams with a game in `week`, or None when the bye map is unknown (so
    callers can't mistake missing data for a bye)."""
    if not bye_weeks:
        return None
    return {team for team, bye in bye_weeks.items() if bye != week}
