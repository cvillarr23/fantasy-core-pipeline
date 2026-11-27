"""Fantasy playoff weeks and trade deadline from ESPN's raw mSettings payload.

Shared by sync_league (persisted to league_settings.playoff_settings for the rest-of-season
projections refresh) and the trade context (the rest-of-season trade score term), so both
agree on which NFL weeks are this league's playoffs.
"""

import math
import re
from datetime import datetime, timezone

LAST_NFL_WEEK = 18


def _integer(value):
    if isinstance(value, bool) or not re.fullmatch(r"\d+", str(value)):
        return None
    return int(value)


def regular_season_end_week(raw):
    """The last NFL week of the regular season's final matchup period, or None."""
    try:
        settings = raw["settings"]["scheduleSettings"]
        count = _integer(settings["matchupPeriodCount"])
        weeks = settings["matchupPeriods"][str(count)]
        last = max(_integer(week) for week in weeks)
    except (KeyError, TypeError, ValueError):
        return None
    return last if last is not None and 1 <= last <= LAST_NFL_WEEK else None


def playoff_weeks(raw):
    """NFL weeks of the fantasy playoffs, in order: ceil(log2(playoffTeamCount)) rounds of
    playoffMatchupPeriodLength weeks each, right after the regular season, capped at ESPN's
    finalScoringPeriod. [] when the settings are missing or describe no playoffs."""
    end = regular_season_end_week(raw)
    try:
        settings = raw["settings"]["scheduleSettings"]
        team_count = _integer(settings["playoffTeamCount"])
        length = _integer(settings["playoffMatchupPeriodLength"])
        final = _integer(raw["status"]["finalScoringPeriod"])
    except (KeyError, TypeError):
        return []
    if end is None or None in (team_count, length, final) or team_count < 2 or length < 1:
        return []
    rounds = math.ceil(math.log2(team_count))
    last = min(end + rounds * length, final, LAST_NFL_WEEK)
    return list(range(end + 1, last + 1))


def trade_deadline(raw):
    """tradeSettings.deadlineDate (epoch ms) as an ISO-8601 UTC string, or None."""
    try:
        millis = raw["settings"]["tradeSettings"]["deadlineDate"]
    except (KeyError, TypeError):
        return None
    if isinstance(millis, bool) or not isinstance(millis, (int, float)) or millis <= 0:
        return None
    return datetime.fromtimestamp(millis / 1000, tz=timezone.utc).isoformat()


def playoff_settings(raw):
    """The league_settings.playoff_settings payload."""
    settings = (raw.get("settings") or {}).get("scheduleSettings") or {}
    return {
        "regular_season_end_week": regular_season_end_week(raw),
        "playoff_team_count": _integer(settings.get("playoffTeamCount")),
        "playoff_matchup_period_length": _integer(settings.get("playoffMatchupPeriodLength")),
        "playoff_weeks": playoff_weeks(raw),
        "trade_deadline": trade_deadline(raw),
    }
