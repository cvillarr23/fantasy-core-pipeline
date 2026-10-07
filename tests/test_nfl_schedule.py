from unittest.mock import patch

import polars as pl
import pytest

from src import nfl_schedule as ns


def _schedule(games, season=2026):
    """games: (week, game_type, home_score, away_score) tuples."""
    return pl.DataFrame(
        {
            "season": [season] * len(games),
            "week": [g[0] for g in games],
            "game_type": [g[1] for g in games],
            "home_score": [g[2] for g in games],
            "away_score": [g[3] for g in games],
        },
        schema={"season": pl.Int64, "week": pl.Int64, "game_type": pl.Utf8,
                "home_score": pl.Int64, "away_score": pl.Int64},
    )


MID_SEASON = _schedule([
    (3, "REG", 20, 17), (4, "REG", 24, 10), (4, "REG", 13, 9),
    (5, "REG", None, None), (6, "REG", None, None),
])


def _resolve(season, frames, sleeps=None, **kwargs):
    """Runs resolve_upcoming_week against a load_schedules that yields each item in
    `frames` in turn (an Exception item is raised)."""
    sleeps = [] if sleeps is None else sleeps
    with patch("src.nfl_schedule.nfl.load_schedules", side_effect=frames) as mock_load:
        result = ns.resolve_upcoming_week(season, sleep=sleeps.append, **kwargs)
    return result, mock_load


def test_upcoming_week_is_the_earliest_unfinished_regular_season_week():
    result, mock_load = _resolve(2026, [MID_SEASON])
    assert result == 5
    mock_load.assert_called_once_with(2026)


def test_upcoming_week_stays_on_a_partly_played_week():
    # Thursday night is final but Sunday/Monday aren't: still the same week.
    df = _schedule([(4, "REG", 24, 10), (5, "REG", 21, 14), (5, "REG", None, None)])
    assert _resolve(2026, [df])[0] == 5


def test_upcoming_week_is_one_before_kickoff():
    df = _schedule([(1, "REG", None, None), (2, "REG", None, None)])
    assert _resolve(2026, [df])[0] == 1


def test_upcoming_week_ignores_postseason_games():
    df = _schedule([(18, "REG", 20, 17), (19, "WC", None, None)], season=2025)
    with patch("src.nfl_schedule.nfl.get_current_season", return_value=2025):
        assert _resolve(2025, [df])[0] is None


def test_season_over_is_none_while_it_is_still_the_current_season():
    df = _schedule([(17, "REG", 20, 17), (18, "REG", 24, 10)], season=2026)
    with patch("src.nfl_schedule.nfl.get_current_season", return_value=2026):
        assert _resolve(2026, [df])[0] is None


def test_season_over_raises_when_a_later_season_has_started():
    df = _schedule([(18, "REG", 20, 17)], season=2026)
    with patch("src.nfl_schedule.nfl.get_current_season", return_value=2027):
        with pytest.raises(ns.WeekResolutionError, match="2027"):
            _resolve(2026, [df])


def test_missing_season_schedule_raises():
    with pytest.raises(ns.WeekResolutionError, match="2027"):
        _resolve(2027, [_schedule([])])


def test_transient_download_failure_is_retried_with_backoff():
    sleeps = []
    result, mock_load = _resolve(
        2026, [ConnectionError("404"), ConnectionError("reset"), MID_SEASON], sleeps=sleeps,
    )
    assert result == 5
    assert mock_load.call_count == 3
    assert sleeps == [5.0, 10.0]


def test_persistent_download_failure_raises_instead_of_returning_a_week():
    sleeps = []
    with pytest.raises(ns.WeekResolutionError, match="6 attempt"):
        _resolve(2026, [ConnectionError("404")] * 6, sleeps=sleeps)
    assert sleeps == [5.0, 10.0, 20.0, 30.0, 30.0]


def test_single_attempt_never_sleeps():
    sleeps = []
    with pytest.raises(ns.WeekResolutionError):
        _resolve(2026, [ConnectionError("404")], sleeps=sleeps, attempts=1)
    assert sleeps == []


def test_require_upcoming_week_raises_when_season_is_over():
    df = _schedule([(18, "REG", 20, 17)])
    with patch("src.nfl_schedule.nfl.load_schedules", return_value=df), \
         patch("src.nfl_schedule.nfl.get_current_season", return_value=2026):
        with pytest.raises(ns.RegularSeasonOverError):
            ns.require_upcoming_week(2026, attempts=1)


def test_require_upcoming_week_returns_the_week():
    with patch("src.nfl_schedule.nfl.load_schedules", return_value=MID_SEASON):
        assert ns.require_upcoming_week(2026, attempts=1) == 5
