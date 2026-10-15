from unittest.mock import patch

import polars as pl
import pytest

from src import nfl_byes


def _games(rows, season=2026):
    """rows: (week, home, away, game_type) tuples."""
    return pl.DataFrame(
        {
            "season": [season] * len(rows),
            "week": [r[0] for r in rows],
            "home_team": [r[1] for r in rows],
            "away_team": [r[2] for r in rows],
            "game_type": [r[3] if len(r) > 3 else "REG" for r in rows],
        }
    )


SCHEDULE = _games([
    (1, "BUF", "KC"), (1, "SF", "LA"),
    (2, "BUF", "SF"),                      # KC and LA idle
    (3, "KC", "LA"),                       # BUF and SF idle
    (19, "BUF", "KC", "WC"),               # postseason never counts
])


@pytest.fixture(autouse=True)
def _fresh_cache():
    nfl_byes.load_bye_weeks.cache_clear()
    yield
    nfl_byes.load_bye_weeks.cache_clear()


def test_bye_weeks_from_schedule_finds_each_teams_idle_regular_season_week():
    assert nfl_byes.bye_weeks_from_schedule(SCHEDULE) == {
        "BUF": 3, "SF": 3, "KC": 2, "LAR": 2,
    }


def test_bye_weeks_from_schedule_is_empty_without_regular_season_games():
    assert nfl_byes.bye_weeks_from_schedule(SCHEDULE.filter(pl.col("game_type") != "REG")) == {}


def test_bye_week_for_maps_espn_and_legacy_codes():
    byes = {"WAS": 12, "LAR": 8}

    assert nfl_byes.bye_week_for("WSH", byes) == 12
    assert nfl_byes.bye_week_for("LA", byes) == 8
    assert nfl_byes.bye_week_for("NYJ", byes) is None
    assert nfl_byes.bye_week_for(None, byes) is None


def test_load_bye_weeks_reads_the_schedule_once_per_season():
    with patch("src.nfl_byes.load_schedule", return_value=SCHEDULE) as mock_load:
        assert nfl_byes.load_bye_weeks(2026)["KC"] == 2
        assert nfl_byes.load_bye_weeks(2026)["BUF"] == 3

    mock_load.assert_called_once_with(2026, attempts=1)


def test_load_bye_weeks_does_not_cache_a_failed_read():
    with patch("src.nfl_byes.load_schedule", side_effect=[RuntimeError("404"), SCHEDULE]):
        with pytest.raises(RuntimeError):
            nfl_byes.load_bye_weeks(2026)
        assert nfl_byes.load_bye_weeks(2026)["SF"] == 3


def test_bye_weeks_or_empty_swallows_schedule_failures():
    with patch("src.nfl_byes.load_schedule", side_effect=RuntimeError("down")):
        assert nfl_byes.bye_weeks_or_empty(2026) == {}


def test_teams_playing_from_byes_leaves_out_only_that_weeks_bye_teams():
    byes = {"BUF": 3, "SF": 3, "KC": 2}

    assert nfl_byes.teams_playing_from_byes(byes, 3) == {"KC"}
    assert nfl_byes.teams_playing_from_byes({}, 3) is None
