import pytest

from src.league_schedule import playoff_settings, playoff_weeks, trade_deadline


def raw(team_count=6, length=1, regular=14, final=17, periods=None):
    schedule = {
        "matchupPeriodCount": regular,
        "matchupPeriods": periods or {str(w): [w] for w in range(1, final + 1)},
    }
    if team_count is not None:
        schedule["playoffTeamCount"] = team_count
    if length is not None:
        schedule["playoffMatchupPeriodLength"] = length
    return {
        "status": {"finalScoringPeriod": final},
        "settings": {
            "scheduleSettings": schedule,
            "tradeSettings": {"deadlineDate": 1796313600000},
        },
    }


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        # This league: 14 regular periods, 6 playoff teams (byes for the top 2), 1-week rounds.
        ({}, [15, 16, 17]),
        ({"team_count": 4, "final": 16}, [15, 16]),
        ({"team_count": 8}, [15, 16, 17]),
        ({"team_count": 2, "final": 15}, [15]),
        ({"length": 2, "regular": 12, "final": 17}, [13, 14, 15, 16, 17]),
        # Capped at ESPN's final scoring period.
        ({"final": 16}, [15, 16]),
        ({"team_count": None}, []),
        ({"length": None}, []),
        ({"team_count": 1}, []),
        ({"length": 0}, []),
    ],
)
def test_playoff_weeks(kwargs, expected):
    assert playoff_weeks(raw(**kwargs)) == expected


def test_regular_season_end_comes_from_the_last_regular_period():
    periods = {"1": [1, 2], "2": [3, 4], "3": [5, 6], "4": [7], "5": [8]}
    assert playoff_weeks(raw(regular=3, team_count=4, final=8, periods=periods)) == [7, 8]


@pytest.mark.parametrize("payload", [{}, {"settings": {}}, {"settings": {"scheduleSettings": None}}])
def test_missing_settings_mean_no_playoff_weeks(payload):
    assert playoff_weeks(payload) == []
    assert playoff_settings(payload)["playoff_weeks"] == []


def test_trade_deadline_is_iso_utc():
    assert trade_deadline(raw()) == "2026-12-03T16:00:00+00:00"
    assert trade_deadline({"settings": {"tradeSettings": {"deadlineDate": 0}}}) is None
    assert trade_deadline({}) is None


def test_playoff_settings_payload():
    assert playoff_settings(raw()) == {
        "regular_season_end_week": 14,
        "playoff_team_count": 6,
        "playoff_matchup_period_length": 1,
        "playoff_weeks": [15, 16, 17],
        "trade_deadline": "2026-12-03T16:00:00+00:00",
    }
