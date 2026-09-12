from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from src.services import roster_service

LEAGUE_SETTINGS_ROW = (10, {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 7})


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


def test_compute_roster_deficits_flags_positions_below_starter_requirement():
    cursor = MagicMock()
    cursor.fetchone.return_value = LEAGUE_SETTINGS_ROW
    cursor.fetchall.side_effect = [
        [("team1",), ("team2",)],  # SELECT DISTINCT team_id
        [
            ("team1", "RB", 1, 12.5),
            ("team1", "QB", 1, 3.0),
            ("team2", "WR", 4, -2.0),
        ],
    ]

    with patch("src.services.roster_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = roster_service.compute_roster_deficits(season=2026, week=3)

    assert result["season"] == 2026
    assert result["week"] == 3
    assert result["roster_snapshot"] == "current"
    # Starting requirement for RB is 3 (2 starting slots + this league's FLEX share),
    # so a team with only 1 rostered RB is short by 2.
    assert result["teams"]["team1"]["RB"] == {
        "rostered": 1,
        "starters_required": 3,
        "deficit": 2,
        "net_vorp": 12.5,
    }
    # QB has no FLEX/bench share, so 1 rostered QB exactly meets the 1-QB requirement.
    assert result["teams"]["team1"]["QB"] == {
        "rostered": 1,
        "starters_required": 1,
        "deficit": 0,
        "net_vorp": 3.0,
    }
    # A team can be over-rostered (no deficit) while still carrying negative net VORP.
    assert result["teams"]["team2"]["WR"]["deficit"] == 0
    assert result["teams"]["team2"]["WR"]["net_vorp"] == -2.0


def test_compute_roster_deficits_reports_zero_rostered_positions_as_full_deficit():
    # team2 has rostered players (it's in the distinct team_id list) but none at any
    # skill position - the grouped query returns nothing for it at all. Every position
    # must still appear for team2, showing the maximum possible deficit, not vanish.
    cursor = MagicMock()
    cursor.fetchone.return_value = LEAGUE_SETTINGS_ROW
    cursor.fetchall.side_effect = [
        [("team1",), ("team2",)],
        [("team1", "RB", 1, 12.5)],
    ]

    with patch("src.services.roster_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = roster_service.compute_roster_deficits(season=2026, week=3)

    assert set(result["teams"]["team2"].keys()) == {"QB", "RB", "WR", "TE"}
    assert result["teams"]["team2"]["RB"] == {
        "rostered": 0,
        "starters_required": 3,
        "deficit": 3,
        "net_vorp": 0.0,
    }
    assert result["teams"]["team2"]["QB"]["deficit"] == 1


def test_compute_roster_deficits_queries_scoped_to_season_and_week():
    cursor = MagicMock()
    cursor.fetchone.return_value = LEAGUE_SETTINGS_ROW
    cursor.fetchall.side_effect = [[], []]

    with patch("src.services.roster_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        roster_service.compute_roster_deficits(season=2026, week=5)

    roster_query_call = cursor.execute.call_args_list[-1]
    params = roster_query_call.args[1]
    assert params[0] == 2026
    assert params[1] == 5
