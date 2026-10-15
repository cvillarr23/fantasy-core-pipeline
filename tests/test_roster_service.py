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


def test_get_team_roster_players_splits_starters_and_bench_sorted_by_vorp():
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("p1", "Low Starter", "WR", "KC", None, "WR", True, 1.5, 9.0),
        ("p2", "High Starter", "RB", "SF", "Questionable", "RB", True, 8.0, 17.2),
        ("p3", "Bye Starter", "TE", "DET", None, "TE", True, None, None),
        ("p4", "Bench Guy", "WR", "NYJ", None, "BE", False, -0.5, 6.1),
    ]

    with patch("src.services.roster_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = roster_service.get_team_roster_players("10", season=2026, week=3)

    assert cursor.execute.call_args.args[1] == (2026, 3, "10")
    assert result["team_id"] == "10"
    assert result["roster_snapshot"] == "current"
    # Highest VORP first; a missing weekly_stats row sorts last instead of as 0.0.
    assert [p["name"] for p in result["starters"]] == ["High Starter", "Low Starter", "Bye Starter"]
    assert [p["name"] for p in result["bench"]] == ["Bench Guy"]
    assert result["starters"][0] == {
        "player_id": "p2", "name": "High Starter", "position": "RB", "nfl_team": "SF", "bye_week": None,
        "slot": "RB", "injury_status": "Questionable", "vorp_score": 8.0, "projected_points": 17.2,
    }
    bye = result["starters"][2]
    assert bye["vorp_score"] is None and bye["projected_points"] is None


def test_get_team_roster_players_falls_back_to_slot_when_is_starter_is_null():
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("p1", "Legacy Starter", "QB", "BUF", None, "QB", None, 5.0, 20.0),
        ("p2", "Legacy Bench", "RB", "MIA", None, "BE", None, 1.0, 8.0),
        ("p3", "Legacy IR", "WR", "LAR", "Out", "IR", None, None, None),
    ]

    with patch("src.services.roster_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = roster_service.get_team_roster_players("10", season=2026, week=3)

    assert [p["name"] for p in result["starters"]] == ["Legacy Starter"]
    assert [p["name"] for p in result["bench"]] == ["Legacy Bench", "Legacy IR"]


def test_get_team_roster_players_unknown_team_returns_empty_lists():
    cursor = MagicMock()
    cursor.fetchall.return_value = []

    with patch("src.services.roster_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = roster_service.get_team_roster_players("nope", season=2026, week=3)

    assert result["starters"] == [] and result["bench"] == []
