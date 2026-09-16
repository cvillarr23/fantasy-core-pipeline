from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from src.services import matchup_service


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


def test_headshot_url_builds_espn_cdn_path_from_espn_id():
    assert (
        matchup_service._headshot_url("4240657")
        == "https://a.espncdn.com/i/headshots/nfl/players/full/4240657.png"
    )


def _make_box_player(playerId=4240657, name="Test Player", position="RB", jersey="22",
                      slot_position="RB", points=14.5, projected_points=11.2):
    bp = MagicMock()
    bp.playerId = playerId
    bp.name = name
    bp.position = position
    bp.jersey = jersey
    bp.slot_position = slot_position
    bp.points = points
    bp.projected_points = projected_points
    return bp


def test_shape_player_line_maps_box_player_fields():
    line = matchup_service._shape_player_line(_make_box_player())

    assert line == {
        "player_id": "4240657",
        "espn_id": "4240657",
        "name": "Test Player",
        "position": "RB",
        "jersey": "22",
        "slot": "RB",
        "points": 14.5,
        "projected_points": 11.2,
        "headshot_url": "https://a.espncdn.com/i/headshots/nfl/players/full/4240657.png",
        "deficit_flag": False,
        "suggested_swap": None,
    }


def test_split_starters_bench_separates_by_slot():
    starter = _make_box_player(playerId=1, slot_position="RB")
    bench = _make_box_player(playerId=2, slot_position="BE")
    ir = _make_box_player(playerId=3, slot_position="IR")

    starters, bench_list = matchup_service._split_starters_bench([starter, bench, ir])

    assert [p["player_id"] for p in starters] == ["1"]
    assert [p["player_id"] for p in bench_list] == ["2", "3"]


def test_apply_badges_flags_deficit_position():
    lines = [matchup_service._shape_player_line(_make_box_player(playerId=1, position="RB"))]
    deficits_by_position = {"RB": {"deficit": 2}, "WR": {"deficit": 0}}

    matchup_service._apply_badges(lines, deficits_by_position, {})

    assert lines[0]["deficit_flag"] is True


def test_apply_badges_does_not_flag_position_with_no_deficit():
    lines = [matchup_service._shape_player_line(_make_box_player(playerId=1, position="WR"))]
    deficits_by_position = {"WR": {"deficit": 0}}

    matchup_service._apply_badges(lines, deficits_by_position, {})

    assert lines[0]["deficit_flag"] is False


def test_apply_badges_sets_suggested_swap_from_map():
    lines = [matchup_service._shape_player_line(_make_box_player(playerId=1, position="RB"))]
    swap_suggestions = {"1": {"bench_for": "Better Back"}}

    matchup_service._apply_badges(lines, {}, swap_suggestions)

    assert lines[0]["suggested_swap"] == {"bench_for": "Better Back"}


def test_suggest_lineup_swaps_maps_drop_id_to_add_players_name():
    cursor = MagicMock()

    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.matchup_service.load_roster_pool") as mock_pool, \
         patch("src.services.matchup_service.load_lineup_slot_counts") as mock_slots, \
         patch("src.services.matchup_service.build_and_solve_lineup") as mock_solve, \
         patch("src.services.matchup_service.load_current_starters") as mock_current, \
         patch("src.services.matchup_service.compute_lineup_swaps") as mock_swaps, \
         patch("src.optimization.lineup_solver.publish_lineup_swaps") as mock_publish:
        mock_pool.return_value = [
            {"player_id": "add1", "name": "Better Back", "position": "RB", "vorp_score": 10.0},
        ]
        mock_slots.return_value = {"RB": 2}
        mock_solve.return_value = [{"player_id": "add1", "name": "Better Back", "position": "RB"}]
        mock_current.return_value = {"drop1": {"position": "RB", "slot_position": "RB"}}
        mock_swaps.return_value = [
            {"drop_player_id": "drop1", "add_player_id": "add1", "slot": "RB", "matchup_multiplier": 1.0},
        ]

        result = matchup_service.suggest_lineup_swaps("team1", 2026, 2)

    assert result == {"drop1": {"bench_for": "Better Back"}}
    mock_publish.assert_not_called()


def _make_box_score(home_team, away_team, home_score=100.0, away_score=90.0,
                     home_projected=95.0, away_projected=92.0, home_lineup=None, away_lineup=None):
    box = MagicMock()
    box.home_team = home_team
    box.away_team = away_team
    box.home_score = home_score
    box.away_score = away_score
    box.home_projected = home_projected
    box.away_projected = away_projected
    box.home_lineup = home_lineup or []
    box.away_lineup = away_lineup or []
    return box


def _make_team(team_id=8, team_name="Our Team", logo_url="https://logo"):
    team = MagicMock()
    team.team_id = team_id
    team.team_name = team_name
    team.logo_url = logo_url
    return team


@contextmanager
def _no_op_cache():
    matchup_service._MATCHUP_CACHE.clear()
    yield
    matchup_service._MATCHUP_CACHE.clear()


def test_get_current_team_matchup_shapes_both_sides_and_badges_our_starters_only(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    our_starter = _make_box_player(playerId=1, position="RB", slot_position="RB")
    opp_starter = _make_box_player(playerId=2, position="RB", slot_position="RB")
    box = _make_box_score(
        home_team=_make_team(team_id=8, team_name="Our Team"),
        away_team=_make_team(team_id=9, team_name="Their Team"),
        home_lineup=[our_starter],
        away_lineup=[opp_starter],
    )
    mock_league = MagicMock()
    mock_league.box_scores.return_value = [box]

    with _no_op_cache(), \
         patch("src.services.matchup_service.build_league", return_value=mock_league), \
         patch("src.services.matchup_service.compute_roster_deficits",
               return_value={"teams": {"8": {"RB": {"deficit": 1}}}}), \
         patch("src.services.matchup_service.suggest_lineup_swaps", return_value={}):
        result = matchup_service.get_current_team_matchup(season=2026, week=2)

    assert result["our_team"]["team"]["name"] == "Our Team"
    assert result["opponent"]["team"]["name"] == "Their Team"
    assert result["our_team"]["starters"][0]["deficit_flag"] is True
    # Opponent starter is also RB (same deficit-flagged position for OUR team), but
    # deficit badges are our-team-only automation - the opponent must never get one.
    assert result["opponent"]["starters"][0]["deficit_flag"] is False


def test_get_current_team_matchup_raises_when_our_team_in_no_box_score(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    box = _make_box_score(home_team=_make_team(team_id=1), away_team=_make_team(team_id=2))
    mock_league = MagicMock()
    mock_league.box_scores.return_value = [box]

    with _no_op_cache(), patch("src.services.matchup_service.build_league", return_value=mock_league):
        with pytest.raises(matchup_service.NoMatchupFoundError):
            matchup_service.get_current_team_matchup(season=2026, week=2)


def test_get_current_team_matchup_raises_on_bye_week_when_opposing_side_is_none(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    box = _make_box_score(home_team=_make_team(team_id=8), away_team=None)
    mock_league = MagicMock()
    mock_league.box_scores.return_value = [box]

    with _no_op_cache(), patch("src.services.matchup_service.build_league", return_value=mock_league):
        with pytest.raises(matchup_service.NoMatchupFoundError):
            matchup_service.get_current_team_matchup(season=2026, week=2)


def test_get_current_team_matchup_caches_within_ttl(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    box = _make_box_score(home_team=_make_team(team_id=8), away_team=_make_team(team_id=9))
    mock_league = MagicMock()
    mock_league.box_scores.return_value = [box]

    with _no_op_cache(), \
         patch("src.services.matchup_service.build_league", return_value=mock_league) as mock_build, \
         patch("src.services.matchup_service.compute_roster_deficits", return_value={"teams": {}}), \
         patch("src.services.matchup_service.suggest_lineup_swaps", return_value={}):
        matchup_service.get_current_team_matchup(season=2026, week=2)
        matchup_service.get_current_team_matchup(season=2026, week=2)

    mock_build.assert_called_once()
