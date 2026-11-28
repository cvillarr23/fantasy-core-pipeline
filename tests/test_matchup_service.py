from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.optimization.lineup_solver import InsufficientLineupInputError
from src.services import matchup_service


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


@pytest.fixture(autouse=True)
def no_scoreboard(monkeypatch):
    """Game status reads ESPN's public scoreboard; unit tests never reach the network."""
    monkeypatch.setattr(matchup_service, "load_team_game_status", lambda season, week: None)


@pytest.mark.parametrize("service_name", ["get_current_team_matchup", "get_league_scoreboard"])
@pytest.mark.parametrize("failure_point", ["build", "box_scores"])
def test_espn_timeout_is_specific_to_espn_calls(service_name, failure_point):
    mock_league = MagicMock()
    if failure_point == "box_scores":
        mock_league.box_scores.side_effect = requests.Timeout("ESPN stalled")
        build_effect = mock_league
    else:
        build_effect = requests.Timeout("ESPN stalled")

    with patch("src.services.matchup_service.build_league", side_effect=[build_effect]) as build:
        with pytest.raises(matchup_service.ESPNTimeoutError):
            getattr(matchup_service, service_name)(2026, 2)

    build.assert_called_once_with(2026, request_timeout_seconds=matchup_service.ESPN_REQUEST_TIMEOUT_SECONDS)


def test_headshot_url_builds_espn_cdn_path_from_espn_id():
    assert (
        matchup_service._headshot_url("4240657")
        == "https://a.espncdn.com/i/headshots/nfl/players/full/4240657.png"
    )


def _make_box_player(playerId=4240657, name="Test Player", position="RB", jersey="22",
                      slot_position="RB", points=14.5, projected_points=11.2, proTeam=None):
    bp = MagicMock()
    bp.proTeam = proTeam
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
        "pro_team": None,
        "points": 14.5,
        "projected_points": 11.2,
        "bye_week": None,
        "game_status": None,
        "headshot_url": "https://a.espncdn.com/i/headshots/nfl/players/full/4240657.png",
        "deficit_flag": False,
        "suggested_swap": None,
    }


def test_shape_player_line_carries_the_status_of_the_players_nfl_game():
    statuses = {"WAS": "in_progress", "DAL": "final"}

    live = matchup_service._shape_player_line(_make_box_player(proTeam="WSH"), game_statuses=statuses)
    done = matchup_service._shape_player_line(_make_box_player(proTeam="DAL"), game_statuses=statuses)

    assert (live["game_status"], done["game_status"]) == ("in_progress", "final")


def test_shape_player_line_carries_the_nflverse_team_code():
    """The live game blocks use nflverse codes, so the line's team must too for vs/@ and W/L."""
    line = matchup_service._shape_player_line(_make_box_player(proTeam="WSH"))

    assert line["pro_team"] == "WAS"


def test_shape_player_line_has_no_game_status_for_a_team_with_no_game_this_week():
    """A bye-week team is absent from the scoreboard; the line's bye_week says why."""
    line = matchup_service._shape_player_line(_make_box_player(proTeam="NYJ"), game_statuses={"DAL": "final"})

    assert line["game_status"] is None


def test_shape_player_line_has_no_game_status_when_the_scoreboard_is_unavailable():
    line = matchup_service._shape_player_line(_make_box_player(proTeam="DAL"), game_statuses=None)

    assert line["game_status"] is None


def test_shape_player_line_coerces_non_string_jersey_to_none():
    """ESPN returns jersey as an empty list (not a string) for D/ST 'players' - live
    /v1/matchup 500'd with a pydantic validation error on this exact case (playerId 7,
    position D/ST) before this coercion existed."""
    line = matchup_service._shape_player_line(_make_box_player(position="D/ST", jersey=[]))

    assert line["jersey"] is None


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


def _make_roster_player(playerId=5000001, name="Bench Guy", position="WR", jersey="12",
                         lineupSlot="BE", proTeam="DET", injuryStatus="ACTIVE"):
    p = MagicMock()
    p.playerId = playerId
    p.name = name
    p.position = position
    p.jersey = jersey
    p.lineupSlot = lineupSlot
    p.proTeam = proTeam
    p.injuryStatus = injuryStatus
    return p


def test_shape_lineup_player_line_maps_roster_player_fields():
    line = matchup_service._shape_lineup_player_line(_make_roster_player())

    assert line == {
        "player_id": "5000001",
        "espn_id": "5000001",
        "name": "Bench Guy",
        "position": "WR",
        "jersey": "12",
        "slot": "BE",
        "pro_team": "DET",
        "injury_status": "ACTIVE",
        "bye_week": None,
        "headshot_url": "https://a.espncdn.com/i/headshots/nfl/players/full/5000001.png",
        "deficit_flag": False,
        "suggested_swap": None,
        "net_vorp": None,
        "projected_points": None,
        "rumor_sentiment": None,
        "rumor_confidence": None,
        "rumor_suggested_action": None,
        "ir_suggestion": None,
    }


def test_shape_lineup_player_line_coerces_non_string_jersey_to_none():
    line = matchup_service._shape_lineup_player_line(_make_roster_player(jersey=[]))

    assert line["jersey"] is None


def test_split_roster_starters_bench_separates_by_lineup_slot():
    starter = _make_roster_player(playerId=1, lineupSlot="RB")
    bench = _make_roster_player(playerId=2, lineupSlot="BE")
    ir = _make_roster_player(playerId=3, lineupSlot="IR")

    starters, bench_list = matchup_service._split_roster_starters_bench([starter, bench, ir])

    assert [p["player_id"] for p in starters] == ["1"]
    assert [p["player_id"] for p in bench_list] == ["2", "3"]


def test_load_weekly_metrics_by_espn_id_maps_query_rows():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("100", 12.5, 18.2), ("200", None, 7.5), ("300", None, None), ("400", -3.2, 0.0)]

    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = matchup_service._load_weekly_metrics_by_espn_id(season=2026, week=2, team_id="8")

    assert result == {
        "100": {"net_vorp": 12.5, "projected_points": 18.2},
        "200": {"net_vorp": None, "projected_points": 7.5},
        "300": {"net_vorp": None, "projected_points": None},
        "400": {"net_vorp": -3.2, "projected_points": 0.0},
    }
    query = " ".join(cursor.execute.call_args[0][0].split())
    assert "LEFT JOIN weekly_stats" in query
    assert "s.season = %s AND s.week = %s" in query
    assert "r.team_id = %s" in query
    cursor.execute.assert_called_once()
    query_args = cursor.execute.call_args[0][1]
    assert query_args == (2026, 2, "8")


def test_load_espn_ownership_by_espn_id_keys_loader_result_by_espn_id():
    cursor = MagicMock()
    # First fetchall: roster player_id -> espn_id; second: the loader's ownership rows.
    cursor.fetchall.side_effect = [[("p1", "100"), ("p2", "200")], [("p1", 4, 70.0, 40.0, 2.0)]]

    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = matchup_service._load_espn_ownership_by_espn_id(season=2026, week=2, team_id="8")

    assert list(result) == ["100"]
    assert result["100"].pos_rank == 4 and result["100"].percent_bench == 30.0
    assert cursor.execute.call_args_list[0].args[1] == ("8",)
    assert cursor.execute.call_args_list[1].args[1] == (["p1", "p2"], 2026, 2)


def test_apply_espn_ownership_leaves_unknown_players_none():
    from src.schemas.espn_ownership import EspnOwnership

    espn = EspnOwnership(pos_rank=1)
    lines = [{"espn_id": "100"}, {"espn_id": "999"}]
    matchup_service._apply_espn_ownership(lines, {"100": espn})
    assert lines == [{"espn_id": "100", "espn": espn}, {"espn_id": "999", "espn": None}]


def test_apply_weekly_metrics_sets_values_for_known_players_only():
    lines = [
        matchup_service._shape_lineup_player_line(_make_roster_player(playerId=1)),
        matchup_service._shape_lineup_player_line(_make_roster_player(playerId=2)),
    ]
    metrics_by_espn_id = {"1": {"net_vorp": 8.4, "projected_points": 12.0}}

    matchup_service._apply_weekly_metrics(lines, metrics_by_espn_id)

    assert lines[0]["net_vorp"] == 8.4
    assert lines[1]["net_vorp"] is None
    assert lines[0]["projected_points"] == 12.0
    assert lines[1]["projected_points"] is None


def test_load_rumor_signal_by_espn_id_maps_query_rows():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("100", "positive", 0.9, "bench_watch")]

    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = matchup_service._load_rumor_signal_by_espn_id(team_id="8")

    assert result == {"100": {"sentiment": "positive", "confidence": 0.9, "suggested_action": "bench_watch"}}
    cursor.execute.assert_called_once()
    query_args = cursor.execute.call_args[0][1]
    assert query_args == ("8",)


def test_apply_rumor_signal_sets_fields_for_known_players_only():
    lines = [
        matchup_service._shape_lineup_player_line(_make_roster_player(playerId=1)),
        matchup_service._shape_lineup_player_line(_make_roster_player(playerId=2)),
    ]
    signal_by_espn_id = {"1": {"sentiment": "negative", "confidence": 0.8, "suggested_action": "bench_watch"}}

    matchup_service._apply_rumor_signal(lines, signal_by_espn_id)

    assert lines[0]["rumor_sentiment"] == "negative"
    assert lines[0]["rumor_confidence"] == 0.8
    assert lines[0]["rumor_suggested_action"] == "bench_watch"
    assert lines[1]["rumor_sentiment"] is None
    assert lines[1]["rumor_confidence"] is None
    assert lines[1]["rumor_suggested_action"] is None


def test_suggest_lineup_swaps_maps_drop_id_to_add_players_name():
    """Uses lineup_solver's pregame evaluation (#178 follow-up) - the exact-week pool it
    used before is empty until the target week's projections land, so the badge vanished."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [("drop1", "5001"), ("add1", "5002")]
    evaluation = {
        "swaps": [{"drop_player_id": "drop1", "add_player_id": "add1", "slot": "RB", "matchup_multiplier": 1.0}],
        "roster_pool": [{"player_id": "add1", "name": "Better Back", "position": "RB", "vorp_score": 10.0}],
        "optimal_starters": [], "source": {},
    }

    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.matchup_service.evaluate_lineup", return_value=evaluation) as mock_evaluate, \
         patch("src.optimization.lineup_solver.publish_lineup_swaps") as mock_publish:
        result = matchup_service.suggest_lineup_swaps("team1", 2026, 2)

    assert result == {"5001": {"bench_for": "Better Back", "add_player_id": "5002", "slot": "RB", "moves": []}}
    mock_publish.assert_not_called()
    mock_evaluate.assert_called_once_with(cursor, "team1", 2026, 2)


def test_suggest_lineup_swaps_skips_swap_without_espn_id(caplog):
    cursor = MagicMock()
    cursor.fetchall.return_value = [("drop1", "5001")]
    evaluation = {
        "swaps": [{"drop_player_id": "drop1", "add_player_id": "add1", "slot": "RB", "matchup_multiplier": 1.0}],
        "roster_pool": [{"player_id": "add1", "name": "Better Back", "position": "RB", "vorp_score": 10.0}],
        "optimal_starters": [], "source": {},
    }

    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.matchup_service.evaluate_lineup", return_value=evaluation):
        result = matchup_service.suggest_lineup_swaps("team1", 2026, 2)

    assert result == {}
    assert "no ESPN id" in caplog.text


def test_suggest_lineup_swap_lands_on_the_line_with_the_bench_players_espn_id():
    """Internal ids (sleeper-style) differ from ESPN's: the badge must still hit the starter's
    line and add_player_id must equal the bench line's player_id."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [("internal-drop", "4001"), ("internal-add", "4002")]
    evaluation = {
        "swaps": [{"drop_player_id": "internal-drop", "add_player_id": "internal-add", "slot": "RB", "matchup_multiplier": 1.0}],
        "roster_pool": [{"player_id": "internal-add", "name": "Bench Back", "position": "RB", "vorp_score": 10.0}],
        "optimal_starters": [], "source": {},
    }
    starter = matchup_service._shape_player_line(_make_box_player(playerId=4001, position="RB"))
    bench = matchup_service._shape_player_line(_make_box_player(playerId=4002, position="RB"))

    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.matchup_service.evaluate_lineup", return_value=evaluation):
        swaps = matchup_service.suggest_lineup_swaps("team1", 2026, 2)
    matchup_service._apply_badges([starter], {}, swaps)

    assert starter["suggested_swap"] == {
        "bench_for": "Bench Back", "add_player_id": bench["player_id"], "slot": "RB", "moves": [],
    }


def test_suggest_lineup_swaps_shows_no_badges_on_insufficient_input(caplog):
    """Display-only: missing lineup inputs must not 500 the whole matchup page."""
    cursor = MagicMock()
    error = InsufficientLineupInputError("no usable weekly_stats.vorp_score")

    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.matchup_service.evaluate_lineup", side_effect=error):
        result = matchup_service.suggest_lineup_swaps("team1", 2026, 2)

    assert result == {}
    assert "no usable weekly_stats.vorp_score" in caplog.text


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

    with patch("src.services.matchup_service.build_league", return_value=mock_league), \
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

    with patch("src.services.matchup_service.build_league", return_value=mock_league):
        with pytest.raises(matchup_service.NoMatchupFoundError):
            matchup_service.get_current_team_matchup(season=2026, week=2)


def test_get_current_team_matchup_raises_on_bye_week_when_opposing_side_is_none(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    box = _make_box_score(home_team=_make_team(team_id=8), away_team=None)
    mock_league = MagicMock()
    mock_league.box_scores.return_value = [box]

    with patch("src.services.matchup_service.build_league", return_value=mock_league):
        with pytest.raises(matchup_service.NoMatchupFoundError):
            matchup_service.get_current_team_matchup(season=2026, week=2)


def test_get_current_team_matchup_caches_within_ttl(monkeypatch, read_cache_redis):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    box = _make_box_score(home_team=_make_team(team_id=8), away_team=_make_team(team_id=9))
    mock_league = MagicMock()
    mock_league.box_scores.return_value = [box]

    with patch("src.services.matchup_service.build_league", return_value=mock_league) as mock_build, \
         patch("src.services.matchup_service.compute_roster_deficits", return_value={"teams": {}}), \
         patch("src.services.matchup_service.suggest_lineup_swaps", return_value={}):
        matchup_service.get_current_team_matchup(season=2026, week=2)
        matchup_service.get_current_team_matchup(season=2026, week=2)

    mock_build.assert_called_once()


def test_get_current_team_roster_shapes_and_badges(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    our_team = _make_team(team_id=8, team_name="Our Team")
    our_team.roster = [
        _make_roster_player(playerId=1, position="RB", lineupSlot="RB"),
        _make_roster_player(playerId=2, position="RB", lineupSlot="BE"),
    ]
    other_team = _make_team(team_id=9, team_name="Other Team")
    other_team.roster = []
    mock_league = MagicMock()
    mock_league.teams = [other_team, our_team]

    with patch("src.services.matchup_service.build_league", return_value=mock_league), \
         patch("src.services.matchup_service.require_upcoming_week", return_value=2), \
         patch("src.services.matchup_service.compute_roster_deficits",
               return_value={"teams": {"8": {"RB": {"deficit": 1}}}}), \
         patch("src.services.matchup_service.suggest_lineup_swaps", return_value={}), \
         patch("src.services.matchup_service._load_weekly_metrics_by_espn_id", return_value={"1": {"net_vorp": 9.9, "projected_points": 15.0}}), \
         patch("src.services.matchup_service._load_rumor_signal_by_espn_id",
               return_value={"1": {"sentiment": "positive", "confidence": 0.85, "suggested_action": "bench_watch"}}):
        result = matchup_service.get_current_team_roster(season=2026)

    assert [p["player_id"] for p in result["starters"]] == ["1"]
    assert [p["player_id"] for p in result["bench"]] == ["2"]
    assert result["starters"][0]["deficit_flag"] is True
    assert result["starters"][0]["net_vorp"] == 9.9
    assert result["bench"][0]["net_vorp"] is None
    assert result["starters"][0]["rumor_sentiment"] == "positive"
    assert result["starters"][0]["rumor_confidence"] == 0.85
    assert result["bench"][0]["rumor_sentiment"] is None


def test_get_current_team_roster_raises_when_team_not_found(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    mock_league = MagicMock()
    mock_league.teams = [_make_team(team_id=9)]

    with patch("src.services.matchup_service.build_league", return_value=mock_league), \
         patch("src.services.matchup_service.require_upcoming_week", return_value=2):
        with pytest.raises(ValueError):
            matchup_service.get_current_team_roster(season=2026)


def test_get_current_team_roster_resolves_the_default_week_without_retry_sleeps(monkeypatch):
    """A web request must not wait out the launcher job's retry backoff, and an unresolvable week must
    propagate for the route to map rather than quietly showing another week."""
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    error = matchup_service.WeekResolutionError("download failed")
    with patch("src.services.matchup_service.require_upcoming_week", side_effect=error) as mock_resolve, \
         patch("src.services.matchup_service.build_league") as mock_build:
        with pytest.raises(matchup_service.WeekResolutionError):
            matchup_service.get_current_team_roster(season=2026)
    mock_resolve.assert_called_once_with(2026, attempts=1)
    mock_build.assert_not_called()



_SYNCED = object()


def _roster_with(players, ir_slot_count, monkeypatch, slot_counts=_SYNCED):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    our_team = _make_team(team_id=8, team_name="Our Team")
    our_team.roster = players
    mock_league = MagicMock()
    mock_league.teams = [our_team]
    with patch("src.services.matchup_service.build_league", return_value=mock_league), \
         patch("src.services.matchup_service.require_upcoming_week", return_value=2), \
         patch("src.services.matchup_service.compute_roster_deficits", return_value={"teams": {}}), \
         patch("src.services.matchup_service.suggest_lineup_swaps", return_value={}), \
         patch("src.services.matchup_service._load_weekly_metrics_by_espn_id", return_value={}), \
         patch("src.services.matchup_service._load_rumor_signal_by_espn_id", return_value={}), \
         patch("src.services.matchup_service._load_lineup_slot_counts",
               return_value={"BE": 7, "IR": ir_slot_count} if slot_counts is _SYNCED else slot_counts):
        return matchup_service.get_current_team_roster(season=2026)


def test_get_current_team_roster_suggests_moving_an_out_player_to_ir(monkeypatch):
    result = _roster_with([
        _make_roster_player(playerId=1, position="RB", lineupSlot="RB", injuryStatus="OUT"),
        _make_roster_player(playerId=2, position="WR", lineupSlot="BE", injuryStatus="ACTIVE"),
    ], ir_slot_count=1, monkeypatch=monkeypatch)

    assert result["starters"][0]["ir_suggestion"]["action"] == "move_to_ir"
    assert result["bench"][0]["ir_suggestion"] is None
    assert result["ir_blocking_transactions"] is False


def test_get_current_team_roster_flags_an_active_player_in_ir(monkeypatch):
    result = _roster_with([
        _make_roster_player(playerId=3, position="TE", lineupSlot="IR", injuryStatus="ACTIVE"),
    ], ir_slot_count=1, monkeypatch=monkeypatch)

    assert result["bench"][0]["ir_suggestion"]["action"] == "activate_from_ir"
    assert result["ir_blocking_transactions"] is True


def test_load_lineup_slot_counts_reads_league_settings_and_fails_soft():
    cursor = MagicMock()
    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.matchup_service.load_lineup_slot_counts_row",
               return_value={"QB": 1, "BE": 7, "IR": "1"}) as mock_counts:
        assert matchup_service._load_lineup_slot_counts(2026) == {"QB": 1, "BE": 7, "IR": 1}
    mock_counts.assert_called_once_with(cursor, 2026)
    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.matchup_service.load_lineup_slot_counts_row", return_value=None):
        assert matchup_service._load_lineup_slot_counts(2026) is None
    with patch("src.services.matchup_service.get_db_cursor", side_effect=RuntimeError("db down")):
        assert matchup_service._load_lineup_slot_counts(2026) is None


def test_get_current_team_roster_returns_lineup_slot_counts(monkeypatch):
    result = _roster_with([
        _make_roster_player(playerId=2, position="WR", lineupSlot="BE", injuryStatus="ACTIVE"),
    ], ir_slot_count=1, monkeypatch=monkeypatch)

    assert result["lineup_slot_counts"] == {"BE": 7, "IR": 1}


def test_get_current_team_roster_without_synced_settings_has_no_capacity_or_ir_hints(monkeypatch):
    """An unsynced league must not guess a shape: no capacity, and no IR slot to suggest."""
    result = _roster_with([
        _make_roster_player(playerId=1, position="RB", lineupSlot="RB", injuryStatus="OUT"),
    ], ir_slot_count=0, monkeypatch=monkeypatch, slot_counts=None)

    assert result["starters"][0]["ir_suggestion"] is None
    assert result["lineup_slot_counts"] is None

def test_lineup_line_carries_the_players_nfl_bye_week():
    line = matchup_service._shape_lineup_player_line(_make_roster_player(), {"DET": 8})

    assert line["bye_week"] == 8


def test_get_current_team_matchup_marks_starters_and_bench_of_both_sides_with_game_status(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    monkeypatch.setattr(
        matchup_service, "load_team_game_status",
        lambda season, week: {"DAL": "not_started", "PHI": "in_progress", "KC": "final"},
    )
    box = _make_box_score(
        home_team=_make_team(team_id=8),
        away_team=_make_team(team_id=9),
        home_lineup=[_make_box_player(playerId=1, proTeam="DAL"), _make_box_player(playerId=2, slot_position="BE", proTeam="KC")],
        away_lineup=[_make_box_player(playerId=3, proTeam="PHI")],
    )
    mock_league = MagicMock()
    mock_league.box_scores.return_value = [box]

    with patch("src.services.matchup_service.build_league", return_value=mock_league), \
         patch("src.services.matchup_service.compute_roster_deficits", return_value={"teams": {}}), \
         patch("src.services.matchup_service.suggest_lineup_swaps", return_value={}):
        result = matchup_service.get_current_team_matchup(season=2026, week=2)

    assert result["our_team"]["starters"][0]["game_status"] == "not_started"
    assert result["our_team"]["bench"][0]["game_status"] == "final"
    assert result["opponent"]["starters"][0]["game_status"] == "in_progress"


def _raw_schedule(*games):
    return {"schedule": [{"matchupPeriodId": 11, **game} for game in games]}


def test_win_probabilities_from_raw_maps_each_side_by_team_id():
    raw = _raw_schedule(
        {"home": {"teamId": 8, "winProbability": 0.34}, "away": {"teamId": 7, "winProbability": 0.66}},
        {"home": {"teamId": 3, "winProbability": 0.99}, "away": {"teamId": 5, "winProbability": 0.01}},
    )

    assert matchup_service._win_probabilities_from_raw(raw) == {"8": 0.34, "7": 0.66, "3": 0.99, "5": 0.01}


def test_win_probabilities_from_raw_skips_sides_without_a_value():
    """ESPN sends no winProbability for a finished or future matchup, and no away side on a bye."""
    raw = _raw_schedule(
        {"home": {"teamId": 1, "winProbability": None}, "away": {"teamId": 2}},
        {"home": {"teamId": 4}},
    )

    assert matchup_service._win_probabilities_from_raw(raw) == {}


def test_fetch_win_probabilities_asks_espn_for_the_weeks_matchup_period():
    requests_client = MagicMock()
    requests_client.league_get.return_value = _raw_schedule(
        {"home": {"teamId": 8, "winProbability": 0.4}, "away": {"teamId": 7, "winProbability": 0.6}}
    )

    with patch("src.services.matchup_service.build_espn_requests", return_value=requests_client) as build:
        result = matchup_service._fetch_win_probabilities(2026, 11)

    assert result == {"8": 0.4, "7": 0.6}
    build.assert_called_once_with(2026, request_timeout_seconds=matchup_service.ESPN_REQUEST_TIMEOUT_SECONDS)
    kwargs = requests_client.league_get.call_args.kwargs
    assert kwargs["params"]["view"] == ["mMatchupScore", "mScoreboard"]
    assert kwargs["params"]["scoringPeriodId"] == 11
    assert '"value": [11]' in kwargs["headers"]["x-fantasy-filter"]


def test_fetch_win_probabilities_is_unknown_when_espn_fails(caplog):
    with patch("src.services.matchup_service.build_espn_requests", side_effect=requests.Timeout("slow")):
        assert matchup_service._fetch_win_probabilities(2026, 11) == {}
    assert "win probability" in caplog.text.lower()


def test_matchup_and_scoreboard_carry_each_teams_win_probability(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    box = _make_box_score(home_team=_make_team(team_id=8), away_team=_make_team(team_id=9, team_name="Theirs"))
    mock_league = MagicMock()
    mock_league.box_scores.return_value = [box]

    with patch("src.services.matchup_service.build_league", return_value=mock_league), \
         patch("src.services.matchup_service._fetch_win_probabilities", return_value={"8": 0.34, "9": 0.66}), \
         patch("src.services.matchup_service.compute_roster_deficits", return_value={"teams": {}}), \
         patch("src.services.matchup_service.suggest_lineup_swaps", return_value={}):
        matchup = matchup_service.get_current_team_matchup(season=2026, week=2)
        board = matchup_service.get_league_scoreboard(season=2026, week=2)

    assert matchup["our_team"]["team"]["win_probability"] == 0.34
    assert matchup["opponent"]["team"]["win_probability"] == 0.66
    assert board["matchups"][0]["home"]["win_probability"] == 0.34
    assert board["matchups"][0]["away"]["win_probability"] == 0.66


def test_win_probability_is_none_when_espn_gave_none(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    box = _make_box_score(home_team=_make_team(team_id=8), away_team=_make_team(team_id=9))
    mock_league = MagicMock()
    mock_league.box_scores.return_value = [box]

    with patch("src.services.matchup_service.build_league", return_value=mock_league), \
         patch("src.services.matchup_service.compute_roster_deficits", return_value={"teams": {}}), \
         patch("src.services.matchup_service.suggest_lineup_swaps", return_value={}):
        matchup = matchup_service.get_current_team_matchup(season=2026, week=2)

    assert matchup["our_team"]["team"]["win_probability"] is None


def _roster_with_ids(monkeypatch, lookup):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    monkeypatch.setattr(matchup_service, "player_ids_by_espn_id", lookup)
    our_team = _make_team(team_id=8)
    our_team.roster = [
        _make_roster_player(playerId=1, lineupSlot="RB"),
        _make_roster_player(playerId=2, lineupSlot="BE"),
    ]
    league = MagicMock()
    league.teams = [our_team]
    with patch("src.services.matchup_service.build_league", return_value=league), \
         patch("src.services.matchup_service.require_upcoming_week", return_value=2), \
         patch("src.services.matchup_service.compute_roster_deficits", return_value={"teams": {}}), \
         patch("src.services.matchup_service.suggest_lineup_swaps", return_value={}):
        return matchup_service.get_current_team_roster(season=2026)


def test_roster_lines_carry_the_core_player_id_beside_espns(monkeypatch):
    result = _roster_with_ids(monkeypatch, lambda espn_ids: {"1": "9221"})

    lines = result["starters"] + result["bench"]
    assert [(p["player_id"], p["core_player_id"]) for p in lines] == [("1", "9221"), ("2", None)]


def test_roster_still_loads_when_the_player_id_lookup_fails(monkeypatch, caplog):
    def boom(espn_ids):
        raise RuntimeError("database down")

    result = _roster_with_ids(monkeypatch, boom)

    assert [p["core_player_id"] for p in result["starters"] + result["bench"]] == [None, None]
    assert "database down" in caplog.text


def test_matchup_lines_carry_core_player_ids_on_both_sides(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    monkeypatch.setattr(matchup_service, "player_ids_by_espn_id", lambda espn_ids: {"1": "9221", "2": "JAX", "3": "4037"})
    box = _make_box_score(
        home_team=_make_team(team_id=8),
        away_team=_make_team(team_id=9),
        home_lineup=[_make_box_player(playerId=1, slot_position="RB"), _make_box_player(playerId=3, slot_position="BE")],
        away_lineup=[_make_box_player(playerId=2, slot_position="D/ST")],
    )
    league = MagicMock()
    league.box_scores.return_value = [box]

    with patch("src.services.matchup_service.build_league", return_value=league), \
         patch("src.services.matchup_service.compute_roster_deficits", return_value={"teams": {}}), \
         patch("src.services.matchup_service.suggest_lineup_swaps", return_value={}):
        result = matchup_service.get_current_team_matchup(season=2026, week=2)

    assert result["our_team"]["starters"][0]["core_player_id"] == "9221"
    assert result["our_team"]["bench"][0]["core_player_id"] == "4037"
    assert result["opponent"]["starters"][0]["core_player_id"] == "JAX"


def test_stored_matchup_lines_carry_core_player_ids(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    monkeypatch.setattr(matchup_service, "player_ids_by_espn_id", lambda espn_ids: {"1": "9221", "2": "4037"})

    def side(team_id, opponent_id, espn_id):
        line = {"player_id": espn_id, "espn_id": espn_id}
        return {"team_id": team_id, "opponent_team_id": opponent_id, "team": {}, "starters": [line], "bench": []}

    monkeypatch.setattr(matchup_service, "load_completed_week", lambda season, week: [side("8", "9", "1"), side("9", "8", "2")])

    result = matchup_service.get_current_team_matchup(season=2026, week=1)

    assert result["our_team"]["starters"][0]["core_player_id"] == "9221"
    assert result["opponent"]["starters"][0]["core_player_id"] == "4037"


def _stored_side(team_id, opponent_id, line):
    return {"team_id": team_id, "opponent_team_id": opponent_id, "team": {}, "starters": [line], "bench": []}


def test_stored_matchup_lines_carry_a_breakdown_from_espns_stored_stats(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    monkeypatch.setattr(matchup_service, "player_ids_by_espn_id", lambda espn_ids: {})
    monkeypatch.setattr(matchup_service, "load_rule_points_or_none", lambda: {(24, None): 0.1, (25, None): 6.0})
    ours = {
        "player_id": "1", "espn_id": "1", "position": "RB", "actual_points": 15.8, "points": 15.8,
        "applied_stats": {"24": 7.8, "25": 6, "9999": 2}, "applied_qty": {"24": 78, "25": 1},
    }
    theirs = {"player_id": "2", "espn_id": "2", "position": "QB", "actual_points": 3.0, "points": 3.0}
    monkeypatch.setattr(
        matchup_service, "load_completed_week",
        lambda season, week: [_stored_side("8", "9", ours), _stored_side("9", "8", theirs)],
    )

    result = matchup_service.get_current_team_matchup(season=2026, week=3)

    line = result["our_team"]["starters"][0]
    assert [(b["stat_key"], b["qty"], b["rule_label"], b["points"]) for b in line["breakdown"]] == [
        ("rushYd", 78.0, "1 pt per 10 rushing yds", 7.8),
        ("rushTD", 1.0, "6 pts per rushing TD", 6.0),
        ("other", None, "Not itemized", 2.0),
    ]
    assert sum(b["points"] for b in line["breakdown"]) == pytest.approx(line["points"])
    assert "applied_stats" not in line and "applied_qty" not in line
    # Stored before ESPN's per-stat points were kept: no breakdown, not an empty one.
    assert result["opponent"]["starters"][0]["breakdown"] is None


def test_stored_matchup_without_stored_stats_never_reads_the_rules(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    monkeypatch.setattr(matchup_service, "player_ids_by_espn_id", lambda espn_ids: {})
    rules = MagicMock()
    monkeypatch.setattr(matchup_service, "load_rule_points_or_none", rules)
    line = {"player_id": "1", "espn_id": "1", "actual_points": 3.0}
    monkeypatch.setattr(
        matchup_service, "load_completed_week",
        lambda season, week: [_stored_side("8", "9", line), _stored_side("9", "8", dict(line))],
    )

    result = matchup_service.get_current_team_matchup(season=2026, week=4)

    assert result["our_team"]["starters"][0]["breakdown"] is None
    rules.assert_not_called()


def test_suggest_lineup_swaps_carries_slot_and_moves_for_a_chain_swap():
    """2026 W12, team 8: bench Chase Brown (RB) for Brenton Strange (TE) in FLEX, which only
    works if Tony Pollard moves FLEX -> RB. Internal ids overlap ESPN's id space on purpose
    ("4430539" is Strange's ESPN id and also Brown's internal id): nothing may be mixed up."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [("brown", "4242335"), ("strange", "4430539"), ("pollard", "3916148")]
    evaluation = {
        "swaps": [{
            "drop_player_id": "brown", "add_player_id": "strange", "slot": "FLEX", "matchup_multiplier": 1.0,
            "moves": [{"kind": "move", "player_id": "pollard", "slot": "RB", "decision_inputs": {}}],
        }],
        "roster_pool": [
            {"player_id": "strange", "name": "Brenton Strange", "position": "TE", "vorp_score": 9.0},
            {"player_id": "pollard", "name": "Tony Pollard", "position": "RB", "vorp_score": 8.0},
        ],
        "current_starters": {
            "brown": {"position": "RB", "slot_position": "RB1"},
            "pollard": {"position": "RB", "slot_position": "RB/WR/TE"},
        },
        "optimal_starters": [], "source": {},
    }

    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.matchup_service.evaluate_lineup", return_value=evaluation):
        result = matchup_service.suggest_lineup_swaps("8", 2026, 12)

    assert result == {"4242335": {
        "bench_for": "Brenton Strange",
        "add_player_id": "4430539",
        "slot": "FLEX",
        "moves": [{"player_id": "3916148", "name": "Tony Pollard", "from_slot": "FLEX", "to_slot": "RB"}],
    }}


def test_suggest_lineup_swaps_skips_a_chain_whose_displaced_starter_has_no_espn_id(caplog):
    cursor = MagicMock()
    cursor.fetchall.return_value = [("brown", "4242335"), ("strange", "4430539")]
    evaluation = {
        "swaps": [{
            "drop_player_id": "brown", "add_player_id": "strange", "slot": "FLEX", "matchup_multiplier": 1.0,
            "moves": [{"kind": "move", "player_id": "pollard", "slot": "RB", "decision_inputs": {}}],
        }],
        "roster_pool": [
            {"player_id": "strange", "name": "Brenton Strange", "position": "TE", "vorp_score": 9.0},
            {"player_id": "pollard", "name": "Tony Pollard", "position": "RB", "vorp_score": 8.0},
        ],
        "current_starters": {"pollard": {"position": "RB", "slot_position": "RB/WR/TE"}},
        "optimal_starters": [], "source": {},
    }

    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.matchup_service.evaluate_lineup", return_value=evaluation):
        result = matchup_service.suggest_lineup_swaps("8", 2026, 12)

    assert result == {}
    assert "displaced starter" in caplog.text
