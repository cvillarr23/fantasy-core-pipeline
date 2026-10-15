from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.optimization.lineup_solver import InsufficientLineupInputError
from src.services import matchup_service


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


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
        "bye_week": None,
        "headshot_url": "https://a.espncdn.com/i/headshots/nfl/players/full/4240657.png",
        "deficit_flag": False,
        "suggested_swap": None,
    }


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
    evaluation = {
        "swaps": [{"drop_player_id": "drop1", "add_player_id": "add1", "slot": "RB", "matchup_multiplier": 1.0}],
        "roster_pool": [{"player_id": "add1", "name": "Better Back", "position": "RB", "vorp_score": 10.0}],
        "optimal_starters": [], "source": {},
    }

    with patch("src.services.matchup_service.get_db_cursor", lambda: _fake_db_cursor(cursor)), \
         patch("src.services.matchup_service.evaluate_lineup", return_value=evaluation) as mock_evaluate, \
         patch("src.optimization.lineup_solver.publish_lineup_swaps") as mock_publish:
        result = matchup_service.suggest_lineup_swaps("team1", 2026, 2)

    assert result == {"drop1": {"bench_for": "Better Back", "add_player_id": "add1"}}
    mock_publish.assert_not_called()
    mock_evaluate.assert_called_once_with(cursor, "team1", 2026, 2)


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
    """A web request must not wait out the CronJob backoff, and an unresolvable week must
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
