from unittest.mock import MagicMock, patch

import polars as pl

from src.backtesting import season_replay


def _player_pool():
    return pl.DataFrame({
        "sleeper_id": ["1", "2"],
        "gsis_id": ["00-01", "00-02"],
        "fantasypros_id": ["100", "200"],
        "name": ["Alpha", "Beta"],
        "position": ["RB", "WR"],
        "team": ["AAA", "BBB"],
    })


def test_build_player_id_actuals_groups_by_week_and_maps_to_player_id():
    actuals = pl.DataFrame({
        "gsis_id": ["00-01", "00-02", "00-01"],
        "week": [1, 1, 2],
        "actual_points": [10.0, 20.0, 15.0],
    })
    result = season_replay.build_player_id_actuals(actuals, _player_pool())
    assert result == {1: {"1": 10.0, "2": 20.0}, 2: {"1": 15.0}}


def test_build_player_id_actuals_empty_input():
    assert season_replay.build_player_id_actuals(pl.DataFrame(), _player_pool()) == {}


def test_seed_weekly_stats_shell_inserts_one_row_per_player_per_playing_week():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    teams_playing_by_week = {1: {"AAA", "BBB"}, 2: {"AAA", "BBB"}, 3: {"AAA", "BBB"}}
    count = season_replay.seed_weekly_stats_shell(
        mock_conn, _player_pool(), {"1": 25.0}, season=2023, weeks=[1, 2, 3],
        teams_playing_by_week=teams_playing_by_week,
    )
    assert count == 6
    args, _ = mock_cursor.executemany.call_args
    rows = args[1]
    assert ("1", 2023, 1, 25.0, "AAA") in rows
    assert ("2", 2023, 1, 0.0, "BBB") in rows


def test_seed_weekly_stats_shell_skips_bye_weeks():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    # AAA is on bye in week 2 (not in that week's playing-teams set)
    teams_playing_by_week = {1: {"AAA", "BBB"}, 2: {"BBB"}}
    count = season_replay.seed_weekly_stats_shell(
        mock_conn, _player_pool(), {"1": 25.0}, season=2023, weeks=[1, 2],
        teams_playing_by_week=teams_playing_by_week,
    )
    assert count == 3  # player 1 (AAA) week1 only, player 2 (BBB) weeks 1+2
    args, _ = mock_cursor.executemany.call_args
    rows = args[1]
    assert ("1", 2023, 2, 25.0, "AAA") not in rows
    assert ("1", 2023, 1, 25.0, "AAA") in rows


def test_seed_weekly_stats_shell_no_data_for_week_seeds_nothing():
    mock_conn = MagicMock()
    count = season_replay.seed_weekly_stats_shell(
        mock_conn, _player_pool(), {}, season=2023, weeks=[1], teams_playing_by_week={},
    )
    assert count == 0
    mock_conn.cursor.assert_not_called()


def test_default_weeks_uses_regular_season_week_count():
    with patch.object(season_replay.historical_data, "load_regular_season_week_count", return_value=18):
        assert list(season_replay.default_weeks(2023)) == list(range(1, 19))


def test_load_roster_with_projected():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("1", "Alpha", "RB", 20.0)]
    result = season_replay.load_roster_with_projected(mock_cursor, "team1", 2023)
    assert result == [{"player_id": "1", "name": "Alpha", "position": "RB", "projected_points": 20.0}]


def test_load_current_roster_ids():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("1",), ("2",)]
    result = season_replay.load_current_roster_ids(mock_cursor, "team1")
    assert result == {"1", "2"}


def test_persist_starters_marks_starters_true_rest_false():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    season_replay.persist_starters(mock_conn, "team1", {"1"}, ["1", "2", "3"])
    args, _ = mock_cursor.executemany.call_args
    rows = args[1]
    assert (True, "team1", "1") in rows
    assert (False, "team1", "2") in rows
    assert (False, "team1", "3") in rows


def test_persist_starters_empty_roster_noop():
    mock_conn = MagicMock()
    season_replay.persist_starters(mock_conn, "team1", set(), [])
    mock_conn.cursor.assert_not_called()


def test_assign_initial_starters_all_teams_covers_every_team():
    mock_conn = MagicMock()
    roster = [{"player_id": "p1", "name": "A", "position": "RB", "projected_points": 10.0}]
    with patch.object(season_replay, "load_roster_with_projected", return_value=roster), \
         patch.object(season_replay.scoring, "compute_naive_starters", return_value=roster) as mock_naive, \
         patch.object(season_replay, "persist_starters") as mock_persist:
        season_replay.assign_initial_starters_all_teams(mock_conn, 2023)

    assert mock_naive.call_count == len(season_replay.draft_simulator.ALL_TEAM_IDS)
    assert mock_persist.call_count == len(season_replay.draft_simulator.ALL_TEAM_IDS)


def test_assign_initial_starters_all_teams_skips_empty_rosters():
    mock_conn = MagicMock()
    with patch.object(season_replay, "load_roster_with_projected", return_value=[]), \
         patch.object(season_replay.scoring, "compute_naive_starters") as mock_naive, \
         patch.object(season_replay, "persist_starters") as mock_persist:
        season_replay.assign_initial_starters_all_teams(mock_conn, 2023)

    mock_naive.assert_not_called()
    mock_persist.assert_not_called()


def test_reveal_week_actuals_updates_each_player():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    season_replay.reveal_week_actuals(mock_conn, 2023, 5, {"1": 12.0, "2": 8.0})
    args, _ = mock_cursor.executemany.call_args
    rows = args[1]
    assert (12.0, "1", 2023, 5) in rows
    assert (8.0, "2", 2023, 5) in rows


def test_reveal_week_actuals_empty_noop():
    mock_conn = MagicMock()
    season_replay.reveal_week_actuals(mock_conn, 2023, 5, {})
    mock_conn.cursor.assert_not_called()


def test_run_week_enforces_no_lookahead_ordering():
    """The single most important regression test in this package: week N's actuals must
    never be revealed before week N's decision phase (VORP, lineup solve, waivers) has
    completed."""
    mock_conn = MagicMock()
    call_order = []

    def track(name, return_value=None):
        def _fn(*args, **kwargs):
            call_order.append(name)
            return return_value
        return _fn

    with patch.object(season_replay.vorp_matrix, "run_vorp_pipeline", side_effect=track("vorp")), \
         patch.object(season_replay.lineup_solver, "load_roster_pool", side_effect=track("load_roster_pool", [])), \
         patch.object(season_replay.scoring, "solve_backtest_lineup", side_effect=track("solve_lineup", [])), \
         patch.object(season_replay, "persist_starters", side_effect=track("persist_starters")), \
         patch.object(season_replay.waiver_strategy, "run_weekly_waivers", side_effect=track("waivers")), \
         patch.object(season_replay, "load_current_roster_ids", side_effect=track("load_roster_ids", set())), \
         patch.object(season_replay, "reveal_week_actuals", side_effect=track("reveal")), \
         patch.object(season_replay.scoring, "score_lineup_week", side_effect=track("score", {})):
        season_replay.run_week(mock_conn, 2023, 5, {"1": 10.0}, naive_starters=[], static_roster_ids=set())

    reveal_index = call_order.index("reveal")
    assert call_order.index("vorp") < reveal_index
    assert call_order.index("solve_lineup") < reveal_index
    assert call_order.index("persist_starters") < reveal_index
    assert call_order.index("waivers") < reveal_index
    assert call_order.index("score") > reveal_index


def test_run_week_returns_lineup_waiver_and_static_points():
    mock_conn = MagicMock()
    with patch.object(season_replay.vorp_matrix, "run_vorp_pipeline"), \
         patch.object(season_replay.lineup_solver, "load_roster_pool", return_value=[]), \
         patch.object(season_replay.scoring, "solve_backtest_lineup", return_value=[]), \
         patch.object(season_replay, "persist_starters"), \
         patch.object(season_replay.waiver_strategy, "run_weekly_waivers"), \
         patch.object(season_replay, "load_current_roster_ids", return_value={"1", "2"}), \
         patch.object(season_replay, "reveal_week_actuals"), \
         patch.object(season_replay.scoring, "score_lineup_week", return_value={"our_points": 1.0}):
        result = season_replay.run_week(
            mock_conn, 2023, 5,
            actuals_for_week={"1": 10.0, "2": 5.0, "3": 100.0},
            naive_starters=[], static_roster_ids={"1"},
        )

    assert result["lineup"] == {"our_points": 1.0, "week": 5}
    assert result["our_waiver_points"] == 15.0  # players 1 and 2 on current roster
    assert result["static_points"] == 10.0  # only player 1 on static roster


def test_run_week_passes_market_weight_to_vorp_pipeline():
    mock_conn = MagicMock()
    with patch.object(season_replay.vorp_matrix, "run_vorp_pipeline") as vorp, \
         patch.object(season_replay.lineup_solver, "load_roster_pool", return_value=[]), \
         patch.object(season_replay.scoring, "solve_backtest_lineup", return_value=[]), \
         patch.object(season_replay, "persist_starters"), \
         patch.object(season_replay.waiver_strategy, "run_weekly_waivers"), \
         patch.object(season_replay, "load_current_roster_ids", return_value=set()), \
         patch.object(season_replay, "reveal_week_actuals"), \
         patch.object(season_replay.scoring, "score_lineup_week", return_value={}):
        season_replay.run_week(mock_conn, 2023, 5, {}, naive_starters=[], static_roster_ids=set())
        season_replay.run_week(mock_conn, 2023, 6, {}, naive_starters=[], static_roster_ids=set(), market_weight=0.3)

    assert vorp.call_args_list[0].kwargs == {"season": 2023, "week": 5, "market_weight": 0.0, "market_source": "espn"}
    assert vorp.call_args_list[1].kwargs == {"season": 2023, "week": 6, "market_weight": 0.3, "market_source": "espn"}


def _patched_run_week(mock_conn, week, **kwargs):
    call_order = []
    with patch.object(season_replay.vorp_matrix, "run_vorp_pipeline", side_effect=lambda **k: call_order.append(("vorp", k))), \
         patch.object(season_replay.lineup_solver, "load_roster_pool", return_value=[]), \
         patch.object(season_replay.scoring, "solve_backtest_lineup", return_value=[]), \
         patch.object(season_replay, "persist_starters"), \
         patch.object(season_replay.waiver_strategy, "run_weekly_waivers"), \
         patch.object(season_replay, "load_current_roster_ids", return_value=set()), \
         patch.object(season_replay, "reveal_week_actuals", side_effect=lambda *a: call_order.append(("reveal",))), \
         patch.object(season_replay, "seed_rolling_projections", side_effect=lambda *a: call_order.append(("rolling", a))), \
         patch.object(season_replay.scoring, "score_lineup_week", return_value={}):
        season_replay.run_week(mock_conn, 2023, week, {}, naive_starters=[], static_roster_ids=set(), **kwargs)
    return call_order


def test_run_week_passes_market_source_to_vorp_pipeline():
    calls = _patched_run_week(MagicMock(), 6, market_weight=0.1, market_source="ecr")
    assert ("vorp", {"season": 2023, "week": 6, "market_weight": 0.1, "market_source": "ecr"}) in calls


def test_run_week_rolling_base_reseeds_projections_before_vorp():
    history = {4: {"1": 10.0}}
    calls = _patched_run_week(MagicMock(), 6, rolling_history=history)
    names = [c[0] for c in calls]
    assert names.index("rolling") < names.index("vorp") < names.index("reveal")
    assert calls[names.index("rolling")][1][1:] == (2023, 6, history)


def test_run_week_commits_rolling_projections_before_vorp_runs():
    """run_vorp_pipeline updates the same weekly_stats rows on its own pooled connection, so
    an uncommitted reseed would hold their row locks and block it forever."""
    mock_conn = MagicMock()
    order = []
    mock_conn.commit.side_effect = lambda: order.append("commit")
    with patch.object(season_replay.vorp_matrix, "run_vorp_pipeline", side_effect=lambda **k: order.append("vorp")), \
         patch.object(season_replay.lineup_solver, "load_roster_pool", return_value=[]), \
         patch.object(season_replay.scoring, "solve_backtest_lineup", return_value=[]), \
         patch.object(season_replay, "persist_starters"), \
         patch.object(season_replay.waiver_strategy, "run_weekly_waivers"), \
         patch.object(season_replay, "load_current_roster_ids", return_value=set()), \
         patch.object(season_replay, "reveal_week_actuals"), \
         patch.object(season_replay, "seed_rolling_projections"), \
         patch.object(season_replay.scoring, "score_lineup_week", return_value={}):
        season_replay.run_week(mock_conn, 2023, 6, {}, naive_starters=[], static_roster_ids=set(), rolling_history={})
    assert order[:2] == ["commit", "vorp"]


def test_run_week_preseason_base_never_reseeds():
    calls = _patched_run_week(MagicMock(), 6)
    assert "rolling" not in [c[0] for c in calls]


def test_rolling_projection_means_up_to_three_prior_weeks():
    history = {1: {"a": 30.0}, 2: {"a": 3.0, "b": 8.0}, 3: {"a": 6.0}, 4: {"a": 9.0, "b": 4.0}, 5: {"a": 99.0}}
    projections = season_replay.rolling_projections(history, week=5)
    assert projections == {"a": 6.0, "b": 6.0}


def test_rolling_projection_never_reads_the_target_week_or_later():
    history = {3: {"a": 6.0}, 5: {"a": 1000.0}, 6: {"a": 1000.0}}
    assert season_replay.rolling_projections(history, week=5) == {"a": 6.0}
    assert season_replay.rolling_projections(history, week=1) == {}


def test_seed_rolling_projections_updates_only_the_target_week_rows():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    season_replay.seed_rolling_projections(mock_conn, 2023, 5, {4: {"a": 9.0}})
    query, rows = mock_cursor.executemany.call_args.args
    assert query.strip().startswith("UPDATE weekly_stats SET projected_points")
    assert rows == [(9.0, "a", 2023, 5)]


def test_seed_rolling_projections_week_one_keeps_preseason():
    mock_conn = MagicMock()
    season_replay.seed_rolling_projections(mock_conn, 2023, 1, {})
    mock_conn.cursor.assert_not_called()


def test_run_week_solves_our_lineup_with_the_backtest_league_slots():
    pool = [
        {"player_id": pid, "name": pid, "position": pos, "vorp_score": 1.0}
        for pid, pos in [("q", "QB"), ("r1", "RB"), ("r2", "RB"), ("w1", "WR"), ("w2", "WR"), ("t", "TE"), ("r3", "RB")]
    ]
    with patch.object(season_replay.vorp_matrix, "run_vorp_pipeline"), \
         patch.object(season_replay.lineup_solver, "load_roster_pool", return_value=pool), \
         patch.object(season_replay, "persist_starters") as persist, \
         patch.object(season_replay.waiver_strategy, "run_weekly_waivers"), \
         patch.object(season_replay, "load_current_roster_ids", return_value=set()), \
         patch.object(season_replay, "reveal_week_actuals"), \
         patch.object(season_replay.scoring, "score_lineup_week", return_value={}):
        season_replay.run_week(MagicMock(), 2023, 5, {}, naive_starters=[], static_roster_ids=set())
    starter_ids = persist.call_args.args[2]
    assert starter_ids == {"q", "r1", "r2", "w1", "w2", "t", "r3"}
