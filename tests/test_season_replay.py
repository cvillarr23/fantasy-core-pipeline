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


def test_seed_weekly_stats_shell_inserts_one_row_per_player_per_week():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    count = season_replay.seed_weekly_stats_shell(
        mock_conn, _player_pool(), {"1": 25.0}, season=2023, weeks=[1, 2, 3],
    )
    assert count == 6
    args, _ = mock_cursor.executemany.call_args
    rows = args[1]
    assert ("1", 2023, 1, 25.0, "AAA") in rows
    assert ("2", 2023, 1, 0.0, "BBB") in rows


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
         patch.object(season_replay.lineup_solver, "build_and_solve_lineup", side_effect=track("solve_lineup", [])), \
         patch.object(season_replay.waiver_strategy, "run_weekly_waivers", side_effect=track("waivers")), \
         patch.object(season_replay, "load_current_roster_ids", side_effect=track("load_roster_ids", set())), \
         patch.object(season_replay, "reveal_week_actuals", side_effect=track("reveal")), \
         patch.object(season_replay.scoring, "score_lineup_week", side_effect=track("score", {})):
        season_replay.run_week(mock_conn, 2023, 5, {"1": 10.0}, naive_starters=[], static_roster_ids=set())

    reveal_index = call_order.index("reveal")
    assert call_order.index("vorp") < reveal_index
    assert call_order.index("solve_lineup") < reveal_index
    assert call_order.index("waivers") < reveal_index
    assert call_order.index("score") > reveal_index


def test_run_week_returns_lineup_waiver_and_static_points():
    mock_conn = MagicMock()
    with patch.object(season_replay.vorp_matrix, "run_vorp_pipeline"), \
         patch.object(season_replay.lineup_solver, "load_roster_pool", return_value=[]), \
         patch.object(season_replay.lineup_solver, "build_and_solve_lineup", return_value=[]), \
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
