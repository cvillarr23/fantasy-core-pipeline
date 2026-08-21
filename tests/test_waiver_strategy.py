from unittest.mock import MagicMock

from src.backtesting import waiver_strategy


def test_propose_waiver_moves_swaps_when_margin_exceeded():
    bench = {"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}
    free_agents = {"RB": {"player_id": "fa1", "vorp_score": 5.0}}
    moves = waiver_strategy.propose_waiver_moves(bench, free_agents, margin=3.0)
    assert moves == [{"position": "RB", "drop_player_id": "bench1", "add_player_id": "fa1"}]


def test_propose_waiver_moves_no_swap_when_margin_not_exceeded():
    bench = {"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}
    free_agents = {"RB": {"player_id": "fa1", "vorp_score": 2.5}}
    moves = waiver_strategy.propose_waiver_moves(bench, free_agents, margin=3.0)
    assert moves == []


def test_propose_waiver_moves_picks_worst_bench_player():
    bench = {"RB": [{"player_id": "good", "vorp_score": 4.0}, {"player_id": "bad", "vorp_score": -1.0}]}
    free_agents = {"RB": {"player_id": "fa1", "vorp_score": 5.0}}
    moves = waiver_strategy.propose_waiver_moves(bench, free_agents, margin=3.0)
    assert moves[0]["drop_player_id"] == "bad"


def test_propose_waiver_moves_evaluates_positions_independently():
    bench = {
        "RB": [{"player_id": "rb_bench", "vorp_score": 1.0}],
        "WR": [{"player_id": "wr_bench", "vorp_score": 10.0}],
    }
    free_agents = {
        "RB": {"player_id": "rb_fa", "vorp_score": 5.0},
        "WR": {"player_id": "wr_fa", "vorp_score": 11.0},
    }
    moves = waiver_strategy.propose_waiver_moves(bench, free_agents, margin=3.0)
    assert len(moves) == 1
    assert moves[0]["position"] == "RB"


def test_propose_waiver_moves_no_bench_at_position_skipped():
    bench = {}
    free_agents = {"RB": {"player_id": "fa1", "vorp_score": 5.0}}
    moves = waiver_strategy.propose_waiver_moves(bench, free_agents)
    assert moves == []


def test_apply_waiver_moves_deletes_and_inserts():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    moves = [{"position": "RB", "drop_player_id": "bench1", "add_player_id": "fa1"}]
    count = waiver_strategy.apply_waiver_moves(mock_conn, "team1", moves)
    assert count == 1
    calls = mock_cursor.execute.call_args_list
    assert "DELETE FROM league_rosters" in calls[0].args[0]
    assert calls[0].args[1] == ("team1", "bench1")
    assert "INSERT INTO league_rosters" in calls[1].args[0]
    assert calls[1].args[1] == ("team1", "fa1")


def test_apply_waiver_moves_empty_noop():
    mock_conn = MagicMock()
    count = waiver_strategy.apply_waiver_moves(mock_conn, "team1", [])
    assert count == 0
    mock_conn.cursor.assert_not_called()


def test_load_bench_by_position_excludes_starters():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("p1", "RB", 2.0), ("p2", "WR", 4.0)]
    result = waiver_strategy.load_bench_by_position(mock_cursor, "team1", 2023, 5)
    assert result == {"RB": [{"player_id": "p1", "vorp_score": 2.0}], "WR": [{"player_id": "p2", "vorp_score": 4.0}]}
    query = mock_cursor.execute.call_args.args[0]
    assert "is_starter" in query


def test_load_best_free_agent_by_position_keeps_highest_per_position():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("p1", "RB", 2.0), ("p2", "RB", 6.0), ("p3", "WR", 3.0)]
    result = waiver_strategy.load_best_free_agent_by_position(mock_cursor, 2023, 5)
    assert result["RB"] == {"player_id": "p2", "vorp_score": 6.0}
    assert result["WR"] == {"player_id": "p3", "vorp_score": 3.0}
