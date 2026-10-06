from unittest.mock import MagicMock

from src.backtesting import waiver_strategy


def test_propose_waiver_moves_swaps_when_margin_exceeded():
    bench = {"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}
    free_agents = {"RB": {"player_id": "fa1", "vorp_score": 5.0}}
    moves = waiver_strategy.propose_waiver_moves(bench, free_agents, margin=3.0)
    assert moves == [{
        "position": "RB", "drop_player_id": "bench1", "add_player_id": "fa1",
        "add_vorp_score": 5.0, "drop_vorp_score": 1.0,
    }]


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
    mock_cursor.fetchall.return_value = [("p1", "RB", 2.0, "OUT"), ("p2", "WR", 4.0, None)]
    result = waiver_strategy.load_bench_by_position(mock_cursor, "team1", 2023, 5)
    assert result == {
        "RB": [{"player_id": "p1", "vorp_score": 2.0, "espn_injury_status": "OUT"}],
        "WR": [{"player_id": "p2", "vorp_score": 4.0, "espn_injury_status": None}],
    }
    query = mock_cursor.execute.call_args.args[0]
    assert "is_starter" in query


def test_load_bench_by_position_never_offers_an_ir_slot_player_as_a_drop():
    """IR rows are also is_starter = FALSE; without this a stashed player was droppable."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    waiver_strategy.load_bench_by_position(mock_cursor, "team1", 2023, 5)
    query = mock_cursor.execute.call_args.args[0]
    assert "r.slot_position <> 'IR'" in query


def test_load_best_free_agent_by_position_keeps_highest_per_position():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("p1", "RB", 2.0), ("p2", "RB", 6.0), ("p3", "WR", 3.0)]
    result = waiver_strategy.load_best_free_agent_by_position(mock_cursor, 2023, 5)
    assert result["RB"] == {"player_id": "p2", "vorp_score": 6.0}
    assert result["WR"] == {"player_id": "p3", "vorp_score": 3.0}


def _move(position, drop, add, add_vorp, drop_vorp):
    return {"position": position, "drop_player_id": drop, "add_player_id": add,
            "add_vorp_score": add_vorp, "drop_vorp_score": drop_vorp}


def _bench(**statuses):
    """position -> [(player_id, vorp, espn_injury_status)]"""
    return {
        pos: [{"player_id": pid, "vorp_score": vorp, "espn_injury_status": status} for pid, vorp, status in rows]
        for pos, rows in statuses.items()
    }


def test_split_ir_moves_turns_an_ir_eligible_drop_into_ir_plus_add():
    bench = _bench(RB=[("hurt", -1.0, "OUT"), ("healthy", 2.0, "ACTIVE")])
    moves = [_move("RB", "hurt", "fa", 4.0, -1.0)]

    swaps, ir_adds = waiver_strategy.split_ir_moves(moves, bench, open_ir_slots=1)

    assert swaps == []
    assert ir_adds == [{
        "kind": "ir_and_add", "position": "RB", "ir_player_id": "hurt", "drop_player_id": None,
        "add_player_id": "fa", "add_vorp_score": 4.0, "ir_vorp_score": -1.0,
    }]


def test_split_ir_moves_keeps_the_drop_when_no_ir_slot_is_open():
    bench = _bench(RB=[("hurt", -1.0, "OUT")])
    moves = [_move("RB", "hurt", "fa", 4.0, -1.0)]

    swaps, ir_adds = waiver_strategy.split_ir_moves(moves, bench, open_ir_slots=0)

    assert swaps == moves
    assert ir_adds == []


def test_split_ir_moves_never_drops_an_ir_eligible_player_while_a_slot_is_open():
    """Two IR-eligible drops, one open slot: the better add becomes IR + add, and the other
    is withheld rather than dropped, since the slot is still open until someone acts."""
    bench = _bench(RB=[("hurt_rb", 0.0, "OUT")], WR=[("hurt_wr", 0.0, "INJURY_RESERVE")])
    moves = [_move("RB", "hurt_rb", "fa_rb", 3.0, 0.0), _move("WR", "hurt_wr", "fa_wr", 6.0, 0.0)]

    swaps, ir_adds = waiver_strategy.split_ir_moves(moves, bench, open_ir_slots=1)

    assert swaps == []
    assert [m["ir_player_id"] for m in ir_adds] == ["hurt_wr"]


def test_split_ir_moves_passes_through_healthy_and_unknown_drops():
    bench = _bench(RB=[("healthy", 1.0, "QUESTIONABLE")])
    moves = [_move("RB", "healthy", "fa", 5.0, 1.0), _move("WR", "not_in_bench", "fa2", 5.0, 1.0)]

    swaps, ir_adds = waiver_strategy.split_ir_moves(moves, bench, open_ir_slots=1)

    assert swaps == moves
    assert ir_adds == []


def test_ir_stash_candidates_lists_eligible_bench_players_by_value():
    bench = _bench(RB=[("a", 1.0, "OUT"), ("b", 0.0, "ACTIVE")], WR=[("c", 3.0, "INJURY_RESERVE")])

    assert waiver_strategy.ir_stash_candidates(bench, open_ir_slots=1) == [
        {"position": "WR", "player_id": "c", "vorp_score": 3.0, "espn_injury_status": "INJURY_RESERVE"},
        {"position": "RB", "player_id": "a", "vorp_score": 1.0, "espn_injury_status": "OUT"},
    ]
    assert waiver_strategy.ir_stash_candidates(bench, open_ir_slots=0) == []
