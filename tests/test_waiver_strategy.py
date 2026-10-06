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


def _fa(**agents):
    return {pos: {"player_id": pid, "vorp_score": vorp} for pos, (pid, vorp) in agents.items()}


def test_plan_waiver_moves_stashes_an_injured_star_instead_of_dropping_a_healthy_player():
    """The injured player isn't the worst on the bench (prior-week VORP stays high), so a
    plain swap would drop the healthy one. With IR open: stash the star, add, no drop."""
    bench = _bench(RB=[("star", 8.0, "OUT"), ("healthy", 1.0, "ACTIVE")])

    swaps, ir_adds = waiver_strategy.plan_waiver_moves(bench, _fa(RB=("fa", 5.0)), open_ir_slots=1, margin=0.0)

    assert swaps == []
    assert ir_adds == [{
        "kind": "ir_and_add", "position": "RB", "ir_player_id": "star", "ir_position": "RB",
        "drop_player_id": None, "add_player_id": "fa", "add_vorp_score": 5.0, "ir_vorp_score": 8.0,
    }]


def test_plan_waiver_moves_pairs_the_freed_spot_with_the_best_agent_at_any_position():
    bench = _bench(RB=[("hurt_rb", 0.0, "OUT")], WR=[("wr_bench", 1.0, None)])
    agents = _fa(RB=("fa_rb", 2.0), WR=("fa_wr", 9.0))

    swaps, ir_adds = waiver_strategy.plan_waiver_moves(bench, agents, open_ir_slots=1, margin=0.0)

    assert [(m["ir_player_id"], m["add_player_id"], m["position"]) for m in ir_adds] == [("hurt_rb", "fa_wr", "WR")]
    # WR's agent is taken by the stash; RB has no healthy bench player to swap.
    assert swaps == []


def test_plan_waiver_moves_is_a_plain_swap_when_ir_is_full():
    bench = _bench(RB=[("hurt", -1.0, "OUT")])

    swaps, ir_adds = waiver_strategy.plan_waiver_moves(bench, _fa(RB=("fa", 4.0)), open_ir_slots=0, margin=0.0)

    assert [m["drop_player_id"] for m in swaps] == ["hurt"]
    assert ir_adds == []


def test_plan_waiver_moves_never_drops_an_ir_eligible_player_while_a_slot_is_open():
    """Two eligible players, one slot: the better one is stashed, and the other is neither
    stashed nor dropped, since dropping is only right once IR is full."""
    bench = _bench(RB=[("hurt_rb", 0.0, "OUT")], WR=[("hurt_wr", 3.0, "INJURY_RESERVE")])
    agents = _fa(RB=("fa_rb", 6.0), WR=("fa_wr", 4.0))

    swaps, ir_adds = waiver_strategy.plan_waiver_moves(bench, agents, open_ir_slots=1, margin=0.0)

    assert [(m["ir_player_id"], m["add_player_id"]) for m in ir_adds] == [("hurt_wr", "fa_rb")]
    assert swaps == []


def test_plan_waiver_moves_still_swaps_healthy_players_at_other_positions():
    bench = _bench(RB=[("hurt", 0.0, "OUT")], WR=[("wr_bad", -2.0, "QUESTIONABLE")])
    agents = _fa(RB=("fa_rb", 6.0), WR=("fa_wr", 4.0))

    swaps, ir_adds = waiver_strategy.plan_waiver_moves(bench, agents, open_ir_slots=1, margin=3.0)

    assert [m["add_player_id"] for m in ir_adds] == ["fa_rb"]
    assert [(m["drop_player_id"], m["add_player_id"]) for m in swaps] == [("wr_bad", "fa_wr")]


def test_plan_waiver_moves_needs_a_free_add_to_clear_the_margin():
    bench = _bench(RB=[("hurt", 0.0, "OUT")])

    swaps, ir_adds = waiver_strategy.plan_waiver_moves(bench, _fa(RB=("fa", 2.0)), open_ir_slots=1, margin=3.0)

    assert (swaps, ir_adds) == ([], [])


def test_ir_stash_candidates_lists_eligible_bench_players_by_value_up_to_the_open_slots():
    bench = _bench(RB=[("a", 1.0, "OUT"), ("b", 0.0, "ACTIVE")], WR=[("c", 3.0, "INJURY_RESERVE")])

    assert waiver_strategy.ir_stash_candidates(bench, open_ir_slots=2) == [
        {"position": "WR", "player_id": "c", "vorp_score": 3.0, "espn_injury_status": "INJURY_RESERVE"},
        {"position": "RB", "player_id": "a", "vorp_score": 1.0, "espn_injury_status": "OUT"},
    ]
    assert [p["player_id"] for p in waiver_strategy.ir_stash_candidates(bench, open_ir_slots=1)] == ["c"]
    assert waiver_strategy.ir_stash_candidates(bench, open_ir_slots=0) == []
