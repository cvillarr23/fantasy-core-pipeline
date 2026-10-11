from unittest.mock import MagicMock

import pytest

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


@pytest.mark.parametrize("counts,occupied,expected", [
    ({"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "K": 1, "D/ST": 1, "BE": 7, "IR": 2}, 15, 1),
    ({"QB": 1, "BE": 2, "IR": 3}, 3, 0),
    ({"QB": 1, "BE": 2}, 4, 0),
    ({"QB": 1, "BE": 2}, 0, 3),
    ('{"QB": 1, "BE": 2, "IR": 1}', 1, 2),
    (None, 0, 0),
])
def test_open_roster_slots_excludes_ir_and_clamps_capacity(counts, occupied, expected):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(counts,), (occupied,)]
    assert waiver_strategy.load_open_roster_slots(cursor, "8", 2026) == expected
    if counts is not None:
        query, params = cursor.execute.call_args.args
        assert "slot_position IS DISTINCT FROM 'IR'" in query
        assert params == ("8",)


def test_adds_take_best_agents_including_multiple_at_same_position_before_other_moves():
    candidates = [
        {"player_id": "wr1", "position": "WR", "vorp_score": 10.0},
        {"player_id": "wr2", "position": "WR", "vorp_score": 9.0},
        {"player_id": "wr3", "position": "WR", "vorp_score": 8.0},
        {"player_id": "rb", "position": "RB", "vorp_score": 7.0},
    ]
    adds, remaining = waiver_strategy.plan_add_only_moves(candidates, 2, rostered_positions=set())
    assert [m["add_player_id"] for m in adds] == ["wr1", "wr2"]
    assert all(m["kind"] == "add" and m["drop_player_id"] is None for m in adds)
    swaps, ir_adds = waiver_strategy.plan_waiver_moves(
        _bench(RB=[("hurt", 1.0, "OUT")], WR=[("bad", -1.0, None)]), remaining, 1, margin=0,
    )
    assert [m["add_player_id"] for m in ir_adds] == ["wr3"]
    assert not swaps
    assert len({m["add_player_id"] for m in adds + swaps + ir_adds}) == 3


@pytest.mark.parametrize("slots,expected", [(0, []), (1, ["good"]), (5, ["good", "small", "zero"])])
def test_adds_respect_capacity_and_pool_size(slots, expected):
    candidates = [
        {"player_id": "good", "position": "WR", "vorp_score": 4.0},
        {"player_id": "small", "position": "WR", "vorp_score": 3.0},
        {"player_id": "zero", "position": "RB", "vorp_score": 0.0},
    ]
    adds, _ = waiver_strategy.plan_add_only_moves(candidates, slots, rostered_positions=set())
    assert [m["add_player_id"] for m in adds] == expected


@pytest.mark.parametrize("position", ["QB", "RB", "WR", "TE"])
def test_negative_vorp_skill_depth_beats_backup_specialists(position):
    candidates = [
        {"player_id": "k", "position": "K", "vorp_score": -0.1},
        {"player_id": "dst", "position": "D/ST", "vorp_score": -0.2},
        {"player_id": "depth", "position": position, "vorp_score": -4.78},
        {"player_id": "worse", "position": position, "vorp_score": -6},
    ]
    adds, remaining = waiver_strategy.plan_add_only_moves(candidates, 1, {"K", "D/ST"})
    assert [(m["kind"], m["add_player_id"]) for m in adds] == [("add", "depth")]
    assert remaining[position]["player_id"] == "worse"
    assert remaining["K"]["player_id"] == "k"


@pytest.mark.parametrize("status", ["OUT", "Out", "IR", "Injured Reserve", "INJURY_RESERVE"])
def test_adds_skip_injured_candidates_but_preserve_swap_pool(status):
    candidates = [
        {"player_id": "hurt", "position": "WR", "vorp_score": 10, "injury_status": status},
        {"player_id": "healthy", "position": "WR", "vorp_score": -5, "injury_status": "Questionable"},
    ]
    adds, remaining = waiver_strategy.plan_add_only_moves(candidates, 1, {"K", "D/ST"})
    assert [m["add_player_id"] for m in adds] == ["healthy"]
    assert remaining["WR"]["player_id"] == "hurt"


@pytest.mark.parametrize("missing", ["K", "D/ST"])
def test_missing_specialist_is_filled_once_before_skill_depth(missing):
    candidates = [
        {"player_id": "first", "position": missing, "vorp_score": -2},
        {"player_id": "backup", "position": missing, "vorp_score": -3},
        {"player_id": "hurt", "position": missing, "vorp_score": 5, "injury_status": "OUT"},
        {"player_id": "depth", "position": "RB", "vorp_score": 8},
    ]
    adds, _ = waiver_strategy.plan_add_only_moves(candidates, 3, {"K", "D/ST"} - {missing})
    assert [m["add_player_id"] for m in adds] == ["first", "depth"]


def test_specialist_only_pool_does_not_add_backups():
    candidates = [{"player_id": "k", "position": "K", "vorp_score": 5}]
    adds, _ = waiver_strategy.plan_add_only_moves(candidates, 1, {"K", "D/ST"})
    assert adds == []


def test_add_candidate_loader_carries_free_agent_injury_status():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("hurt", "WR", -4.78, "Out")]
    assert waiver_strategy.load_add_candidates(cursor, 2026, 6) == [
        {"player_id": "hurt", "position": "WR", "vorp_score": -4.78, "injury_status": "Out"}]
    query, params = cursor.execute.call_args.args
    assert "p.injury_status" in query
    assert "NOT EXISTS" in query
    assert params == (2026, 6)


def test_rostered_positions_includes_starters_without_projections():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("K",), ("D/ST",), ("WR",)]
    assert waiver_strategy.load_rostered_positions(cursor, "8") == {"K", "D/ST", "WR"}
    query, params = cursor.execute.call_args.args
    assert "weekly_stats" not in query
    assert "is_starter" not in query
    assert params == ("8",)
