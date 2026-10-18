from unittest.mock import MagicMock

import pytest

from src.backtesting import waiver_strategy as ws

WEEKS = [7, 8, 9, 10]


def row(pid, pos, week, vorp, ours=False, starter=False, slot="BE", team="KC", espn_injury=None, injury=None):
    return (pid, f"e{pid}", f"P{pid}", pos, team, week, vorp, ours, starter if ours else None,
            slot if ours else None, espn_injury if ours else None, injury)


def rows_for(pid, pos, by_week, **kwargs):
    return [row(pid, pos, w, v, **kwargs) for w, v in by_week.items()]


def test_horizon_mean_counts_a_confirmed_bye_as_replacement_level():
    assert ws.horizon_mean_vorp({8: 12.0, 9: 12.0, 10: 12.0}, WEEKS, bye_week=7) == pytest.approx(9.0)


def test_horizon_mean_skips_unknown_weeks_instead_of_zeroing_them():
    assert ws.horizon_mean_vorp({7: 6.0, 9: 2.0}, WEEKS, bye_week=12) == pytest.approx(4.0)
    assert ws.horizon_mean_vorp({}, WEEKS, bye_week=None) is None


def test_stud_on_bye_this_week_stays_on_the_bench_pool_at_his_horizon_value():
    """The old single-week pools left him out (or, with a 0.0 row, made him the worst bench
    player and the drop). His horizon value keeps him from ever being the drop."""
    rows = (
        rows_for("stud", "RB", {8: 12.0, 9: 12.0, 10: 12.0}, ours=True, team="BUF")
        + rows_for("scrub", "RB", {7: 1.0, 8: 1.0, 9: 1.0, 10: 1.0}, ours=True)
        + rows_for("fa", "RB", {7: 5.0, 8: 5.0, 9: 5.0, 10: 5.0})
    )

    pools = ws.build_horizon_pools(rows, 7, WEEKS, {"BUF": 7, "KC": 12})

    bench = {p["player_id"]: p["vorp_score"] for p in pools["bench_by_position"]["RB"]}
    assert bench == {"stud": pytest.approx(9.0), "scrub": pytest.approx(1.0)}
    moves = ws.propose_waiver_moves(pools["bench_by_position"], pools["free_agents_by_position"])
    assert [m["drop_player_id"] for m in moves] == ["scrub"]


def test_free_agent_on_bye_this_week_is_still_a_candidate():
    rows = rows_for("fa", "WR", {8: 9.0, 9: 9.0, 10: 9.0}, team="BUF")

    pools = ws.build_horizon_pools(rows, 7, WEEKS, {"BUF": 7})

    assert pools["free_agents_by_position"]["WR"]["player_id"] == "fa"
    assert pools["free_agents_by_position"]["WR"]["vorp_score"] == pytest.approx(6.75)


def test_a_player_without_a_target_week_row_who_is_not_on_bye_is_left_out():
    """Same rule as the single-week pools: no row for a week he's playing means no usable
    projection (released, inactive), not a bye."""
    rows = rows_for("ghost", "WR", {8: 9.0}, team="KC")

    pools = ws.build_horizon_pools(rows, 7, WEEKS, {"KC": 12})

    assert pools["free_agents_by_position"] == {}
    assert pools["add_candidates"] == []


def test_starters_and_ir_are_in_the_roster_but_not_the_bench_pool():
    rows = (
        rows_for("qb", "QB", {7: 5.0}, ours=True, starter=True, slot="QB")
        + rows_for("hurt", "WR", {7: 2.0}, ours=True, slot="IR", espn_injury="INJURY_RESERVE")
        + rows_for("fa", "WR", {7: 3.0})
    )

    pools = ws.build_horizon_pools(rows, 7, WEEKS, {})

    assert pools["bench_by_position"] == {}
    assert {p["player_id"] for p in pools["roster"]} == {"qb", "hurt"}
    roster = {p["player_id"]: p for p in pools["roster"]}
    assert roster["qb"]["metrics"]["7"] == {"vorp_score": 5.0}
    assert roster["hurt"]["slot"] == "IR"
    assert pools["free_agents"]["fa"]["position"] == "WR"


def test_load_horizon_waiver_pools_scopes_rows_to_our_team_and_free_agents():
    cursor = MagicMock()
    cursor.fetchall.return_value = rows_for("fa", "WR", {7: 3.0})

    pools = ws.load_horizon_waiver_pools(cursor, "4", 2026, 7, WEEKS, {})

    query, params = cursor.execute.call_args.args
    assert "r.team_id = %s" in query
    assert params == ("4", 2026, 7, 10, "4")
    assert pools["free_agents_by_position"]["WR"]["player_id"] == "fa"


def test_horizon_weeks_stop_at_the_regular_season():
    assert ws.horizon_weeks(7) == [7, 8, 9, 10]
    assert ws.horizon_weeks(17) == [17, 18]
    assert ws.horizon_weeks(7, horizon=1) == [7]


def test_a_row_in_the_players_bye_week_is_ignored():
    """The schedule wins over a stale or zero projection for a week his team is idle."""
    rows = rows_for("fa", "QB", {7: 4.0, 8: 4.0, 9: 4.0, 10: 4.0}, team="MIN")

    pools = ws.build_horizon_pools(rows, 7, WEEKS, {"MIN": 7})

    assert pools["free_agents"]["fa"]["metrics"].get("7") is None
    assert pools["free_agents_by_position"]["QB"]["vorp_score"] == pytest.approx(3.0)
