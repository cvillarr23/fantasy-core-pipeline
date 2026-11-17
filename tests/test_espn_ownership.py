from unittest.mock import MagicMock

from src.services.espn_ownership import load_espn_ownership


def _cursor(rows):
    cur = MagicMock()
    cur.fetchall.return_value = rows
    return cur


def test_bench_is_owned_minus_started():
    cur = _cursor([("p1", 3, 80.0, 55.5, 1.25)])
    result = load_espn_ownership(cur, ["p1"], 2026, 5)
    espn = result["p1"]
    assert (espn.pos_rank, espn.percent_owned, espn.percent_started) == (3, 80.0, 55.5)
    assert espn.percent_bench == 24.5
    assert espn.percent_change == 1.25


def test_bench_is_rounded_to_two_decimals_to_drop_float_noise():
    cur = _cursor([("p1", 1, 97.48, 61.51, 0.0)])
    assert 97.48 - 61.51 != 35.97
    assert load_espn_ownership(cur, ["p1"], 2026, 5)["p1"].percent_bench == 35.97


def test_bench_floors_at_zero_and_none_when_input_missing():
    cur = _cursor([("a", 1, 40.0, 60.0, None), ("b", None, None, 10.0, None), ("c", None, 10.0, None, None)])
    result = load_espn_ownership(cur, ["a", "b", "c"], 2026, 5)
    assert result["a"].percent_bench == 0.0
    assert result["b"].percent_bench is None
    assert result["c"].percent_bench is None


def test_single_query_uses_latest_week_at_or_before_requested():
    cur = _cursor([])
    assert load_espn_ownership(cur, ["p1", "p2", "p1"], 2026, 7) == {}
    cur.execute.assert_called_once()
    sql, params = cur.execute.call_args[0]
    assert "DISTINCT ON (player_id)" in sql and "week <= %s" in sql and "week DESC" in sql
    assert params == (["p1", "p2"], 2026, 7)


def test_empty_ids_skip_the_query():
    cur = _cursor([])
    assert load_espn_ownership(cur, [], 2026, 1) == {}
    cur.execute.assert_not_called()
