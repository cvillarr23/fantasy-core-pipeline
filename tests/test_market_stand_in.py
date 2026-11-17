import polars as pl
import pytest

from src.backtesting import market_stand_in


def _espn(rows):
    return pl.DataFrame(rows, schema={"player_id": pl.Utf8, "position": pl.Utf8, "percent_started": pl.Float64}, orient="row")


def _ecr(rows):
    return pl.DataFrame(rows, schema={"player_id": pl.Utf8, "ecr": pl.Float64}, orient="row")


def test_matching_orders_give_spearman_one_per_position():
    espn = _espn([("a", "RB", 90.0), ("b", "RB", 50.0), ("c", "RB", 5.0), ("q1", "QB", 80.0), ("q2", "QB", 20.0)])
    ecr = _ecr([("a", 1.0), ("b", 2.0), ("c", 3.0), ("q1", 1.0), ("q2", 2.0)])

    out = {r["position"]: r for r in market_stand_in.spearman_by_position(espn, ecr).to_dicts()}

    assert out["RB"]["spearman"] == pytest.approx(1.0)
    assert out["RB"]["players"] == 3
    assert out["QB"]["spearman"] == pytest.approx(1.0)


def test_reversed_order_gives_minus_one_and_only_common_players_count():
    espn = _espn([("a", "WR", 90.0), ("b", "WR", 50.0), ("c", "WR", 5.0), ("espn_only", "WR", 99.0)])
    ecr = _ecr([("a", 3.0), ("b", 2.0), ("c", 1.0), ("ecr_only", 1.0)])

    row = market_stand_in.spearman_by_position(espn, ecr).to_dicts()[0]

    assert row["spearman"] == pytest.approx(-1.0)
    assert row["players"] == 3


def test_flags_positions_under_the_floor():
    espn = _espn([("a", "TE", 90.0), ("b", "TE", 50.0), ("c", "TE", 5.0)])
    ecr = _ecr([("a", 2.0), ("b", 3.0), ("c", 1.0)])
    row = market_stand_in.spearman_by_position(espn, ecr).to_dicts()[0]
    assert row["below_floor"] is True
