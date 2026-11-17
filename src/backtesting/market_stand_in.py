"""
Is FantasyPros weekly ECR a fair stand-in for ESPN percent_started in the market step?

The market multiplier only uses a player's percentile within position, so the stand-in is
fair when both signals order each position the same way. This ranks both with
vorp_matrix._position_percentile, the function the live and backtest paths share, over the
players present in both, and reports Spearman per position (Pearson on those average-rank
percentiles).

ESPN rows come from a CSV dump (player_id, position, percent_started) so this never needs
production credentials, e.g. from the fantasy-db pod:

    \\copy (SELECT o.player_id, p.position, o.percent_started FROM espn_player_ownership o
            JOIN players p USING (player_id) WHERE o.season = 2026 AND o.week = 10
            AND o.percent_started IS NOT NULL) TO STDOUT CSV HEADER

    python -m src.backtesting.market_stand_in --season 2026 --week 10 --espn-csv espn.csv
"""
import argparse
import logging

import polars as pl

from src.backtesting import historical_data
from src.optimization import vorp_matrix

logger = logging.getLogger("BacktestMarketStandIn")

# Below this, a position's ECR ordering is too far from ESPN's to stand in for it.
SPEARMAN_FLOOR = 0.8


def spearman_by_position(espn: pl.DataFrame, ecr: pl.DataFrame) -> pl.DataFrame:
    """espn: player_id, position, percent_started. ecr: player_id, ecr. One row per position:
    players (in both), spearman, below_floor."""
    common = espn.join(ecr, on="player_id", how="inner")
    ranked = _with_percentile(common, pl.col("percent_started"), "espn_pctile")
    ranked = _with_percentile(ranked, -pl.col("ecr"), "ecr_pctile")
    return (
        ranked.group_by("position")
        .agg(pl.len().alias("players"), pl.corr("espn_pctile", "ecr_pctile").alias("spearman"))
        .with_columns((pl.col("spearman") < SPEARMAN_FLOOR).alias("below_floor"))
        .sort("position")
    )


def _with_percentile(df: pl.DataFrame, score: pl.Expr, name: str) -> pl.DataFrame:
    """Adds `name`: vorp_matrix's within-position percentile of `score` (higher = better)."""
    return (
        df.with_columns(score.alias("market_score"))
        .with_columns(vorp_matrix._position_percentile().alias(name))
        .drop("market_score")
    )


def load_week_ecr(season: int, week: int) -> pl.DataFrame:
    ecr = historical_data.load_weekly_ecr(season, historical_data.build_player_pool(season))
    return ecr.filter(pl.col("week") == week).select(["player_id", "ecr"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare FantasyPros weekly ECR with ESPN start% per position.")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int, required=True)
    parser.add_argument("--espn-csv", required=True, help="player_id,position,percent_started dump.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    espn = pl.read_csv(args.espn_csv, schema_overrides={"player_id": pl.Utf8, "percent_started": pl.Float64})
    print(spearman_by_position(espn, load_week_ecr(args.season, args.week)))


if __name__ == "__main__":
    main()
