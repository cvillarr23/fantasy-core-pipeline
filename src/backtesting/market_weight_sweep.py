"""
Sweep vorp_matrix's market weight over historical seasons to choose ESPN_MARKET_WEIGHT.

No archive of weekly ESPN start% exists, so each replay ranks FantasyPros weekly ECR instead
(market_source="ecr"; see market_stand_in for how closely the two agree). Replays use the
rolling base projection, so the market prior competes with in-season information as it does
live, rather than being the only in-season signal (see season_replay's module docstring).

Each replay drops and rebuilds its backtest_<season> schema and writes public.backtest_results,
so point DB_HOST/PGPORT at a throwaway Postgres, never production:

    python -m src.backtesting.market_weight_sweep --seasons 2023 2024 2025
"""
import argparse
import logging
import os

from src.backtesting import report, season_replay

logger = logging.getLogger("BacktestMarketWeightSweep")

DEFAULT_SEASONS = (2023, 2024, 2025)
DEFAULT_WEIGHTS = (0.0, 0.05, 0.1, 0.15, 0.2, 0.3)


def run_sweep(seasons, weights, replay=season_replay.run_season_replay,
              out_root: str = report.DEFAULT_OUTPUT_ROOT) -> list:
    """One row per (season, weight): waiver_gain (our waiver roster's points minus the
    no-moves roster's), lineup_points and optimal_points."""
    rows = []
    for season in seasons:
        for weight in weights:
            logger.info(f"Replaying {season} at market weight {weight}...")
            summary = replay(
                season=season, market_weight=weight, market_source="ecr", base_projection="rolling",
                out_dir=os.path.join(out_root, "market_sweep", f"{season}_w{weight}"),
            )
            rows.append(_sweep_row(season, weight, summary))
    return rows


def _sweep_row(season: int, weight: float, summary: dict) -> dict:
    waiver, lineup = summary["waiver"], summary["lineup"]
    return {
        "season": season, "weight": weight,
        "waiver_gain": waiver["our_waiver_points"] - waiver["no_moves_points"],
        "lineup_points": lineup["our_lineup_points"],
        "optimal_points": lineup["optimal_lineup_points"],
    }


def results_table(rows: list) -> str:
    """Markdown: per season, then pooled over seasons ("all"), each weight's waiver gain and
    lineup points with their change from weight 0 in the same group, and lineup efficiency
    (our lineup points / optimal)."""
    lines = [
        "| season | weight | waiver gain | vs w=0 | lineup pts | vs w=0 | lineup eff. |",
        "|---|---|---|---|---|---|---|",
    ]
    for group, group_rows in _groups(rows):
        base = next(r for r in group_rows if r["weight"] == 0.0)
        for r in group_rows:
            lines.append(_table_line(group, r, base))
    return "\n".join(lines)


def _groups(rows: list) -> list:
    """[(season, rows)] per season, then ("all", rows summed over seasons per weight)."""
    seasons = sorted({r["season"] for r in rows})
    groups = [(s, [r for r in rows if r["season"] == s]) for s in seasons]
    pooled = []
    for weight in sorted({r["weight"] for r in rows}):
        same = [r for r in rows if r["weight"] == weight]
        pooled.append({key: sum(r[key] for r in same) for key in ("waiver_gain", "lineup_points", "optimal_points")}
                      | {"weight": weight})
    return groups + [("all", pooled)]


def _table_line(group, row: dict, base: dict) -> str:
    efficiency = row["lineup_points"] / row["optimal_points"] if row["optimal_points"] else 0.0
    return (
        f"| {group} | {row['weight']} | {row['waiver_gain']:.1f} | {row['waiver_gain'] - base['waiver_gain']:+.1f} "
        f"| {row['lineup_points']:.1f} | {row['lineup_points'] - base['lineup_points']:+.1f} | {efficiency:.1%} |"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sweep the market weight over historical seasons (ECR stand-in).")
    parser.add_argument("--seasons", type=int, nargs="+", default=list(DEFAULT_SEASONS))
    parser.add_argument("--weights", type=float, nargs="+", default=list(DEFAULT_WEIGHTS))
    parser.add_argument("--out-root", default=report.DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    print(results_table(run_sweep(args.seasons, args.weights, out_root=args.out_root)))


if __name__ == "__main__":
    main()
