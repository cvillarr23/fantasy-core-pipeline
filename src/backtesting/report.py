"""
Backtest run output (CORE-15): a local JSON+CSV report every run, plus a `backtest_results`
row persisted to Postgres so a future Grafana panel can chart strategy performance over
time - that dashboard itself is fantasy-infra follow-up work, not built here.
"""
import csv
import json
import logging
import os

logger = logging.getLogger("BacktestReport")

DEFAULT_OUTPUT_ROOT = "backtest_output"


def write_json(summary: dict, season: int, out_dir: str = DEFAULT_OUTPUT_ROOT) -> str:
    season_dir = os.path.join(out_dir, str(season))
    os.makedirs(season_dir, exist_ok=True)
    path = os.path.join(season_dir, f"{season}_report.json")
    with open(path, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Wrote backtest JSON summary to {path}.")
    return path


def write_csv(weekly_records: list, season: int, out_dir: str = DEFAULT_OUTPUT_ROOT) -> str:
    season_dir = os.path.join(out_dir, str(season))
    os.makedirs(season_dir, exist_ok=True)
    path = os.path.join(season_dir, f"{season}_weekly.csv")
    fieldnames = ["week", "our_points", "optimal_points", "naive_points"]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for record in weekly_records:
            writer.writerow({k: record.get(k) for k in fieldnames})
    logger.info(f"Wrote backtest weekly CSV to {path}.")
    return path


def persist_backtest_results(conn, season: int, summary: dict) -> None:
    lineup = summary.get("lineup", {})
    waiver = summary.get("waiver", {})
    draft = summary.get("draft", {})
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO backtest_results (
                season, our_lineup_points, optimal_lineup_points, naive_baseline_points,
                our_waiver_points, no_moves_points, our_draft_points, pure_adp_draft_points,
                summary_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                season,
                lineup.get("our_lineup_points"), lineup.get("optimal_lineup_points"), lineup.get("naive_baseline_points"),
                waiver.get("our_waiver_points"), waiver.get("no_moves_points"),
                draft.get("our_draft_points"), draft.get("pure_adp_draft_points"),
                json.dumps(summary),
            ),
        )
    logger.info(f"Persisted backtest_results row for season {season}.")
