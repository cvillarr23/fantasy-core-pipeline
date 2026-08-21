"""
Week-by-week backtest orchestrator (CORE-15).

NO-LOOKAHEAD INVARIANT (the single property most worth protecting against a refactor):
for every week N, `reveal_week_actuals` (which writes that week's real actual_points into
weekly_stats and hands the in-memory actuals dict to the scoring functions) is called ONLY
AFTER that week's decision phase - vorp_matrix.run_vorp_pipeline, the lineup solve, and
waiver_strategy.run_weekly_waivers - has already run. Every week's actual results are held
in memory from the season's start (loaded once via historical_data.load_actual_weekly_stats)
specifically so this ordering is a property of this module's control flow, not of what data
happens to exist in the database. Getting this backwards would silently let the system
"cheat" by seeing results before deciding - see tests/test_season_replay.py.
"""
import argparse
import logging

import polars as pl

from src.backtesting import backtest_schema, draft_simulator, historical_data, report, scoring, waiver_strategy
from src.database import get_db_connection
from src.optimization import lineup_solver, vorp_matrix

logger = logging.getLogger("BacktestSeasonReplay")

DEFAULT_WEEKS = range(1, 18)
DEFAULT_ROUNDS = 16


def build_player_id_actuals(actuals: pl.DataFrame, player_pool: pl.DataFrame) -> dict:
    """gsis_id-keyed actuals -> {week: {player_id: actual_points}}, via player_pool's
    sleeper_id/gsis_id crosswalk (this repo's player_id == sleeper_id)."""
    if actuals.is_empty():
        return {}
    joined = actuals.join(player_pool.select(["sleeper_id", "gsis_id"]), on="gsis_id", how="inner")
    by_week: dict = {}
    for row in joined.iter_rows(named=True):
        by_week.setdefault(row["week"], {})[row["sleeper_id"]] = row["actual_points"]
    return by_week


def seed_weekly_stats_shell(conn, player_pool: pl.DataFrame, projected_points_by_player: dict, season: int, weeks) -> int:
    """One weekly_stats row per player per week, all weeks, with the fixed preseason
    projected_points (0.0 for anyone outside the ADP-ranked pool) - vorp_matrix and the
    waiver free-agent lookup both need a row to exist for every player/week to consider
    them at all, matching production's players-JOIN-weekly_stats query shape."""
    rows = []
    for player in player_pool.iter_rows(named=True):
        pid = player["sleeper_id"]
        projected = projected_points_by_player.get(pid, 0.0)
        for week in weeks:
            rows.append((pid, season, week, projected, player["team"]))
    if not rows:
        return 0
    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO weekly_stats (player_id, season, week, projected_points, team)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (player_id, season, week) DO UPDATE SET
                projected_points = EXCLUDED.projected_points, team = EXCLUDED.team;
            """,
            rows,
        )
    return len(rows)


def load_roster_with_projected(cursor, team_id: str, season: int) -> list:
    cursor.execute(
        """
        SELECT p.player_id, p.name, p.position, COALESCE(s.projected_points, 0.0) AS projected_points
        FROM players p
        JOIN league_rosters r ON p.player_id = r.player_id
        JOIN weekly_stats s ON p.player_id = s.player_id
        WHERE r.team_id = %s AND s.season = %s AND s.week = 1;
        """,
        (team_id, season),
    )
    return [
        {"player_id": pid, "name": name, "position": pos, "projected_points": pp}
        for pid, name, pos, pp in cursor.fetchall()
    ]


def load_current_roster_ids(cursor, team_id: str) -> set:
    cursor.execute("SELECT player_id FROM league_rosters WHERE team_id = %s;", (team_id,))
    return {row[0] for row in cursor.fetchall()}


def reveal_week_actuals(conn, season: int, week: int, actuals_for_week: dict) -> None:
    """Writes week N's real actual_points into weekly_stats. MUST be called only after
    week N's decision phase has already run - see module docstring."""
    if not actuals_for_week:
        return
    rows = [(points, player_id, season, week) for player_id, points in actuals_for_week.items()]
    with conn.cursor() as cursor:
        cursor.executemany(
            "UPDATE weekly_stats SET actual_points = %s WHERE player_id = %s AND season = %s AND week = %s;",
            rows,
        )


def run_week(conn, season: int, week: int, actuals_for_week: dict, naive_starters: list, static_roster_ids: set) -> dict:
    """One week's decision phase (VORP -> lineup solve -> waivers) followed by the reveal
    phase (write actuals, then score). See module docstring for the no-lookahead ordering
    this function exists to enforce."""
    vorp_matrix.run_vorp_pipeline(season=season, week=week)

    with conn.cursor() as cursor:
        roster_pool = lineup_solver.load_roster_pool(cursor, draft_simulator.OUR_TEAM_ID, season, week)
    our_starters = lineup_solver.build_and_solve_lineup(roster_pool)

    waiver_strategy.run_weekly_waivers(conn, draft_simulator.OUR_TEAM_ID, season, week)

    with conn.cursor() as cursor:
        current_roster_ids = load_current_roster_ids(cursor, draft_simulator.OUR_TEAM_ID)

    # --- reveal phase: only from here on does this week's real actuals get used ---
    reveal_week_actuals(conn, season, week, actuals_for_week)

    lineup_record = scoring.score_lineup_week(our_starters, roster_pool, naive_starters, actuals_for_week)
    lineup_record["week"] = week

    our_waiver_points = sum(actuals_for_week.get(pid, 0.0) for pid in current_roster_ids)
    static_points = sum(actuals_for_week.get(pid, 0.0) for pid in static_roster_ids)

    return {"lineup": lineup_record, "our_waiver_points": our_waiver_points, "static_points": static_points}


def run_season_replay(season: int, weeks=DEFAULT_WEEKS, rounds: int = DEFAULT_ROUNDS, out_dir: str = report.DEFAULT_OUTPUT_ROOT) -> dict:
    schema = backtest_schema.schema_name(season)
    backtest_schema.provision_schema(schema, wipe=True)

    with backtest_schema.backtest_db(schema):
        player_pool = historical_data.build_player_pool(season)
        prior_actuals = historical_data.load_actual_weekly_stats(season - 1)
        adp_df = historical_data.resolve_adp(season, player_pool, prior_season_actuals=prior_actuals)
        adp = {row["player_id"]: row["adp_rank"] for row in adp_df.iter_rows(named=True)}
        projected_points_by_player = {
            pid: historical_data.rank_to_synthetic_points(rank) for pid, rank in adp.items()
        }

        with get_db_connection() as conn:
            historical_data.seed_players(conn, player_pool)
            historical_data.seed_league_settings(conn, season)
            seed_weekly_stats_shell(conn, player_pool, projected_points_by_player, season, weeks)

        with get_db_connection() as conn:
            picks = draft_simulator.run_draft(conn, adp, season, rounds=rounds, week=1)
            draft_simulator.seed_rosters_from_draft(conn, picks)

        our_player_ids = [p["player_id"] for p in picks if p["team_id"] == draft_simulator.OUR_TEAM_ID]
        adp_rosters = draft_simulator.simulate_pure_adp_draft(adp, team_count=len(draft_simulator.ALL_TEAM_IDS), rounds=rounds)
        adp_our_player_ids = adp_rosters.get(draft_simulator.OUR_TEAM_ID, [])

        actuals_df = historical_data.load_actual_weekly_stats(season)
        actuals_by_week = build_player_id_actuals(actuals_df, player_pool)
        season_actual_totals: dict = {}
        for week_actuals in actuals_by_week.values():
            for pid, pts in week_actuals.items():
                season_actual_totals[pid] = season_actual_totals.get(pid, 0.0) + pts

        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                draft_roster_pool = load_roster_with_projected(cursor, draft_simulator.OUR_TEAM_ID, season)
            naive_starters = scoring.compute_naive_starters(draft_roster_pool)
            static_roster_ids = {p["player_id"] for p in draft_roster_pool}

        weekly_lineup_records = []
        our_weekly_waiver_points = []
        static_weekly_points = []
        for week in weeks:
            with get_db_connection() as conn:
                week_result = run_week(conn, season, week, actuals_by_week.get(week, {}), naive_starters, static_roster_ids)
            weekly_lineup_records.append(week_result["lineup"])
            our_weekly_waiver_points.append(week_result["our_waiver_points"])
            static_weekly_points.append(week_result["static_points"])

        waiver_summary = scoring.score_waivers(our_weekly_waiver_points, static_weekly_points)
        draft_summary = scoring.score_draft(our_player_ids, adp_our_player_ids, season_actual_totals)
        summary = scoring.summarize_season(weekly_lineup_records, waiver_summary, draft_summary)

        report.write_json(summary, season, out_dir=out_dir)
        report.write_csv(weekly_lineup_records, season, out_dir=out_dir)
        with get_db_connection() as conn:
            report.persist_backtest_results(conn, season, summary)

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay a historical season through the fantasy pipeline's decision logic.")
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    run_season_replay(season=args.season)
