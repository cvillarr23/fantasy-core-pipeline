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
from typing import Optional

import polars as pl

from src.backtesting import backtest_schema, draft_simulator, historical_data, report, scoring, waiver_strategy
from src.database import get_db_connection
from src.optimization import lineup_solver, vorp_matrix

logger = logging.getLogger("BacktestSeasonReplay")


def default_weeks(season: int) -> range:
    """1..N regular-season weeks for this season (18 since 2021, 17 before), derived from
    the real schedule rather than a hardcoded year cutoff - modern seasons have a week 18
    that a fixed range(1, 18) would silently drop."""
    return range(1, historical_data.load_regular_season_week_count(season) + 1)


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


def seed_weekly_stats_shell(
    conn, player_pool: pl.DataFrame, projected_points_by_player: dict, season: int, weeks,
    teams_playing_by_week: dict,
) -> int:
    """One weekly_stats row per player per week, for every week that player's team actually
    plays, with the fixed preseason projected_points (0.0 for anyone outside the ADP-ranked
    pool). Restricted by teams_playing_by_week (see historical_data.load_teams_playing_by_week)
    because lineup_solver treats the *absence* of a weekly_stats row as its only bye-week
    signal (see CLAUDE.md's CORE-06 notes) - seeding every player every week regardless of
    bye would make lineup_solver start players whose team isn't even playing that week."""
    rows = []
    for player in player_pool.iter_rows(named=True):
        pid = player["sleeper_id"]
        team = player["team"]
        projected = projected_points_by_player.get(pid, 0.0)
        for week in weeks:
            if team not in teams_playing_by_week.get(week, set()):
                continue
            rows.append((pid, season, week, projected, team))
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


def persist_starters(conn, team_id: str, starter_ids: set, roster_player_ids: list) -> None:
    """Marks exactly `starter_ids` as is_starter=TRUE (rest of the team's roster FALSE).
    Without this, every drafted player stays is_starter=FALSE forever: vorp_matrix's
    rosters_populated branch (see calculate_replacement_baselines) uses is_starter to
    separate real starters from bench/free-agent replacement candidates, and
    waiver_strategy's bench query treats is_starter IS NOT TRUE as droppable - both would
    silently misbehave (a below-replacement free agent looks like the baseline; a locked-in
    starter looks droppable) if nobody's is_starter is ever actually set."""
    if not roster_player_ids:
        return
    rows = [(pid in starter_ids, team_id, pid) for pid in roster_player_ids]
    with conn.cursor() as cursor:
        cursor.executemany(
            "UPDATE league_rosters SET is_starter = %s WHERE team_id = %s AND player_id = %s;",
            rows,
        )


def assign_initial_starters_all_teams(conn, season: int) -> None:
    """Sets is_starter for every team's drafted roster (by projected_points, the only signal
    available before any actual/VORP data exists) immediately after the draft and before the
    first VORP run - vorp_matrix's replacement-baseline math depends on is_starter being
    meaningful league-wide, not just for our own team."""
    for team_id in draft_simulator.ALL_TEAM_IDS:
        with conn.cursor() as cursor:
            roster = load_roster_with_projected(cursor, team_id, season)
        if not roster:
            continue
        starters = scoring.compute_naive_starters(roster)
        starter_ids = {p["player_id"] for p in starters}
        persist_starters(conn, team_id, starter_ids, [p["player_id"] for p in roster])


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
    persist_starters(
        conn, draft_simulator.OUR_TEAM_ID,
        {p["player_id"] for p in our_starters}, [p["player_id"] for p in roster_pool],
    )

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


def run_season_replay(season: int, weeks=None, rounds: Optional[int] = None, out_dir: str = report.DEFAULT_OUTPUT_ROOT) -> dict:
    weeks = list(weeks) if weeks is not None else list(default_weeks(season))
    rounds = rounds if rounds is not None else draft_simulator.DEFAULT_ROUNDS

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
        teams_playing_by_week = historical_data.load_teams_playing_by_week(season, weeks)

        with get_db_connection() as conn:
            historical_data.seed_players(conn, player_pool)
            historical_data.seed_league_settings(conn, season)
            seed_weekly_stats_shell(conn, player_pool, projected_points_by_player, season, weeks, teams_playing_by_week)

        with get_db_connection() as conn:
            picks = draft_simulator.run_draft(conn, adp, season, rounds=rounds, week=1)
            draft_simulator.seed_rosters_from_draft(conn, picks)
            assign_initial_starters_all_teams(conn, season)

        our_player_ids = [p["player_id"] for p in picks if p["team_id"] == draft_simulator.OUR_TEAM_ID]
        adp_rosters = draft_simulator.simulate_pure_adp_draft(adp, team_count=len(draft_simulator.ALL_TEAM_IDS), rounds=rounds)
        adp_our_player_ids = adp_rosters.get(draft_simulator.OUR_TEAM_ID, [])

        actuals_df = historical_data.load_actual_weekly_stats(season)
        actuals_by_week = build_player_id_actuals(actuals_df, player_pool)
        # Restricted to the weeks actually replayed - draft scoring must cover the same
        # period as lineup/waiver scoring, or a comparator with more weeks of data always
        # looks artificially better/worse regardless of decision quality.
        season_actual_totals: dict = {}
        for week in weeks:
            for pid, pts in actuals_by_week.get(week, {}).items():
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

    # Outside backtest_db(schema): src.database.db_pool has been restored to the real
    # (public-schema) pool. persist_backtest_results MUST run out here - the isolated
    # backtest_<season> schema also has its own backtest_results table (created by the same
    # run_migrations() call provision_schema uses), and that schema gets DROPPED on the next
    # run, so writing to it instead of public.backtest_results would silently lose every
    # run's result the moment someone re-runs the harness.
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
