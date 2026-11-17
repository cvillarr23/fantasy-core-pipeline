"""
Scoring comparators for the backtesting harness (CORE-15): our system's decisions vs. a
true-optimal-in-hindsight lineup and a naive baseline (lineup), a static no-moves roster
(waiver), and a pure-ADP-drafted roster (draft).
"""
from src.backtesting.historical_data import HARDCODED_LEAGUE_CONFIG
from src.optimization.lineup_solver import build_and_solve_lineup


def solve_backtest_lineup(roster_pool: list) -> list:
    """build_and_solve_lineup over the backtest league's slots. The solver's defaults
    include K and D/ST, which the skill-only backtest pool can never fill, so every solve
    would be infeasible and start nobody."""
    return build_and_solve_lineup(roster_pool, lineup_slot_counts=HARDCODED_LEAGUE_CONFIG["lineup_slot_counts"])


def build_hindsight_pool(roster_pool: list, actual_points_by_player: dict) -> list:
    """roster_pool entries (player_id/name/position/vorp_score) with vorp_score swapped for
    that week's real actual points - reuses solve_backtest_lineup unmodified, since it only
    depends on a "vorp_score"-named key, not its provenance."""
    return [
        {**p, "vorp_score": actual_points_by_player.get(p["player_id"], 0.0)}
        for p in roster_pool
    ]


def compute_naive_starters(roster_pool_with_projected: list) -> list:
    """Naive baseline: always start the highest fixed preseason projected_points, computed
    once (from the draft-time roster) and reused unchanged for every week."""
    naive_pool = [
        {**p, "vorp_score": p.get("projected_points", 0.0)}
        for p in roster_pool_with_projected
    ]
    return solve_backtest_lineup(naive_pool)


def _sum_actual_points(starters: list, actual_points_by_player: dict) -> float:
    return sum(actual_points_by_player.get(p["player_id"], 0.0) for p in starters)


def score_lineup_week(our_starters: list, roster_pool: list, naive_starters: list, actual_points_by_player: dict) -> dict:
    hindsight_pool = build_hindsight_pool(roster_pool, actual_points_by_player)
    optimal_starters = solve_backtest_lineup(hindsight_pool)
    return {
        "our_points": _sum_actual_points(our_starters, actual_points_by_player),
        "optimal_points": _sum_actual_points(optimal_starters, actual_points_by_player),
        "naive_points": _sum_actual_points(naive_starters, actual_points_by_player),
    }


def score_waivers(our_weekly_points: list, static_weekly_points: list) -> dict:
    return {
        "our_waiver_points": sum(our_weekly_points),
        "no_moves_points": sum(static_weekly_points),
    }


def score_draft(our_player_ids: list, adp_player_ids: list, season_actual_totals: dict) -> dict:
    return {
        "our_draft_points": sum(season_actual_totals.get(pid, 0.0) for pid in our_player_ids),
        "pure_adp_draft_points": sum(season_actual_totals.get(pid, 0.0) for pid in adp_player_ids),
    }


def summarize_season(weekly_lineup_records: list, waiver_summary: dict, draft_summary: dict) -> dict:
    return {
        "lineup": {
            "our_lineup_points": sum(r["our_points"] for r in weekly_lineup_records),
            "optimal_lineup_points": sum(r["optimal_points"] for r in weekly_lineup_records),
            "naive_baseline_points": sum(r["naive_points"] for r in weekly_lineup_records),
        },
        "waiver": waiver_summary,
        "draft": draft_summary,
        "weekly": weekly_lineup_records,
    }
