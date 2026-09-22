"""
Rumor-signal action evaluation (#124): turns WORKER-13's advisory rumor_signals rows into
real waiver_actions/lineup_updates queue messages, reusing this repo's existing weekly
decision logic (waiver_strategy/waiver_evaluator, lineup_solver) rather than inventing a
new one - a rumor signal only tells us a player is worth a second look, not who to drop or
which bench player replaces them, so the actual add/drop pairing still comes from the same
VORP-based evaluation the weekly cadence already runs. This just re-runs that evaluation
early and filters its output to the player(s) a qualifying signal actually named.
"""
import argparse
import logging
import os

from src.api.draft import _redis_client
from src.backtesting import waiver_strategy
from src.database import get_db_connection
from src.optimization import lineup_solver
from src.optimization.lineup_solver import resolve_default_week
from src.optimization.waiver_evaluator import publish_waiver_claim, resolve_live_waiver_moves

logger = logging.getLogger("RumorActionService")

RUMOR_ACTION_CONFIDENCE_THRESHOLD = float(os.getenv("RUMOR_ACTION_CONFIDENCE_THRESHOLD", "0.75"))
WATERMARK_REDIS_KEY = "rumor_action:last_processed_id"

BOOST_WAIVER_ACTION = "boost_waiver_priority"
BENCH_WATCH_ACTION = "bench_watch"


def fetch_new_signals(cursor, since_id: int) -> list:
    """All rumor_signals rows past the watermark, regardless of threshold - the watermark
    must advance past everything considered (including no_op/low-confidence rows), or a
    flood of those gets re-read forever. Threshold filtering happens separately, in
    _qualifying_signals_by_action."""
    cursor.execute(
        """
        SELECT id, player_id, sentiment, confidence, suggested_action
        FROM rumor_signals
        WHERE id > %s
        ORDER BY id;
        """,
        (since_id,),
    )
    return [
        {"id": rid, "player_id": pid, "sentiment": sentiment, "confidence": confidence, "suggested_action": action}
        for rid, pid, sentiment, confidence, action in cursor.fetchall()
    ]


def _get_watermark(redis_client) -> int:
    value = redis_client.get(WATERMARK_REDIS_KEY)
    return int(value) if value is not None else 0


def _set_watermark(redis_client, value: int) -> None:
    redis_client.set(WATERMARK_REDIS_KEY, str(value))


def _qualifying_signals_by_action(signals: list, action: str) -> dict:
    """player_id -> signal dict for signals matching `action` at/above threshold. Within
    one batch, the highest-id (most recent) signal for a player wins if there are
    duplicates."""
    result = {}
    for signal in signals:
        if signal["suggested_action"] != action:
            continue
        if signal["confidence"] < RUMOR_ACTION_CONFIDENCE_THRESHOLD:
            continue
        result[signal["player_id"]] = signal
    return result


def _decorate_decision_inputs(existing: dict, signal: dict) -> dict:
    return {
        **(existing or {}),
        "rumor_signal_id": signal["id"],
        "rumor_sentiment": signal["sentiment"],
        "rumor_confidence": signal["confidence"],
    }


def _evaluate_and_publish_waiver_claims(team_id: str, season: int, week: int, waiver_targets: dict) -> list:
    """Re-runs waiver_strategy's normal weekly evaluation, but only publishes the proposed
    move(s) whose add_player_id is one of this run's qualifying boost_waiver_priority
    signals - any other move the evaluator would also propose today is left for the
    regular weekly cadence, not published early just because this endpoint happened to
    run."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            bench_by_position = waiver_strategy.load_bench_by_position(cursor, team_id, season, week)
            free_agents_by_position = waiver_strategy.load_best_free_agent_by_position(cursor, season, week)
            moves = waiver_strategy.propose_waiver_moves(bench_by_position, free_agents_by_position)
            matched_moves = [move for move in moves if move["add_player_id"] in waiver_targets]

            published = []
            for move in matched_moves:
                # Resolved one move at a time (rather than batching matched_moves through
                # a single resolve_live_waiver_moves call) so a move dropped for missing
                # espn_id doesn't desync the 1:1 correspondence with its source signal.
                resolved = resolve_live_waiver_moves(cursor, [move], season, week)
                if not resolved:
                    continue
                target = resolved[0]
                signal = waiver_targets[move["add_player_id"]]
                target["decision_inputs"] = _decorate_decision_inputs(target.get("decision_inputs"), signal)
                publish_waiver_claim(team_id, target)
                published.append(target)

    return published


def _evaluate_and_publish_lineup_swaps(team_id: str, season: int, week: int, bench_watch_targets: dict) -> list:
    """Re-runs lineup_solver's normal weekly ILP, but only publishes swap(s) whose
    drop_player_id is one of this run's qualifying bench_watch signals - a bench_watch
    signal is bad news about a currently-started player, so it surfaces as a drop, not
    an add."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            roster_pool = lineup_solver.load_roster_pool(cursor, team_id, season, week)
            lineup_slot_counts = lineup_solver.load_lineup_slot_counts(cursor, season)
            optimal_starters = lineup_solver.build_and_solve_lineup(roster_pool, lineup_slot_counts=lineup_slot_counts)
            current_starters = lineup_solver.load_current_starters(cursor, team_id, season, week)

    swaps = lineup_solver.compute_lineup_swaps(optimal_starters, current_starters)
    matched_swaps = [swap for swap in swaps if swap["drop_player_id"] in bench_watch_targets]
    for swap in matched_swaps:
        signal = bench_watch_targets[swap["drop_player_id"]]
        swap["decision_inputs"] = _decorate_decision_inputs(swap.get("decision_inputs"), signal)

    if matched_swaps:
        lineup_solver.publish_lineup_swaps(team_id, matched_swaps)

    return matched_swaps


def evaluate_and_publish_rumor_actions(team_id: str, season: int, week: int) -> dict:
    redis_client = _redis_client()
    watermark = _get_watermark(redis_client)

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            signals = fetch_new_signals(cursor, watermark)

    if not signals:
        return {
            "considered": 0,
            "waiver_claims_published": [],
            "lineup_swaps_published": [],
            "watermark": watermark,
        }

    waiver_targets = _qualifying_signals_by_action(signals, BOOST_WAIVER_ACTION)
    bench_watch_targets = _qualifying_signals_by_action(signals, BENCH_WATCH_ACTION)

    published_waiver_claims: list = []
    published_lineup_swaps: list = []
    try:
        if waiver_targets:
            published_waiver_claims = _evaluate_and_publish_waiver_claims(team_id, season, week, waiver_targets)
        if bench_watch_targets:
            published_lineup_swaps = _evaluate_and_publish_lineup_swaps(team_id, season, week, bench_watch_targets)
    finally:
        # Advance past everything fetched even on a raise (fail-open, like
        # _record_bot_decision) - a stuck watermark blocks all future progress.
        new_watermark = max(signal["id"] for signal in signals)
        _set_watermark(redis_client, new_watermark)

    return {
        "considered": len(signals),
        "waiver_claims_published": published_waiver_claims,
        "lineup_swaps_published": published_lineup_swaps,
        "watermark": new_watermark,
    }


if __name__ == "__main__":
    # Mirrors waiver_evaluator.py's __main__ shape - every CronJob in fantasy-infra runs a
    # `python -m` module directly rather than curling the live FastAPI service.
    parser = argparse.ArgumentParser(description="Evaluate and publish today's rumor-driven actions.")
    parser.add_argument("--team-id", required=True)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--week", type=int, default=None)
    args = parser.parse_args()
    week = args.week if args.week is not None else resolve_default_week()
    evaluate_and_publish_rumor_actions(args.team_id, args.season, week)
