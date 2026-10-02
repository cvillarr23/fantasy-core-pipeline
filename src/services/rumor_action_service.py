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
import json
from collections import Counter
from uuid import uuid4

from psycopg2.extras import Json
import logging
import os
import time

from src.api.draft import _redis_client
from src.backtesting import waiver_strategy
from src.database import get_db_connection
from src.optimization import lineup_solver
from src.optimization.lineup_solver import resolve_default_week
from src.optimization.waiver_evaluator import (
    load_live_waiver_inputs,
    publish_waiver_claim,
    resolve_live_waiver_moves,
)

logger = logging.getLogger("RumorActionService")

RUMOR_ACTION_CONFIDENCE_THRESHOLD = float(
    os.getenv("RUMOR_ACTION_CONFIDENCE_THRESHOLD", "0.75")
)
WATERMARK_REDIS_KEY = "rumor_action:last_processed_id"

BOOST_WAIVER_ACTION = "boost_waiver_priority"
BENCH_WATCH_ACTION = "bench_watch"


class RunOutcome:
    """Mutable per-invocation accounting; keeps progress even if a helper raises.

    Signal reasons and action counters are separate: one signal may propose several
    actions. Unfinished signals and unaccounted actions must never become success.
    """

    def __init__(self, team_id, season, week, source):
        self.details = dict(
            version=1,
            run_id=str(uuid4()),
            source=source,
            job_name=os.getenv("JOB_NAME"),
            team_id=team_id,
            season=season,
            week=week,
            watermark_start=None,
            watermark_end=None,
            signals_fetched=0,
            signals_qualifying=0,
            actions_proposed=0,
            actions_accepted=0,
            actions_already_queued=0,
            actions_missing_identity=0,
            actions_failed=0,
        )
        self.signals = {}

    def add(self, name, count=1):
        self.details[name] += count

    def finish_targets(self, targets, matched_players):
        for player, signal in targets.items():
            self.signals[signal["id"]] = (
                "evaluated" if player in matched_players else "no_beneficial_move"
            )

    def finalize(self, error):
        d = self.details
        d["signal_reasons"] = dict(Counter(self.signals.values()))
        d["signals_unaccounted"] = d["signal_reasons"].get("pending", 0)
        d["actions_unaccounted"] = max(
            0,
            d["actions_proposed"]
            - sum(
                d[key]
                for key in (
                    "actions_accepted",
                    "actions_already_queued",
                    "actions_missing_identity",
                    "actions_failed",
                )
            ),
        )
        if error:
            reason = "exception"
        elif d["signals_unaccounted"] or d["actions_unaccounted"]:
            reason = "unaccounted_work"
        elif d["actions_missing_identity"]:
            reason = "missing_identity"
        elif d["actions_failed"]:
            reason = "publish_failed"
        else:
            reason = (
                "no_new_signals"
                if not d["signals_fetched"]
                else (
                    "non_qualifying"
                    if not d["signals_qualifying"]
                    else (
                        "no_beneficial_move"
                        if not d["actions_proposed"]
                        else (
                            "already_queued"
                            if not d["actions_accepted"]
                            else "completed"
                        )
                    )
                )
            )
            d["reason"] = reason
            return "success"
        d["reason"] = reason
        return (
            "partial_failure"
            if d["actions_accepted"] or d["actions_already_queued"]
            else "failure"
        )


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
        {
            "id": rid,
            "player_id": pid,
            "sentiment": sentiment,
            "confidence": confidence,
            "suggested_action": action,
        }
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


def _evaluate_and_publish_waiver_claims(
    team_id: str, season: int, week: int, waiver_targets: dict, outcome=None
) -> list:
    """Re-runs waiver_strategy's normal weekly evaluation, but only publishes the proposed
    move(s) whose add_player_id is one of this run's qualifying boost_waiver_priority
    signals - any other move the evaluator would also propose today is left for the
    regular weekly cadence, not published early just because this endpoint happened to
    run."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            bench_by_position, free_agents_by_position = load_live_waiver_inputs(
                cursor, team_id, season, week
            )
            moves = waiver_strategy.propose_waiver_moves(
                bench_by_position, free_agents_by_position
            )
            matched_moves = [
                move for move in moves if move["add_player_id"] in waiver_targets
            ]

            if outcome:
                outcome.add("actions_proposed", len(matched_moves))
            targets = []
            for move in matched_moves:
                # Resolved one move at a time (rather than batching matched_moves through
                # a single resolve_live_waiver_moves call) so a move dropped for missing
                # espn_id doesn't desync the 1:1 correspondence with its source signal.
                resolved = resolve_live_waiver_moves(cursor, [move], season, week)
                if not resolved:
                    if outcome:
                        outcome.add("actions_missing_identity")
                    continue
                target = resolved[0]
                signal = waiver_targets[move["add_player_id"]]
                target["decision_inputs"] = _decorate_decision_inputs(
                    target.get("decision_inputs"), signal
                )
                targets.append(target)

    # Published after the DB connection is returned: publish_waiver_claim retries with
    # backoff (up to ~2 min), which mustn't pin a pooled connection. Claims the gateway
    # reports as already queued aren't counted as published.
    published = []
    for target in targets:
        try:
            accepted = publish_waiver_claim(team_id, target)
        except Exception:
            if outcome:
                outcome.add("actions_failed")
            raise
        if outcome:
            outcome.add("actions_accepted" if accepted else "actions_already_queued")
        if accepted:
            published.append(target)
    if outcome:
        outcome.finish_targets(
            waiver_targets, {m["add_player_id"] for m in matched_moves}
        )
    return published


def _evaluate_and_publish_lineup_swaps(
    team_id: str, season: int, week: int, bench_watch_targets: dict, outcome=None
) -> list:
    """Re-runs lineup_solver's weekly pregame evaluation, but only publishes swap(s) whose
    drop_player_id is one of this run's qualifying bench_watch signals - a bench_watch
    signal is bad news about a currently-started player, so it surfaces as a drop, not
    an add. Missing lineup inputs raise InsufficientLineupInputError, same as the weekly
    optimizer, rather than looking like "no swap needed". Signals match on the internal
    drop_player_id; only then are the ids translated to ESPN's for publishing (#187)."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            swaps = lineup_solver.evaluate_lineup(cursor, team_id, season, week)[
                "swaps"
            ]

            matched_swaps = [
                swap for swap in swaps if swap["drop_player_id"] in bench_watch_targets
            ]
            if outcome:
                outcome.add("actions_proposed", len(matched_swaps))
            for swap in matched_swaps:
                signal = bench_watch_targets[swap["drop_player_id"]]
                swap["decision_inputs"] = _decorate_decision_inputs(
                    swap.get("decision_inputs"), signal
                )
            try:
                resolved_swaps = lineup_solver.resolve_lineup_swaps(
                    cursor, matched_swaps
                )
            except lineup_solver.InsufficientLineupInputError:
                if outcome:
                    outcome.add("actions_missing_identity", len(matched_swaps))
                raise
            if outcome:
                outcome.add(
                    "actions_missing_identity", len(matched_swaps) - len(resolved_swaps)
                )

    if resolved_swaps:
        try:
            accepted = lineup_solver.publish_lineup_swaps(team_id, resolved_swaps)
        except Exception:
            if outcome:
                outcome.add("actions_failed", len(resolved_swaps))
            raise
        if outcome:
            outcome.add(
                "actions_already_queued" if accepted is False else "actions_accepted",
                len(resolved_swaps),
            )
    if outcome:
        outcome.finish_targets(
            bench_watch_targets, {s["drop_player_id"] for s in matched_swaps}
        )

    return resolved_swaps


def evaluate_and_publish_rumor_actions(
    team_id: str, season: int, week: int, *, source="service"
) -> dict:
    """Shared CLI/HTTP boundary: record exactly once, including early failures/no-ops."""
    outcome = RunOutcome(team_id, season, week, source)
    started_at = time.monotonic()
    error = None
    try:
        return _evaluate_and_publish_rumor_actions(team_id, season, week, outcome)
    except Exception as exc:
        error = str(exc)
        raise
    finally:
        status = outcome.finalize(error is not None)
        _record_execution(
            status=status,
            duration_ms=int((time.monotonic() - started_at) * 1000),
            error_message=error,
            season=season,
            outcome_details=outcome.details,
        )


def _evaluate_and_publish_rumor_actions(team_id, season, week, outcome):
    redis_client = _redis_client()
    watermark = _get_watermark(redis_client)
    outcome.details.update(watermark_start=watermark, watermark_end=watermark)

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            signals = fetch_new_signals(cursor, watermark)

    outcome.details["signals_fetched"] = len(signals)
    outcome.signals = {s["id"]: "pending" for s in signals}
    if not signals:
        return {
            "considered": 0,
            "waiver_claims_published": [],
            "lineup_swaps_published": [],
            "watermark": watermark,
        }

    waiver_targets = _qualifying_signals_by_action(signals, BOOST_WAIVER_ACTION)
    bench_watch_targets = _qualifying_signals_by_action(signals, BENCH_WATCH_ACTION)

    selected = {
        s["id"] for s in [*waiver_targets.values(), *bench_watch_targets.values()]
    }
    outcome.details["signals_qualifying"] = len(selected)
    for signal in signals:
        if signal["id"] not in selected:
            qualifies = (
                signal["suggested_action"] in (BOOST_WAIVER_ACTION, BENCH_WATCH_ACTION)
                and signal["confidence"] >= RUMOR_ACTION_CONFIDENCE_THRESHOLD
            )
            outcome.signals[signal["id"]] = (
                "superseded" if qualifies else "non_qualifying"
            )

    published_waiver_claims: list = []
    published_lineup_swaps: list = []
    try:
        if waiver_targets:
            published_waiver_claims = _evaluate_and_publish_waiver_claims(
                team_id, season, week, waiver_targets, outcome
            )
        if bench_watch_targets:
            published_lineup_swaps = _evaluate_and_publish_lineup_swaps(
                team_id, season, week, bench_watch_targets, outcome
            )
    finally:
        # Advance past everything fetched even on a raise (fail-open, like
        # _record_bot_decision) - a stuck watermark blocks all future progress.
        new_watermark = max(signal["id"] for signal in signals)
        _set_watermark(redis_client, new_watermark)
        outcome.details["watermark_end"] = new_watermark

    return {
        "considered": len(signals),
        "waiver_claims_published": published_waiver_claims,
        "lineup_swaps_published": published_lineup_swaps,
        "watermark": new_watermark,
    }


def _record_execution(
    status: str,
    duration_ms: int,
    error_message: str = None,
    season: int = None,
    outcome_details: dict = None,
) -> None:
    # Keep a searchable fallback even if PostgreSQL cannot record the outcome.
    logger.info(
        "rumor_action_outcome %s",
        json.dumps(
            dict(
                status=status,
                duration_ms=duration_ms,
                error_message=error_message,
                **(outcome_details or {}),
            )
        ),
    )
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO execution_log
                        (transaction_type, bot_name, status, duration_ms, error_message, season, outcome_details)
                    VALUES ('rumor_action_trigger', 'rumor_action_service', %s, %s, %s, %s, %s);
                    """,
                    (
                        status,
                        duration_ms,
                        error_message,
                        season,
                        Json(outcome_details or {}),
                    ),
                )
    except Exception as error:
        logger.error(
            "Failed to record rumor_action_service execution outcome: %s", error
        )


def main(argv=None):
    # Mirrors waiver_evaluator.py's __main__ shape - every CronJob in fantasy-infra runs a
    # `python -m` module directly rather than curling the live FastAPI service.
    parser = argparse.ArgumentParser(
        description="Evaluate and publish today's rumor-driven actions."
    )
    parser.add_argument("--team-id", required=True)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--week", type=int, default=None)
    args = parser.parse_args(argv)
    week = args.week if args.week is not None else resolve_default_week()

    evaluate_and_publish_rumor_actions(args.team_id, args.season, week, source="cli")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
