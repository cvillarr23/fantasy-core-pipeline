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
from src.services.rumor_action_checkpoint import (
    load_checkpoint as _load_checkpoint,
    save_checkpoint as _save_checkpoint,
)
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
MAX_PUBLISH_FAILURES = 3
MAX_SIGNAL_EVALUATION_FAILURES = 3
SIGNAL_RETRY_WINDOW_SECONDS = 24 * 60 * 60
WAIVER_RETRY_WINDOW_SECONDS = 24 * 60 * 60
# The lineup gateway retains keys for 30 minutes. Never retry an ambiguous
# delivery once that protection could have expired.
LINEUP_RETRY_WINDOW_SECONDS = 20 * 60
FAILED_JOB_MARKER_TTL_SECONDS = 7 * 24 * 60 * 60


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
        self.deferred_signals = set()
        self.missing_signals = set()

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
        d["signals_deferred"] = len(self.deferred_signals)
        d["signals_unaccounted"] = sum(
            reason == "pending" and signal_id not in self.deferred_signals
            for signal_id, reason in self.signals.items()
        )
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
            reason = d.get("failure_reason", "exception")
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


def _prepare_waiver_claims(
    team_id: str, season: int, week: int, waiver_targets: dict, outcome
) -> list:
    """Re-runs waiver_strategy's normal weekly evaluation, preparing only the proposed
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
                        outcome.missing_signals.add(
                            waiver_targets[move["add_player_id"]]["id"]
                        )
                    continue
                target = resolved[0]
                signal = waiver_targets[move["add_player_id"]]
                target["decision_inputs"] = _decorate_decision_inputs(
                    target.get("decision_inputs"), signal
                )
                targets.append(target)

    outcome.finish_targets(waiver_targets, {m["add_player_id"] for m in matched_moves})
    return targets


def _prepare_lineup_swaps(
    team_id: str, season: int, week: int, bench_watch_targets: dict, outcome
) -> list:
    """Re-runs lineup_solver's weekly pregame evaluation, preparing only swap(s) whose
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
                    outcome.missing_signals.update(
                        s["decision_inputs"]["rumor_signal_id"] for s in matched_swaps
                    )
                resolved_swaps = []
            if outcome:
                if not (matched_swaps and not resolved_swaps):
                    outcome.add(
                        "actions_missing_identity",
                        len(matched_swaps) - len(resolved_swaps),
                    )
                resolved_ids = Counter(
                    s["decision_inputs"]["rumor_signal_id"] for s in resolved_swaps
                )
                matched_ids = Counter(
                    s["decision_inputs"]["rumor_signal_id"] for s in matched_swaps
                )
                outcome.missing_signals.update(
                    s["decision_inputs"]["rumor_signal_id"]
                    for s in matched_swaps
                    if matched_ids[s["decision_inputs"]["rumor_signal_id"]]
                    > resolved_ids[s["decision_inputs"]["rumor_signal_id"]]
                )

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
    version, state = _load_checkpoint(team_id)
    watermark = state["watermark"]
    outcome.details.update(
        watermark_start=watermark, watermark_end=watermark, dropped_signals=[]
    )
    job = os.getenv("JOB_NAME")
    # Before the infra rollout, OnFailure restarts share a pod hostname. HTTP calls
    # must not use the long-lived API pod's hostname as a Job identity.
    if (
        not job
        and outcome.details["source"] == "cli"
        and os.getenv("HOSTNAME", "").startswith("fantasy-rumor-action-trigger-")
    ):
        job = os.getenv("HOSTNAME")
    outcome.details["job_name"] = job

    def save():
        nonlocal version
        version = _save_checkpoint(team_id, version, state)

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            signals = fetch_new_signals(cursor, watermark)
    outcome.details["signals_fetched"] = len(signals)
    outcome.signals = {s["id"]: "pending" for s in signals}
    entries = state["signals"]
    actions = state["actions"]
    # Keep latches across unrelated runs so an HTTP/manual trigger cannot turn a
    # failed Kubernetes Job into success. Expire old Job IDs after a week.
    now = time.time()
    failed_jobs = {}
    for failed_job, marker in state.get("failed_jobs", {}).items():
        if isinstance(marker, dict):
            reason = marker.get("reason", "Previous rumor-action Job failed")
            updated_at = marker.get("updated_at", now)
        else:
            reason, updated_at = marker, now
        if now - updated_at < FAILED_JOB_MARKER_TTL_SECONDS:
            failed_jobs[failed_job] = {"reason": reason, "updated_at": updated_at}
    state["failed_jobs"] = failed_jobs
    # Drop details are written to execution_log; they are not retry state.
    state["drops"] = {}
    errors = (
        [state["failed_jobs"][job]["reason"]]
        if job and job in state["failed_jobs"]
        else []
    )

    def record_job_failure(reason):
        if job:
            state["failed_jobs"].setdefault(
                job, {"reason": reason, "updated_at": time.time()}
            )

    # Only unprepared signals participate in latest-per-player selection. A prepared
    # action must retain its payload and identity, even if new rumors arrive.
    fresh = [
        s
        for s in signals
        if str(s["id"]) not in entries or not entries[str(s["id"])].get("prepared")
    ]
    pending_actions_by_player = {}
    pending_lineup_actions = []
    now = time.time()
    for action_id, action in actions.items():
        if action["status"] == "pending":
            retry_window = (
                LINEUP_RETRY_WINDOW_SECONDS
                if action["kind"] == "lineup"
                else WAIVER_RETRY_WINDOW_SECONDS
            )
            expired = now - action["prepared_at"] >= retry_window
            stale = (action["season"], action["week"]) != (season, week)
            if expired or stale:
                reason = (
                    f"{action['kind']}_retry_window_expired"
                    if expired
                    else "decision_week_expired"
                )
                for covered_id in action.get("covered_signal_ids", []):
                    entry = entries.get(str(covered_id))
                    if entry and entry.get("covered_action_id") == action_id:
                        covered_reason = f"covered_action_dropped:{reason}"
                        entry.update(reason=covered_reason, prepared=True)
                        outcome.details["dropped_signals"].append(
                            dict(
                                signal_id=int(covered_id),
                                reason=covered_reason,
                                attempts=action["attempts"],
                                error="Covered action is no longer safe to replay",
                            )
                        )
                continue
            if action["kind"] == "lineup":
                pending_lineup_actions.append(action_id)
            for player_id in action.get("signal_players", []):
                pending_actions_by_player.setdefault(
                    (action["kind"], str(player_id)), action_id
                )
    # Signals covered by an expired action in an earlier run are terminal drops;
    # newly fetched signals remain eligible for fresh evaluation below.
    fresh = [
        signal
        for signal in fresh
        if not entries.get(str(signal["id"]), {}).get("prepared")
    ]
    covered = set()
    deferred_lineup = set()
    for signal in fresh:
        kind = {
            BOOST_WAIVER_ACTION: "waiver",
            BENCH_WATCH_ACTION: "lineup",
        }.get(signal["suggested_action"])
        if (
            kind == "lineup"
            and signal["confidence"] >= RUMOR_ACTION_CONFIDENCE_THRESHOLD
            and pending_lineup_actions
        ):
            entries[str(signal["id"])] = {
                "reason": "pending_lineup_action",
                "actions": [],
                "prepared": False,
                "evaluation_attempts": 0,
                "first_seen_at": time.time(),
                "player_id": signal["player_id"],
            }
            deferred_lineup.add(signal["id"])
            outcome.deferred_signals.add(signal["id"])
        elif (
            kind
            and signal["confidence"] >= RUMOR_ACTION_CONFIDENCE_THRESHOLD
            and (kind, str(signal["player_id"])) in pending_actions_by_player
        ):
            action_id = pending_actions_by_player[(kind, str(signal["player_id"]))]
            entries[str(signal["id"])] = {
                "reason": "covered_by_pending_action",
                "actions": [],
                "prepared": False,
                "player_id": signal["player_id"],
                "covered_action_id": action_id,
            }
            covered_ids = actions[action_id].setdefault("covered_signal_ids", [])
            if signal["id"] not in covered_ids:
                covered_ids.append(signal["id"])
            covered.add(signal["id"])
    fresh = [s for s in fresh if s["id"] not in covered | deferred_lineup]
    waiver_targets = _qualifying_signals_by_action(fresh, BOOST_WAIVER_ACTION)
    bench_targets = _qualifying_signals_by_action(fresh, BENCH_WATCH_ACTION)
    selected = {s["id"] for s in [*waiver_targets.values(), *bench_targets.values()]}
    for signal in fresh:
        sid = signal["id"]
        if sid not in selected:
            qualifies = (
                signal["suggested_action"] in (BOOST_WAIVER_ACTION, BENCH_WATCH_ACTION)
                and signal["confidence"] >= RUMOR_ACTION_CONFIDENCE_THRESHOLD
            )
            entries[str(sid)] = {
                "reason": "superseded" if qualifies else "non_qualifying",
                "actions": [],
                "prepared": True,
            }

    def drop(action, reason, error):
        action["status"] = "dropped"
        action["error"] = error
        for sid in action["signal_ids"]:
            detail = dict(
                signal_id=sid, reason=reason, attempts=action["attempts"], error=error
            )
            outcome.details["dropped_signals"].append(detail)
            entry = entries.get(str(sid))
            if entry:
                entry.update(reason=reason, prepared=True)
        for covered_id in action.get("covered_signal_ids", []):
            entry = entries.get(str(covered_id))
            if entry and not entry.get("prepared"):
                covered_reason = f"covered_action_dropped:{reason}"
                entry.update(reason=covered_reason, prepared=True)
                detail = dict(
                    signal_id=int(covered_id),
                    reason=covered_reason,
                    attempts=action["attempts"],
                    error=error,
                )
                outcome.details["dropped_signals"].append(detail)
        record_job_failure(f"Rumor actions deliberately dropped: {reason}: {error}")

    for kind, targets, helper in (
        ("waiver", waiver_targets, _prepare_waiver_claims),
        ("lineup", bench_targets, _prepare_lineup_swaps),
    ):
        if not targets:
            continue
        eligible = {}
        now = time.time()
        for signal in targets.values():
            sid = signal["id"]
            entry = entries.setdefault(
                str(sid),
                {
                    "reason": "pending",
                    "actions": [],
                    "prepared": False,
                    "evaluation_attempts": 0,
                    "first_seen_at": now,
                    "player_id": signal["player_id"],
                },
            )
            entry.setdefault("evaluation_attempts", 0)
            entry.setdefault("first_seen_at", now)
            if now - entry["first_seen_at"] >= SIGNAL_RETRY_WINDOW_SECONDS:
                entry["reason"] = "evaluation_retry_window_expired"
                entry["prepared"] = True
                detail = dict(
                    signal_id=sid,
                    reason=entry["reason"],
                    attempts=entry["evaluation_attempts"],
                    error=entry.get("error", "Rumor action could not be evaluated"),
                )
                outcome.details["dropped_signals"].append(detail)
                errors.append(detail["reason"])
                continue
            eligible[signal["player_id"]] = signal
        if not eligible:
            save()
            continue
        targets = eligible
        try:
            payloads = helper(team_id, season, week, targets, outcome)
        except Exception as exc:
            # Infrastructure failures keep their separate budget, but repeated
            # failures for these signals are bounded independently of publisher retries.
            for signal in targets.values():
                entry = entries[str(signal["id"])]
                entry["evaluation_attempts"] += 1
                entry["error"] = str(exc)
                if entry["evaluation_attempts"] >= MAX_SIGNAL_EVALUATION_FAILURES:
                    entry["reason"] = "evaluation_retry_limit_exceeded"
                    entry["prepared"] = True
                    detail = dict(
                        signal_id=signal["id"],
                        reason=entry["reason"],
                        attempts=entry["evaluation_attempts"],
                        error=str(exc),
                    )
                    outcome.details["dropped_signals"].append(detail)
                    record_job_failure(
                        f"Rumor signal {signal['id']} evaluation retry limit exceeded; inspect execution outcome details"
                    )
            errors.append(str(exc))
            save()
            continue
        for signal in targets.values():
            sid = signal["id"]
            reason = outcome.signals[sid]
            if reason == "pending":
                outcome.details["failure_reason"] = "unaccounted_work"
                errors.append(f"Unaccounted rumor signal {sid}")
                continue
            entries[str(sid)].update(reason=reason, prepared=True)
        groups = (
            [[p] for p in payloads]
            if kind == "waiver"
            else ([payloads] if payloads else [])
        )
        for group in groups:
            ids = sorted({p["decision_inputs"]["rumor_signal_id"] for p in group})
            aid = str(uuid4())
            actions[aid] = dict(
                kind=kind,
                payload=group,
                signal_ids=ids,
                signal_players=sorted(
                    {entries[str(sid)]["player_id"] for sid in ids}, key=str
                ),
                attempts=0,
                status="pending",
                season=season,
                week=week,
                first_attempt_at=None,
                prepared_at=time.time(),
            )
            if kind == "lineup":
                actions[aid]["key"] = lineup_solver.lineup_idempotency_key(
                    team_id, group, job_name=job
                )
            for sid in ids:
                entries[str(sid)]["actions"].append(aid)
        missing_ids = outcome.missing_signals & {s["id"] for s in targets.values()}
        for sid in missing_ids:
            if str(sid) in entries:
                entries[str(sid)]["reason"] = "missing_identity"
                detail = dict(
                    signal_id=sid,
                    reason="missing_identity",
                    attempts=0,
                    error="No ESPN identity",
                )
                outcome.details["dropped_signals"].append(detail)
        if missing_ids:
            outcome.details["failure_reason"] = "missing_identity"
            errors.append("Missing ESPN identity for proposed rumor actions")
            record_job_failure(errors[-1])
    if outcome.details["dropped_signals"]:
        record_job_failure(
            "Rumor signals were deliberately dropped; inspect execution outcome details"
        )
    # Commit immutable payloads BEFORE any HTTP calls. A failed commit must prevent
    # publishing, and a process death after delivery replays the same gateway key.
    save()
    published = {"waiver": [], "lineup": []}
    # Preparation accounts proposals; retries account their previously saved actions.
    outcome.details["actions_proposed"] = outcome.details["actions_missing_identity"]
    for action in actions.values():
        if action["status"] != "pending":
            continue
        count = len(action["payload"])
        outcome.add("actions_proposed", count)
        retry_window = (
            LINEUP_RETRY_WINDOW_SECONDS
            if action["kind"] == "lineup"
            else WAIVER_RETRY_WINDOW_SECONDS
        )
        expired = time.time() - action["prepared_at"] >= retry_window
        stale = (action["season"], action["week"]) != (season, week)
        if expired or stale:
            reason = (
                f"{action['kind']}_retry_window_expired"
                if expired
                else "decision_week_expired"
            )
            drop(action, reason, "Prepared action is no longer safe to replay")
            outcome.add("actions_failed", count)
            errors.append(reason)
            save()
            continue
        if action["first_attempt_at"] is None:
            action["first_attempt_at"] = time.time()
        save()
        try:
            if action["kind"] == "waiver":
                accepted = publish_waiver_claim(team_id, action["payload"][0])
            else:
                accepted = lineup_solver.publish_lineup_swaps(
                    team_id, action["payload"], idempotency_key=action["key"]
                )
        except Exception as exc:
            action["attempts"] += 1
            action["error"] = str(exc)
            outcome.add("actions_failed", count)
            errors.append(str(exc))
            if action["attempts"] >= MAX_PUBLISH_FAILURES:
                drop(action, "retry_limit_exceeded", str(exc))
        else:
            action["status"] = "already_queued" if accepted is False else "accepted"
            for covered_id in action.get("covered_signal_ids", []):
                entry = entries.get(str(covered_id))
                if entry:
                    entry.update(reason="covered_by_pending_action", prepared=True)
            outcome.add(
                "actions_already_queued" if accepted is False else "actions_accepted",
                count,
            )
            if accepted is not False:
                published[action["kind"]].extend(action["payload"])
        save()

    for signal in signals:
        sid = signal["id"]
        entry = entries.get(str(sid))
        outcome.signals[sid] = (
            entry["reason"]
            if entry
            and entry.get("prepared")
            and all(actions[aid]["status"] != "pending" for aid in entry["actions"])
            else "pending"
        )
    new_watermark = watermark
    for signal in signals:
        if outcome.signals[signal["id"]] == "pending":
            break
        new_watermark = signal["id"]
    outcome.details["signals_qualifying"] = sum(
        reason not in ("superseded", "non_qualifying")
        for reason in outcome.signals.values()
    )
    state["watermark"] = new_watermark
    # Completed history is in execution_log; retain only work behind a cursor gap.
    state["signals"] = {
        sid: entry for sid, entry in entries.items() if int(sid) > new_watermark
    }
    state["actions"] = {
        aid: action
        for aid, action in actions.items()
        if any(sid > new_watermark for sid in action["signal_ids"])
    }
    save()
    outcome.details["watermark_end"] = new_watermark
    try:
        _set_watermark(_redis_client(), new_watermark)
    except Exception as exc:
        logger.warning("Could not mirror rumor-action watermark to Redis: %s", exc)
    if errors:
        raise RuntimeError("; ".join(errors))
    if any(
        reason == "pending" and signal_id not in outcome.deferred_signals
        for signal_id, reason in outcome.signals.items()
    ):
        raise RuntimeError("Unaccounted rumor signals remain pending")
    return {
        "considered": len(signals),
        "waiver_claims_published": published["waiver"],
        "lineup_swaps_published": published["lineup"],
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
