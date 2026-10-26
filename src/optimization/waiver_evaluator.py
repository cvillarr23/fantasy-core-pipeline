"""
Live weekly waiver evaluator (#117).

Reuses waiver_strategy.py's margin-threshold decision logic (the same functions
season_replay.py backtests against a static baseline) unchanged for live weekly use -
the decision math is identical, only whether the result gets published to ESPN differs.

waiver_strategy's queries key moves by players.player_id (Sleeper-namespaced), but
POST /v1/trigger/waiver's id-namespace guard requires players.espn_id, so each move is
translated through resolve_live_waiver_moves before publishing. A move involving a
player with no espn_id yet is skipped rather than published - not every player has one
resolved (see src/espn_identity.py), so an unresolved id is an expected gap, not a bug,
and there is no ESPN identity to safely act on.

Neither publisher here (the weekly CronJob, rumor_action_service's boost_waiver path)
drops an IR-eligible player while an IR slot is open. plan_live_waiver_moves uses the
same waiver_strategy.plan_waiver_moves as the waiver page, and a free agent that should
come in through an IR stash is never claimed with a healthy player's drop instead.
Rosters are refreshed first, since a week-old snapshot would misstate who sits in IR.

Those IR + add moves are only published with AUTO_PUBLISH_IR_MOVES on; otherwise they are
logged as suggestions. Publishing one is two writes in a fixed order: the IR move through
/v1/trigger/lineup, then an add-only claim chained to that move's tracking id. The gateway
holds the claim until the IR move is verified on ESPN's roster and cancels it if the move
fails, so the add can never overfill the roster.

No FAAB/budget modeling: neither players nor weekly_stats carries such a column, and
waiver_strategy's margin-threshold rule has none either - out of scope for #117,
tracked as a follow-up.
"""
import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime
from typing import Optional

from opentelemetry import trace

from src.backtesting import waiver_strategy
from src.database import get_db_connection
from src.espn_identity import resolve_espn_identity
from src.ingestion import sync_league
from src.ir_eligibility import load_ineligible_ir_occupants, load_open_ir_slots
from src.nfl_schedule import WeekResolutionError, resolve_upcoming_week
from src.nfl_byes import bye_weeks_or_empty
from src.optimization import lineup_solver
from src.telemetry import configure_tracing
from src.trigger_client import CORE_API_BASE_URL, post_trigger, post_trigger_idempotent, response_status
from src.services import run_log_service

logger = logging.getLogger("WaiverEvaluator")
tracer = trace.get_tracer("waiver_evaluator")


def ir_auto_publish_enabled() -> bool:
    """AUTO_PUBLISH_IR_MOVES gates every unattended IR write, off unless explicitly on.
    Read per call rather than at import so a long-lived API process honours a change."""
    return os.getenv("AUTO_PUBLISH_IR_MOVES", "").strip().lower() in {"1", "true", "yes"}


class IrAutoPublishDisabledError(Exception):
    """An IR + add move reached a publisher while AUTO_PUBLISH_IR_MOVES is off - e.g. a
    rumor checkpoint prepared before the flag was turned off."""


class IrMoveNotChainableError(Exception):
    """The add-only claim can't be tied to its IR move (no season/week to key it, or no
    tracking id back from the IR move), so it is not sent at all."""


def resolve_live_waiver_moves(cursor, moves: list, season: Optional[int] = None, week: Optional[int] = None) -> list:
    """Translates waiver_strategy.propose_waiver_moves' Sleeper-namespaced output into the
    ESPN-identity payload /v1/trigger/waiver requires, dropping any move whose add or drop
    player has no espn_id resolved yet - publishing it would either fail the gateway's
    namespace guard or silently target nothing in ESPN's UI.

    decision_inputs (CORE-17/#35) carries the VORP numbers that justified the move plus
    season/week, for /v1/trigger/waiver's bot_decisions audit trail write - forwarded
    through publish_waiver_claim's **target unpacking with no further wiring needed there.
    """
    resolved = []
    for move in moves:
        add_identity = resolve_espn_identity(cursor, move["add_player_id"])
        if add_identity is None:
            logger.warning(f"Skipping waiver move adding {move['add_player_id']}: no espn_id resolved yet.")
            continue

        ir_identity = None
        if move.get("ir_player_id"):
            ir_identity = resolve_espn_identity(cursor, move["ir_player_id"])
            if ir_identity is None:
                logger.warning(f"Skipping IR + add move stashing {move['ir_player_id']}: no espn_id resolved yet.")
                continue

        drop_espn_id = None
        drop_player_id = move.get("drop_player_id")
        if drop_player_id:
            drop_identity = resolve_espn_identity(cursor, drop_player_id)
            if drop_identity is None:
                logger.warning(f"Skipping waiver move dropping {drop_player_id}: no espn_id resolved yet.")
                continue
            drop_espn_id = drop_identity["espn_id"]

        target = {
            "player_id": add_identity["espn_id"],
            "drop_player_id": drop_espn_id,
            "name": add_identity["name"],
            "team": add_identity["team"],
            "decision_inputs": {
                "add_player_vorp_score": move.get("add_vorp_score"),
                "drop_player_vorp_score": move.get("drop_vorp_score"),
                "season": season,
                "week": week,
            },
        }
        if ir_identity is not None:
            target["ir_player_id"] = ir_identity["espn_id"]
            target["ir_player_name"] = ir_identity["name"]
            target["decision_inputs"].update(
                ir_player_vorp_score=move.get("ir_vorp_score"),
                ir_player_internal_id=move["ir_player_id"],
            )
        resolved.append(target)
    return resolved


def waiver_idempotency_key(team_id: str, target: dict) -> Optional[str]:
    """Stable key for one claim in one waiver week. Unlike lineup_idempotency_key it has no
    Job name: a claim stays queued on ESPN until waivers process, invisible to the worker's
    roster check, so a re-run (any Job) proposing the same add/drop that week must come back
    "duplicate" rather than queue it again. VORP inputs are left out on purpose - a rescored
    re-run is still the same claim. None without season/week: the key would then be the same
    every week, and the 7-day TTL could swallow next week's legitimate claim."""
    inputs = target.get("decision_inputs") or {}
    if inputs.get("season") is None or inputs.get("week") is None:
        return None
    material = {
        "team_id": str(team_id),
        "season": inputs.get("season"),
        "week": inputs.get("week"),
        "add": str(target["player_id"]),
        "drop": str(target.get("drop_player_id")),
    }
    # A claim chained to an IR move is held on that move, so it must not dedupe against
    # a plain add-only claim of the same player; plain claims keep their existing keys.
    if target.get("ir_player_id"):
        material["ir"] = str(target["ir_player_id"])
    material = json.dumps(material, sort_keys=True)
    return f"waiver-{team_id}-{hashlib.sha256(material.encode()).hexdigest()[:32]}"


def publish_waiver_claim(team_id: str, target: dict, core_api_base_url: str = CORE_API_BASE_URL, timeout: float = 10.0) -> bool:
    """POSTs one claim to /v1/trigger/waiver, retrying transient failures (#189's client)
    under its week-scoped idempotency key. True if newly queued, False if the gateway said
    it was already queued. Raises once retries are exhausted or on a 4xx. Without a key
    (no season/week) it's a single un-retried POST, since a retry could double-queue.
    A target with ir_player_id is an IR + add move and goes through publish_ir_and_add."""
    if target.get("ir_player_id"):
        return publish_ir_and_add(team_id, target, core_api_base_url, timeout)
    payload = {"team_id": team_id, **target}
    key = waiver_idempotency_key(team_id, target)
    if key is None:
        post_trigger("waiver", payload, core_api_base_url=core_api_base_url, timeout=timeout)
        logger.warning(f"Published waiver claim for Team {team_id}: {target['name']} without an idempotency key (no season/week).")
        return True
    response = post_trigger_idempotent("waiver", payload, key, core_api_base_url=core_api_base_url, timeout=timeout)
    if response_status(response) == "duplicate":
        logger.info(f"Waiver claim for Team {team_id}: {target['name']} already queued under {key}; not re-queued.")
        return False
    logger.info(f"Published waiver claim for Team {team_id}: {target['name']} (idempotency key {key}).")
    return True


def publish_ir_and_add(team_id: str, target: dict, core_api_base_url: str = CORE_API_BASE_URL, timeout: float = 10.0) -> bool:
    """Publishes an IR stash and the add-only claim it makes room for. The IR move is
    POSTed first; the claim is then POSTed with after_tracking_id, which makes the gateway
    hold it until that IR move is verified_complete and cancel it if the move fails. Each
    step has its own key: a pod retry soon after gets "duplicate" for both, and a later
    replay, once the IR key has lapsed, queues an IR move the worker finds already applied
    and verifies at once. True if the claim was newly queued.

    Raises, sending nothing further, when the flag is off, when there's no season/week to
    key the steps, or when the IR move can't be confirmed as accepted with a tracking id:
    an add sent without its IR move would overfill the roster."""
    if not ir_auto_publish_enabled():
        raise IrAutoPublishDisabledError(
            f"Not publishing IR {target['ir_player_id']} + add {target['name']} for Team {team_id}: "
            "AUTO_PUBLISH_IR_MOVES is off."
        )
    add_key = waiver_idempotency_key(team_id, target)
    if add_key is None:
        raise IrMoveNotChainableError(
            f"IR + add {target['name']} for Team {team_id} has no season/week to key it; nothing published."
        )
    inputs = target["decision_inputs"]
    ir_entry = {
        "kind": "ir",
        "player_id": str(target["ir_player_id"]),
        "player_name": target.get("ir_player_name"),
        "slot": "IR",
        "decision_inputs": {
            "season": inputs["season"],
            "week": inputs["week"],
            "ir_player_vorp_score": inputs.get("ir_player_vorp_score"),
            "ir_player_internal_id": inputs.get("ir_player_internal_id"),
            "makes_room_for_player_id": str(target["player_id"]),
        },
    }
    # No Job name: the IR move belongs to this waiver week, like the claim it precedes.
    ir_key = lineup_solver.lineup_idempotency_key(team_id, [ir_entry])
    ir_response = post_trigger_idempotent(
        "lineup", {"team_id": team_id, "optimized_swaps": [ir_entry]}, ir_key,
        core_api_base_url=core_api_base_url, timeout=timeout,
    )
    try:
        ir_tracking_id = ir_response.json().get("tracking_id")
    except Exception:
        ir_tracking_id = None
    if not ir_tracking_id:
        raise IrMoveNotChainableError(
            f"IR move for {target['ir_player_id']} (key {ir_key}) returned no tracking id; "
            f"not sending the add of {target['name']}."
        )
    logger.info(
        f"Team {team_id}: IR move for {target.get('ir_player_name') or target['ir_player_id']} "
        f"queued as {ir_tracking_id} ({response_status(ir_response)})."
    )

    claim = {
        "team_id": team_id,
        "player_id": target["player_id"],
        "drop_player_id": None,
        "name": target["name"],
        "team": target["team"],
        "decision_inputs": inputs,
        "after_tracking_id": ir_tracking_id,
    }
    response = post_trigger_idempotent("waiver", claim, add_key, core_api_base_url=core_api_base_url, timeout=timeout)
    if response_status(response) == "duplicate":
        logger.info(f"Add of {target['name']} for Team {team_id} already queued under {add_key}; not re-queued.")
        return False
    logger.info(
        f"Queued add of {target['name']} for Team {team_id}, held until IR move {ir_tracking_id} "
        f"is verified (idempotency key {add_key})."
    )
    return True


class InsufficientWaiverInputError(Exception):
    """The target week's weekly_stats rows needed to compare bench vs. free agents are
    missing - distinct from "no waiver move clears the margin" (a valid empty result)."""


def _horizon_pools(cursor, team_id: str, season: int, week: int) -> tuple:
    """(bench_by_position, free_agents_by_position) valued over waiver_strategy's horizon:
    average VORP per week, a bye counting as replacement level. A stud on bye this week
    keeps his value instead of reading as the worst bench player, so the margin rule
    never auto-drops him. An unreadable schedule only means byes count as unknown weeks
    (skipped), which can overrate but never underrate a bench player."""
    pools = waiver_strategy.load_horizon_waiver_pools(
        cursor, team_id, season, week, waiver_strategy.horizon_weeks(week),
        bye_weeks_or_empty(season),
    )
    return pools["bench_by_position"], pools["free_agents_by_position"]


def load_live_waiver_inputs(cursor, team_id: str, season: int, week: int) -> tuple:
    """(bench_by_position, free_agents_by_position) for the target week, valued over the
    horizon (_horizon_pools). Missing free agents, or missing bench metrics with no
    roster vacancy, fail instead of reporting no moves."""
    bench_by_position, free_agents_by_position = _horizon_pools(cursor, team_id, season, week)
    open_roster_slots = (
        waiver_strategy.load_open_roster_slots(cursor, team_id, season)
        if not bench_by_position else 0
    )
    missing = [
        label for label, pool in (("bench", bench_by_position), ("free-agent", free_agents_by_position))
        if not pool and (label != "bench" or open_roster_slots <= 0)
    ]
    if missing:
        raise InsufficientWaiverInputError(
            f"no {' or '.join(missing)} players with weekly_stats rows for Season {season} Week {week}"
        )
    return bench_by_position, free_agents_by_position


def plan_live_waiver_moves(cursor, team_id: str, season: int, bench_by_position: dict,
                           free_agents_by_position: dict, week: int) -> list:
    """Fill existing non-IR vacancies before planning swaps or IR chains.
    IR slots are only queried when a bench player is
    IR-eligible. With AUTO_PUBLISH_IR_MOVES off, IR + add moves are logged at WARNING and
    only the swaps are returned. With it on, the IR + add moves follow the swaps, so one
    that fails to publish can't hold back the week's ordinary claims. They are withheld
    when one of our IR slots holds a player ESPN no longer lists as injured (or one whose
    status we can't see): ESPN may then refuse new transactions, and activating that
    player is a manager's call."""
    adds = []
    open_roster_slots = waiver_strategy.load_open_roster_slots(cursor, team_id, season)
    if open_roster_slots > 0:
        candidates = waiver_strategy.load_add_candidates(cursor, season, week)
        adds, free_agents_by_position = waiver_strategy.plan_add_only_moves(
            candidates, open_roster_slots,
            rostered_positions=waiver_strategy.load_rostered_positions(cursor, team_id),
        )
    open_slots = (
        load_open_ir_slots(cursor, team_id, season)
        if waiver_strategy.ir_eligible_bench(bench_by_position) else 0
    )
    swaps, ir_and_adds = waiver_strategy.plan_waiver_moves(bench_by_position, free_agents_by_position, open_slots)
    if not ir_and_adds:
        return adds + swaps
    if not ir_auto_publish_enabled():
        for move in ir_and_adds:
            logger.warning(
                f"Team {team_id}: not published, suggest IR {move['ir_player_id']} + add {move['add_player_id']} "
                f"({move['add_vorp_score']:.2f} VORP); IR moves are suggestions only."
            )
        return adds + swaps
    blocking = load_ineligible_ir_occupants(cursor, team_id)
    if blocking:
        names = ", ".join(
            f"{name or pid or 'an unmatched player'} ({status or 'no injury status'})"
            for pid, name, status in blocking
        )
        logger.error(
            f"Team {team_id}: {names} sit in IR without an IR-eligible ESPN status; activate them from IR. "
            f"Not publishing {len(ir_and_adds)} IR + add move(s) until then, since ESPN may reject them."
        )
        return adds + swaps
    for move in ir_and_adds:
        logger.info(
            f"Team {team_id}: publishing IR {move['ir_player_id']} + add {move['add_player_id']} "
            f"({move['add_vorp_score']:.2f} VORP)."
        )
    return adds + swaps + ir_and_adds


def run_weekly_waivers_live(team_id: str, season: int, week: int, core_api_base_url: str = CORE_API_BASE_URL) -> list:
    logger.info(f"Evaluating live waivers for Team {team_id} Season {season} Week {week}...")

    with tracer.start_as_current_span("waiver_evaluator.run", attributes={"team_id": team_id, "season": season, "week": week}):
        sync_league.refresh_rosters_if_stale(season)
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                bench_by_position, free_agents_by_position = load_live_waiver_inputs(cursor, team_id, season, week)
                moves = plan_live_waiver_moves(cursor, team_id, season, bench_by_position, free_agents_by_position, week=week)
                resolved = resolve_live_waiver_moves(cursor, moves, season, week)

        if not moves:
            logger.info(
                f"No waiver moves proposed for Team {team_id} Season {season} Week {week}; nothing to publish."
            )
        elif not resolved:
            # Worthwhile claims exist but none has an ESPN identity yet - a crosswalk gap
            # blocking real moves, not "nothing worth claiming".
            logger.warning(
                f"All {len(moves)} proposed waiver move(s) for Team {team_id} Season {season} Week {week} "
                f"were dropped: no espn_id resolved; nothing published."
            )
        for target in resolved:
            publish_waiver_claim(team_id, target, core_api_base_url)

    return resolved


def main(argv=None) -> int:
    """CronJob entrypoint: 0 when claims were evaluated (published or none worth making),
    1 when the target week's inputs are missing, so the Job fails instead of looking
    healthy. A publish that fails after its in-process retries (or on a 4xx) propagates,
    exiting nonzero, so the CronJob's pod retry re-runs every claim: already-queued ones
    come back "duplicate" under their week-scoped keys (#191)."""
    parser = argparse.ArgumentParser(description="Evaluate and publish this week's live waiver claims.")
    parser.add_argument("--team-id", default=os.getenv("ESPN_TEAM_ID"))
    parser.add_argument("--season", type=int, default=int(os.getenv("CURRENT_YEAR", datetime.now().year)))
    parser.add_argument("--week", type=int, default=None)
    args = parser.parse_args(argv)
    if not args.team_id:
        parser.error("--team-id is required (or set the ESPN_TEAM_ID env var)")

    week = args.week
    if week is None:
        try:
            week = resolve_upcoming_week(args.season)
        except WeekResolutionError as exc:
            logger.error(f"Could not resolve the upcoming week for Season {args.season}: {exc}")
            return 1
        if week is None:
            logger.info(f"The {args.season} regular season is over; no waiver claims to evaluate.")
            return 0
    try:
        run_weekly_waivers_live(args.team_id, args.season, week)
    except InsufficientWaiverInputError as exc:
        logger.error(f"Waiver evaluator cannot evaluate Team {args.team_id} Season {args.season} Week {week}: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    configure_tracing("waiver-evaluator")
    sys.exit(run_log_service.run_main("waiver_evaluator", main))
