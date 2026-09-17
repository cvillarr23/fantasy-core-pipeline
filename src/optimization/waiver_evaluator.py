"""
Live weekly waiver evaluator (#117).

Reuses waiver_strategy.py's margin-threshold decision logic (the same functions
season_replay.py backtests against a static baseline) unchanged for live weekly use -
the decision math is identical, only whether the result gets published to ESPN differs.

waiver_strategy's queries key moves by players.player_id (Sleeper-namespaced), but
POST /v1/trigger/waiver's id-namespace guard requires players.espn_id, so each move is
translated through resolve_live_waiver_moves before publishing. A move involving a
player with no espn_id yet is skipped rather than published - the DynastyProcess
crosswalk covers ~82% of players (see sync_id_crosswalk.py), so an unresolved id is an
expected gap, not a bug, and there is no ESPN identity to safely act on.

No FAAB/budget modeling: neither players nor weekly_stats carries such a column, and
waiver_strategy's margin-threshold rule has none either - out of scope for #117,
tracked as a follow-up.
"""
import argparse
import logging
import os
from typing import Optional

import requests
from opentelemetry import propagate, trace

from src.backtesting import waiver_strategy
from src.database import get_db_connection
from src.telemetry import configure_tracing, configure_trace_logging

configure_trace_logging()
logger = logging.getLogger("WaiverEvaluator")
tracer = trace.get_tracer("waiver_evaluator")

CORE_API_BASE_URL = os.getenv("CORE_API_BASE_URL", "http://fantasy-core-pipeline.core.svc.cluster.local:8000")


def _resolve_espn_identity(cursor, player_id: str) -> Optional[dict]:
    cursor.execute("SELECT espn_id, name, team FROM players WHERE player_id = %s;", (player_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return None
    return {"espn_id": row[0], "name": row[1], "team": row[2]}


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
        add_identity = _resolve_espn_identity(cursor, move["add_player_id"])
        if add_identity is None:
            logger.warning(f"Skipping waiver move adding {move['add_player_id']}: no espn_id resolved yet.")
            continue

        drop_espn_id = None
        drop_player_id = move.get("drop_player_id")
        if drop_player_id:
            drop_identity = _resolve_espn_identity(cursor, drop_player_id)
            if drop_identity is None:
                logger.warning(f"Skipping waiver move dropping {drop_player_id}: no espn_id resolved yet.")
                continue
            drop_espn_id = drop_identity["espn_id"]

        resolved.append({
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
        })
    return resolved


def publish_waiver_claim(team_id: str, target: dict, core_api_base_url: str = CORE_API_BASE_URL, timeout: float = 10.0) -> None:
    # Injects this run's trace context so the FastAPI-side root span (CORE-12) becomes a
    # child of this trace instead of starting a disconnected one.
    headers: dict = {}
    propagate.inject(headers)

    response = requests.post(
        f"{core_api_base_url}/v1/trigger/waiver",
        json={"team_id": team_id, **target},
        headers=headers,
        timeout=timeout,
    )
    response.raise_for_status()
    logger.info(f"Published waiver claim for Team {team_id}: {target['name']}.")


def run_weekly_waivers_live(team_id: str, season: int, week: int, core_api_base_url: str = CORE_API_BASE_URL) -> list:
    logger.info(f"Evaluating live waivers for Team {team_id} Season {season} Week {week}...")

    with tracer.start_as_current_span("waiver_evaluator.run", attributes={"team_id": team_id, "season": season, "week": week}):
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                bench_by_position = waiver_strategy.load_bench_by_position(cursor, team_id, season, week)
                free_agents_by_position = waiver_strategy.load_best_free_agent_by_position(cursor, season, week)
                moves = waiver_strategy.propose_waiver_moves(bench_by_position, free_agents_by_position)
                resolved = resolve_live_waiver_moves(cursor, moves, season, week)

        for target in resolved:
            publish_waiver_claim(team_id, target, core_api_base_url)

    return resolved


if __name__ == "__main__":
    configure_tracing()
    parser = argparse.ArgumentParser(description="Evaluate and publish this week's live waiver claims.")
    parser.add_argument("--team-id", required=True)
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--week", required=True, type=int)
    args = parser.parse_args()
    run_weekly_waivers_live(args.team_id, args.season, args.week)
