"""
Weekly lineup ILP solver (CORE-06).

Pipeline: load roster pool (CORE-05 vorp_score as objective weight) -> solve ILP ->
diff against current ESPN-synced starters -> publish swaps via the FastAPI
/trigger/lineup endpoint (optimization -> api -> mq, per ARCHITECTURE.md).
"""
import argparse
import logging
import os

import pulp
import requests
from opentelemetry import propagate, trace

from src.database import get_db_connection
from src.telemetry import configure_tracing, configure_trace_logging

configure_trace_logging()
logger = logging.getLogger("LineupSolver")
tracer = trace.get_tracer("lineup_solver")

CORE_API_BASE_URL = os.getenv("CORE_API_BASE_URL", "http://fantasy-api.core.svc.cluster.local:8000")
TOTAL_STARTERS = 7
POSITION_MINIMUMS = {"QB": 1, "RB": 2, "WR": 2, "TE": 1}
QB_MAXIMUM = 1


def load_roster_pool(cursor, team_id: str, season: int, week: int) -> list:
    """Roster-eligible players for a team/season/week: excludes OUT players and anyone
    with no weekly_stats row (bye week). weekly_stats is keyed by (player_id, season,
    week), so both must be filtered - week alone lets a player accumulate multiple rows
    across seasons and returns an arbitrary one's vorp_score. COALESCE guards a player
    somehow missing a CORE-05 vorp_score by treating them as replacement-level rather
    than crashing."""
    query = """
        SELECT p.player_id, p.name, p.position, COALESCE(s.vorp_score, 0.0) AS vorp_score
        FROM players p
        JOIN league_rosters r ON p.player_id = r.player_id
        JOIN weekly_stats s ON p.player_id = s.player_id
        WHERE r.team_id = %s AND s.season = %s AND s.week = %s
          AND (p.injury_status IS NULL OR p.injury_status != 'Out');
    """
    cursor.execute(query, (team_id, season, week))
    return [
        {"player_id": pid, "name": name, "position": pos, "vorp_score": vorp_score}
        for pid, name, pos, vorp_score in cursor.fetchall()
    ]


def build_and_solve_lineup(roster_pool: list) -> list:
    """Pure ILP solve: pick TOTAL_STARTERS players maximizing total vorp_score subject to
    position minimums. QB pinned at exactly 1 and RB/WR/TE floors summing to 5 leave
    exactly one open slot in the ==7 total - the FLEX slot - without a separate variable."""
    if not roster_pool:
        return []

    prob = pulp.LpProblem("Lineup_Optimization", pulp.LpMaximize)
    player_vars = {p["player_id"]: pulp.LpVariable(f"start_{p['player_id']}", cat="Binary") for p in roster_pool}
    by_id = {p["player_id"]: p for p in roster_pool}

    prob += pulp.lpSum(player_vars[pid] * by_id[pid]["vorp_score"] for pid in player_vars), "Total_VORP"

    prob += pulp.lpSum(player_vars.values()) == TOTAL_STARTERS, "Total_Starters"
    prob += (
        pulp.lpSum(var for pid, var in player_vars.items() if by_id[pid]["position"] == "QB") == QB_MAXIMUM,
        "QB_Exact",
    )
    for pos, minimum in POSITION_MINIMUMS.items():
        if pos == "QB":
            continue
        prob += (
            pulp.lpSum(var for pid, var in player_vars.items() if by_id[pid]["position"] == pos) >= minimum,
            f"Min_{pos}",
        )

    prob.solve(pulp.PULP_CBC_CMD(msg=False))

    if pulp.LpStatus[prob.status] != "Optimal":
        logger.error("Lineup solver could not find a feasible optimal lineup for this roster.")
        return []

    return [
        {"player_id": pid, "name": by_id[pid]["name"], "position": by_id[pid]["position"]}
        for pid, var in player_vars.items()
        if var.varValue == 1.0
    ]


def load_current_starters(cursor, team_id: str) -> dict:
    """Maps player_id -> {"position", "slot_position"} for the team's currently-started
    players. slot_position (e.g. "RB1", "FLEX") is the actual roster slot, distinct from
    the player's real position - a WR started in FLEX has position="WR" but
    slot_position="FLEX", and a swap must report the slot being vacated, not the
    position of whoever happened to be sitting in it."""
    query = """
        SELECT p.player_id, p.position, r.slot_position
        FROM league_rosters r
        JOIN players p ON r.player_id = p.player_id
        WHERE r.team_id = %s AND r.is_starter = TRUE;
    """
    cursor.execute(query, (team_id,))
    return {
        pid: {"position": pos, "slot_position": slot_position}
        for pid, pos, slot_position in cursor.fetchall()
    }


def compute_lineup_swaps(optimal_starters: list, current_starters: dict) -> list:
    """Diffs the solver's optimal lineup against the currently-set starters, pairing
    each add with a same-position drop where possible (falling back to any remaining
    drop, e.g. an added RB filling what had been a WR's FLEX slot). The emitted "slot"
    is the *dropped* starter's actual roster slot_position (e.g. "FLEX"), not the
    added player's position - that's the lineup frame actually being replaced."""
    optimal_by_id = {p["player_id"]: p for p in optimal_starters}
    optimal_ids = set(optimal_by_id)
    current_ids = set(current_starters)

    to_add = [optimal_by_id[pid] for pid in optimal_ids - current_ids]
    to_drop = list(current_ids - optimal_ids)

    swaps = []
    remaining_drops = list(to_drop)
    for add_player in to_add:
        drop_id = next(
            (pid for pid in remaining_drops if current_starters[pid]["position"] == add_player["position"]),
            remaining_drops[0] if remaining_drops else None,
        )
        if drop_id is None:
            continue
        remaining_drops.remove(drop_id)
        swaps.append({
            "drop_player_id": drop_id,
            "add_player_id": add_player["player_id"],
            "slot": current_starters[drop_id]["slot_position"],
        })

    return swaps


def publish_lineup_swaps(team_id: str, swaps: list) -> None:
    if not swaps:
        logger.info(f"Lineup already optimal for Team {team_id}; nothing to publish.")
        return

    # Inject this pipeline run's trace context so the FastAPI-side root span (CORE-12)
    # becomes a child of this trace instead of starting a disconnected one.
    headers: dict = {}
    propagate.inject(headers)

    response = requests.post(
        f"{CORE_API_BASE_URL}/v1/trigger/lineup",
        json={"team_id": team_id, "optimized_swaps": swaps},
        headers=headers,
        timeout=10,
    )
    response.raise_for_status()
    logger.info(f"Published {len(swaps)} lineup swap(s) for Team {team_id} to the control plane.")


def optimize_weekly_lineup(team_id: str, season: int, week: int) -> int:
    logger.info(f"Assembling mathematical optimization model for Team {team_id} Season {season} Week {week}...")

    with tracer.start_as_current_span("lineup_solver.run", attributes={"team_id": team_id, "season": season, "week": week}):
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                with tracer.start_as_current_span("lineup_solver.load_roster_pool"):
                    roster_pool = load_roster_pool(cursor, team_id, season, week)
                    if not roster_pool:
                        logger.error(f"No active roster metrics found for Team {team_id} Season {season} Week {week}.")
                        return 0

                with tracer.start_as_current_span("lineup_solver.build_and_solve_lineup"):
                    optimal_starters = build_and_solve_lineup(roster_pool)
                    if not optimal_starters:
                        return 0

                with tracer.start_as_current_span("lineup_solver.load_current_starters"):
                    current_starters = load_current_starters(cursor, team_id)

        with tracer.start_as_current_span("lineup_solver.compute_lineup_swaps"):
            swaps = compute_lineup_swaps(optimal_starters, current_starters)
        logger.info(
            f"Optimal lineup for Team {team_id} Season {season} Week {week}: "
            f"{[p['name'] for p in optimal_starters]}"
        )
        with tracer.start_as_current_span("lineup_solver.publish_lineup_swaps"):
            publish_lineup_swaps(team_id, swaps)
        return len(swaps)


if __name__ == "__main__":
    configure_tracing("lineup-solver")

    parser = argparse.ArgumentParser(description="Solve the optimal weekly lineup for a team via ILP.")
    parser.add_argument("--team-id", type=str, required=True)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int, required=True)
    args = parser.parse_args()
    optimize_weekly_lineup(team_id=args.team_id, season=args.season, week=args.week)
