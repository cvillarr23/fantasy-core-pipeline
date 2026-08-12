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

from src.database import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LineupSolver")

CORE_API_BASE_URL = os.getenv("CORE_API_BASE_URL", "http://fantasy-api.core.svc.cluster.local:8000")
TOTAL_STARTERS = 7
POSITION_MINIMUMS = {"QB": 1, "RB": 2, "WR": 2, "TE": 1}
QB_MAXIMUM = 1


def load_roster_pool(cursor, team_id: str, week: int) -> list:
    """Roster-eligible players for a team/week: excludes OUT players and anyone with
    no weekly_stats row (bye week). COALESCE guards a player somehow missing a
    CORE-05 vorp_score by treating them as replacement-level rather than crashing."""
    query = """
        SELECT p.player_id, p.name, p.position, COALESCE(s.vorp_score, 0.0) AS vorp_score
        FROM players p
        JOIN league_rosters r ON p.player_id = r.player_id
        JOIN weekly_stats s ON p.player_id = s.player_id
        WHERE r.team_id = %s AND s.week = %s
          AND (p.injury_status IS NULL OR p.injury_status != 'Out');
    """
    cursor.execute(query, (team_id, week))
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
    query = """
        SELECT p.player_id, p.position
        FROM league_rosters r
        JOIN players p ON r.player_id = p.player_id
        WHERE r.team_id = %s AND r.is_starter = TRUE;
    """
    cursor.execute(query, (team_id,))
    return {pid: pos for pid, pos in cursor.fetchall()}


def compute_lineup_swaps(optimal_starters: list, current_starters: dict) -> list:
    """Diffs the solver's optimal lineup against the currently-set starters, pairing
    each add with a same-position drop where possible (falling back to any remaining
    drop, e.g. an added RB filling what had been a WR's FLEX slot)."""
    optimal_by_id = {p["player_id"]: p for p in optimal_starters}
    optimal_ids = set(optimal_by_id)
    current_ids = set(current_starters)

    to_add = [optimal_by_id[pid] for pid in optimal_ids - current_ids]
    to_drop = list(current_ids - optimal_ids)

    swaps = []
    remaining_drops = list(to_drop)
    for add_player in to_add:
        drop_id = next(
            (pid for pid in remaining_drops if current_starters[pid] == add_player["position"]),
            remaining_drops[0] if remaining_drops else None,
        )
        if drop_id is None:
            continue
        remaining_drops.remove(drop_id)
        swaps.append({
            "drop_player_id": drop_id,
            "add_player_id": add_player["player_id"],
            "slot": add_player["position"],
        })

    return swaps


def publish_lineup_swaps(team_id: str, swaps: list) -> None:
    if not swaps:
        logger.info(f"Lineup already optimal for Team {team_id}; nothing to publish.")
        return

    response = requests.post(
        f"{CORE_API_BASE_URL}/trigger/lineup",
        json={"team_id": team_id, "optimized_swaps": swaps},
        timeout=10,
    )
    response.raise_for_status()
    logger.info(f"Published {len(swaps)} lineup swap(s) for Team {team_id} to the control plane.")


def optimize_weekly_lineup(team_id: str, week: int) -> int:
    logger.info(f"Assembling mathematical optimization model for Team {team_id} Week {week}...")

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            roster_pool = load_roster_pool(cursor, team_id, week)
            if not roster_pool:
                logger.error(f"No active roster metrics found for Team {team_id} Week {week}.")
                return 0

            optimal_starters = build_and_solve_lineup(roster_pool)
            if not optimal_starters:
                return 0

            current_starters = load_current_starters(cursor, team_id)

    swaps = compute_lineup_swaps(optimal_starters, current_starters)
    logger.info(
        f"Optimal lineup for Team {team_id} Week {week}: "
        f"{[p['name'] for p in optimal_starters]}"
    )
    publish_lineup_swaps(team_id, swaps)
    return len(swaps)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Solve the optimal weekly lineup for a team via ILP.")
    parser.add_argument("--team-id", type=str, required=True)
    parser.add_argument("--week", type=int, required=True)
    args = parser.parse_args()
    optimize_weekly_lineup(team_id=args.team_id, week=args.week)
