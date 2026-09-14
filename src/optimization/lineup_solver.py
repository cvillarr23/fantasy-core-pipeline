"""
Weekly lineup ILP solver (CORE-06).

Pipeline: load roster pool (CORE-05 vorp_score as objective weight) -> solve ILP ->
diff against current ESPN-synced starters -> publish swaps via the FastAPI
/trigger/lineup endpoint (optimization -> api -> mq, per ARCHITECTURE.md).
"""
import argparse
import json
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

CORE_API_BASE_URL = os.getenv("CORE_API_BASE_URL", "http://fantasy-core-pipeline.core.svc.cluster.local:8000")
BENCH_AND_IR_SLOT_NAMES = {"BE", "Bench", "IR"}
FLEX_SLOT_ELIGIBLE_POSITIONS = {
    "RB/WR/TE": ("RB", "WR", "TE"),
    "RB/WR": ("RB", "WR"),
    "WR/TE": ("WR", "TE"),
}
DEFAULT_LINEUP_SLOT_COUNTS = {
    "QB": 1,
    "RB": 2,
    "WR": 2,
    "TE": 1,
    "D/ST": 1,
    "K": 1,
    "RB/WR/TE": 1,
}


def load_roster_pool(cursor, team_id: str, season: int, week: int) -> list:
    """Roster-eligible players for a team/season/week: excludes OUT players and anyone
    with no weekly_stats row (bye week). weekly_stats is keyed by (player_id, season,
    week), so both must be filtered - week alone lets a player accumulate multiple rows
    across seasons and returns an arbitrary one's vorp_score. COALESCE guards a player
    somehow missing a CORE-05 vorp_score by treating them as replacement-level rather
    than crashing."""
    query = """
        SELECT p.player_id, p.name, p.position, COALESCE(s.vorp_score, 0.0) AS vorp_score,
               COALESCE(s.defense_matchup_multiplier, 1.0) AS defense_matchup_multiplier
        FROM players p
        JOIN league_rosters r ON p.player_id = r.player_id
        JOIN weekly_stats s ON p.player_id = s.player_id
        WHERE r.team_id = %s AND s.season = %s AND s.week = %s
          AND (p.injury_status IS NULL OR p.injury_status != 'Out');
    """
    cursor.execute(query, (team_id, season, week))
    return [
        {
            "player_id": pid, "name": name, "position": pos, "vorp_score": vorp_score,
            "defense_matchup_multiplier": defense_matchup_multiplier,
        }
        for pid, name, pos, vorp_score, defense_matchup_multiplier in cursor.fetchall()
    ]


def load_lineup_slot_counts(cursor, season: int) -> dict:
    """Reads lineup slot shape synced from ESPN (sync_league); falls back to default
    9-starter shape if no league_settings row exists yet."""
    cursor.execute(
        """
        SELECT lineup_slot_counts
        FROM league_settings
        WHERE season = %s
        ORDER BY updated_at DESC
        LIMIT 1;
        """,
        (season,),
    )
    row = cursor.fetchone()
    if not row:
        logger.warning("No league_settings lineup_slot_counts found; using default lineup shape.")
        return dict(DEFAULT_LINEUP_SLOT_COUNTS)
    counts = row[0]
    if isinstance(counts, str):
        counts = json.loads(counts)
    return counts or dict(DEFAULT_LINEUP_SLOT_COUNTS)


def build_and_solve_lineup(roster_pool: list, lineup_slot_counts: dict | None = None) -> list:
    """Pure ILP solve: assign players to configured starter slots (including FLEX and
    non-skill slots like D/ST and K) maximizing total vorp_score."""
    if not roster_pool:
        return []
    slot_counts = lineup_slot_counts or DEFAULT_LINEUP_SLOT_COUNTS

    prob = pulp.LpProblem("Lineup_Optimization", pulp.LpMaximize)
    slots = []
    for slot_name, raw_count in slot_counts.items():
        if slot_name in BENCH_AND_IR_SLOT_NAMES:
            continue
        count = int(raw_count)
        if count <= 0:
            continue
        eligible_positions = FLEX_SLOT_ELIGIBLE_POSITIONS.get(slot_name, (slot_name,))
        for i in range(count):
            slots.append({
                "name": slot_name,
                "index": i,
                "eligible_positions": set(eligible_positions),
            })

    if not slots:
        return []

    player_vars = {}
    for player in roster_pool:
        for slot in slots:
            if player["position"] in slot["eligible_positions"]:
                slot_slug = slot["name"].replace("/", "_").replace(" ", "_").replace("-", "_")
                player_vars[(player["player_id"], slot["name"], slot["index"])] = pulp.LpVariable(
                    f"start_{player['player_id']}_{slot_slug}_{slot['index']}",
                    cat="Binary",
                )
    by_id = {p["player_id"]: p for p in roster_pool}

    # Week 1 opponent-defense nudge (#93) is applied here, to the objective weight only -
    # weekly_stats.vorp_score itself stays canonical (vorp_matrix.py never overwrites it).
    # .get(..., 1.0) so a roster_pool built without this key (e.g. season_replay's naive/
    # hindsight pools) is unaffected rather than raising.
    prob += pulp.lpSum(
        var * by_id[pid]["vorp_score"] * by_id[pid].get("defense_matchup_multiplier", 1.0)
        for (pid, _, _), var in player_vars.items()
    ), "Total_VORP"

    for slot in slots:
        slot_slug = slot["name"].replace("/", "_").replace(" ", "_").replace("-", "_")
        slot_vars = [
            var
            for (pid, slot_name, slot_index), var in player_vars.items()
            if slot_name == slot["name"] and slot_index == slot["index"]
        ]
        prob += (
            pulp.lpSum(slot_vars) == 1,
            f"Fill_{slot_slug}_{slot['index']}",
        )
    for player in roster_pool:
        prob += (
            pulp.lpSum(
                var
                for (pid, _, _), var in player_vars.items()
                if pid == player["player_id"]
            ) <= 1,
            f"One_Slot_Per_Player_{player['player_id']}",
        )

    prob.solve(pulp.PULP_CBC_CMD(msg=False))

    if pulp.LpStatus[prob.status] != "Optimal":
        logger.error("Lineup solver could not find a feasible optimal lineup for this roster.")
        return []

    return [
        {
            "player_id": pid, "name": by_id[pid]["name"], "position": by_id[pid]["position"],
            "defense_matchup_multiplier": by_id[pid].get("defense_matchup_multiplier", 1.0),
        }
        for pid in by_id
        if any(
            var.varValue == 1.0
            for (var_pid, _, _), var in player_vars.items()
            if var_pid == pid
        )
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
            # Opponent-defense-strength multiplier applied to the added player's VORP this
            # week (#93); 1.0/neutral when the feature is off or add_player predates this
            # field (e.g. a caller building starter dicts without it).
            "matchup_multiplier": add_player.get("defense_matchup_multiplier", 1.0),
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
                    lineup_slot_counts = load_lineup_slot_counts(cursor, season)

                with tracer.start_as_current_span("lineup_solver.build_and_solve_lineup"):
                    optimal_starters = build_and_solve_lineup(roster_pool, lineup_slot_counts=lineup_slot_counts)
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
