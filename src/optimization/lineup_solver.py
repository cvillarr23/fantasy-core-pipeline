"""
Weekly lineup ILP solver (CORE-06).

Pipeline: load pregame roster pool (CORE-05 vorp_score as objective weight) -> solve ILP
-> diff against current ESPN-synced starters -> publish swaps via the FastAPI
/trigger/lineup endpoint (optimization -> api -> mq, per ARCHITECTURE.md).

The scheduled run evaluates an *upcoming* week (#178), so it can't require that week to
have been played. See load_pregame_roster_pool for the source order and season boundary.
Swaps are computed on internal players.player_id (Sleeper-namespaced) but published with
players.espn_id (resolve_lineup_swaps, #187): the browser worker acts on ESPN ids, and the
two id spaces overlap numerically, so an untranslated id can swap the wrong player.

Exit codes: 0 = a real decision was made (swaps published, or lineup already optimal);
nonzero = missing/insufficient input - including proposed swaps none of which resolve to an
espn_id - so the CronJob's Job fails and alerts (fantasy-infra#97) instead of looking healthy.
"""
import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime

import nflreadpy as nfl
import pulp
from opentelemetry import trace

from src.database import get_db_connection
from src.espn_identity import resolve_espn_identity
from src.ingestion.metrics_extractor import normalize_team
from src.telemetry import configure_tracing, configure_trace_logging
from src.trigger_client import post_trigger_idempotent

configure_trace_logging()
logger = logging.getLogger("LineupSolver")
tracer = trace.get_tracer("lineup_solver")

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
# Sleeper projections (sync_projections, the only scheduled vorp_score source) cover
# QB/RB/WR/TE only, and sync_kicker_dst is free-agent-only, so rostered K/D-ST normally
# have no weekly_stats metrics at all. When no rostered player has data for one of these
# slots, its current starter is held rather than failing the whole run.
HOLDABLE_WITHOUT_METRICS = {"K", "D/ST"}
PREGAME_WINDOW_WEEKS = 3
METRIC_SOURCE = "weekly_stats.vorp_score"


class InsufficientLineupInputError(Exception):
    """The optimizer can't make a defensible decision from the data available - distinct
    from "lineup already optimal" (a valid zero-swap result)."""


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


def load_teams_playing(season: int, week: int) -> set:
    """Team abbreviations (normalized) with a game in season/week, from nflverse's
    schedule - needed to spot bye weeks when the pool comes from prior weeks' rows."""
    df = nfl.load_schedules(season)
    games = df.filter(df["week"] == week)
    return {normalize_team(t) for t in games["home_team"].to_list() + games["away_team"].to_list()}


def load_pregame_roster_pool(cursor, team_id: str, season: int, target_week: int,
                             window: int = PREGAME_WINDOW_WEEKS) -> tuple:
    """Roster pool for a week that hasn't been played yet. Returns
    (pool, vorp_by_player, source).

    Source order, always within `season` only - week 1 never reads last season's rows:
      1. target_week's own vorp_score, if any rostered player has one. These are pregame
         Sleeper projections (draft-prep-refresher: sync_projections -> vorp_matrix), and
         a player without a row is on bye, same as load_roster_pool.
      2. Otherwise, each player's mean vorp_score over the up-to-`window` prior weeks of
         the same season. Byes come from the nflverse schedule, since prior-week rows say
         nothing about the target week. The newest usable week must be target_week - 1,
         or the data is stale.

    Raises InsufficientLineupInputError when neither source is usable."""
    cursor.execute(
        """
        SELECT p.player_id, p.name, p.position, p.team, p.injury_status
        FROM league_rosters r
        JOIN players p ON p.player_id = r.player_id
        WHERE r.team_id = %s;
        """,
        (team_id,),
    )
    roster = cursor.fetchall()
    if not roster:
        raise InsufficientLineupInputError(f"no league_rosters rows for Team {team_id}")

    first_week = max(1, target_week - window)
    cursor.execute(
        """
        SELECT s.player_id, s.week, s.vorp_score, s.defense_matchup_multiplier
        FROM weekly_stats s
        JOIN league_rosters r ON r.player_id = s.player_id
        WHERE r.team_id = %s AND s.season = %s AND s.week BETWEEN %s AND %s
          AND s.vorp_score IS NOT NULL;
        """,
        (team_id, season, first_week, target_week),
    )
    metrics = cursor.fetchall()

    target_rows = [row for row in metrics if row[1] == target_week]
    if target_rows:
        vorp_by_player = {pid: vorp for pid, _, vorp, _ in target_rows}
        multiplier_by_player = {pid: mult for pid, _, _, mult in target_rows}
        mode, weeks = "target_week", [target_week]
        teams_playing = None
    else:
        prior_rows = [row for row in metrics if row[1] < target_week]
        if not prior_rows:
            if target_week == 1:
                reason = (
                    f"no usable {METRIC_SOURCE} for Season {season} Week 1: no pregame "
                    "projections for Week 1, and prior seasons are deliberately not used"
                )
            else:
                reason = (
                    f"no usable {METRIC_SOURCE} for Season {season} Week {target_week} "
                    f"or Weeks {first_week}-{target_week - 1}"
                )
            raise InsufficientLineupInputError(reason)
        weeks = sorted({row[1] for row in prior_rows})
        if weeks[-1] < target_week - 1:
            raise InsufficientLineupInputError(
                f"stale {METRIC_SOURCE}: newest usable week is Season {season} Week {weeks[-1]}, "
                f"expected Week {target_week - 1} before evaluating Week {target_week}"
            )
        values = {}
        for pid, _, vorp, _ in prior_rows:
            values.setdefault(pid, []).append(vorp)
        vorp_by_player = {pid: sum(v) / len(v) for pid, v in values.items()}
        # vorp_matrix only sets defense_matchup_multiplier for Week 1 (#93), so a prior
        # week's value says nothing about this week's opponent.
        multiplier_by_player = {}
        mode = "rolling_prior_weeks"
        try:
            teams_playing = {normalize_team(t) for t in load_teams_playing(season, target_week)}
        except Exception as exc:
            raise InsufficientLineupInputError(
                f"could not load the nflverse schedule for Season {season} Week {target_week} "
                f"to exclude bye-week players: {exc}"
            ) from exc

    pool = []
    for pid, name, position, team, injury_status in roster:
        if pid not in vorp_by_player or injury_status == "Out":
            continue
        if teams_playing is not None and normalize_team(team) not in teams_playing:
            continue
        pool.append({
            "player_id": pid, "name": name, "position": position,
            "vorp_score": vorp_by_player[pid],
            "defense_matchup_multiplier": multiplier_by_player.get(pid) or 1.0,
        })
    if not pool:
        raise InsufficientLineupInputError(
            f"no eligible players for Team {team_id} after excluding Out and bye-week players "
            f"({mode}, Season {season} Weeks {weeks})"
        )

    source = {"metric_source": METRIC_SOURCE, "source_mode": mode, "source_season": season, "source_weeks": weeks}
    return pool, vorp_by_player, source


def hold_slots_without_metrics(lineup_slot_counts: dict, roster_pool: list) -> tuple:
    """Drops HOLDABLE_WITHOUT_METRICS slots no pool player can fill. Returns
    (slot counts to solve, sorted held slot names)."""
    pool_positions = {p["position"] for p in roster_pool}
    held = sorted(
        slot for slot, count in lineup_slot_counts.items()
        if slot in HOLDABLE_WITHOUT_METRICS and int(count) > 0 and slot not in pool_positions
    )
    return {slot: count for slot, count in lineup_slot_counts.items() if slot not in held}, held


def describe_unfilled_slots(roster_pool: list, lineup_slot_counts: dict) -> str:
    """Human-readable slot demand vs. pool supply, for an infeasible-solve error."""
    starter_slots = {
        slot: int(count) for slot, count in lineup_slot_counts.items()
        if slot not in BENCH_AND_IR_SLOT_NAMES and int(count) > 0
    }
    supply = {}
    for player in roster_pool:
        supply[player["position"]] = supply.get(player["position"], 0) + 1
    short = [
        slot for slot, count in starter_slots.items()
        if sum(supply.get(pos, 0) for pos in FLEX_SLOT_ELIGIBLE_POSITIONS.get(slot, (slot,))) < count
    ]
    return f"slots {starter_slots} vs players with metrics by position {supply}; short: {short or 'combined FLEX demand'}"


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
            # CORE-17 (#35): compute_lineup_swaps needs the added player's own vorp_score
            # for decision_inputs - dropped here previously since only the objective
            # weighting used it, not the returned starter dict.
            "vorp_score": by_id[pid]["vorp_score"],
        }
        for pid in by_id
        if any(
            var.varValue == 1.0
            for (var_pid, _, _), var in player_vars.items()
            if var_pid == pid
        )
    ]


def load_current_starters(cursor, team_id: str, season: int, week: int) -> dict:
    """Maps player_id -> {"position", "slot_position", "vorp_score"} for the team's
    currently-started players. slot_position (e.g. "RB1", "FLEX") is the actual roster
    slot, distinct from the player's real position - a WR started in FLEX has
    position="WR" but slot_position="FLEX", and a swap must report the slot being
    vacated, not the position of whoever happened to be sitting in it.

    vorp_score (CORE-17/#35) is the dropped starter's own VORP, for compute_lineup_swaps'
    decision_inputs. LEFT JOIN, not INNER like load_roster_pool - a starter being dropped
    can be on a bye (no weekly_stats row) and must still appear here as droppable."""
    query = """
        SELECT p.player_id, p.position, r.slot_position, COALESCE(s.vorp_score, 0.0) AS vorp_score
        FROM league_rosters r
        JOIN players p ON r.player_id = p.player_id
        LEFT JOIN weekly_stats s ON s.player_id = p.player_id AND s.season = %s AND s.week = %s
        WHERE r.team_id = %s AND r.is_starter = TRUE;
    """
    cursor.execute(query, (season, week, team_id))
    return {
        pid: {"position": pos, "slot_position": slot_position, "vorp_score": vorp_score}
        for pid, pos, slot_position, vorp_score in cursor.fetchall()
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
        matchup_multiplier = add_player.get("defense_matchup_multiplier", 1.0)
        swaps.append({
            "drop_player_id": drop_id,
            "add_player_id": add_player["player_id"],
            "slot": current_starters[drop_id]["slot_position"],
            # Opponent-defense-strength multiplier applied to the added player's VORP this
            # week (#93); 1.0/neutral when the feature is off or add_player predates this
            # field (e.g. a caller building starter dicts without it).
            "matchup_multiplier": matchup_multiplier,
            # CORE-17 (#35): the numbers that justified this swap, for bot_decisions'
            # audit trail. season/week deliberately not included here - this is a pure
            # diff function and widening its signature just to pass through two values
            # isn't worth it; those columns are simply NULL for lineup_swap rows.
            "decision_inputs": {
                "add_player_vorp_score": add_player.get("vorp_score"),
                "drop_player_vorp_score": current_starters[drop_id].get("vorp_score"),
                "matchup_multiplier": matchup_multiplier,
            },
        })

    return swaps


def resolve_lineup_swaps(cursor, swaps: list) -> list:
    """Translates compute_lineup_swaps' internal (Sleeper) ids into the ESPN ids the browser
    worker acts on, returning new swap dicts. A swap whose add or drop player has no espn_id
    yet is skipped with a warning, like waiver_evaluator.resolve_live_waiver_moves - there's
    no ESPN identity to safely act on. Names ride along so /v1/trigger/lineup's namespace
    guard can catch an id that exists in both namespaces; the internal ids stay in
    decision_inputs for the bot_decisions audit trail.

    Raises InsufficientLineupInputError when swaps were proposed but none resolve, so that
    can't pass for a valid "already optimal" zero-swap run."""
    resolved = []
    for swap in swaps:
        add_identity = resolve_espn_identity(cursor, swap["add_player_id"])
        drop_identity = resolve_espn_identity(cursor, swap["drop_player_id"])
        missing = [
            f"{label} {swap[key]}"
            for label, key, identity in (
                ("add", "add_player_id", add_identity), ("drop", "drop_player_id", drop_identity),
            )
            if identity is None
        ]
        if missing:
            logger.warning(f"Skipping lineup swap into {swap['slot']}: no espn_id resolved yet for {', '.join(missing)}.")
            continue
        resolved.append(dict(
            swap,
            add_player_id=str(add_identity["espn_id"]),
            drop_player_id=str(drop_identity["espn_id"]),
            add_player_name=add_identity["name"],
            drop_player_name=drop_identity["name"],
            decision_inputs=dict(
                swap.get("decision_inputs") or {},
                add_player_internal_id=swap["add_player_id"],
                drop_player_internal_id=swap["drop_player_id"],
            ),
        ))
    if swaps and not resolved:
        raise InsufficientLineupInputError(
            f"none of {len(swaps)} proposed lineup swap(s) have a resolvable players.espn_id; "
            "refusing to publish Sleeper ids"
        )
    return resolved


def lineup_idempotency_key(team_id: str, swaps: list, job_name: str | None = None) -> str:
    """Stable key for one lineup decision (#189), so a retry - in-process after an ambiguous
    failure, or a whole re-run when the CronJob retries a failed pod - can't enqueue the same
    swaps twice. A re-run recomputes identical swaps (league_rosters isn't resynced from
    ESPN in between), so the same inputs give the same key. The Job name, when set
    (JOB_NAME, from the CronJob's downward API), scopes it to one Job: a new scheduled or
    manual Job is a new decision and always publishes."""
    weeks = sorted({
        (s.get("decision_inputs") or {}).get("season", 0) * 100 + (s.get("decision_inputs") or {}).get("week", 0)
        for s in swaps
    })
    material = json.dumps({
        "job": job_name,
        "team_id": str(team_id),
        "weeks": weeks,
        "swaps": sorted([str(s["drop_player_id"]), str(s["add_player_id"]), str(s["slot"])] for s in swaps),
    }, sort_keys=True)
    return f"lineup-{team_id}-{hashlib.sha256(material.encode()).hexdigest()[:32]}"


def publish_lineup_swaps(team_id: str, swaps: list) -> bool:
    """POSTs swaps to /v1/trigger/lineup, retrying transient failures (#189) under a stable
    idempotency key. Returns True for newly accepted swaps, False for no-op/duplicate.
    Raises once retries are exhausted or on a 4xx."""
    if not swaps:
        logger.info(f"Lineup already optimal for Team {team_id}; nothing to publish.")
        return False

    key = lineup_idempotency_key(team_id, swaps, job_name=os.getenv("JOB_NAME"))
    response = post_trigger_idempotent("lineup", {"team_id": team_id, "optimized_swaps": swaps}, key, timeout=10)
    try:
        status = response.json().get("status")
    except Exception:
        status = None
    if status == "duplicate":
        logger.info(f"Lineup swap(s) for Team {team_id} were already published under {key}; not re-enqueued.")
        return False
    logger.info(f"Published {len(swaps)} lineup swap(s) for Team {team_id} to the control plane (idempotency key {key}).")
    return True


def evaluate_lineup(cursor, team_id: str, season: int, week: int) -> dict:
    """Pregame lineup decision for team/season/week, without publishing: pool ->
    held slots -> ILP -> diff. Shared by the weekly optimizer, the matchup page's swap
    badge, and rumor bench_watch actions, so all three see the same upcoming-week data.
    Returns {"swaps", "optimal_starters", "roster_pool", "source"}; raises
    InsufficientLineupInputError when there isn't enough data to decide."""
    with tracer.start_as_current_span("lineup_solver.load_roster_pool"):
        roster_pool, vorp_by_player, source = load_pregame_roster_pool(cursor, team_id, season, week)
        lineup_slot_counts, held_slots = hold_slots_without_metrics(
            load_lineup_slot_counts(cursor, season), roster_pool,
        )
        source["held_slots"] = held_slots
    logger.info(
        f"Evaluating Team {team_id} Season {season} Week {week} from {METRIC_SOURCE} "
        f"({source['source_mode']}, Season {season} Week(s) {source['source_weeks']}); "
        f"{len(roster_pool)} eligible player(s); held slots without metrics: {held_slots or 'none'}."
    )
    span = trace.get_current_span()
    span.set_attribute("source_mode", source["source_mode"])
    span.set_attribute("source_weeks", source["source_weeks"])
    span.set_attribute("held_slots", held_slots)

    with tracer.start_as_current_span("lineup_solver.build_and_solve_lineup"):
        optimal_starters = build_and_solve_lineup(roster_pool, lineup_slot_counts=lineup_slot_counts)
        if not optimal_starters:
            raise InsufficientLineupInputError(
                "could not fill the starting lineup from players with usable metrics: "
                + describe_unfilled_slots(roster_pool, lineup_slot_counts)
            )

    with tracer.start_as_current_span("lineup_solver.load_current_starters"):
        current_starters = load_current_starters(cursor, team_id, season, week)
    # Held slots' starters stay put, so they must not appear as droppable.
    current_starters = {
        pid: dict(info, vorp_score=vorp_by_player.get(pid, info["vorp_score"]))
        for pid, info in current_starters.items()
        if info["position"] not in held_slots
    }
    with tracer.start_as_current_span("lineup_solver.compute_lineup_swaps"):
        swaps = compute_lineup_swaps(optimal_starters, current_starters)
    for swap in swaps:
        # season/week are lifted into bot_decisions' own columns by the trigger route.
        swap["decision_inputs"].update(source, season=season, week=week)
    return {"swaps": swaps, "optimal_starters": optimal_starters, "roster_pool": roster_pool, "source": source}


def optimize_weekly_lineup(team_id: str, season: int, week: int, dry_run: bool = False) -> int:
    """Returns the number of swaps published (0 = lineup already optimal). Raises
    InsufficientLineupInputError when there isn't enough data to decide, or when every
    proposed swap lacks an espn_id."""
    logger.info(f"Assembling mathematical optimization model for Team {team_id} Season {season} Week {week}...")

    with tracer.start_as_current_span("lineup_solver.run", attributes={"team_id": team_id, "season": season, "week": week}):
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                evaluation = evaluate_lineup(cursor, team_id, season, week)
                swaps = resolve_lineup_swaps(cursor, evaluation["swaps"])

        logger.info(
            f"Optimal lineup for Team {team_id} Season {season} Week {week}: "
            f"{[p['name'] for p in evaluation['optimal_starters']]}"
        )
        if dry_run:
            logger.info(f"Dry run: not publishing {len(swaps)} swap(s): {swaps}")
            return len(swaps)
        with tracer.start_as_current_span("lineup_solver.publish_lineup_swaps"):
            publish_lineup_swaps(team_id, swaps)
        return len(swaps)


def resolve_default_week() -> int:
    """Resolves a safe CLI default without letting nflreadpy's "current" helper crash startup."""
    fallback_week = 1
    try:
        return int(nfl.get_current_week())
    except Exception as exc:
        logger.warning(
            f"Failed to resolve current NFL week via nflreadpy ({exc}); defaulting --week to {fallback_week}."
        )
        return fallback_week


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Solve the optimal weekly lineup for a team via ILP.")
    parser.add_argument("--team-id", type=str, default=os.getenv("ESPN_TEAM_ID"))
    parser.add_argument("--season", type=int, default=int(os.getenv("CURRENT_YEAR", datetime.now().year)))
    parser.add_argument("--week", type=int, default=None, help="target week; defaults to nflreadpy's current week")
    parser.add_argument("--dry-run", action="store_true", help="compute and log swaps without publishing them")
    args = parser.parse_args(argv)

    if not args.team_id:
        parser.error("--team-id is required (or set the ESPN_TEAM_ID env var)")
    week = args.week if args.week is not None else resolve_default_week()

    try:
        optimize_weekly_lineup(team_id=args.team_id, season=args.season, week=week, dry_run=args.dry_run)
    except InsufficientLineupInputError as exc:
        logger.error(f"Lineup optimizer cannot evaluate Team {args.team_id} Season {args.season} Week {week}: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    configure_tracing("lineup-solver")
    sys.exit(main())
