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
from src.nfl_schedule import WeekResolutionError, resolve_upcoming_week
from src.telemetry import configure_tracing
from src.trigger_client import post_trigger_idempotent
from src.services import run_log_service

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
# Sleeper projections (sync_projections) cover QB/RB/WR/TE only; K/D-ST target-week
# projections come from ESPN via sync_kicker_dst, run in the same daily refresher (#183).
# If that data is missing (ESPN down, refresher failed), no rostered player can fill one of
# these slots, and its current starter is held rather than failing the whole run.
HOLDABLE_WITHOUT_METRICS = {"K", "D/ST"}
PREGAME_WINDOW_WEEKS = 3
METRIC_SOURCE = "weekly_stats.vorp_score"
# resolve_default_week's "regular season over" answer: one past the last regular-season
# week, which fantasy-draft-prep-refresher's inline script treats as offseason (exit 0).
SEASON_OVER_WEEK = 19


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
      1. target_week's own vorp_score, if any rostered non-K/D-ST player has one. These
         are pregame projections (draft-prep-refresher: sync_projections and
         sync_kicker_dst -> vorp_matrix), and a player without a row is on bye, same as
         load_roster_pool.
      2. Otherwise, each player's mean vorp_score over the up-to-`window` prior weeks of
         the same season. Byes come from the nflverse schedule, since prior-week rows say
         nothing about the target week. The newest usable week must be target_week - 1,
         or the data is stale.

    IR-slot players are left out of both queries. They can't start without being
    activated first, yet in mode 2 they keep their prior-week VORP, and a Sleeper status
    other than Out would let them through the injury filter as a proposed starter.

    Raises InsufficientLineupInputError when neither source is usable."""
    cursor.execute(
        """
        SELECT p.player_id, p.name, p.position, p.team, p.injury_status
        FROM league_rosters r
        JOIN players p ON p.player_id = r.player_id
        WHERE r.team_id = %s AND r.slot_position IS DISTINCT FROM 'IR';
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
          AND s.vorp_score IS NOT NULL AND r.slot_position IS DISTINCT FROM 'IR';
        """,
        (team_id, season, first_week, target_week),
    )
    metrics = cursor.fetchall()

    target_rows = [row for row in metrics if row[1] == target_week]
    # K/D-ST target-week rows (ESPN, sync_kicker_dst) can land before Sleeper's skill
    # projections for that week; on their own they must not select target_week mode,
    # which would read every skill player's missing row as a bye.
    position_by_player = {pid: position for pid, _, position, _, _ in roster}
    if any(position_by_player.get(row[0]) not in HOLDABLE_WITHOUT_METRICS for row in target_rows):
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
    # Positions of players left out only because their metrics are missing. In
    # target_week mode a missing row is a bye; in rolling mode the schedule already
    # handled byes, so a missing value is a data gap.
    missing_metric_positions = set()
    for pid, name, position, team, injury_status in roster:
        if injury_status == "Out":
            continue
        if teams_playing is not None and normalize_team(team) not in teams_playing:
            continue
        if pid not in vorp_by_player:
            if mode == "rolling_prior_weeks" and position not in HOLDABLE_WITHOUT_METRICS:
                missing_metric_positions.add(position)
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

    source = {
        "metric_source": METRIC_SOURCE, "source_mode": mode, "source_season": season, "source_weeks": weeks,
        "missing_metric_positions": sorted(missing_metric_positions),
    }
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


def _starter_slots(lineup_slot_counts: dict) -> dict:
    return {
        slot: int(count) for slot, count in lineup_slot_counts.items()
        if slot not in BENCH_AND_IR_SLOT_NAMES and int(count) > 0
    }


def _supply_by_position(roster_pool: list) -> dict:
    supply = {}
    for player in roster_pool:
        supply[player["position"]] = supply.get(player["position"], 0) + 1
    return supply


def short_slots(roster_pool: list, lineup_slot_counts: dict) -> list:
    """Starter slots with fewer eligible pool players than the slot count."""
    supply = _supply_by_position(roster_pool)
    return [
        slot for slot, count in _starter_slots(lineup_slot_counts).items()
        if sum(supply.get(pos, 0) for pos in FLEX_SLOT_ELIGIBLE_POSITIONS.get(slot, (slot,))) < count
    ]


def describe_unfilled_slots(roster_pool: list, lineup_slot_counts: dict) -> str:
    """Human-readable slot demand vs. pool supply, for an infeasible-solve error."""
    short = short_slots(roster_pool, lineup_slot_counts)
    return (
        f"slots {_starter_slots(lineup_slot_counts)} vs players with metrics by position "
        f"{_supply_by_position(roster_pool)}; short: {short or 'combined FLEX demand'}"
    )


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


# Objective weight per filled slot when empty slots are allowed. It dwarfs any VORP gap,
# so the solver fills as many slots as it can before comparing VORP: an empty slot scores
# nothing, so even a below-replacement starter beats it.
FILLED_SLOT_WEIGHT = 1000.0


def build_and_solve_lineup(roster_pool: list, lineup_slot_counts: dict | None = None,
                           allow_unfilled: bool = False) -> list:
    """Pure ILP solve: assign players to configured starter slots (including FLEX and
    non-skill slots like D/ST and K) maximizing total vorp_score.

    By default every slot must be filled, or the solve fails and returns []. With
    allow_unfilled, a slot no pool player can fill (e.g. both RBs on bye) is left empty
    and the rest of the lineup is still optimized."""
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
    fill_weight = FILLED_SLOT_WEIGHT if allow_unfilled else 0.0
    prob += pulp.lpSum(
        var * (by_id[pid]["vorp_score"] * by_id[pid].get("defense_matchup_multiplier", 1.0) + fill_weight)
        for (pid, _, _), var in player_vars.items()
    ), "Total_VORP"

    for slot in slots:
        slot_slug = slot["name"].replace("/", "_").replace(" ", "_").replace("-", "_")
        slot_vars = [
            var
            for (pid, slot_name, slot_index), var in player_vars.items()
            if slot_name == slot["name"] and slot_index == slot["index"]
        ]
        filled = pulp.lpSum(slot_vars)
        prob += (
            filled <= 1 if allow_unfilled else filled == 1,
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


def _slot_key(slot_position: str) -> str:
    """Normalises a roster slot_position ("RB1", "WR2", "FLEX") to the ESPN slot it names.
    Ordinal suffixes are not distinct slots, and ESPN's "FLEX" is the RB/WR/TE slot."""
    base = slot_position.rstrip("0123456789").upper()
    return "RB/WR/TE" if base == "FLEX" else base


def _worker_slot(slot_position: str) -> str:
    """The slot name the browser worker resolves (lineup_slots.json). league_rosters stores
    the flex slot as "RB/WR/TE", which the worker rejects as unrecognised - a permanent
    failure that discards the whole message - so it is published as ESPN's "FLEX"."""
    return "FLEX" if _slot_key(slot_position) == "RB/WR/TE" else slot_position


def _slot_accepts(slot_position: str, position: str) -> bool:
    """Whether a roster slot_position can hold a player of `position`. Unknown slot names
    accept only their own name, so an unrecognised slot is never treated as a wildcard."""
    key = _slot_key(slot_position)
    return position in FLEX_SLOT_ELIGIBLE_POSITIONS.get(key, (key,))


def compute_lineup_swaps(optimal_starters: list, current_starters: dict) -> list:
    """Diffs the solver's optimal lineup against the currently-set starters.

    Each add (a bench player the solver starts) is placed in a slot freed by a drop (a
    starter it sits). An add whose only legal slot is held by a kept starter - an RB who
    can only start in FLEX while a kept WR sits there - displaces that starter into
    another slot it can fill, which may in turn displace another, until the chain ends in
    a slot a drop vacated. Shortest chains win, and a same-position drop is preferred, so
    the common case is still a plain add-for-drop swap.

    A swap benches `drop_player_id` and starts `add_player_id` in `slot`, the slot the add
    ends in. Displaced starters ride along under the swap's "moves" as {kind: "move"}
    entries, so the swap keeps its meaning for every consumer that reads only drop and add
    (matchup badges, rumor bench_watch); flatten_lineup_entries expands them at publish
    time. An add with no chain to a freed slot is skipped, since pairing it with an
    ineligible slot makes ESPN reject the whole message (HTTP 409 TRAN_ROSTER_INELIGIBLE)."""
    optimal_by_id = {p["player_id"]: p for p in optimal_starters}

    def position_of(pid):
        return optimal_by_id[pid]["position"] if pid in optimal_by_id else current_starters[pid]["position"]

    # A slot is identified by the starter who sits in it now. Kept starters stay put unless
    # displaced; a drop's slot is free.
    slot_of = {pid: info["slot_position"] for pid, info in current_starters.items()}
    free = [pid for pid in current_starters if pid not in optimal_by_id]
    occupant = {pid: pid for pid in current_starters if pid in optimal_by_id}
    adds = [p for p in optimal_starters if p["player_id"] not in current_starters]

    def fits(pid, slot_id):
        return _slot_accepts(slot_of[slot_id], position_of(pid))

    # Most constrained adds first, so a flexible add can't take the only slot a stricter one fits.
    adds.sort(key=lambda p: sum(fits(p["player_id"], s) for s in slot_of))

    swaps: list[dict] = []
    displaced_by: dict[str, int] = {}  # kept player_id -> index of the swap whose chain last moved them
    for add_player in adds:
        add_id = add_player["player_id"]
        # Breadth-first from the add: each visited slot's occupant must find another slot.
        parent: dict[str, str | None] = {}  # slot_id -> slot_id the displaced occupant came from (None: the add itself)
        queue = []
        for slot_id in sorted(
            (s for s in slot_of if fits(add_id, s)),
            key=lambda s: current_starters[s]["position"] != add_player["position"],
        ):
            parent[slot_id] = None
            queue.append(slot_id)
        end = next((s for s in queue if s in free), None)
        head = 0
        while end is None and head < len(queue):
            here = queue[head]
            head += 1
            mover = occupant.get(here)
            # Only a starter we found in place can move; an add placed by an earlier chain stays.
            if mover not in current_starters:
                continue
            for slot_id in slot_of:
                if slot_id in parent or not fits(mover, slot_id):
                    continue
                parent[slot_id] = here
                queue.append(slot_id)
                if slot_id in free:
                    end = slot_id
                    break
        if end is None:
            logger.warning(
                "No chain of legal slot moves places %s (%s) in the lineup; skipping that swap.",
                add_player.get("name", add_id), add_player["position"],
            )
            continue

        # Walk back from the freed slot: each occupant shifts one step toward it.
        chain = [end]
        while parent[chain[-1]] is not None:
            chain.append(parent[chain[-1]])
        chain.reverse()  # [slot the add enters, ..., freed slot]
        free.remove(end)
        for slot_id, next_slot in zip(reversed(chain[:-1]), reversed(chain[1:])):
            occupant[next_slot] = occupant[slot_id]
            displaced_by[occupant[slot_id]] = len(swaps)
        occupant[chain[0]] = add_id

        matchup_multiplier = add_player.get("defense_matchup_multiplier", 1.0)
        swaps.append({
            "drop_player_id": end,
            "add_player_id": add_id,
            "slot": _worker_slot(slot_of[chain[0]]),
            "matchup_multiplier": matchup_multiplier,
            # CORE-17 (#35): the numbers that justified this swap, for bot_decisions'
            # audit trail. season/week deliberately not included here - this is a pure
            # diff function and widening its signature just to pass through two values
            # isn't worth it; those columns are simply NULL for lineup_swap rows.
            "decision_inputs": {
                "add_player_vorp_score": add_player.get("vorp_score"),
                "drop_player_vorp_score": current_starters[end].get("vorp_score"),
                "matchup_multiplier": matchup_multiplier,
            },
        })

    # A displaced player's final slot, once every chain has run. Moving between slots of one
    # kind (RB1 -> RB2) is the same ESPN slot, so it is not a move.
    final_slot = {pid: slot_id for slot_id, pid in occupant.items() if pid in current_starters}
    for pid, swap_index in displaced_by.items():
        new_slot = slot_of[final_slot[pid]]
        if _slot_key(new_slot) == _slot_key(slot_of[pid]):
            continue
        swap = swaps[swap_index]
        swap.setdefault("moves", []).append({
            "kind": "move",
            "player_id": pid,
            "slot": _worker_slot(new_slot),
            "decision_inputs": {
                "reason": f"frees {_worker_slot(slot_of[pid])} for {optimal_by_id[swap['add_player_id']].get('name', swap['add_player_id'])}",
            },
        })

    return swaps


def flatten_lineup_entries(swaps: list) -> list:
    """The /v1/trigger/lineup body for `swaps`: each swap, then the moves it carries."""
    entries = []
    for swap in swaps:
        entries.append({k: v for k, v in swap.items() if k != "moves"})
        entries.extend(swap.get("moves", []))
    return entries


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
        # A swap's moves free its slot, so publishing the swap without one would overfill it.
        resolved_moves = []
        for move in swap.get("moves", []):
            move_identity = resolve_espn_identity(cursor, move["player_id"])
            if move_identity is None:
                missing.append(f"move {move['player_id']}")
                continue
            resolved_moves.append(dict(
                move,
                player_id=str(move_identity["espn_id"]),
                player_name=move_identity["name"],
                decision_inputs=dict(move.get("decision_inputs") or {}, player_internal_id=move["player_id"]),
            ))
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
            **({"moves": resolved_moves} if resolved_moves else {}),
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
    manual Job is a new decision and always publishes.

    IR and move entries ({kind, player_id, slot}) are keyed by all three; a swap keeps its original
    [drop, add, slot] material, so keys computed before IR entries existed still match."""
    swaps = flatten_lineup_entries(swaps)
    weeks = sorted({
        (s.get("decision_inputs") or {}).get("season", 0) * 100 + (s.get("decision_inputs") or {}).get("week", 0)
        for s in swaps
    })
    material = json.dumps({
        "job": job_name,
        "team_id": str(team_id),
        "weeks": weeks,
        "swaps": sorted(
            [str(s["kind"]), str(s["player_id"]), str(s["slot"])] if s.get("kind") in ("ir", "activate_ir", "move")
            else [str(s["drop_player_id"]), str(s["add_player_id"]), str(s["slot"])]
            for s in swaps
        ),
    }, sort_keys=True)
    return f"lineup-{team_id}-{hashlib.sha256(material.encode()).hexdigest()[:32]}"


def publish_lineup_swaps(team_id: str, swaps: list, *, idempotency_key: str | None = None) -> bool:
    """POSTs swaps to /v1/trigger/lineup, retrying transient failures (#189) under a stable
    idempotency key. Returns True for newly accepted swaps, False for no-op/duplicate.
    Raises once retries are exhausted or on a 4xx."""
    if not swaps:
        logger.info(f"Lineup already optimal for Team {team_id}; nothing to publish.")
        return False

    key = idempotency_key or lineup_idempotency_key(team_id, swaps, job_name=os.getenv("JOB_NAME"))
    entries = flatten_lineup_entries(swaps)
    response = post_trigger_idempotent("lineup", {"team_id": team_id, "optimized_swaps": entries}, key, timeout=10)
    try:
        status = response.json().get("status")
    except Exception:
        status = None
    if status == "duplicate":
        logger.info(f"Lineup swap(s) for Team {team_id} were already published under {key}; not re-enqueued.")
        return False
    logger.info(f"Published {len(swaps)} lineup swap(s) ({len(entries)} entries) for Team {team_id} to the control plane (idempotency key {key}).")
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
        # Byes can leave a slot with no one to start. That slot stays as it is, and the
        # rest of the lineup is still optimized, so an Out starter elsewhere still gets
        # swapped for his backup.
        # A slot short only because of a data gap stays an error: the missing player
        # might be the right starter.
        unfilled_slots = short_slots(roster_pool, lineup_slot_counts)
        gap_positions = set(source.pop("missing_metric_positions"))
        if any(gap_positions & set(FLEX_SLOT_ELIGIBLE_POSITIONS.get(slot, (slot,))) for slot in unfilled_slots):
            raise InsufficientLineupInputError(
                "could not fill the starting lineup from players with usable metrics: "
                + describe_unfilled_slots(roster_pool, lineup_slot_counts)
            )
        source["unfilled_slots"] = unfilled_slots
    logger.info(
        f"Evaluating Team {team_id} Season {season} Week {week} from {METRIC_SOURCE} "
        f"({source['source_mode']}, Season {season} Week(s) {source['source_weeks']}); "
        f"{len(roster_pool)} eligible player(s); held slots without metrics: {held_slots or 'none'}; "
        f"slots short of eligible players: {unfilled_slots or 'none'}."
    )
    span = trace.get_current_span()
    span.set_attribute("source_mode", source["source_mode"])
    span.set_attribute("source_weeks", source["source_weeks"])
    span.set_attribute("held_slots", held_slots)

    with tracer.start_as_current_span("lineup_solver.build_and_solve_lineup"):
        optimal_starters = build_and_solve_lineup(roster_pool, lineup_slot_counts=lineup_slot_counts, allow_unfilled=True)
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
        for entry in (swap, *swap.get("moves", [])):
            entry["decision_inputs"].update(source, season=season, week=week)
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


def resolve_default_week(season: int | None = None) -> int:
    """The upcoming week for fantasy-infra's shell/inline CronJob scripts, which print it
    and pass it on. Raises WeekResolutionError rather than guessing, so those scripts fail
    the Job. After the regular season it returns SEASON_OVER_WEEK, which the refresher
    reads as offseason; Python callers use resolve_upcoming_week's explicit None instead."""
    if season is None:
        season = int(os.getenv("CURRENT_YEAR") or nfl.get_current_season())
    week = resolve_upcoming_week(season)
    return SEASON_OVER_WEEK if week is None else week


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Solve the optimal weekly lineup for a team via ILP.")
    parser.add_argument("--team-id", type=str, default=os.getenv("ESPN_TEAM_ID"))
    parser.add_argument("--season", type=int, default=int(os.getenv("CURRENT_YEAR", datetime.now().year)))
    parser.add_argument("--week", type=int, default=None, help="target week; defaults to the upcoming week in the nflverse schedule")
    parser.add_argument("--dry-run", action="store_true", help="compute and log swaps without publishing them")
    args = parser.parse_args(argv)

    if not args.team_id:
        parser.error("--team-id is required (or set the ESPN_TEAM_ID env var)")
    week = args.week
    if week is None:
        # Resolved before anything reads the roster or publishes: a wrong week here would
        # set a live ESPN lineup for that week.
        try:
            week = resolve_upcoming_week(args.season)
        except WeekResolutionError as exc:
            logger.error(f"Could not resolve the upcoming week for Season {args.season}: {exc}")
            return 1
        if week is None:
            logger.info(f"The {args.season} regular season is over; no lineup to optimize.")
            return 0

    try:
        optimize_weekly_lineup(team_id=args.team_id, season=args.season, week=week, dry_run=args.dry_run)
    except InsufficientLineupInputError as exc:
        logger.error(f"Lineup optimizer cannot evaluate Team {args.team_id} Season {args.season} Week {week}: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    configure_tracing("lineup-solver")
    sys.exit(run_log_service.run_main("lineup_optimizer", main))
