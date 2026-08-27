"""
Live draft-day board matrix & roster constraint solver (CORE-08).

Pipeline: consume a pick event from fantasy-browser-worker's `draft_picks:{leagueId}`
Redis Stream -> persist it -> shrink the undrafted pool -> recompute scarcity-adjusted
VORP over what's left (reusing CORE-05's vorp_matrix functions verbatim - this is exactly
its pre-draft/no-rosters-yet branch, just re-run after every pick instead of once) -> solve
a small ILP over the undrafted pool respecting our remaining roster construction ->
recommend the best target -> hold for a cancellable delay -> publish to the FastAPI
/trigger/draft-pick route (optimization -> api -> mq, same as lineup_solver.py), which
activates fantasy-browser-worker's click bot.

Uses a Redis Stream consumer group (XREADGROUP), not the plain `draft_picks` pub/sub
channel the worker also publishes to - a missed pub/sub message would silently corrupt the
undrafted pool for the rest of the draft, whereas a Stream can be resumed from the last
acked ID after a crash/restart.
"""
import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from collections import Counter
from typing import Optional

import polars as pl
import pulp
import redis
import requests
from opentelemetry import context, propagate, trace

from src.database import get_db_connection
from src.telemetry import configure_tracing, configure_trace_logging
from src.optimization.vorp_matrix import (
    VORP_POSITIONS,
    FLEX_DEFAULT_WEIGHTS,
    FLEX_SLOT_ELIGIBLE_POSITIONS,
    _largest_remainder_allocate,
    load_league_config,
    resolve_flex_allocation,
    resolve_all_flex_allocations,
    calculate_rank_at,
    calculate_replacement_baselines,
    compute_matchup_coefficient,
    compute_vorp,
    compute_scarcity_multipliers,
    apply_scarcity,
)

configure_trace_logging()
logger = logging.getLogger("DraftMatrix")
tracer = trace.get_tracer("draft_matrix")

CORE_API_BASE_URL = os.getenv("CORE_API_BASE_URL", "http://fantasy-api.core.svc.cluster.local:8000")
REDIS_HOST = os.getenv("REDIS_HOST", "fantasy-cache.default.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

CONSUMER_GROUP = "draft_matrix"
DEFAULT_ROSTER_SIZE = 16
DEFAULT_AUTO_DRAFT_DELAY_SECONDS = int(os.getenv("AUTO_DRAFT_DELAY_SECONDS", "8"))
PAUSE_POLL_INTERVAL_SECONDS = 1
# The pick clock bounds how long a failed publish may keep retrying. It is deliberately
# configurable rather than a constant: ESPN practice rooms run a 30s clock while real
# drafts are 90s+, and a hardcoded 90 would let a practice run retry for a full minute
# after the clock it was meant to protect had already expired and autodrafted.
DRAFT_PICK_CLOCK_SECONDS = int(os.getenv("DRAFT_PICK_CLOCK_SECONDS", "90"))
DRAFT_PUBLISH_SAFETY_SECONDS = 5
# Set by _draft_sync_env() alongside DRAFT_PICK_CLOCK_SECONDS - the listener's own practice/live
# tag, forwarded on every published target so fantasy-browser-worker's click bot can safely
# submit for real in a practice room without needing its own DRY_RUN=false set (which would also,
# and unsafely, apply to any live-league target the same worker pod ever processes).
DRAFT_MODE = os.getenv("DRAFT_MODE", "live")
PUBLISH_RETRY_INTERVAL_SECONDS = 2
# Retry only what a retry can actually fix. A deterministic 4xx (notably the 422 that
# motivated this work) returns the same result to a byte-identical request, so retrying it
# just burns the clock and floods the log while the real fix is upstream.
RETRYABLE_PUBLISH_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
# An attempt must finish inside the pick window, not merely start inside it: a request begun
# a second before the deadline with a fixed 10s timeout could enqueue nine seconds after the
# clock it was bounded by. The per-attempt timeout is therefore clamped to whatever window is
# actually left, and an attempt is not started at all below the floor.
PUBLISH_TIMEOUT_SECONDS = 10
MIN_PUBLISH_WINDOW_SECONDS = 1
# The main loop's XREADGROUP asks the *server* to hold the connection open for up to
# XREADGROUP_BLOCK_MS before replying with a nil - but redis-py's own client-side socket
# read timeout is a separate, independent setting, left unset here until now. This
# repo's pinned requirements.txt (redis==5.0.4, confirmed installed in isolation) defaults
# an unset socket_timeout to None - unbounded - so today a genuinely dropped connection
# (not a normal quiet gap, which BLOCK already handles server-side) leaves the listener
# hung forever with no exception, no log, and no chance for the consumer-group backlog
# drain on restart (see run_draft_listener's docstring) to ever kick in, since the process
# never actually dies. (Locally testing against this repo's documented unpinned-venv
# workaround for newer Python - see CLAUDE.md's Testing section - happened to install
# redis-py 8.1, whose *different* default of 5s collided with a 5000ms BLOCK and raised
# a real, reproducible redis.exceptions.TimeoutError on any ~5s quiet gap; that specific
# collision is a dev-venv artifact, not something the pinned 5.0.4 client does.)
# Comfortably larger than the block duration either way: with 5.0.4 it turns an
# undetectable infinite hang on a genuinely dropped connection into a loud, retried
# condition instead of a silent one (the retry loop below still doesn't crash the process
# for a transient TimeoutError/ConnectionError - see its own comment - but at least it's
# now visible and retried instead of hanging with no signal at all); with a newer redis-py
# it also avoids the 8.1-style collision.
XREADGROUP_BLOCK_MS = 5000
REDIS_SOCKET_TIMEOUT_SECONDS = 15
# The generated Job (src/api/draft.py's start_draft_sync) sets backoff_limit=0 and
# restart_policy="Never" - nothing outside this process will ever restart it. Now that
# XREADGROUP_BLOCK_MS is bounded above, an unhandled TimeoutError/ConnectionError from a
# merely transient Redis stall (a brief network blip, not a truly dead connection) would
# permanently end recommendations for the rest of the draft instead of just this one poll.
# Retried indefinitely rather than propagated - there is no external supervisor to fall
# back on, so an internal retry loop is the only way a transient stall can ever recover.
TRANSIENT_REDIS_RETRY_SECONDS = 2
# CORE-14 web UI wants to show the system's top-N alternatives, not just the one it would
# auto-draft - see rank_recommended_targets/compute_draft_recommendation_candidates.
TOP_CANDIDATES_LIMIT = 5


def _listener_board_key(league_id: str) -> str:
    return f"draft:{league_id}:board"


def _write_listener_state(r_client, league_id: str, state: str, error: Optional[str] = None) -> None:
    # listener_error_owner is reset on every listener write: the listener owns the field it
    # just wrote, so a publisher thread holding a stale token must not be able to retract it.
    mapping = {"listener_state": state, "listener_error": error or "", "listener_error_owner": ""}
    r_client.hset(_listener_board_key(league_id), mapping=mapping)


# ------------------------------------------------------------------------------
# Undrafted pool tracking
# ------------------------------------------------------------------------------
def load_undrafted_pool(cursor, league_id: str, season: int, week: int) -> pl.DataFrame:
    """Every player with a projection for this draft's season/week, minus anyone already
    recorded in draft_picks for this league/season. Mirrors vorp_matrix's base query
    (players JOIN weekly_stats) with is_rostered/is_starter hardcoded False/NULL - a
    predraft pool has no league_rosters rows, so every remaining player is a free agent
    by definition, exactly vorp_matrix's pre-draft branch."""
    cursor.execute("""
        SELECT p.player_id, p.espn_id, p.name, p.position, s.projected_points, s.team_implied_total,
               s.avg_separation, s.pressure_rate_allowed, s.depth_chart_delta, COALESCE(s.team, p.team) AS team
        FROM players p
        JOIN weekly_stats s ON p.player_id = s.player_id
        WHERE s.season = %s AND s.week = %s
          AND NOT EXISTS (
              SELECT 1 FROM draft_picks d
              WHERE d.league_id = %s AND d.season = %s AND d.player_id = p.player_id
          );
    """, (season, week, league_id, season))
    rows = cursor.fetchall()
    schema = {
        "player_id": pl.Utf8, "espn_id": pl.Utf8, "name": pl.Utf8, "position": pl.Utf8, "projected_points": pl.Float64,
        "team_implied_total": pl.Float64, "avg_separation": pl.Float64,
        "pressure_rate_allowed": pl.Float64, "depth_chart_delta": pl.Int64, "team": pl.Utf8,
    }
    df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    return df.with_columns([
        pl.lit(season).alias("season"),
        pl.lit(week).alias("week"),
        pl.lit(False).alias("is_rostered"),
        pl.lit(None, dtype=pl.Boolean).alias("is_starter"),
    ])


def recompute_scarcity_vorp(df: pl.DataFrame, league_config: dict) -> pl.DataFrame:
    """Re-runs CORE-05's coefficient -> baseline -> VORP -> scarcity pipeline against the
    current undrafted pool. No FLEX/roster observation data exists pre-draft, so
    flex_alloc always falls back to FLEX_DEFAULT_WEIGHTS (empty roster_df), and
    rosters_populated is always False - this *is* vorp_matrix's pre-draft branch, just
    invoked once per pick instead of once per week."""
    if df.is_empty():
        return df
    empty_roster_df = pl.DataFrame(schema={"slot_position": pl.Utf8, "position": pl.Utf8})
    df = compute_matchup_coefficient(df)
    flex_alloc = resolve_all_flex_allocations(league_config, empty_roster_df)
    rank_at = calculate_rank_at(league_config, flex_alloc)
    baselines = calculate_replacement_baselines(df, league_config, flex_alloc, rosters_populated=False)
    df = compute_vorp(df, baselines)
    multipliers = compute_scarcity_multipliers(df, rank_at)
    return apply_scarcity(df, multipliers)


# ------------------------------------------------------------------------------
# Roster-constrained ILP target selection
# ------------------------------------------------------------------------------
def load_team_position_counts(cursor, league_id: str, season: int, team_id: str) -> dict:
    """Positions our team has already drafted this league/season, from the local
    draft_picks ledger (not league_rosters - that's only populated once the regular
    season starts syncing from ESPN, long after the draft is over)."""
    cursor.execute("""
        SELECT p.position, COUNT(*) FROM draft_picks d
        JOIN players p ON d.player_id = p.player_id
        WHERE d.league_id = %s AND d.season = %s AND d.team_id = %s
        GROUP BY p.position;
    """, (league_id, season, team_id))
    return {pos: count for pos, count in cursor.fetchall()}


def resolve_own_team_flex_allocation(league_config: dict) -> dict:
    """Per-*our-team* FLEX share (one team's own slot counts), unlike
    vorp_matrix.resolve_all_flex_allocations which multiplies by team_count to get the
    league-wide FLEX total needed for replacement-baseline/scarcity math. Pre-draft there's
    no roster_df to observe real usage from, so this always falls back to
    FLEX_DEFAULT_WEIGHTS via resolve_flex_allocation's empty-roster_df path."""
    empty_roster_df = pl.DataFrame(schema={"slot_position": pl.Utf8, "position": pl.Utf8})
    lineup_slot_counts = league_config["lineup_slot_counts"]
    combined: dict = {}
    for slot_name, eligible_positions in FLEX_SLOT_ELIGIBLE_POSITIONS.items():
        slot_count = lineup_slot_counts.get(slot_name, 0)
        if slot_count <= 0:
            continue
        alloc = resolve_flex_allocation(slot_count, empty_roster_df, eligible_positions)
        for pos, n in alloc.items():
            combined[pos] = combined.get(pos, 0) + n
    return combined


def compute_draftable_roster_size(league_config: dict) -> tuple:
    """Returns (starting_total, bench_total) for OUR team's draftable skill-position
    roster - the actual capacity the ILP/remaining-picks budget should agree on.
    bench_total comes directly from lineup_slot_counts['BE'] when this league has synced
    real settings (BE is a real ESPN slot type, confirmed via sync_league), NOT from
    summing every lineup_slot_counts key: that sum also includes IR (never filled during
    the initial draft) and would silently inflate bench capacity by one slot per IR spot.
    Falls back to DEFAULT_ROSTER_SIZE only when no league has synced yet (no 'BE' key at
    all)."""
    flex_alloc = resolve_own_team_flex_allocation(league_config)
    lineup_slot_counts = league_config["lineup_slot_counts"]
    starting_total = sum(lineup_slot_counts.get(pos, 0) for pos in VORP_POSITIONS) + sum(flex_alloc.values())
    bench_total = lineup_slot_counts.get("BE")
    if bench_total is None:
        return starting_total, max(0, DEFAULT_ROSTER_SIZE - starting_total)
    return starting_total, max(0, bench_total)


def compute_total_picks(team_count: int, league_config: dict) -> int:
    """Expected total picks for a standard snake draft: every team drafts the same
    roster shape, so this is just team_count * (starting + bench) slots (#82) - the
    threshold a draft's pick_number must reach before anything should treat it as over."""
    starting_total, bench_total = compute_draftable_roster_size(league_config)
    return team_count * (starting_total + bench_total)


def compute_position_caps(league_config: dict, drafted_by_us: dict) -> tuple:
    """Derives per-position draft caps from league_settings (not the issue's hardcoded
    QB>=1/RB>=3/WR>=3/TE>=1 thresholds) - starting slots + this league's FLEX share +
    a bench allocation (bench slots distributed by the same FLEX_DEFAULT_WEIGHTS used for
    FLEX itself), minus what we've already drafted at that position. Also returns
    starting_needs (slots + flex only, no bench) so the target-selection step can tell a
    "still need a starter here" position apart from "just filling bench depth"."""
    flex_alloc = resolve_own_team_flex_allocation(league_config)
    lineup_slot_counts = league_config["lineup_slot_counts"]
    starting_slots = {pos: lineup_slot_counts.get(pos, 0) for pos in VORP_POSITIONS}
    _, bench_total = compute_draftable_roster_size(league_config)
    bench_alloc = _largest_remainder_allocate(dict(FLEX_DEFAULT_WEIGHTS), bench_total)

    starting_needs = {}
    position_caps = {}
    for pos in VORP_POSITIONS:
        target = starting_slots.get(pos, 0) + flex_alloc.get(pos, 0)
        drafted = drafted_by_us.get(pos, 0)
        starting_needs[pos] = max(0, target - drafted)
        position_caps[pos] = max(0, target + bench_alloc.get(pos, 0) - drafted)
    return position_caps, starting_needs


def build_and_solve_draft_pick(undrafted_pool: list, position_caps: dict, remaining_picks: int) -> list:
    """Pure ILP solve: which subset of the undrafted pool maximizes total scarcity-adjusted
    VORP, subject to our remaining draft-pick budget and per-position caps. Mirrors
    lineup_solver.build_and_solve_lineup's shape - this answers "what's the ideal
    remaining team from what's left right now", not "who do I pick this instant" (that's
    select_recommended_target, applied to this ILP's output)."""
    if not undrafted_pool or remaining_picks <= 0:
        return []

    # vorp_score is intentionally signed (vorp_matrix.compute_vorp) - a "<=" budget
    # constraint would let CBC maximize by taking nobody once remaining candidates are
    # all zero/negative, even though a real draft still forces a pick every round. Force
    # exactly as many picks as are actually fillable, so the solver can't dodge a
    # mandatory pick by declining value.
    #
    # "Fillable" must be bounded per position, not just by the sum of caps: summing caps
    # alone assumes every capped position can actually supply that many candidates, which
    # a small, fast-depleting pool (K/D-ST, capped at 1 each with no bench share) can
    # violate well before the draft is likely to have used up the deep skill-position
    # pools. Two kickers left in the pool with caps {K: 1, D/ST: 1} sums to 2, but only 1
    # is actually achievable (no D/ST candidates exist to fill that cap) - the old sum
    # would set target_picks=2, the Remaining_Picks equality constraint could never be
    # satisfied, and CBC would report the whole problem infeasible instead of returning
    # the one pick that *is* available.
    pool_counts = Counter(p["position"] for p in undrafted_pool)
    fillable = sum(min(position_caps.get(pos, 0), pool_counts.get(pos, 0)) for pos in VORP_POSITIONS)
    target_picks = min(remaining_picks, len(undrafted_pool), fillable)
    if target_picks <= 0:
        return []

    prob = pulp.LpProblem("Draft_Pool_Optimization", pulp.LpMaximize)
    player_vars = {p["player_id"]: pulp.LpVariable(f"take_{p['player_id']}", cat="Binary") for p in undrafted_pool}
    by_id = {p["player_id"]: p for p in undrafted_pool}

    prob += pulp.lpSum(player_vars[pid] * by_id[pid]["vorp_score"] for pid in player_vars), "Total_VORP"
    prob += pulp.lpSum(player_vars.values()) == target_picks, "Remaining_Picks"
    for pos in VORP_POSITIONS:
        cap = position_caps.get(pos, 0)
        prob += (
            pulp.lpSum(var for pid, var in player_vars.items() if by_id[pid]["position"] == pos) <= cap,
            f"Cap_{pos}",
        )

    prob.solve(pulp.PULP_CBC_CMD(msg=False))
    if pulp.LpStatus[prob.status] != "Optimal":
        logger.error("Draft pool solver could not find a feasible optimal selection.")
        return []

    return [by_id[pid] for pid, var in player_vars.items() if var.varValue == 1.0]


def rank_recommended_targets(selected: list, starting_needs: dict, limit: int = TOP_CANDIDATES_LIMIT) -> list:
    """Ranks the ILP's selected set the same way select_recommended_target picks its single
    winner - starting-need players first (a positional run making that tier disappear is
    already captured by scarcity_multiplier) - but, unlike the single-winner case, does not
    drop the rest of the selected set once at least one starter-needed candidate exists:
    best-available fills out the remainder of `limit` behind them, so a comparison list of
    fewer than `limit` starter-needed players still shows real alternatives instead of
    truncating early. select_recommended_target's own single-winner behavior is unaffected -
    it only ever looks at index 0, which is unchanged (still the top starter-needed pick, or
    the single best-available if none are needed)."""
    if not selected:
        return []
    starter_needed = sorted(
        (p for p in selected if starting_needs.get(p["position"], 0) > 0),
        key=lambda p: p["vorp_score"], reverse=True,
    )
    if not starter_needed:
        return sorted(selected, key=lambda p: p["vorp_score"], reverse=True)[:limit]
    starter_needed_ids = {p["player_id"] for p in starter_needed}
    best_available = sorted(
        (p for p in selected if p["player_id"] not in starter_needed_ids),
        key=lambda p: p["vorp_score"], reverse=True,
    )
    return (starter_needed + best_available)[:limit]


def select_recommended_target(selected: list, starting_needs: dict) -> Optional[dict]:
    """The single actual pick recommendation: the top of rank_recommended_targets'
    ranking, limited to just the winner."""
    ranked = rank_recommended_targets(selected, starting_needs, limit=1)
    return ranked[0] if ranked else None


# ------------------------------------------------------------------------------
# Snake-draft turn order (CORE-10)
# ------------------------------------------------------------------------------
def league_team_count(conn, league_id: str, season: int) -> Optional[int]:
    """Team count for snake-draft math. Returns None when it isn't known yet, which callers
    must treat as "round unknown" rather than guessing a default - a wrong team count silently
    produces a wrong draft slot map and therefore a wrong on_the_clock_team for the rest of
    the draft.

    Prefers league_settings (synced from ESPN by sync_league.py), falling back to the count
    derived from pick arrivals and cached on draft_config. Only the one real ESPN_LEAGUE_ID is
    ever synced, so for practice/mock rooms that fallback is the only source there is.

    The draft_config fallback is scoped to the season. draft_config is keyed by league_id
    alone, so a room reused in a later season keeps the same row; without the season match a
    stale count would apply immediately, block re-derivation, and compute every round and slot
    at the wrong size."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT team_count FROM league_settings WHERE league_id = %s;", (league_id,))
        row = cursor.fetchone()
        if row and row[0]:
            return int(row[0])
        cursor.execute(
            "SELECT team_count FROM draft_config WHERE league_id = %s AND season = %s;",
            (league_id, season),
        )
        row = cursor.fetchone()
    return int(row[0]) if row and row[0] else None


def _connector_expected_total_picks(r_client, league_id: str) -> Optional[int]:
    """Reads draft_sync.js connector health and returns expected_total_picks when valid."""
    if r_client is None:
        return None
    raw = r_client.get(f"draft:{league_id}:health")
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        logger.warning("Ignoring malformed connector health for league %s while deriving team_count.", league_id)
        return None
    if not isinstance(payload, dict):
        return None
    expected_total_picks = payload.get("expected_total_picks")
    try:
        parsed = int(expected_total_picks)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _team_count_from_connector_health(conn, r_client, league_id: str) -> Optional[int]:
    """Converts connector health's expected_total_picks into a team count."""
    expected_total_picks = _connector_expected_total_picks(r_client, league_id)
    if expected_total_picks is None:
        return None
    with conn.cursor() as cursor:
        league_config = load_league_config(cursor)
    per_team_slots = sum(compute_draftable_roster_size(league_config))
    if per_team_slots <= 0 or expected_total_picks % per_team_slots != 0:
        logger.warning(
            "Ignoring connector expected_total_picks=%s for league %s; roster slots per team=%s are incompatible.",
            expected_total_picks, league_id, per_team_slots,
        )
        return None
    team_count = expected_total_picks // per_team_slots
    return team_count if team_count >= 2 else None


def derive_team_count(conn, league_id: str, season: int, r_client=None) -> Optional[int]:
    """Infers the team count for practice drafts.

    Prefers fantasy-browser-worker's authoritative connector-health signal
    (`draft:{league_id}:health.expected_total_picks`) when available, converting it into a
    team count using this league's draftable roster size. Falls back to a pick-sequence
    heuristic only when that signal is absent/unusable.

    The fallback heuristic uses pick arrivals: a snake draft turns around at the round-1/2
    boundary, so picks team_count and team_count + 1 belong to the same team.

    Restricted to practice/mock rooms. ESPN forbids trading picks in a mock room, so round-1
    ownership there is guaranteed to be one pick per team and the turnaround is unambiguous.
    A live league can trade picks, which makes an adjacent repeat like a, b, c, c genuinely
    indistinguishable from a turnaround - and a live league is exactly the case that has an
    authoritative count available from sync_league.py anyway.

    Returns None until the pair is observed, and refuses outright in three cases:

    - the picks seen so far are not a gapless run starting at 1. If an early pick were
      dropped, the boundary would land early and yield a count one too small.
    - a team repeats without being the immediately preceding picker. Round 1 is every team
      exactly once, so that can only mean malformed ownership data - treating it as the
      boundary would derive a count far below the real one.
    - the pair appears at pick 2, implying a one-team league.

    Every rejection is deliberate: an unknown count leaves rounds NULL and visibly repairable,
    while a wrong one quietly mis-sizes every round and draft slot for the whole draft with
    nothing to indicate it."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT mode FROM draft_config WHERE league_id = %s AND season = %s;", (league_id, season))
        row = cursor.fetchone()
        if row is None or row[0] != "practice":
            return None
    connector_team_count = _team_count_from_connector_health(conn, r_client, league_id)
    if connector_team_count is not None:
        return connector_team_count
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT pick_number, team_id FROM draft_picks
             WHERE league_id = %s AND season = %s
             ORDER BY pick_number;
        """, (league_id, season))
        picks = cursor.fetchall()

    seen: set = set()
    previous_team_id = None
    for expected, (pick_number, team_id) in enumerate(picks, start=1):
        if int(pick_number) != expected:
            return None
        if team_id in seen:
            if team_id != previous_team_id:
                return None
            team_count = expected - 1
            return team_count if team_count >= 2 else None
        seen.add(team_id)
        previous_team_id = team_id
    return None


def persist_derived_team_count(conn, league_id: str, season: int, team_count: int) -> None:
    """Caches a derived team count on the league's draft_config row. Never overwrites an
    existing value: the count is fixed for the life of a draft, and a later re-derivation off
    a partial pick set must not be able to move it. Scoped to the season so the cache can
    never be written against, or read back for, a different season's draft."""
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE draft_config SET team_count = %s
             WHERE league_id = %s AND season = %s AND team_count IS NULL;
        """, (team_count, league_id, season))


def backfill_missing_rounds(conn, league_id: str, season: int, team_count: int) -> int:
    """Repairs picks recorded while league_settings had no team_count. Those rows were stored
    with round NULL, and persist_draft_pick's ON CONFLICT DO NOTHING means a Stream replay can
    never fill them in - so without this, a league configured even one pick late would have a
    permanently incomplete draft_slots map and therefore no on_the_clock_team for the whole
    draft. Recomputes the round for every NULL row and claims a draft slot for any that turn
    out to be round 1. Returns the number of rows repaired."""
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE draft_picks
               SET round = div(pick_number - 1, %s) + 1
             WHERE league_id = %s AND season = %s AND round IS NULL
            RETURNING pick_number, team_id, round;
        """, (team_count, league_id, season))
        repaired = cursor.fetchall()
    for pick_number, team_id, round_ in repaired:
        if round_ == 1:
            persist_draft_slot(conn, league_id, season, str(team_id), int(pick_number))
    if repaired:
        logger.info(f"Backfilled round for {len(repaired)} pick(s) in league {league_id} recorded before league_settings existed.")
    return len(repaired)


def persist_draft_slot(conn, league_id: str, season: int, team_id: str, slot_number: int) -> None:
    """Records a team's round-1 pick_number as its permanent draft slot. ON CONFLICT DO
    NOTHING keyed on (league_id, season, team_id) - a team's slot is fixed the moment they
    make their first pick, so a replayed Stream entry (see persist_draft_pick) must never
    overwrite it."""
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO draft_slots (league_id, season, team_id, slot_number)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (league_id, season, team_id) DO NOTHING;
        """, (league_id, season, team_id, slot_number))


def compute_on_the_clock_team(next_pick_number: int, team_count: int, slot_map: dict) -> Optional[str]:
    """Standard snake-draft order: round 1 goes slot 1 -> team_count, round 2 reverses
    team_count -> 1, and so on. slot_map is {team_id: slot_number} as recorded by
    persist_draft_slot - only ever populated from real round-1 pick arrivals (see module
    docstring in handle_pick_event: ESPN's API can't supply this pre-draft). Returns None
    if the slot for the computed position isn't known yet, e.g. asking about round 2+
    before every team has made its round-1 pick."""
    if team_count <= 0:
        return None
    round_num = (next_pick_number - 1) // team_count + 1
    position_in_round = (next_pick_number - 1) % team_count + 1
    slot = position_in_round if round_num % 2 == 1 else team_count - position_in_round + 1
    by_slot = {v: k for k, v in slot_map.items()}
    return by_slot.get(slot)


# ------------------------------------------------------------------------------
# Draft pick persistence
# ------------------------------------------------------------------------------
def resolve_player_id(conn, espn_player_id: str) -> Optional[str]:
    """Maps ESPN's player id, which is what the draft stream carries, to the internal
    players.player_id that every other table in this schema is keyed by.

    Deliberately has no "maybe it's already an internal id" fallback. The two id spaces
    overlap numerically - a real pick was recorded against the wrong player because an ESPN
    id happened to also exist as an internal player_id - and a pick silently attributed to
    someone else is worse than one recorded with an unresolved id, because nothing downstream
    can tell it apart from a correct one.

    Returns None for players the roster sync doesn't carry, notably D/ST and K. Callers must
    still record those picks: draft order integrity matters more than the name."""
    with conn.cursor() as cursor:
        cursor.execute("SELECT player_id FROM players WHERE espn_id = %s;", (str(espn_player_id),))
        row = cursor.fetchone()
    return row[0] if row else None


def persist_draft_pick(conn, league_id: str, season: int, pick_number: int, round_: Optional[int], team_id: str, player_id: Optional[str], espn_player_id: Optional[str] = None) -> bool:
    """ON CONFLICT DO NOTHING keyed on (league_id, season, pick_number) - a Stream
    consumer group redelivers an entry if it crashes before XACK, and reprocessing an
    already-recorded pick must be a no-op, not a duplicate. RETURNING tells the caller
    whether this call actually inserted a new row: DO NOTHING alone only suppresses the
    insert, it doesn't stop handle_pick_event from recomputing and auto-publishing a
    second RabbitMQ draft command for a pick that already happened."""
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO draft_picks (league_id, season, pick_number, round, team_id, player_id, espn_player_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (league_id, season, pick_number) DO NOTHING
            RETURNING pick_number;
        """, (league_id, season, pick_number, round_, team_id, player_id, espn_player_id))
        return cursor.fetchone() is not None


# ------------------------------------------------------------------------------
# Delayed, cancellable publish
# ------------------------------------------------------------------------------
def _is_paused_or_cancelled(r_client, league_id: str, pick_number: int, cancel_event: Optional[threading.Event]) -> bool:
    if cancel_event is not None and cancel_event.is_set():
        logger.info(f"Draft target for pick {pick_number} superseded by a newer pick; skipping auto-publish.")
        return True
    if r_client.get(f"draft:{league_id}:pause:{pick_number}"):
        logger.info(f"Auto-draft paused by operator for pick {pick_number}; skipping auto-publish.")
        return True
    return False


def wait_for_pause_or_timeout(
    r_client, league_id: str, pick_number: int, delay_seconds: int,
    cancel_event: Optional[threading.Event] = None, sleep_fn=time.sleep,
) -> bool:
    """Polls a human-settable Redis pause key (and an in-process cancel_event, set when a
    newer pick supersedes this one - see hold_and_publish) once a second for up to
    delay_seconds. This is the "full autopilot, but interruptible" window from CORE-08
    planning: the future draft-day web UI (CORE-10/CORE-11) can SET
    draft:{league_id}:pause:{pick_number} to stop the auto-publish for that pick. Returns
    True if the delay elapsed with no pause/cancel (proceed to publish), False otherwise.

    Checks BEFORE every sleep *and once more after the last one* - checking only before
    each sleep misses a pause/cancel that lands during the final polling window, and would
    publish anyway despite the operator's last-second override."""
    elapsed = 0
    while elapsed < delay_seconds:
        if _is_paused_or_cancelled(r_client, league_id, pick_number, cancel_event):
            return False
        sleep_fn(PAUSE_POLL_INTERVAL_SECONDS)
        elapsed += PAUSE_POLL_INTERVAL_SECONDS
    return not _is_paused_or_cancelled(r_client, league_id, pick_number, cancel_event)


def publish_draft_target(team_id: str, target: dict, idempotency_key: Optional[str] = None, timeout: float = PUBLISH_TIMEOUT_SECONDS, expires_at: Optional[float] = None, league_id: Optional[str] = None, pick_number: Optional[int] = None) -> None:
    # 182 of 1027 players have no crosswalk entry, and the worker has nothing to click
    # without one. Fail loudly here rather than posting a null id: the trigger would either
    # time out in the draft room or, worse, be coerced into matching someone else.
    if not target.get("espn_id"):
        raise ValueError(
            f"Cannot auto-draft {target.get('name')} ({target.get('player_id')}): no espn_id in the crosswalk, "
            "so the browser worker has no data-player-id to click. Pick this player manually."
        )

    headers: dict = {}
    propagate.inject(headers)

    response = requests.post(
        f"{CORE_API_BASE_URL}/v1/trigger/draft-pick",
        json={
            "team_id": team_id,
            # The browser worker clicks button.Button--draft[data-player-id="..."], which is
            # ESPN's id. target["player_id"] is Sleeper's - sending it means the selector
            # never matches (a 15s timeout, no pick) or, where the id spaces collide,
            # matches a real but entirely different player.
            "player_id": target["espn_id"],
            "name": target["name"],
            "position": target["position"],
            "pro_team": target["team"],
            # Stable across every retry of this pick, so an ambiguous failure (a read
            # timeout, or a reset after the request was sent) cannot enqueue the same draft
            # action twice and have the worker click twice.
            "idempotency_key": idempotency_key,
            # The request timeout only stops *this* client waiting; the FastAPI handler is
            # synchronous and can resume from a blocked broker call long after we gave up.
            # An absolute deadline lets the gateway discard the pick itself rather than
            # queue a click for a slot ESPN has already autodrafted.
            "expires_at": expires_at,
            # cancel_event cannot interrupt a request already inside requests.post, so if the
            # slot is filled mid-flight only the gateway can still stop the enqueue. It
            # rechecks these against the listener board immediately before publishing.
            "league_id": league_id,
            "pick_number": pick_number,
            "mode": DRAFT_MODE,
        },
        headers=headers,
        timeout=timeout,
    )
    if response.status_code >= 400:
        logger.error(
            "Draft target API rejected %s for Team %s with HTTP %s: %s",
            target["name"], team_id, response.status_code, response.text,
        )
    response.raise_for_status()
    logger.info(f"Published draft target for Team {team_id}: {target['name']} ({target['position']}).")


def _publish_error_detail(error: Exception) -> str:
    response = getattr(error, "response", None)
    response_body = getattr(response, "text", None)
    return f"{error}; response body: {response_body}" if response_body else str(error)


def _is_retryable_publish_error(error: Exception) -> bool:
    """Distinguishes "the request never landed" from "the API considered it and said no".

    Only the former can be fixed by sending the same bytes again. A 422 (the failure this
    whole path was built for) or the missing-espn_id ValueError will answer identically
    every time, so retrying them spends the remaining pick clock re-asking a settled
    question and buries the one useful log line under dozens of copies.
    """
    response = getattr(error, "response", None)
    status = getattr(response, "status_code", None)
    if status is not None:
        return status in RETRYABLE_PUBLISH_STATUSES
    # No response at all means the request never completed - a connection reset, DNS blip
    # or timeout against a service that may well be up by the next attempt.
    return isinstance(error, requests.exceptions.RequestException)


def _record_publish_error(league_id: str, target: dict, error_message: str, season: Optional[int] = None) -> None:
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO execution_log
                        (transaction_type, bot_name, status, error_message, league_id, season, player_id, player_name)
                    VALUES ('draft_target_publish', 'draft_matrix', 'failure', %s, %s, %s, %s, %s);
                    """,
                    (error_message, league_id, season, target.get("espn_id"), target.get("name")),
                )
    except Exception as error:
        logger.error("Failed to record draft target publish error for league %s: %s", league_id, error)


# Records a publish failure without asserting the consumer's health, and without stepping on
# an error the consumer owns. The publisher runs on a separate thread from the Stream
# consumer, which owns listener_state: a plain write of "Active" here can land after the
# consumer recorded "Failed" and report a dead listener as healthy, and a plain write of
# listener_error can replace the fatal listener reason with an unrelated HTTP error.
# listener_error_owner records who wrote the message, so only that writer may retract it.
_SURFACE_PUBLISH_ERROR_LUA = """
local state = redis.call('HGET', KEYS[1], 'listener_state')
if state == 'Failed' then
    return 0
end
if not state or state == '' then
    redis.call('HSET', KEYS[1], 'listener_state', 'Active')
end
redis.call('HSET', KEYS[1], 'listener_error', ARGV[1], 'listener_error_owner', ARGV[2])
return 1
"""


def _surface_publish_failure(r_client, league_id: str, target: dict, error_message: str, owner: str = "", season: Optional[int] = None) -> None:
    logger.error("Failed to publish draft target for league %s: %s", league_id, error_message)
    try:
        r_client.eval(
            _SURFACE_PUBLISH_ERROR_LUA, 1, _listener_board_key(league_id), error_message, owner,
        )
    except Exception as state_error:
        logger.error("Failed to update listener error state for league %s: %s", league_id, state_error)
    # Always recorded, even when the board write was skipped: execution_log is the durable
    # record and must not lose a publish failure just because the listener is already down.
    _record_publish_error(league_id, target, error_message, season=season)


def _surface_publish_error(r_client, league_id: str, target: dict, error: Exception, owner: str = "", season: Optional[int] = None) -> str:
    error_message = _publish_error_detail(error)
    _surface_publish_failure(r_client, league_id, target, error_message, owner=owner, season=season)
    return error_message


# Clears listener_error only while this publisher still owns it. Matching the message text is
# not an ownership check - two overlapping publisher threads can record the identical string
# ("connection refused"), and the older one succeeding would then clear the newer one's
# still-current failure.
_CLEAR_PUBLISH_ERROR_LUA = """
if redis.call('HGET', KEYS[1], 'listener_state') == 'Failed' then
    return 0
end
if redis.call('HGET', KEYS[1], 'listener_error_owner') == ARGV[1] then
    return redis.call('HSET', KEYS[1], 'listener_error', '', 'listener_error_owner', '')
end
return 0
"""


def _clear_publish_error(r_client, league_id: str, owner: str) -> None:
    """Retracts our own transient publish error after a retry succeeded, leaving any error
    another writer recorded in the meantime intact."""
    try:
        r_client.eval(_CLEAR_PUBLISH_ERROR_LUA, 1, _listener_board_key(league_id), owner)
    except Exception as state_error:
        logger.error("Failed to clear listener error state for league %s: %s", league_id, state_error)


def _surface_clock_expired(r_client, league_id: str, target: dict, pick_number: int, detail: str, owner: str = "", season: Optional[int] = None) -> None:
    """A pick abandoned because the clock ran out is a missed pick, not a quiet no-op: it
    hands the slot to ESPN autodraft. It has to reach /v1/draft/status and execution_log,
    not just pod logs, or the operator's first sign of it is a player they did not choose."""
    _surface_publish_failure(
        r_client, league_id, target,
        f"Pick clock expired before pick {pick_number} could be published ({detail}); "
        f"target '{target.get('name')}' was not sent and the pick may be autodrafted.",
        owner=owner, season=season,
    )


def hold_and_publish(
    r_client, league_id: str, team_id: str, pick_number: int, target: dict, delay_seconds: int,
    cancel_event: Optional[threading.Event] = None, sleep_fn=time.sleep,
    trace_context: Optional[context.Context] = None,
    monotonic_fn=time.monotonic, clock_origin: Optional[float] = None, time_fn=time.time,
    season: Optional[int] = None,
) -> None:
    """Runs the cancellable delay window and publishes if it elapses uninterrupted. Called
    from a background thread (see _schedule_publish) so the Stream consumer loop keeps
    reading new picks instead of blocking through the delay - a positional run during the
    wait would otherwise queue up unprocessed while draft_matrix sits idle.

    Runs on a separate OS thread, which (unlike an asyncio task) does not automatically
    inherit the calling thread's OTel context - trace_context is the parent span's context
    captured by _schedule_publish before spawning this thread, and must be explicitly
    re-attached here or the eventual publish span silently starts a disconnected trace.

    clock_origin is a monotonic reading anchored to when the *upstream* pick was written to
    the Redis Stream (see _stream_entry_clock_origin). Starting the deadline at thread start
    would exclude everything handle_pick_event does synchronously first - persisting the
    pick, reloading the undrafted pool, and solving the ILP - and would also give a drained
    backlog entry a fresh clock, so a slow recompute or a restart could publish a target for
    a pick ESPN had already autodrafted."""
    token = context.attach(trace_context) if trace_context is not None else None
    try:
        with tracer.start_as_current_span("draft_pick.hold_and_publish", attributes={"team_id": team_id, "pick_number": pick_number}):
            origin = monotonic_fn() if clock_origin is None else clock_origin
            deadline = origin + DRAFT_PICK_CLOCK_SECONDS - DRAFT_PUBLISH_SAFETY_SECONDS
            # Identifies this publisher thread on the board, so a retry that finally succeeds
            # retracts only its own error. Two overlapping threads can record the identical
            # message, which makes the text alone useless as an ownership check.
            error_owner = f"publish:{league_id}:{pick_number}:{uuid.uuid4()}"
            try:
                should_publish = wait_for_pause_or_timeout(
                    r_client, league_id, pick_number, delay_seconds,
                    cancel_event=cancel_event, sleep_fn=sleep_fn,
                )
                if not should_publish:
                    return

                failure_surfaced = False
                # Identifies this pick, not this attempt, so every retry claims the same key
                # at the gateway and an ambiguous failure cannot enqueue the action twice.
                idempotency_key = f"{league_id}:{pick_number}:{target.get('espn_id')}"
                while True:
                    # Both guards are re-evaluated before *every* attempt, not just the
                    # first. The pause key and cancel_event are set by other threads and a
                    # retry sleep is a wide window for either to land; publishing a target
                    # the operator just paused (or that a newer pick superseded) is exactly
                    # the override this whole mechanism exists to honour.
                    # Read first, deliberately. A cancellation or pause landing during the
                    # final retry sleep would otherwise be reported as a missed pick - an
                    # error in Redis and execution_log - when the retry was stopped on
                    # purpose because the slot was filled or the operator intervened.
                    if _is_paused_or_cancelled(r_client, league_id, pick_number, cancel_event):
                        return
                    if monotonic_fn() >= deadline:
                        logger.warning(
                            "Pick clock expired before the draft target for league %s pick %s could be published; "
                            "abandoning rather than sending a target for a pick that has already been made.",
                            league_id, pick_number,
                        )
                        _surface_clock_expired(
                            r_client, league_id, target, pick_number, "deadline reached",
                            owner=error_owner, season=season,
                        )
                        return
                    # Clamp the attempt to the window that is actually left. Starting one is
                    # not enough - a request begun just inside the deadline with the full
                    # timeout would enqueue well outside it, publishing a target for a pick
                    # ESPN had already made.
                    remaining = deadline - monotonic_fn()
                    if remaining < MIN_PUBLISH_WINDOW_SECONDS:
                        logger.warning(
                            "Too little of the pick clock left for league %s pick %s to complete a publish (%.1fs); abandoning.",
                            league_id, pick_number, remaining,
                        )
                        _surface_clock_expired(
                            r_client, league_id, target, pick_number,
                            f"only {remaining:.1f}s left, below the {MIN_PUBLISH_WINDOW_SECONDS}s minimum request window",
                            owner=error_owner, season=season,
                        )
                        return
                    try:
                        publish_draft_target(
                            team_id, target, idempotency_key=idempotency_key,
                            timeout=min(PUBLISH_TIMEOUT_SECONDS, remaining),
                            expires_at=time_fn() + remaining,
                            league_id=league_id, pick_number=pick_number,
                        )
                        if failure_surfaced:
                            _clear_publish_error(r_client, league_id, error_owner)
                        return
                    except Exception as error:
                        retryable = _is_retryable_publish_error(error)
                        # Surface the first failure, and again for whatever finally stops us.
                        # Reporting only the first meant that a connection reset followed by a
                        # 422 left /v1/draft/status and execution_log describing the reset -
                        # a transient blip - while hiding the validation error that actually
                        # ended the attempt and is the one a human needs to act on.
                        if not failure_surfaced or not retryable:
                            _surface_publish_error(r_client, league_id, target, error, owner=error_owner, season=season)
                            failure_surfaced = True
                        if not retryable:
                            logger.error(
                                "Draft target publish for league %s pick %s failed permanently; not retrying: %s",
                                league_id, pick_number, _publish_error_detail(error),
                            )
                            return
                        if monotonic_fn() + PUBLISH_RETRY_INTERVAL_SECONDS >= deadline:
                            # Also terminal, and for a reason the first error does not convey.
                            _surface_publish_error(r_client, league_id, target, error, owner=error_owner, season=season)
                            return
                        logger.warning(
                            "Draft target publish failed for league %s pick %s; retrying in %ss: %s",
                            league_id, pick_number, PUBLISH_RETRY_INTERVAL_SECONDS,
                            _publish_error_detail(error),
                        )
                        sleep_fn(PUBLISH_RETRY_INTERVAL_SECONDS)
            except Exception as error:
                _surface_publish_error(r_client, league_id, target, error, owner=error_owner, season=season)
    finally:
        if token is not None:
            context.detach(token)


# ------------------------------------------------------------------------------
# Orchestration
# ------------------------------------------------------------------------------
def compute_draft_recommendation_candidates(cursor, league_id: str, season: int, week: int, team_id: str, limit: int = TOP_CANDIDATES_LIMIT, pool_out: Optional[list] = None) -> list:
    """One full recompute pass: undrafted pool -> scarcity VORP -> ILP -> up to `limit`
    ranked recommended targets. Returns [] if the pool is empty or no feasible selection
    exists.

    pool_out, if given, is extended in place with the full scored undrafted pool (every
    candidate, not just the ILP's top picks) - an output param rather than a second return
    value so every existing caller/mock (draft_simulator.py, this module's own test suite,
    which mocks this function with plain candidate lists) keeps working unchanged. Only
    handle_pick_event passes it, to publish a VORP lookup covering any undrafted player, not
    just the handful the ILP recommends for our own roster."""
    league_config = load_league_config(cursor)
    pool_df = load_undrafted_pool(cursor, league_id, season, week)
    pool_df = recompute_scarcity_vorp(pool_df, league_config)
    if pool_df.is_empty():
        return []

    drafted_by_us = load_team_position_counts(cursor, league_id, season, team_id)
    position_caps, starting_needs = compute_position_caps(league_config, drafted_by_us)
    starting_total, bench_total = compute_draftable_roster_size(league_config)
    remaining_picks = max(0, starting_total + bench_total - sum(drafted_by_us.values()))

    undrafted_pool = [
        {"player_id": r["player_id"], "espn_id": r["espn_id"], "name": r["name"], "position": r["position"], "vorp_score": r["vorp_score"], "team": r["team"]}
        for r in pool_df.iter_rows(named=True)
    ]
    if pool_out is not None:
        pool_out.extend(undrafted_pool)
    selected = build_and_solve_draft_pick(undrafted_pool, position_caps, remaining_picks)
    return rank_recommended_targets(selected, starting_needs, limit)


def compute_draft_recommendation(cursor, league_id: str, season: int, week: int, team_id: str) -> Optional[dict]:
    """Single-target convenience wrapper around compute_draft_recommendation_candidates,
    used by callers (e.g. /v1/draft/resume) that only need the one winning pick."""
    candidates = compute_draft_recommendation_candidates(cursor, league_id, season, week, team_id, limit=1)
    return candidates[0] if candidates else None


def _vorp_by_espn_id_json(undrafted_pool: list) -> str:
    """CORE-19's ESPN-suggested-pick column needs a VORP score for whichever player ESPN
    suggests, which is frequently NOT one of the roster-constrained top_candidates - so this
    rides the board hash as its own JSON-encoded lookup covering the full scored pool, keyed
    by espn_id (not the internal player_id) because that's the only id the ESPN suggestion
    itself carries."""
    return json.dumps({r["espn_id"]: r["vorp_score"] for r in undrafted_pool if r["espn_id"]})


def seed_initial_vorp_lookup(r_client, league_id: str, season: int, week: int) -> None:
    """Populates draft:{league_id}:board's vorp_by_espn_id before any pick has landed
    (reviewer-caught, PR #97): handle_pick_event only (re)publishes this lookup when an
    OPPONENT pick triggers a recompute - our own picks return early with no board write - so
    if our team is on the clock first, any ESPN suggestion surfaced before that first
    opponent pick would resolve against a lookup that doesn't exist yet and report a null
    VORP score despite the player being in a fully scored pool.

    Deliberately computes only the VORP lookup, not top_candidates/a recommendation: there is
    no pick to react to yet, and the very first handle_pick_event call already (re)computes
    those correctly. Best-effort - a failure here must not block the listener from starting,
    since the per-pick recompute still catches up as soon as the first opponent pick lands."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                league_config = load_league_config(cursor)
                pool_df = load_undrafted_pool(cursor, league_id, season, week)
                pool_df = recompute_scarcity_vorp(pool_df, league_config)
        undrafted_pool = [{"espn_id": r["espn_id"], "vorp_score": r["vorp_score"]} for r in pool_df.iter_rows(named=True)]
        r_client.hset(_listener_board_key(league_id), mapping={"vorp_by_espn_id": _vorp_by_espn_id_json(undrafted_pool)})
    except Exception as e:
        logger.warning(f"Could not seed initial vorp_by_espn_id for league {league_id}; it will populate after the first opponent pick: {e}")


def handle_pick_event(r_client, league_id: str, season: int, week: int, our_team_id: str, event: dict) -> Optional[tuple]:
    """Runs on every pick event off the Stream: persist it, then - unless it's a replayed
    duplicate or we're the team that just picked - recompute a recommendation and write it
    to the Redis board. Returns (target, next_pick_number) for the caller to hold-and-
    publish, or None if there's nothing to act on. The delay/publish is deliberately NOT
    done here - see hold_and_publish - so this function stays fast and the Stream consumer
    loop is never blocked by the auto-draft delay window.

    KNOWN LIMITATION: this always recomputes after any *other* team's pick, not
    specifically "the pick right before our turn". Draft slots are modeled (see
    persist_draft_slot) and the API uses them for turn order, but this handler does not
    consult them before scheduling, so a hold-and-publish job gets scheduled once per
    opponent pick rather than only once, right before we're actually on the clock. In
    practice this is harmless: the recommendation stays continuously fresh, and each new
    job supersedes the last via _schedule_publish's cancel_event, so only the freshest ever
    actually publishes. Gating on the persisted slot map is a follow-up, not solved here."""
    with tracer.start_as_current_span("draft_pick.handle_event", attributes={"league_id": league_id, "team_id": our_team_id}):
        with get_db_connection() as conn:
            pick_number = int(event["pick_number"])
            # The event's own "round" is NOT trustworthy: the WebSocket ingest tier parses
            # SELECTED <teamId> <playerId> <slotId>, where the third token is ESPN's lineup
            # slot id, not a round (proved against a 160-pick practice draft: value 1 was
            # QB x10, value 6 TE x10, value 7 the FLEX). Its distribution is indistinguishable
            # from a round's - a 10-team/16-slot league yields ten of each either way - so it
            # has to be derived rather than read. Snake order makes pick_number the only
            # thing needed, given team count.
            team_count = league_team_count(conn, league_id, season)
            round_ = (pick_number - 1) // team_count + 1 if team_count else None
            if team_count:
                # A team count that only became known partway through leaves earlier picks with
                # round NULL and no draft slot, and ON CONFLICT DO NOTHING blocks any replay
                # from fixing them. Repair them the moment it becomes readable.
                backfill_missing_rounds(conn, league_id, season, team_count)
            # The stream carries ESPN's player id; draft_picks, VORP and the pick feed's
            # player join all key off the internal players.player_id. Both are recorded:
            # dropping an unresolvable pick would leave a gap in pick_number, which breaks the
            # gapless run team-count derivation requires and mis-sizes every round after it.
            espn_player_id = str(event["player_id"])
            resolved_player_id = resolve_player_id(conn, espn_player_id)
            if resolved_player_id is None:
                logger.warning(
                    f"No players row for ESPN player id {espn_player_id} (pick {pick_number}); recording the pick "
                    "with player_id NULL so draft order stays intact. It shows in the feed without a name until "
                    "the crosswalk covers this player - always the case for K and D/ST."
                )
            inserted = persist_draft_pick(
                conn, league_id, season,
                pick_number=pick_number,
                round_=round_,
                team_id=event["team_id"],
                player_id=resolved_player_id,
                espn_player_id=espn_player_id,
            )
            if not inserted:
                logger.info(f"Pick {event['pick_number']} already recorded; skipping (replayed Stream entry).")
                return None
            if team_count is None:
                # Practice/mock rooms are never synced by sync_league.py, so league_settings
                # has nothing for them and this is the only way they ever get a team count.
                # Derived *after* the insert, because the pick that reveals the count is the
                # snake turnaround - which may well be this one. Returns None for anything
                # that isn't a practice room; an unsynced live league has to stay unknown.
                team_count = derive_team_count(conn, league_id, season, r_client=r_client)
                if team_count:
                    logger.info(f"Derived team_count={team_count} for league {league_id} from round-1 pick arrivals.")
                    persist_derived_team_count(conn, league_id, season, team_count)
                    # Repairs every round left NULL up to now, this pick included, and claims
                    # the round-1 draft slots that could not be recorded without a count.
                    backfill_missing_rounds(conn, league_id, season, team_count)
                    round_ = (pick_number - 1) // team_count + 1
                else:
                    logger.warning(
                        f"League {league_id} has no known team_count yet; recording pick {pick_number} "
                        "with round unknown. Rounds and draft slots are backfilled automatically as "
                        "soon as a count becomes available - for a practice room that happens on its "
                        "own at the snake turnaround, but a live league needs sync_league.py to "
                        "populate league_settings, since pick trading makes derivation unsafe there."
                    )
            if round_ == 1:
                # Round-1 arrival order *is* each team's snake-draft slot (see
                # compute_on_the_clock_team) - the only reliable source, since ESPN's API
                # can't supply draft-slot assignment pre-draft. Gated on a derived round,
                # never the event's: trusting the event mapped slots off the ten picks that
                # happened to carry lineup slot 1 (each team's QB, scattered across all 16
                # rounds), which produced a plausible-looking but wrong slot map.
                persist_draft_slot(conn, league_id, season, event["team_id"], pick_number)
            now = int(time.time())
            r_client.hset(
                _listener_board_key(league_id),
                mapping={
                    "listener_state": "Active",
                    "listener_error": "",
                    "listener_last_event_at": now,
                    "last_processed_pick_at": now,
                    "last_processed_pick_number": pick_number,
                },
            )
            if not r_client.hexists(_listener_board_key(league_id), "listener_first_event_at"):
                r_client.hset(_listener_board_key(league_id), mapping={"listener_first_event_at": now})
                logger.info(
                    "listener_first_pick_observed league_id=%s team_id=%s pick_number=%s",
                    league_id, our_team_id, pick_number,
                )
            if team_count:
                # #82: the draft is over once pick_number reaches every team's full
                # roster - checked regardless of whose pick this was (including our own
                # last pick), since a finished draft has no recommendation left to make
                # either way. draft_state rides the same board hash the API already
                # reads (_build_status), so both /status and /stream pick it up with no
                # separate plumbing.
                with conn.cursor() as cursor:
                    league_config = load_league_config(cursor)
                total_picks = compute_total_picks(team_count, league_config)
                if pick_number >= total_picks:
                    logger.info(
                        "draft_complete league_id=%s team_id=%s pick_number=%s total_picks=%s",
                        league_id, our_team_id, pick_number, total_picks,
                    )
                    r_client.hset(_listener_board_key(league_id), mapping={
                        "draft_state": "complete",
                        # Otherwise the earlier "Active" this same call already wrote above
                        # (and _listener_status's job_state == "Failed" check aside, trusts
                        # verbatim) survives on the board even after the Job exits and
                        # becomes Succeeded, reproducing #82's contradictory-status problem
                        # in listener_state instead of draft_state.
                        "listener_state": "Complete",
                        "recommended_player_id": "",
                        "recommended_name": "",
                        "top_candidates": "[]",
                    })
                    return None
            if event["team_id"] == our_team_id:
                logger.info("We just picked; no recommendation to compute until the next opponent pick.")
                return None
            undrafted_pool: list = []
            with conn.cursor() as cursor:
                candidates = compute_draft_recommendation_candidates(cursor, league_id, season, week, our_team_id, pool_out=undrafted_pool)

        if not candidates:
            logger.info("No feasible draft recommendation for the current undrafted pool.")
            return None

        target = candidates[0]
        next_pick_number = int(event["pick_number"]) + 1
        now = int(time.time())
        r_client.hset(_listener_board_key(league_id), mapping={
            "recommended_player_id": target["player_id"],
            "recommended_name": target["name"],
            "pick_number": next_pick_number,
            "listener_state": "Active",
            "listener_error": "",
            "listener_last_event_at": now,
            "last_processed_pick_at": now,
            "last_processed_pick_number": pick_number,
            # CORE-14 web UI's comparison panel wants the full ranked list, not just the
            # winner - redis hashes are flat string->string, so the ranked candidates ride
            # as a JSON-encoded field rather than their own set of hash keys.
            "top_candidates": json.dumps(candidates),
            "vorp_by_espn_id": _vorp_by_espn_id_json(undrafted_pool),
        })
        return target, next_pick_number


def _cancel_pending_publish(pending_state: dict) -> None:
    cancel_event = pending_state.get("cancel_event")
    if cancel_event is not None:
        cancel_event.set()
    pending_state.pop("cancel_event", None)
    pending_state.pop("pick_number", None)


def _max_auto_publish_delay_seconds() -> int:
    return max(0, DRAFT_PICK_CLOCK_SECONDS - DRAFT_PUBLISH_SAFETY_SECONDS)


def _clamp_auto_draft_delay_seconds(delay_seconds: int) -> int:
    max_delay = _max_auto_publish_delay_seconds()
    if delay_seconds > max_delay:
        logger.warning(
            "Configured auto-draft delay %ss exceeds pick-clock window %ss; clamping to %ss.",
            delay_seconds, max_delay, max_delay,
        )
        return max_delay
    return delay_seconds


def _schedule_publish(
    r_client, league_id: str, team_id: str, pick_number: int, target: dict, delay_seconds: int,
    pending_state: dict, clock_origin: Optional[float] = None, season: Optional[int] = None,
    monotonic_fn=time.monotonic, time_fn=time.time,
) -> None:
    """Supersedes any still-pending hold-and-publish job (a newer pick means the old
    recommendation is stale) and schedules the new one on a background thread, so
    run_draft_listener's XREADGROUP loop keeps consuming instead of blocking through the
    delay window."""
    _cancel_pending_publish(pending_state)
    cancel_event = threading.Event()
    pending_state["cancel_event"] = cancel_event
    # The slot this pending target is for. _process_entries uses it to supersede the thread
    # the moment that slot is taken, without waiting for another recommendation.
    pending_state["pick_number"] = pick_number
    clamped_delay_seconds = _clamp_auto_draft_delay_seconds(delay_seconds)
    resolved_clock_origin = monotonic_fn() if clock_origin is None else clock_origin
    now_monotonic = monotonic_fn()
    now_epoch = time_fn()
    auto_publish_at = int(now_epoch + ((resolved_clock_origin + clamped_delay_seconds) - now_monotonic))
    pick_clock_expires_at = int(now_epoch + ((resolved_clock_origin + DRAFT_PICK_CLOCK_SECONDS) - now_monotonic))
    r_client.hset(
        _listener_board_key(league_id),
        mapping={
            "auto_publish_at": auto_publish_at,
            "pick_clock_expires_at": pick_clock_expires_at,
        },
    )

    # Capture the calling thread's OTel context (this span, e.g. handle_pick_event's) so
    # hold_and_publish can re-attach it on the new OS thread - threading.Thread does not
    # inherit context automatically the way asyncio tasks do.
    trace_context = context.get_current()
    threading.Thread(
        target=hold_and_publish,
        args=(r_client, league_id, team_id, pick_number, target, clamped_delay_seconds, cancel_event, time.sleep, trace_context),
        kwargs={"clock_origin": resolved_clock_origin, "season": season},
        daemon=True,
    ).start()


def decode_pick_event(fields: dict) -> dict:
    """fantasy-browser-worker publishes each pick as a single JSON-encoded Stream field
    (xAdd(key, '*', {payload: JSON.stringify(event)})) so the event keeps real types and
    nesting, which flat Redis Stream fields cannot carry. Reading fields directly yields
    only {'payload': '...'}, so every access like event['pick_number'] raised KeyError and
    the listener acked away the whole draft while reporting itself Failed.

    Entries without a payload wrapper are passed through unchanged: the DOM/REST ingest
    tiers may write flat fields, and unwrapping must not become a second contract to break."""
    payload = fields.get("payload")
    if payload is None:
        return fields
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode()
    return json.loads(payload)


def _stream_entry_clock_origin(entry_id, monotonic_fn=time.monotonic, time_fn=time.time) -> float:
    """Converts a Redis Stream id ("<unixMillis>-<seq>") into a monotonic origin.

    The pick clock starts when the upstream pick landed, which for a backlog entry may be
    minutes before this process even read it. Stamping monotonic() at read time gave every
    drained entry a fresh clock, so a restart draining up to 100 old entries could publish
    a target for a pick ESPN autodrafted long ago. Falls back to "now" when the id is not
    parseable (the DOM/REST tiers may write ids we did not mint) - the old behaviour, which
    is wrong only in the direction of being too generous, and never blocks a live pick.
    """
    now_monotonic = monotonic_fn()
    try:
        entry_ms = int(str(entry_id).split("-")[0])
    except (ValueError, AttributeError, IndexError):
        return now_monotonic
    age_seconds = time_fn() - (entry_ms / 1000.0)
    if age_seconds <= 0:
        return now_monotonic
    return now_monotonic - age_seconds


def _supersede_pending_publish_if_stale(pending_state: dict, event: dict) -> None:
    """Cancels the pending publisher once the slot it targets has been filled. Older or
    replayed events leave it alone: a redelivered pick must not cancel a live publish."""
    pending_pick = pending_state.get("pick_number")
    if pending_pick is None:
        return
    try:
        event_pick = int(event["pick_number"])
    except (KeyError, TypeError, ValueError):
        return
    if event_pick >= pending_pick:
        logger.info(
            "Pick %s arrived for a slot with a publish still pending (%s); superseding it.",
            event_pick, pending_pick,
        )
        _cancel_pending_publish(pending_state)


def _process_entries(r_client, stream_key: str, league_id: str, season: int, week: int, team_id: str, delay_seconds: int, pending_state: dict, entries: list) -> None:
    with tracer.start_as_current_span("draft_listener.process_batch", attributes={"league_id": league_id}):
        for _stream, messages in entries:
            for entry_id, fields in messages:
                # Anchored to when the pick was written to the Stream, not to when we got
                # round to it. Persisting the pick and solving the ILP happen synchronously
                # below, and a restart may drain a backlog of entries that are already
                # minutes old - both would otherwise silently extend the real clock.
                clock_origin = _stream_entry_clock_origin(entry_id)
                try:
                    event = decode_pick_event(fields)
                    # Superseding cannot wait for _schedule_publish. handle_pick_event returns
                    # None whenever the new pick produces no recommendation - our own pick is
                    # the common case - so the pending thread would survive, recover from a
                    # transient failure and enqueue a target for a slot already taken, which
                    # lands the click on the *next* team's pick.
                    _supersede_pending_publish_if_stale(pending_state, event)
                    result = handle_pick_event(r_client, league_id, season, week, team_id, event)
                    if result is not None:
                        target, next_pick_number = result
                        _schedule_publish(r_client, league_id, team_id, next_pick_number, target, delay_seconds, pending_state, clock_origin=clock_origin, season=season)
                except Exception as e:
                    logger.error(f"Failed to process draft pick event {entry_id}: {e}")
                    _write_listener_state(r_client, league_id, "Failed", error=str(e))
                finally:
                    r_client.xack(stream_key, CONSUMER_GROUP, entry_id)


def run_draft_listener(league_id: str, team_id: str, season: int, week: int, delay_seconds: Optional[int] = None) -> None:
    """Long-running entrypoint: consumes fantasy-browser-worker's
    draft_picks:{leagueId} Redis Stream via a consumer group so a restart resumes from the
    last acked pick instead of silently missing one."""
    if delay_seconds is None:
        delay_seconds = int(os.getenv("AUTO_DRAFT_DELAY_SECONDS", str(DEFAULT_AUTO_DRAFT_DELAY_SECONDS)))
    logger.info("listener_start_attempt league_id=%s team_id=%s season=%s week=%s", league_id, team_id, season, week)
    r_client = None
    stream_key = None
    consumer_name = None
    pending_state = {}
    with tracer.start_as_current_span("draft_listener.startup", attributes={"league_id": league_id, "team_id": team_id}):
        try:
            r_client = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                socket_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
            )
            r_client.ping()

            stream_key = f"draft_picks:{league_id}"
            consumer_name = f"draft-matrix-{team_id}"
            try:
                r_client.xgroup_create(stream_key, CONSUMER_GROUP, id="0", mkstream=True)
            except redis.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

            _write_listener_state(r_client, league_id, "Starting")
            logger.info("listener_start_success league_id=%s team_id=%s consumer=%s", league_id, team_id, consumer_name)
            # Before consuming any picks - see seed_initial_vorp_lookup's header for why an
            # opponent-pick-triggered recompute alone leaves a gap when our team is on the
            # clock first.
            seed_initial_vorp_lookup(r_client, league_id, season, week)

            # Drain this consumer's own unacked backlog first (crash/restart recovery): reading
            # only ">" returns brand-new entries and would treat anything delivered-but-never-
            # acked before a crash as gone forever, breaking the durability the consumer group
            # exists for. (Scope note: this recovers this exact consumer name's own backlog, not
            # another dead consumer's via XAUTOCLAIM - out of scope for a single-team listener.)
            while True:
                backlog = r_client.xreadgroup(CONSUMER_GROUP, consumer_name, {stream_key: "0"}, count=100)
                if not backlog or not backlog[0][1]:
                    break
                _process_entries(r_client, stream_key, league_id, season, week, team_id, delay_seconds, pending_state, backlog)
        except Exception as e:
            logger.error("listener_start_failure league_id=%s team_id=%s error=%s", league_id, team_id, e)
            if r_client is not None:
                try:
                    _write_listener_state(r_client, league_id, "Failed", error=str(e))
                except Exception:
                    pass
            raise

    logger.info(f"Listening on '{stream_key}' as consumer '{consumer_name}' for league {league_id}...")
    while True:
        # #82: checked at the top of every iteration - unconditionally, before blocking on
        # the next read - not just after processing a non-empty batch. draft_state can
        # already be "complete" the moment this loop is first reached (the startup backlog
        # drain above processes entries too, and a restart can land after completion was
        # already recorded), and once the draft is over no new entries ever arrive, so an
        # empty poll's `continue` would otherwise skip this check forever. A plain return,
        # not a crash - the generated Job sets backoff_limit=0/restart_policy="Never" (see
        # XREADGROUP_BLOCK_MS's comment above), so this is the only way the Job ever reaches
        # a real terminal state (Active -> Succeeded) instead of running forever against an
        # ESPN socket that has nothing left to send. Both existing "already active" guards
        # in src/api/draft.py (_active_listeners_on_other_leagues, start_draft_sync's
        # same-league check) key strictly off job_state == "Active", so Succeeded already
        # unblocks the next practice run with no separate reaping step needed here.
        if r_client.hget(_listener_board_key(league_id), "draft_state") == "complete":
            logger.info("listener_stopping_draft_complete league_id=%s team_id=%s", league_id, team_id)
            return
        try:
            entries = r_client.xreadgroup(CONSUMER_GROUP, consumer_name, {stream_key: ">"}, count=1, block=XREADGROUP_BLOCK_MS)
        except (redis.exceptions.TimeoutError, redis.exceptions.ConnectionError) as e:
            logger.warning(
                "Transient Redis error in listener main loop (league_id=%s); retrying in %ss: %s",
                league_id, TRANSIENT_REDIS_RETRY_SECONDS, e,
            )
            time.sleep(TRANSIENT_REDIS_RETRY_SECONDS)
            continue
        if not entries:
            continue
        _process_entries(r_client, stream_key, league_id, season, week, team_id, delay_seconds, pending_state, entries)


if __name__ == "__main__":
    # This process runs as container PID 1 (the k8s Job's only container, no tini/--init
    # wrapper), which does not get the default terminate-on-SIGTERM disposition unless a
    # handler installs one - so fantasy-core-pipeline's own teardown (_teardown_practice_run
    # and the /v1/draft/stop path) was sending a graceful SIGTERM this process silently
    # ignored, only dying once kubelet escalated to SIGKILL at the end of the grace period
    # (same root cause as fantasy-browser-worker's draft_sync.js, #103). Safe to exit
    # immediately rather than draining anything: the main loop below reads via a Redis
    # consumer group (XREADGROUP) specifically so a killed-mid-entry pick is redelivered and
    # reprocessed from the backlog on the next run, per this module's own docstring.
    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(0))

    configure_tracing("draft-matrix")

    parser = argparse.ArgumentParser(description="Run the live draft-day board matrix listener.")
    parser.add_argument("--league-id", type=str, required=True)
    parser.add_argument("--team-id", type=str, required=True, help="Our own team_id in the league")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int, default=1, help="weekly_stats week key holding preseason projections")
    parser.add_argument("--auto-draft-delay-seconds", type=int, default=None)
    args = parser.parse_args()
    run_draft_listener(
        league_id=args.league_id,
        team_id=args.team_id,
        season=args.season,
        week=args.week,
        delay_seconds=args.auto_draft_delay_seconds,
    )
