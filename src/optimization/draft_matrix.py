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
import threading
import time
from typing import Optional

import polars as pl
import pulp
import redis
import requests
from opentelemetry import context, propagate, trace

from src.database import get_db_connection
from src.telemetry import configure_tracing, configure_trace_logging
from src.optimization.vorp_matrix import (
    SKILL_POSITIONS,
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
DEFAULT_AUTO_DRAFT_DELAY_SECONDS = 8
PAUSE_POLL_INTERVAL_SECONDS = 1
# CORE-14 web UI wants to show the system's top-N alternatives, not just the one it would
# auto-draft - see rank_recommended_targets/compute_draft_recommendation_candidates.
TOP_CANDIDATES_LIMIT = 5


def _listener_board_key(league_id: str) -> str:
    return f"draft:{league_id}:board"


def _write_listener_state(r_client, league_id: str, state: str, error: Optional[str] = None) -> None:
    mapping = {"listener_state": state}
    if error:
        mapping["listener_error"] = error
    else:
        mapping["listener_error"] = ""
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
        SELECT p.player_id, p.name, p.position, s.projected_points, s.team_implied_total,
               s.avg_separation, s.pressure_rate_allowed, s.depth_chart_delta, s.team
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
        "player_id": pl.Utf8, "name": pl.Utf8, "position": pl.Utf8, "projected_points": pl.Float64,
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
    starting_total = sum(lineup_slot_counts.get(pos, 0) for pos in SKILL_POSITIONS) + sum(flex_alloc.values())
    bench_total = lineup_slot_counts.get("BE")
    if bench_total is None:
        return starting_total, max(0, DEFAULT_ROSTER_SIZE - starting_total)
    return starting_total, max(0, bench_total)


def compute_position_caps(league_config: dict, drafted_by_us: dict) -> tuple:
    """Derives per-position draft caps from league_settings (not the issue's hardcoded
    QB>=1/RB>=3/WR>=3/TE>=1 thresholds) - starting slots + this league's FLEX share +
    a bench allocation (bench slots distributed by the same FLEX_DEFAULT_WEIGHTS used for
    FLEX itself), minus what we've already drafted at that position. Also returns
    starting_needs (slots + flex only, no bench) so the target-selection step can tell a
    "still need a starter here" position apart from "just filling bench depth"."""
    flex_alloc = resolve_own_team_flex_allocation(league_config)
    lineup_slot_counts = league_config["lineup_slot_counts"]
    starting_slots = {pos: lineup_slot_counts.get(pos, 0) for pos in SKILL_POSITIONS}
    _, bench_total = compute_draftable_roster_size(league_config)
    bench_alloc = _largest_remainder_allocate(dict(FLEX_DEFAULT_WEIGHTS), bench_total)

    starting_needs = {}
    position_caps = {}
    for pos in SKILL_POSITIONS:
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
    # exactly as many picks as are actually fillable (bounded by both the position caps
    # and the pool size), so the solver can't dodge a mandatory pick by declining value.
    target_picks = min(remaining_picks, len(undrafted_pool), sum(position_caps.get(pos, 0) for pos in SKILL_POSITIONS))
    if target_picks <= 0:
        return []

    prob = pulp.LpProblem("Draft_Pool_Optimization", pulp.LpMaximize)
    player_vars = {p["player_id"]: pulp.LpVariable(f"take_{p['player_id']}", cat="Binary") for p in undrafted_pool}
    by_id = {p["player_id"]: p for p in undrafted_pool}

    prob += pulp.lpSum(player_vars[pid] * by_id[pid]["vorp_score"] for pid in player_vars), "Total_VORP"
    prob += pulp.lpSum(player_vars.values()) == target_picks, "Remaining_Picks"
    for pos in SKILL_POSITIONS:
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


def derive_team_count(conn, league_id: str, season: int) -> Optional[int]:
    """Infers the team count from pick arrivals. A snake draft turns around at the round-1/2
    boundary: the last team of round 1 picks again immediately, so picks team_count and
    team_count + 1 belong to the same team. That back-to-back pair is the signature this
    looks for, and it is the only thing it will accept.

    Returns None until the pair is observed, and refuses outright in three cases:

    - the picks seen so far are not a gapless run starting at 1. If an early pick were
      dropped, the boundary would land early and yield a count one too small.
    - a team repeats without being the immediately preceding picker. Round 1 is every team
      exactly once, so that can only mean traded picks or malformed ownership data - treating
      it as the boundary would derive a count far below the real one.
    - the pair appears at pick 2, implying a one-team league. Real back-to-back picks that
      early are a traded pick, not a turnaround.

    Every rejection is deliberate: an unknown count leaves rounds NULL and visibly repairable,
    while a wrong one quietly mis-sizes every round and draft slot for the whole draft with
    nothing to indicate it."""
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
def persist_draft_pick(conn, league_id: str, season: int, pick_number: int, round_: Optional[int], team_id: str, player_id: str) -> bool:
    """ON CONFLICT DO NOTHING keyed on (league_id, season, pick_number) - a Stream
    consumer group redelivers an entry if it crashes before XACK, and reprocessing an
    already-recorded pick must be a no-op, not a duplicate. RETURNING tells the caller
    whether this call actually inserted a new row: DO NOTHING alone only suppresses the
    insert, it doesn't stop handle_pick_event from recomputing and auto-publishing a
    second RabbitMQ draft command for a pick that already happened."""
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO draft_picks (league_id, season, pick_number, round, team_id, player_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (league_id, season, pick_number) DO NOTHING
            RETURNING pick_number;
        """, (league_id, season, pick_number, round_, team_id, player_id))
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


def publish_draft_target(team_id: str, target: dict) -> None:
    headers: dict = {}
    propagate.inject(headers)

    response = requests.post(
        f"{CORE_API_BASE_URL}/v1/trigger/draft-pick",
        json={
            "team_id": team_id,
            "player_id": target["player_id"],
            "name": target["name"],
            "position": target["position"],
            "pro_team": target["team"],
        },
        headers=headers,
        timeout=10,
    )
    response.raise_for_status()
    logger.info(f"Published draft target for Team {team_id}: {target['name']} ({target['position']}).")


def hold_and_publish(
    r_client, league_id: str, team_id: str, pick_number: int, target: dict, delay_seconds: int,
    cancel_event: Optional[threading.Event] = None, sleep_fn=time.sleep,
    trace_context: Optional[context.Context] = None,
) -> None:
    """Runs the cancellable delay window and publishes if it elapses uninterrupted. Called
    from a background thread (see _schedule_publish) so the Stream consumer loop keeps
    reading new picks instead of blocking through the delay - a positional run during the
    wait would otherwise queue up unprocessed while draft_matrix sits idle.

    Runs on a separate OS thread, which (unlike an asyncio task) does not automatically
    inherit the calling thread's OTel context - trace_context is the parent span's context
    captured by _schedule_publish before spawning this thread, and must be explicitly
    re-attached here or the eventual publish span silently starts a disconnected trace."""
    token = context.attach(trace_context) if trace_context is not None else None
    try:
        with tracer.start_as_current_span("draft_pick.hold_and_publish", attributes={"team_id": team_id, "pick_number": pick_number}):
            if wait_for_pause_or_timeout(r_client, league_id, pick_number, delay_seconds, cancel_event=cancel_event, sleep_fn=sleep_fn):
                publish_draft_target(team_id, target)
    finally:
        if token is not None:
            context.detach(token)


# ------------------------------------------------------------------------------
# Orchestration
# ------------------------------------------------------------------------------
def compute_draft_recommendation_candidates(cursor, league_id: str, season: int, week: int, team_id: str, limit: int = TOP_CANDIDATES_LIMIT) -> list:
    """One full recompute pass: undrafted pool -> scarcity VORP -> ILP -> up to `limit`
    ranked recommended targets. Returns [] if the pool is empty or no feasible selection
    exists."""
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
        {"player_id": r["player_id"], "name": r["name"], "position": r["position"], "vorp_score": r["vorp_score"], "team": r["team"]}
        for r in pool_df.iter_rows(named=True)
    ]
    selected = build_and_solve_draft_pick(undrafted_pool, position_caps, remaining_picks)
    return rank_recommended_targets(selected, starting_needs, limit)


def compute_draft_recommendation(cursor, league_id: str, season: int, week: int, team_id: str) -> Optional[dict]:
    """Single-target convenience wrapper around compute_draft_recommendation_candidates,
    used by callers (e.g. /v1/draft/resume) that only need the one winning pick."""
    candidates = compute_draft_recommendation_candidates(cursor, league_id, season, week, team_id, limit=1)
    return candidates[0] if candidates else None


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
            inserted = persist_draft_pick(
                conn, league_id, season,
                pick_number=pick_number,
                round_=round_,
                team_id=event["team_id"],
                player_id=event["player_id"],
            )
            if not inserted:
                logger.info(f"Pick {event['pick_number']} already recorded; skipping (replayed Stream entry).")
                return None
            if team_count is None:
                # Practice/mock rooms are never synced by sync_league.py, so league_settings
                # has nothing for them and this is the only way they ever get a team count.
                # Derived *after* the insert, because the pick that reveals the count is the
                # first repeat - which may well be this one.
                team_count = derive_team_count(conn, league_id, season)
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
                        "with round unknown. Rounds and draft slots are backfilled automatically once "
                        "round 1 completes and the count can be derived."
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
            if event["team_id"] == our_team_id:
                logger.info("We just picked; no recommendation to compute until the next opponent pick.")
                return None
            with conn.cursor() as cursor:
                candidates = compute_draft_recommendation_candidates(cursor, league_id, season, week, our_team_id)

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
        })
        return target, next_pick_number


def _schedule_publish(r_client, league_id: str, team_id: str, pick_number: int, target: dict, delay_seconds: int, pending_state: dict) -> None:
    """Supersedes any still-pending hold-and-publish job (a newer pick means the old
    recommendation is stale) and schedules the new one on a background thread, so
    run_draft_listener's XREADGROUP loop keeps consuming instead of blocking through the
    delay window."""
    old_cancel_event = pending_state.get("cancel_event")
    if old_cancel_event is not None:
        old_cancel_event.set()
    cancel_event = threading.Event()
    pending_state["cancel_event"] = cancel_event

    # Capture the calling thread's OTel context (this span, e.g. handle_pick_event's) so
    # hold_and_publish can re-attach it on the new OS thread - threading.Thread does not
    # inherit context automatically the way asyncio tasks do.
    trace_context = context.get_current()
    threading.Thread(
        target=hold_and_publish,
        args=(r_client, league_id, team_id, pick_number, target, delay_seconds, cancel_event, time.sleep, trace_context),
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


def _process_entries(r_client, stream_key: str, league_id: str, season: int, week: int, team_id: str, delay_seconds: int, pending_state: dict, entries: list) -> None:
    with tracer.start_as_current_span("draft_listener.process_batch", attributes={"league_id": league_id}):
        for _stream, messages in entries:
            for entry_id, fields in messages:
                try:
                    result = handle_pick_event(r_client, league_id, season, week, team_id, decode_pick_event(fields))
                    if result is not None:
                        target, next_pick_number = result
                        _schedule_publish(r_client, league_id, team_id, next_pick_number, target, delay_seconds, pending_state)
                except Exception as e:
                    logger.error(f"Failed to process draft pick event {entry_id}: {e}")
                    _write_listener_state(r_client, league_id, "Failed", error=str(e))
                finally:
                    r_client.xack(stream_key, CONSUMER_GROUP, entry_id)


def run_draft_listener(league_id: str, team_id: str, season: int, week: int, delay_seconds: int) -> None:
    """Long-running entrypoint: consumes fantasy-browser-worker's
    draft_picks:{leagueId} Redis Stream via a consumer group so a restart resumes from the
    last acked pick instead of silently missing one."""
    logger.info("listener_start_attempt league_id=%s team_id=%s season=%s week=%s", league_id, team_id, season, week)
    r_client = None
    stream_key = None
    consumer_name = None
    pending_state = {}
    with tracer.start_as_current_span("draft_listener.startup", attributes={"league_id": league_id, "team_id": team_id}):
        try:
            r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
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
        entries = r_client.xreadgroup(CONSUMER_GROUP, consumer_name, {stream_key: ">"}, count=1, block=5000)
        if not entries:
            continue
        _process_entries(r_client, stream_key, league_id, season, week, team_id, delay_seconds, pending_state, entries)


if __name__ == "__main__":
    configure_tracing("draft-matrix")

    parser = argparse.ArgumentParser(description="Run the live draft-day board matrix listener.")
    parser.add_argument("--league-id", type=str, required=True)
    parser.add_argument("--team-id", type=str, required=True, help="Our own team_id in the league")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int, default=1, help="weekly_stats week key holding preseason projections")
    parser.add_argument("--auto-draft-delay-seconds", type=int, default=DEFAULT_AUTO_DRAFT_DELAY_SECONDS)
    args = parser.parse_args()
    run_draft_listener(
        league_id=args.league_id,
        team_id=args.team_id,
        season=args.season,
        week=args.week,
        delay_seconds=args.auto_draft_delay_seconds,
    )
