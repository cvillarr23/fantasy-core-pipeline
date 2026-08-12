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
import logging
import os
import time
from typing import Optional

import polars as pl
import pulp
import redis
import requests

from src.database import get_db_connection
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DraftMatrix")

CORE_API_BASE_URL = os.getenv("CORE_API_BASE_URL", "http://fantasy-api.core.svc.cluster.local:8000")
REDIS_HOST = os.getenv("REDIS_HOST", "fantasy-cache.default.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

CONSUMER_GROUP = "draft_matrix"
DEFAULT_ROSTER_SIZE = 16
DEFAULT_AUTO_DRAFT_DELAY_SECONDS = 8
PAUSE_POLL_INTERVAL_SECONDS = 1


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
    starting_total = sum(starting_slots.values()) + sum(flex_alloc.values())
    roster_size = sum(lineup_slot_counts.values()) or DEFAULT_ROSTER_SIZE
    bench_total = max(0, roster_size - starting_total)
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

    prob = pulp.LpProblem("Draft_Pool_Optimization", pulp.LpMaximize)
    player_vars = {p["player_id"]: pulp.LpVariable(f"take_{p['player_id']}", cat="Binary") for p in undrafted_pool}
    by_id = {p["player_id"]: p for p in undrafted_pool}

    prob += pulp.lpSum(player_vars[pid] * by_id[pid]["vorp_score"] for pid in player_vars), "Total_VORP"
    prob += pulp.lpSum(player_vars.values()) <= remaining_picks, "Remaining_Picks"
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


def select_recommended_target(selected: list, starting_needs: dict) -> Optional[dict]:
    """The ILP's selected set is "the ideal remaining team from here" - the single actual
    pick recommendation is the highest-vorp_score player in that set at a position where
    we still have an open *starting* slot (a positional run making that tier disappear is
    exactly what scarcity_multiplier already captures), falling back to best-player-
    available across the whole selected set once every starting need is filled."""
    if not selected:
        return None
    starter_needed = [p for p in selected if starting_needs.get(p["position"], 0) > 0]
    pool = starter_needed or selected
    return max(pool, key=lambda p: p["vorp_score"])


# ------------------------------------------------------------------------------
# Draft pick persistence
# ------------------------------------------------------------------------------
def persist_draft_pick(conn, league_id: str, season: int, pick_number: int, round_: Optional[int], team_id: str, player_id: str) -> None:
    """ON CONFLICT DO NOTHING keyed on (league_id, season, pick_number) - a Stream
    consumer group redelivers an entry if it crashes before XACK, and reprocessing an
    already-recorded pick must be a no-op, not a duplicate/error."""
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO draft_picks (league_id, season, pick_number, round, team_id, player_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (league_id, season, pick_number) DO NOTHING;
        """, (league_id, season, pick_number, round_, team_id, player_id))


# ------------------------------------------------------------------------------
# Delayed, cancellable publish
# ------------------------------------------------------------------------------
def wait_for_pause_or_timeout(r_client, league_id: str, pick_number: int, delay_seconds: int, sleep_fn=time.sleep) -> bool:
    """Polls a human-settable Redis pause key once a second for up to delay_seconds. This
    is the "full autopilot, but interruptible" window from CORE-08 planning: the future
    draft-day web UI (CORE-10/CORE-11) can SET draft:{league_id}:pause:{pick_number} to
    stop the auto-publish for that pick. Returns True if the delay elapsed with no pause
    set (proceed to publish), False if paused (skip auto-publish for this pick)."""
    elapsed = 0
    while elapsed < delay_seconds:
        if r_client.get(f"draft:{league_id}:pause:{pick_number}"):
            logger.info(f"Auto-draft paused by operator for pick {pick_number}; skipping auto-publish.")
            return False
        sleep_fn(PAUSE_POLL_INTERVAL_SECONDS)
        elapsed += PAUSE_POLL_INTERVAL_SECONDS
    return True


def publish_draft_target(team_id: str, target: dict) -> None:
    response = requests.post(
        f"{CORE_API_BASE_URL}/v1/trigger/draft-pick",
        json={"team_id": team_id, "player_id": target["player_id"], "name": target["name"]},
        timeout=10,
    )
    response.raise_for_status()
    logger.info(f"Published draft target for Team {team_id}: {target['name']} ({target['position']}).")


# ------------------------------------------------------------------------------
# Orchestration
# ------------------------------------------------------------------------------
def compute_draft_recommendation(cursor, league_id: str, season: int, week: int, team_id: str) -> Optional[dict]:
    """One full recompute pass: undrafted pool -> scarcity VORP -> ILP -> recommended
    target. Returns None if the pool is empty or no feasible selection exists."""
    league_config = load_league_config(cursor)
    pool_df = load_undrafted_pool(cursor, league_id, season, week)
    pool_df = recompute_scarcity_vorp(pool_df, league_config)
    if pool_df.is_empty():
        return None

    drafted_by_us = load_team_position_counts(cursor, league_id, season, team_id)
    position_caps, starting_needs = compute_position_caps(league_config, drafted_by_us)
    roster_size = sum(league_config["lineup_slot_counts"].values()) or DEFAULT_ROSTER_SIZE
    remaining_picks = max(0, roster_size - sum(drafted_by_us.values()))

    undrafted_pool = [
        {"player_id": r["player_id"], "name": r["name"], "position": r["position"], "vorp_score": r["vorp_score"]}
        for r in pool_df.iter_rows(named=True)
    ]
    selected = build_and_solve_draft_pick(undrafted_pool, position_caps, remaining_picks)
    return select_recommended_target(selected, starting_needs)


def handle_pick_event(r_client, league_id: str, season: int, week: int, our_team_id: str, delay_seconds: int, event: dict) -> None:
    """Runs on every pick event off the Stream: persist it, then - unless we're the team
    that just picked, in which case there's nothing to act on yet - recompute and
    hold-then-publish a recommendation.

    KNOWN LIMITATION: this always recomputes/holds/publishes after any *other* team's
    pick, not specifically "the pick right before our turn" - the worker's event schema
    (player_id/pick_number/team_id/round) carries no snake-draft turn order, and deriving
    one needs each team's draft-slot assignment, which isn't modeled anywhere yet. In
    practice this means the recommendation stays continuously fresh (harmless), but the
    auto-publish's delay window can fire once per opponent pick rather than only once,
    right before we're actually on the clock. Tracking real turn order is a follow-up,
    not solved here."""
    with get_db_connection() as conn:
        persist_draft_pick(
            conn, league_id, season,
            pick_number=int(event["pick_number"]),
            round_=int(event.get("round", 0)) or None,
            team_id=event["team_id"],
            player_id=event["player_id"],
        )
        if event["team_id"] == our_team_id:
            logger.info("We just picked; no recommendation to compute until the next opponent pick.")
            return
        with conn.cursor() as cursor:
            target = compute_draft_recommendation(cursor, league_id, season, week, our_team_id)

    if target is None:
        logger.info("No feasible draft recommendation for the current undrafted pool.")
        return

    next_pick_number = int(event["pick_number"]) + 1
    r_client.hset(f"draft:{league_id}:board", mapping={
        "recommended_player_id": target["player_id"],
        "recommended_name": target["name"],
        "pick_number": next_pick_number,
    })
    if wait_for_pause_or_timeout(r_client, league_id, next_pick_number, delay_seconds):
        publish_draft_target(our_team_id, target)


def run_draft_listener(league_id: str, team_id: str, season: int, week: int, delay_seconds: int) -> None:
    """Long-running entrypoint: consumes fantasy-browser-worker's
    draft_picks:{leagueId} Redis Stream via a consumer group so a restart resumes from the
    last acked pick instead of silently missing one."""
    r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r_client.ping()

    stream_key = f"draft_picks:{league_id}"
    consumer_name = f"draft-matrix-{team_id}"
    try:
        r_client.xgroup_create(stream_key, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    logger.info(f"Listening on '{stream_key}' as consumer '{consumer_name}' for league {league_id}...")
    while True:
        entries = r_client.xreadgroup(CONSUMER_GROUP, consumer_name, {stream_key: ">"}, count=1, block=5000)
        if not entries:
            continue
        for _stream, messages in entries:
            for entry_id, fields in messages:
                try:
                    handle_pick_event(r_client, league_id, season, week, team_id, delay_seconds, fields)
                except Exception as e:
                    logger.error(f"Failed to process draft pick event {entry_id}: {e}")
                finally:
                    r_client.xack(stream_key, CONSUMER_GROUP, entry_id)


if __name__ == "__main__":
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
