"""
Dynamic VORP (Value Over Replacement Player) matrix evaluator (CORE-05).

Pipeline: matchup coefficient -> FLEX allocation -> replacement baselines -> VORP base ->
scarcity multipliers -> persist. Projections are scaled by the matchup coefficient before
baselines are computed, per the issue's spec, so a replacement-level player's own matchup
already factors into where the replacement line sits.
"""
import argparse
import logging
import os
from collections import defaultdict

import polars as pl

from src.database import get_db_connection
from src.telemetry import configure_tracing, configure_trace_logging
from src.services import run_log_service
from opentelemetry import trace

tracer = trace.get_tracer("vorp_matrix")

configure_trace_logging()
logger = logging.getLogger("VORPMatrix")

W_MARKET = 0.06
W_SIGNAL = 0.03
Z_CLAMP = 2.0
COEFFICIENT_BOUNDS = (0.85, 1.15)
# Off by default (#93): behavior must be byte-identical to pre-#93 vorp_score when unset.
# String-equality against "true", same style as telemetry.py's OTEL_CONSOLE_EXPORT flag -
# no shared boolean-env-parsing helper exists in this repo.
MATCHUP_WEIGHTING_ENABLED = os.getenv("MATCHUP_WEIGHTING_ENABLED", "false") == "true"
W_DEFENSE = 0.05
# Distinct from COEFFICIENT_BOUNDS (0.85, 1.15) above, which bounds matchup_coefficient - an
# unrelated, own-team-signal-based multiplier. This bound is #93's opponent-defense-based
# defense_matchup_multiplier.
DEFENSE_MATCHUP_BOUNDS = (0.9, 1.1)
FLEX_DEFAULT_WEIGHTS = {"RB": 0.5, "WR": 0.4, "TE": 0.1}
SCARCITY_DEPTH_FACTOR = 0.5
SCARCITY_MAX_MULTIPLIER = 1.35
DEFAULT_BASELINES = {"QB": 10.0, "RB": 8.0, "WR": 8.0, "TE": 5.0, "K": 7.0, "D/ST": 7.0}
FLEX_SLOT_NAMES = {"RB/WR/TE", "RB/WR", "WR/TE"}
FLEX_SLOT_ELIGIBLE_POSITIONS = {
    "RB/WR/TE": ("RB", "WR", "TE"),
    "RB/WR": ("RB", "WR"),
    "WR/TE": ("WR", "TE"),
}
SKILL_POSITIONS = ["QB", "RB", "WR", "TE"]
# Baseline/rank/scarcity math (calculate_replacement_baselines, calculate_rank_at,
# compute_scarcity_multipliers) needs nothing position-specific beyond adjusted_projected_
# points and lineup_slot_counts - unlike compute_matchup_coefficient's _signal_raw branch,
# which genuinely has no equivalent metric for K/D-ST and is deliberately left
# SKILL_POSITIONS-only. Leaving K/D-ST out of baseline/scarcity (as an earlier version of
# this fix did) left their vorp_score equal to raw projected_points with no baseline
# subtracted - not comparable to a skill position's delta-over-replacement score, so a
# 9-point D/ST could outrank an 8-point-of-actual-VORP WR and get recommended in an early
# round instead of being properly scarcity-ranked among its own tiny, fast-depleting pool.
VORP_POSITIONS = SKILL_POSITIONS + ["K", "D/ST"]


def _zscore_within(df: pl.DataFrame, value_col: str, group_cols: list, out_col: str) -> pl.DataFrame:
    """
    Z-scores `value_col` within each `group_cols` group, clamped to +/-Z_CLAMP. Any NULL
    input, or a group with std==0 or fewer than 2 rows, contributes 0.0 rather than NaN -
    a missing/degenerate signal must stay neutral, not poison the whole column.
    """
    return df.with_columns(
        pl.when(pl.col(value_col).is_null())
        .then(0.0)
        .otherwise(
            pl.when((pl.col(value_col).std().over(group_cols).fill_null(0.0) == 0.0)
                     | (pl.col(value_col).count().over(group_cols) < 2))
            .then(0.0)
            .otherwise(
                ((pl.col(value_col) - pl.col(value_col).mean().over(group_cols))
                 / pl.col(value_col).std().over(group_cols)).clip(-Z_CLAMP, Z_CLAMP)
            )
        )
        .alias(out_col)
    )


def compute_matchup_coefficient(df: pl.DataFrame) -> pl.DataFrame:
    """
    Runs before baselines, per the issue's spec: projections are scaled before VORP is
    computed. market_z is z-scored over the distinct (team, season, week) implied totals
    (not per-player, or heavy-roster teams get weighted twice). signal_z is z-scored
    within (position, season, week) using a position-specific quality signal.

    Deliberately excludes target_share/rush_share/snap_share - Sleeper's projection
    already embeds expected usage, so re-adding it would double-count.
    """
    if df.is_empty():
        return df.with_columns([
            pl.lit(1.0).alias("matchup_coefficient"),
            pl.col("projected_points").alias("adjusted_projected_points"),
        ])

    teams = (
        df.select(["team", "season", "week", "team_implied_total"])
        .unique(subset=["team", "season", "week"])
    )
    teams = _zscore_within(teams, "team_implied_total", ["season", "week"], "market_z")
    df = df.join(teams.select(["team", "season", "week", "market_z"]), on=["team", "season", "week"], how="left")
    df = df.with_columns(pl.col("market_z").fill_null(0.0))

    # depth_chart_delta (from metrics_extractor.compute_depth_chart_delta) is
    # prior_rank - current_rank: POSITIVE means the player moved up the depth chart, i.e.
    # improved. Used directly here, unnegated, so climbers get a higher signal_z/coefficient.
    df = df.with_columns(
        pl.when(pl.col("position") == "QB").then(-pl.col("pressure_rate_allowed"))
        .when(pl.col("position").is_in(["WR", "TE"])).then(pl.col("avg_separation"))
        .when(pl.col("position") == "RB").then(pl.col("depth_chart_delta"))
        .otherwise(None)
        .alias("_signal_raw")
    )

    df = _zscore_within(df, "_signal_raw", ["position", "season", "week"], "signal_z")
    df = df.drop("_signal_raw")

    df = df.with_columns(
        (1 + W_MARKET * pl.col("market_z") + W_SIGNAL * pl.col("signal_z"))
        .clip(*COEFFICIENT_BOUNDS)
        .alias("matchup_coefficient")
    )
    df = df.with_columns(
        (pl.col("projected_points") * pl.col("matchup_coefficient")).alias("adjusted_projected_points")
    )
    return df.drop(["market_z", "signal_z"])


def _largest_remainder_allocate(weights: dict, total: int) -> dict:
    """Distributes `total` integer units across `weights` (proportions summing to ~1.0)
    by largest-remainder rounding, so the allocated integers sum back exactly to `total`."""
    if total <= 0 or not weights:
        return {pos: 0 for pos in weights}
    raw = {pos: w * total for pos, w in weights.items()}
    floors = {pos: int(v) for pos, v in raw.items()}
    remainder = total - sum(floors.values())
    remainders_sorted = sorted(raw.keys(), key=lambda p: raw[p] - floors[p], reverse=True)
    for pos in remainders_sorted[:remainder]:
        floors[pos] += 1
    return floors


def _default_weights_for(eligible_positions) -> dict:
    """FLEX_DEFAULT_WEIGHTS restricted to a slot type's eligible positions and
    renormalized to sum to 1.0 (e.g. WR/TE has no RB share to redistribute)."""
    subset = {pos: FLEX_DEFAULT_WEIGHTS[pos] for pos in eligible_positions}
    total = sum(subset.values())
    return {pos: w / total for pos, w in subset.items()}


def resolve_flex_allocation(flex_slots_total: int, roster_df: pl.DataFrame, eligible_positions=("RB", "WR", "TE")) -> dict:
    """
    Derives FLEX-eligible slot weights from observed league_rosters usage restricted to
    `eligible_positions` (grouped by the player's real position), falling back to
    FLEX_DEFAULT_WEIGHTS (renormalized to that subset) when fewer than 10 such rows exist
    - the only path that runs pre-draft, when league_rosters is empty.
    """
    flex_rows = (
        roster_df.filter(
            pl.col("slot_position").is_in(list(FLEX_SLOT_NAMES)) & pl.col("position").is_in(list(eligible_positions))
        )
        if not roster_df.is_empty() else roster_df
    )
    if roster_df.is_empty() or flex_rows.height < 10:
        weights = _default_weights_for(eligible_positions)
    else:
        counts = flex_rows.group_by("position").agg(pl.len().alias("n"))
        total = counts["n"].sum()
        weights = {row["position"]: row["n"] / total for row in counts.iter_rows(named=True)}
        for pos in eligible_positions:
            weights.setdefault(pos, 0.0)

    return _largest_remainder_allocate(weights, flex_slots_total)


def resolve_all_flex_allocations(league_config: dict, roster_df: pl.DataFrame) -> dict:
    """
    Aggregates FLEX allocation across every configured FLEX slot type (RB/WR/TE, RB/WR,
    WR/TE), each restricted to its own eligible positions, and sums the resulting
    per-position counts. A league using more than one FLEX slot type must not have its
    other types silently ignored (this repo's real league only uses RB/WR/TE, but the
    schema/constants explicitly declare support for all three).
    """
    lineup_slot_counts = league_config["lineup_slot_counts"]
    team_count = league_config["team_count"]
    combined = defaultdict(int)
    for slot_name, eligible_positions in FLEX_SLOT_ELIGIBLE_POSITIONS.items():
        slot_count = lineup_slot_counts.get(slot_name, 0)
        if slot_count <= 0:
            continue
        alloc = resolve_flex_allocation(slot_count * team_count, roster_df, eligible_positions)
        for pos, n in alloc.items():
            combined[pos] += n
    return dict(combined)


def calculate_replacement_baselines(
    df: pl.DataFrame, league_config: dict, flex_alloc: dict, rosters_populated: bool
) -> dict:
    """
    Once real rosters exist (`rosters_populated`), the baseline is the best actual
    non-starter (or free agent) at the position - rank N+1 in raw sorted order is not
    reliable post-draft, since a benched high-projection player occupies a slot in that
    ranking without being a starter. Pre-draft (rosters_populated=False, the only state
    that exists before the Sept 8 draft), there is no starter data to consult, so this
    falls back to the structural rank_at[pos] = starting slots (including FLEX share)
    across the league - i.e. rank N+1, the first non-starter, per the issue's spec.
    Falls back further to the pool's minimum if it's shorter than that, or to
    DEFAULT_BASELINES if the pool is empty.
    """
    lineup_slot_counts = league_config["lineup_slot_counts"]
    team_count = league_config["team_count"]
    baselines = {}
    for pos in VORP_POSITIONS:
        pool = (
            df.filter(pl.col("position") == pos)
            .sort(["adjusted_projected_points", "player_id"], descending=[True, False])
        )
        if pool.is_empty():
            baselines[pos] = DEFAULT_BASELINES.get(pos, 0.0)
            continue

        if rosters_populated:
            # is_starter is NULL for free agents (not on any roster) - fill_null(False)
            # correctly treats them as eligible replacement candidates, not starters.
            non_starters = pool.filter(~pl.col("is_starter").fill_null(False))
            if not non_starters.is_empty():
                baselines[pos] = float(non_starters.row(0, named=True)["adjusted_projected_points"])
                continue

        rank_at = lineup_slot_counts.get(pos, 0) * team_count + flex_alloc.get(pos, 0)
        if pool.height > rank_at:
            baselines[pos] = float(pool.row(rank_at, named=True)["adjusted_projected_points"])
        else:
            baselines[pos] = float(pool["adjusted_projected_points"].min())
    return baselines


def calculate_rank_at(league_config: dict, flex_alloc: dict) -> dict:
    lineup_slot_counts = league_config["lineup_slot_counts"]
    team_count = league_config["team_count"]
    return {
        pos: lineup_slot_counts.get(pos, 0) * team_count + flex_alloc.get(pos, 0)
        for pos in VORP_POSITIONS
    }


def compute_vorp(df: pl.DataFrame, baselines: dict) -> pl.DataFrame:
    """vorp_base is signed, with no clamp - the explicit regression fix for the old
    `max(0.0, ...)` clamp that made every below-replacement player look identically 0.0."""
    baseline_expr = pl.col("position").replace(baselines, default=0.0).cast(pl.Float64)
    return df.with_columns((pl.col("adjusted_projected_points") - baseline_expr).alias("vorp_base"))


def compute_scarcity_multipliers(df: pl.DataFrame, rank_at: dict) -> dict:
    """
    Free-agent depth ratio, smooth-interpolated. Only ever computed off free-agent rows
    with vorp_base > 0 - scaling a below-replacement free agent up would make a bad
    player look worse purely because his position is thin, which is backwards.
    """
    multipliers = {}
    for pos in VORP_POSITIONS:
        pool = df.filter((pl.col("position") == pos) & (~pl.col("is_rostered")) & (pl.col("vorp_base") > 0))
        surplus = pool.height
        expected = max(1, round(SCARCITY_DEPTH_FACTOR * rank_at.get(pos, 0)))
        ratio = max(0.0, min(1.0, surplus / expected))
        multipliers[pos] = 1 + (SCARCITY_MAX_MULTIPLIER - 1) * (1 - ratio)
    return multipliers


def apply_scarcity(df: pl.DataFrame, multipliers: dict) -> pl.DataFrame:
    """Scarcity scaling applies only to free-agent rows with a positive vorp_base;
    rostered players and anyone at/below replacement get a flat 1.0 multiplier."""
    multiplier_expr = pl.col("position").replace(multipliers, default=1.0).cast(pl.Float64)
    return df.with_columns(
        pl.when((~pl.col("is_rostered")) & (pl.col("vorp_base") > 0))
        .then(multiplier_expr)
        .otherwise(1.0)
        .alias("scarcity_multiplier")
    ).with_columns(
        (pl.col("vorp_base") * pl.col("scarcity_multiplier")).alias("vorp_score")
    )


def compute_defense_matchup_multiplier(df: pl.DataFrame) -> pl.DataFrame:
    """
    Opponent-defense-strength multiplier (#93), clamped to DEFENSE_MATCHUP_BOUNDS
    [0.9, 1.1]. Distinct from compute_matchup_coefficient (own-team/own-signal based) -
    keyed off the opponent's defense_position_strength.strength_z (positive = weaker
    defense = good for the offense), itself derived from LAST season's positional
    points-allowed (sync_defense_strength.py), since Week 1 has no current-season data by
    definition. NULL strength_z (no ingested row yet, bye week, or non-skill position) is
    neutral: 1.0, not clamped toward either bound.

    Deliberately does NOT multiply this into vorp_score: weekly_stats.vorp_score is the
    canonical season-long VORP other consumers (waiver_strategy.py, trade/player APIs) read
    directly - baking the Week 1 opponent nudge into it would silently distort every one of
    those, not just Week 1 lineup selection. lineup_solver.py is the only place that
    multiplies vorp_score by this column, and only for its own ILP objective.
    """
    if df.is_empty() or "defense_strength_z" not in df.columns:
        return df.with_columns(pl.lit(1.0).alias("defense_matchup_multiplier"))
    return df.with_columns(
        pl.when(pl.col("defense_strength_z").is_null())
        .then(1.0)
        .otherwise((1 + W_DEFENSE * pl.col("defense_strength_z")).clip(*DEFENSE_MATCHUP_BOUNDS))
        .alias("defense_matchup_multiplier")
    )


def persist_vorp_matrix(conn, rows: list) -> int:
    if not rows:
        return 0
    update_query = """
        UPDATE weekly_stats
        SET matchup_coefficient = %s, adjusted_projected_points = %s,
            vorp_base = %s, scarcity_multiplier = %s, vorp_score = %s,
            defense_matchup_multiplier = %s
        WHERE player_id = %s AND season = %s AND week = %s;
    """
    with conn.cursor() as cursor:
        cursor.executemany(update_query, rows)
    return len(rows)


def load_league_config(cursor) -> dict:
    cursor.execute("SELECT team_count, lineup_slot_counts FROM league_settings LIMIT 1;")
    row = cursor.fetchone()
    if row is None:
        # Pre-draft/no league synced yet: fall back to this league's known real shape
        # (10 teams, 1/2/2/1 + FLEX) rather than crashing the whole pipeline.
        return {"team_count": 10, "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1}}
    team_count, lineup_slot_counts = row
    return {"team_count": team_count, "lineup_slot_counts": lineup_slot_counts}


def load_roster_positions(cursor) -> pl.DataFrame:
    cursor.execute("""
        SELECT r.slot_position, p.position
        FROM league_rosters r JOIN players p ON r.player_id = p.player_id;
    """)
    rows = cursor.fetchall()
    schema = {"slot_position": pl.Utf8, "position": pl.Utf8}
    return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)


def run_vorp_pipeline(season: int, week: int) -> int:
    """Orchestrates load -> coefficient -> flex -> baselines -> vorp_base -> scarcity ->
    persist, inside one connection."""
    logger.info(f"Running dynamic VORP matrix pipeline for season {season} week {week}...")

    with tracer.start_as_current_span("vorp_pipeline.run", attributes={"season": season, "week": week}):
        with get_db_connection() as conn:
            with tracer.start_as_current_span("vorp_pipeline.load_config"):
                with conn.cursor() as cursor:
                    league_config = load_league_config(cursor)
                    roster_df = load_roster_positions(cursor)

            with tracer.start_as_current_span("vorp_pipeline.load_stats"):
                with conn.cursor() as cursor:
                    # LEFT JOIN defense_position_strength is always present, regardless of
                    # MATCHUP_WEIGHTING_ENABLED - the table exists post-migration and is empty
                    # pre-ingestion, so this stays NULL rather than requiring a second SQL path.
                    cursor.execute("""
                        SELECT p.player_id, p.position, s.projected_points, s.team_implied_total,
                               s.avg_separation, s.pressure_rate_allowed, s.depth_chart_delta, s.team,
                               s.opponent, (r.player_id IS NOT NULL) AS is_rostered, r.is_starter,
                               d.strength_z AS defense_strength_z
                        FROM players p
                        JOIN weekly_stats s ON p.player_id = s.player_id
                        LEFT JOIN (
                            SELECT DISTINCT ON (player_id) player_id, is_starter FROM league_rosters
                        ) r ON p.player_id = r.player_id
                        LEFT JOIN defense_position_strength d
                            ON d.defense_team = s.opponent AND d.position = p.position AND d.season = %s
                        WHERE s.season = %s AND s.week = %s;
                    """, (season, season, week))
                    rows = cursor.fetchall()

                schema = {
                    "player_id": pl.Utf8, "position": pl.Utf8, "projected_points": pl.Float64,
                    "team_implied_total": pl.Float64, "avg_separation": pl.Float64,
                    "pressure_rate_allowed": pl.Float64, "depth_chart_delta": pl.Int64,
                    "team": pl.Utf8, "opponent": pl.Utf8, "is_rostered": pl.Boolean, "is_starter": pl.Boolean,
                    "defense_strength_z": pl.Float64,
                }
                df = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
                if df.is_empty():
                    logger.warning(f"No weekly_stats rows found for {season} week {week}; nothing to compute.")
                    return 0
                df = df.with_columns([pl.lit(season).alias("season"), pl.lit(week).alias("week")])

            with tracer.start_as_current_span("vorp_pipeline.matchup_coefficient"):
                df = compute_matchup_coefficient(df)

            with tracer.start_as_current_span("vorp_pipeline.flex_allocation"):
                flex_alloc = resolve_all_flex_allocations(league_config, roster_df)
                rank_at = calculate_rank_at(league_config, flex_alloc)

            with tracer.start_as_current_span("vorp_pipeline.baselines"):
                baselines = calculate_replacement_baselines(df, league_config, flex_alloc, rosters_populated=not roster_df.is_empty())

            with tracer.start_as_current_span("vorp_pipeline.vorp_and_scarcity"):
                df = compute_vorp(df, baselines)
                multipliers = compute_scarcity_multipliers(df, rank_at)
                df = apply_scarcity(df, multipliers)

            with tracer.start_as_current_span("vorp_pipeline.defense_matchup"):
                # Off by default: the persisted multiplier is a real NULL (not 1.0) when
                # the flag is unset, or when week != 1 - byte-identical to pre-#93 behavior
                # in both cases, per the issue's acceptance criteria. week == 1 specifically
                # (not just the flag) because defense_position_strength is a last-season
                # proxy, meaningful only as a Week 1 cold-start signal - this pipeline also
                # runs once per week inside season_replay's backtest loop, and a bare flag
                # check would have applied a stale prior-season multiplier to every week of
                # a season, not just Week 1. vorp_score itself is never touched here - see
                # compute_defense_matchup_multiplier's docstring.
                if MATCHUP_WEIGHTING_ENABLED and week == 1:
                    df = compute_defense_matchup_multiplier(df)
                else:
                    df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("defense_matchup_multiplier"))

            with tracer.start_as_current_span("vorp_pipeline.persist"):
                persist_rows = [
                    (
                        r["matchup_coefficient"], r["adjusted_projected_points"], r["vorp_base"],
                        r["scarcity_multiplier"], r["vorp_score"], r["defense_matchup_multiplier"],
                        r["player_id"], season, week,
                    )
                    for r in df.iter_rows(named=True)
                ]
                written = persist_vorp_matrix(conn, persist_rows)

    logger.info(f"VORP matrix pipeline complete: {written} row(s) updated for {season} week {week}.")
    return written


if __name__ == "__main__":
    configure_tracing("vorp-matrix")

    parser = argparse.ArgumentParser(description="Compute the dynamic VORP matrix for a season/week.")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int, required=True)
    args = parser.parse_args()
    with run_log_service.run_scope("vorp_matrix", trigger={"season": args.season, "week": args.week}):
        run_vorp_pipeline(season=args.season, week=args.week)
