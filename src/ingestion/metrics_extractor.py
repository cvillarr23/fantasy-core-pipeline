import argparse
import logging
from datetime import datetime, timedelta

import nflreadpy as nfl
import polars as pl

from src.database import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("NFLVerseIngestor")

RELEVANT_PLAY_TYPES = ["pass", "run"]
# CORE-04 defines high_value_touches as carries/targets "explicitly inside the opponent's
# 10-yard line" - i.e. yardline_100 <= 10, not the broader 20-yard-line red zone.
HIGH_VALUE_TOUCH_YARDLINE = 10
ROUTE_RUNNING_POSITIONS = {"WR", "RB", "TE"}

# nflverse team abbreviations occasionally diverge from PFR/legacy codes across the
# datasets this module joins (schedules, snap counts, pbp). Own a small local map rather
# than depending on any one dataset's convention.
TEAM_ABBR_ALIASES = {
    "LA": "LAR", "STL": "LAR",
    "OAK": "LV", "LVR": "LV",
    "SD": "LAC",
    "ARZ": "ARI",
    "BLT": "BAL",
    "CLV": "CLE",
    "HST": "HOU",
}

# Column order for the weekly_stats upsert. Deliberately excludes projected_points,
# actual_points, and vorp_score, which are owned by other jobs and must never be
# clobbered by this ingestion.
UPSERT_COLUMNS = [
    "player_id", "season", "week",
    "snap_share", "target_share", "route_rate", "high_value_touches",
    "rush_share", "targets", "carries",
    "avg_separation", "avg_intended_air_yards", "avg_time_to_throw", "aggressiveness",
    "contested_ball_rate", "pressure_rate_allowed", "pass_block_win_rate",
    "depth_chart_rank", "depth_chart_delta",
    "team", "opponent", "is_home", "spread_line", "total_line",
    "team_moneyline", "opponent_moneyline", "team_implied_total",
]


def normalize_team(abbr) -> str | None:
    """Maps legacy/alternate team abbreviations onto the current nflverse code."""
    if abbr is None:
        return None
    return TEAM_ABBR_ALIASES.get(abbr, abbr)


def filter_week_plays(df_pbp: pl.DataFrame, week: int) -> pl.DataFrame:
    """Filters play-by-play down to the target week's offensive pass/run plays."""
    if df_pbp.is_empty():
        return df_pbp
    return (
        df_pbp
        .filter((pl.col("week") == week) & (pl.col("play_type").is_in(RELEVANT_PLAY_TYPES)))
        .with_columns(pl.col("posteam").map_elements(normalize_team, return_dtype=pl.Utf8).alias("team"))
    )


def compute_usage_shares(df_week: pl.DataFrame) -> pl.DataFrame:
    """
    Target/rush share. CORE-04 defines target_share as targets divided by the team's total
    passing attempts - so the denominator is built from `pass_attempt == 1` rows (which
    still excludes sacks/scrambles, since those aren't attempts), not from the count of
    plays that happened to have a receiver_id, which would undercount attempts by
    excluding throwaways (an attempt with no intended receiver). rush_share's denominator
    is the count of actual carries, since the issue doesn't define a separate "rush
    attempts" concept the way it does for targets. Uses a coalescing full join so a pure
    rusher with zero targets (or vice versa) still survives with a non-null gsis_id,
    rather than being dropped the way a non-coalescing outer join would drop it.
    """
    empty_schema = {
        "gsis_id": pl.Utf8, "team": pl.Utf8, "targets": pl.Int64, "carries": pl.Int64,
        "target_share": pl.Float64, "rush_share": pl.Float64, "high_value_touches": pl.Int64,
    }
    if df_week.is_empty():
        return pl.DataFrame(schema=empty_schema)

    targets = (
        df_week.filter(pl.col("receiver_id").is_not_null())
        .group_by(["receiver_id", "team"])
        .agg([
            pl.len().alias("targets"),
            (pl.col("yardline_100") <= HIGH_VALUE_TOUCH_YARDLINE).fill_null(False).sum().alias("redzone_targets"),
        ])
        .rename({"receiver_id": "gsis_id"})
    )
    carries = (
        df_week.filter(pl.col("rusher_id").is_not_null())
        .group_by(["rusher_id", "team"])
        .agg([
            pl.len().alias("carries"),
            (pl.col("yardline_100") <= HIGH_VALUE_TOUCH_YARDLINE).fill_null(False).sum().alias("redzone_carries"),
        ])
        .rename({"rusher_id": "gsis_id"})
    )
    team_targets = (
        df_week.filter(pl.col("pass_attempt").fill_null(0) == 1)
        .group_by("team")
        .agg(pl.len().alias("team_targets"))
    )
    team_carries = carries.group_by("team").agg(pl.col("carries").sum().alias("team_carries"))

    usage = (
        targets.join(carries, on=["gsis_id", "team"], how="full", coalesce=True)
        .join(team_targets, on="team", how="left", coalesce=True)
        .join(team_carries, on="team", how="left", coalesce=True)
        .with_columns([
            pl.col("targets").fill_null(0).cast(pl.Int64),
            pl.col("carries").fill_null(0).cast(pl.Int64),
            pl.col("redzone_targets").fill_null(0),
            pl.col("redzone_carries").fill_null(0),
        ])
        .with_columns([
            pl.when(pl.col("team_targets").fill_null(0) > 0)
            .then(pl.col("targets") / pl.col("team_targets"))
            .otherwise(None)
            .alias("target_share"),
            pl.when(pl.col("team_carries").fill_null(0) > 0)
            .then(pl.col("carries") / pl.col("team_carries"))
            .otherwise(None)
            .alias("rush_share"),
            (pl.col("redzone_targets") + pl.col("redzone_carries")).cast(pl.Int64).alias("high_value_touches"),
        ])
        .select(list(empty_schema.keys()))
    )
    return usage


def build_gsis_pfr_crosswalk(df_players: pl.DataFrame) -> pl.DataFrame:
    """gsis_id<->pfr_id crosswalk from nflverse's player roster, needed because
    load_snap_counts() is keyed on pfr_player_id, not gsis_id."""
    if df_players.is_empty():
        return pl.DataFrame(schema={"gsis_id": pl.Utf8, "pfr_id": pl.Utf8})
    return (
        df_players.select(["gsis_id", "pfr_id"])
        .drop_nulls(subset=["gsis_id", "pfr_id"])
        .unique(subset=["pfr_id"], keep="first")
    )


def compute_snap_share(df_snaps: pl.DataFrame, week: int, crosswalk: pl.DataFrame) -> pl.DataFrame:
    """Offense snap share for the week, joined onto gsis_id via the pfr_id crosswalk.
    Players with no crosswalk match are dropped here (they can still get pbp-derived
    metrics elsewhere)."""
    empty_schema = {"gsis_id": pl.Utf8, "team": pl.Utf8, "snap_share": pl.Float64}
    if df_snaps.is_empty():
        return pl.DataFrame(schema=empty_schema)
    return (
        df_snaps.filter(pl.col("week") == week)
        .rename({"pfr_player_id": "pfr_id", "offense_pct": "snap_share"})
        .with_columns(pl.col("team").map_elements(normalize_team, return_dtype=pl.Utf8))
        .join(crosswalk, on="pfr_id", how="inner")
        .select(list(empty_schema.keys()))
    )


def compute_route_participation(df_participation: pl.DataFrame, df_week: pl.DataFrame, week: int) -> pl.DataFrame:
    """
    Route participation, derived from nflverse's play-participation feed: explode each
    dropback play's offense_players/offense_positions lists (semicolon-delimited, aligned
    index-wise) and count appearances by WR/RB/TE players, divided by the team's total
    dropbacks that week.

    KNOWN LIMITATION: nflverse's participation data records who was on the field for a
    dropback, not who actually ran a route - a pass-blocking RB or in-line blocking TE
    is indistinguishable here from one who released into a route. There is no per-player
    route/block flag in any nflverse dataset (the participation feed's own `route` column
    only describes the single targeted receiver's route on that play, and FTN charting has
    no player IDs at all), so this is the closest available proxy and will overstate route
    rate for run-blocking-heavy backs/tight ends. Treat it as "eligible offensive skill
    player on the field for a dropback" rather than a confirmed route-run count.

    If the participation feed has no rows for this season/year (nflreadpy currently caps
    load_participation() at a fixed season range and raises for years outside it - see
    _safe_load's handling in extract_weekly_metrics), this degrades to an empty/NULL frame
    rather than raising, since route_rate is a nice-to-have on top of target/rush share.
    """
    empty_schema = {"gsis_id": pl.Utf8, "route_rate": pl.Float64}
    if df_participation.is_empty() or df_week.is_empty():
        return pl.DataFrame(schema=empty_schema)

    dropbacks = df_week.filter(pl.col("play_type") == "pass")
    if dropbacks.is_empty():
        return pl.DataFrame(schema=empty_schema)

    team_dropbacks = dropbacks.group_by("team").agg(pl.len().alias("team_dropbacks"))

    participation_week = (
        df_participation
        .join(
            dropbacks.select(["game_id", "play_id", "team"]),
            left_on=["nflverse_game_id", "play_id"],
            right_on=["game_id", "play_id"],
            how="inner",
        )
        .filter(pl.col("offense_players").is_not_null() & pl.col("offense_positions").is_not_null())
        .with_columns([
            pl.col("offense_players").str.split(";").alias("offense_players"),
            pl.col("offense_positions").str.split(";").alias("offense_positions"),
        ])
        .explode(["offense_players", "offense_positions"])
        .rename({"offense_players": "gsis_id", "offense_positions": "position"})
        .filter(pl.col("position").is_in(list(ROUTE_RUNNING_POSITIONS)))
    )
    if participation_week.is_empty():
        return pl.DataFrame(schema=empty_schema)

    routes = participation_week.group_by(["gsis_id", "team"]).agg(pl.len().alias("routes"))
    return (
        routes.join(team_dropbacks, on="team", how="left", coalesce=True)
        .with_columns(
            pl.when(pl.col("team_dropbacks").fill_null(0) > 0)
            .then(pl.col("routes") / pl.col("team_dropbacks"))
            .otherwise(None)
            .alias("route_rate")
        )
        .select(list(empty_schema.keys()))
    )


def normalize_nextgen_stats(df_ngs_passing: pl.DataFrame, df_ngs_receiving: pl.DataFrame, week: int) -> pl.DataFrame:
    """
    Merges NGS passing + receiving tables for the target week. Both files include week=0
    season-aggregate rows that must be filtered out before joining, or they'd pollute a
    weekly join with duplicate season-level rows. avg_intended_air_yards exists in both
    tables; the receiving-table value is preferred for pass-catchers, falling back to the
    passing-table value for QBs.
    """
    empty_schema = {
        "gsis_id": pl.Utf8, "avg_time_to_throw": pl.Float64, "aggressiveness": pl.Float64,
        "avg_intended_air_yards": pl.Float64, "avg_separation": pl.Float64,
    }
    passing = (
        df_ngs_passing.filter(pl.col("week") == week)
        .rename({"player_gsis_id": "gsis_id"})
        .select(["gsis_id", "avg_time_to_throw", "aggressiveness", "avg_intended_air_yards"])
        if not df_ngs_passing.is_empty() else pl.DataFrame(schema={
            "gsis_id": pl.Utf8, "avg_time_to_throw": pl.Float64,
            "aggressiveness": pl.Float64, "avg_intended_air_yards": pl.Float64,
        })
    )
    receiving = (
        df_ngs_receiving.filter(pl.col("week") == week)
        .rename({"player_gsis_id": "gsis_id"})
        .select(["gsis_id", "avg_separation", "avg_intended_air_yards"])
        if not df_ngs_receiving.is_empty() else pl.DataFrame(schema={
            "gsis_id": pl.Utf8, "avg_separation": pl.Float64, "avg_intended_air_yards": pl.Float64,
        })
    )
    if passing.is_empty() and receiving.is_empty():
        return pl.DataFrame(schema=empty_schema)

    merged = passing.join(
        receiving, on="gsis_id", how="full", coalesce=True, suffix="_rec"
    )
    air_yards_cols = [c for c in ["avg_intended_air_yards", "avg_intended_air_yards_rec"] if c in merged.columns]
    if len(air_yards_cols) == 2:
        merged = merged.with_columns(
            pl.coalesce(["avg_intended_air_yards_rec", "avg_intended_air_yards"]).alias("avg_intended_air_yards")
        ).drop("avg_intended_air_yards_rec")

    for col in empty_schema:
        if col not in merged.columns:
            merged = merged.with_columns(pl.lit(None, dtype=empty_schema[col]).alias(col))

    return merged.select(list(empty_schema.keys()))


def compute_contested_ball_rate(df_ftn: pl.DataFrame, df_week: pl.DataFrame, week: int) -> pl.DataFrame:
    """
    FTN charting is play-level with no player ID, so contested-ball rate per receiver
    requires joining it onto pbp by (game_id, play_id) to recover receiver_id.
    """
    empty_schema = {"gsis_id": pl.Utf8, "contested_ball_rate": pl.Float64}
    if df_ftn.is_empty() or df_week.is_empty():
        return pl.DataFrame(schema=empty_schema)

    targets = df_week.filter(pl.col("receiver_id").is_not_null()).select(
        ["game_id", "play_id", "receiver_id"]
    ).with_columns(pl.col("play_id").cast(pl.Int64))

    ftn_week = (
        df_ftn.filter(pl.col("week") == week)
        .with_columns(pl.col("nflverse_play_id").cast(pl.Int64))
        .join(
            targets,
            left_on=["nflverse_game_id", "nflverse_play_id"],
            right_on=["game_id", "play_id"],
            how="inner",
        )
    )
    if ftn_week.is_empty():
        return pl.DataFrame(schema=empty_schema)

    return (
        ftn_week.group_by("receiver_id")
        .agg([
            pl.col("is_contested_ball").fill_null(False).sum().alias("contested"),
            pl.len().alias("total_targets_charted"),
        ])
        .with_columns((pl.col("contested") / pl.col("total_targets_charted")).alias("contested_ball_rate"))
        .rename({"receiver_id": "gsis_id"})
        .select(list(empty_schema.keys()))
    )


def compute_oline_pressure_metrics(df_week: pl.DataFrame, df_ftn: pl.DataFrame, week: int) -> pl.DataFrame:
    """
    Team-level pressure rate allowed: pressures (sacks + QB hits) over dropbacks (pass
    attempts + sacks). When FTN charting is available, sacks it flags as the QB's own
    fault (scrambles, holding the ball too long) are excluded from "pressures allowed" so
    the metric reflects O-line performance rather than QB decision-making. Falls back to
    the pbp-only definition when FTN is unavailable.
    """
    empty_schema = {"team": pl.Utf8, "pressure_rate_allowed": pl.Float64, "pass_block_win_rate": pl.Float64}
    if df_week.is_empty():
        return pl.DataFrame(schema=empty_schema)

    dropbacks = df_week.filter((pl.col("pass_attempt").fill_null(0) == 1) | (pl.col("sack").fill_null(0) == 1))
    if dropbacks.is_empty():
        return pl.DataFrame(schema=empty_schema)

    qb_fault_play_ids = set()
    if not df_ftn.is_empty():
        qb_fault = (
            df_ftn.filter((pl.col("week") == week) & pl.col("is_qb_fault_sack").fill_null(False))
            .select(["nflverse_game_id", "nflverse_play_id"])
        )
        qb_fault_play_ids = set(
            zip(qb_fault["nflverse_game_id"].to_list(), qb_fault["nflverse_play_id"].to_list())
        )

    dropbacks = dropbacks.with_columns(pl.col("play_id").cast(pl.Int64))
    if qb_fault_play_ids:
        dropbacks = dropbacks.with_columns(
            pl.struct(["game_id", "play_id"]).map_elements(
                lambda s: (s["game_id"], s["play_id"]) in qb_fault_play_ids, return_dtype=pl.Boolean
            ).alias("_qb_fault_sack")
        )
    else:
        dropbacks = dropbacks.with_columns(pl.lit(False).alias("_qb_fault_sack"))

    pressures = dropbacks.with_columns(
        (
            (pl.col("qb_hit").fill_null(0) == 1)
            | ((pl.col("sack").fill_null(0) == 1) & (~pl.col("_qb_fault_sack")))
        ).cast(pl.Int64).alias("is_pressure")
    )

    return (
        pressures.group_by("team")
        .agg([
            pl.len().alias("dropbacks"),
            pl.col("is_pressure").sum().alias("pressures"),
        ])
        .with_columns(
            (pl.col("pressures") / pl.col("dropbacks")).alias("pressure_rate_allowed")
        )
        .with_columns((1 - pl.col("pressure_rate_allowed")).alias("pass_block_win_rate"))
        .select(list(empty_schema.keys()))
    )


def compute_depth_chart_ranks(
    df_depth: pl.DataFrame, *, week: int | None = None, as_of: str | None = None
) -> pl.DataFrame:
    """
    Depth chart rank per player, restricted to QB/RB/WR/TE (the only positions the
    `players` table tracks) and deduplicated to one row per gsis_id (the raw feed has
    exact duplicate rows for some players, and can list the same player under multiple
    position groups) by keeping their single best (lowest) rank. nflverse's depth chart
    schema changed between seasons: older seasons carry `season`/`week` columns directly
    (filter to `week`); current seasons (2025+) instead publish rolling `dt` snapshots
    with no week column, so the latest snapshot at or before `as_of` is used instead.
    """
    empty_schema = {"gsis_id": pl.Utf8, "team": pl.Utf8, "depth_chart_rank": pl.Int64}
    if df_depth.is_empty():
        return pl.DataFrame(schema=empty_schema)

    if "week" in df_depth.columns:
        if week is None:
            return pl.DataFrame(schema=empty_schema)
        snapshot = df_depth.filter((pl.col("week") == week) & pl.col("position").is_in(list(ROUTE_RUNNING_POSITIONS | {"QB"})))
        if snapshot.is_empty():
            return pl.DataFrame(schema=empty_schema)
        return (
            snapshot
            .with_columns(pl.col("club_code").map_elements(normalize_team, return_dtype=pl.Utf8).alias("team"))
            .select([
                pl.col("gsis_id"),
                pl.col("team"),
                pl.col("depth_team").cast(pl.Int64, strict=False).alias("depth_chart_rank"),
            ])
            .drop_nulls(subset=["gsis_id", "depth_chart_rank"])
            .sort("depth_chart_rank")
            .unique(subset=["gsis_id"], keep="first")
        )

    if as_of is None:
        return pl.DataFrame(schema=empty_schema)
    snapshot = df_depth.filter((pl.col("dt") <= as_of) & pl.col("pos_abb").is_in(list(ROUTE_RUNNING_POSITIONS | {"QB"})))
    if snapshot.is_empty():
        return pl.DataFrame(schema=empty_schema)
    return (
        snapshot
        .with_columns(pl.col("team").map_elements(normalize_team, return_dtype=pl.Utf8))
        .select([
            pl.col("gsis_id"),
            pl.col("team"),
            pl.col("pos_rank").cast(pl.Int64, strict=False).alias("depth_chart_rank"),
            pl.col("dt"),
        ])
        .drop_nulls(subset=["gsis_id", "depth_chart_rank"])
        .sort(["dt", "depth_chart_rank"], descending=[True, False])
        .unique(subset=["gsis_id"], keep="first")
        .drop("dt")
    )


def compute_depth_chart_delta(current: pl.DataFrame, prior: pl.DataFrame) -> pl.DataFrame:
    """Positive delta = player moved up the depth chart (lower rank number is better)."""
    empty_schema = {"gsis_id": pl.Utf8, "depth_chart_delta": pl.Int64}
    if current.is_empty() or prior.is_empty():
        return pl.DataFrame(schema=empty_schema)
    return (
        current.select(["gsis_id", "depth_chart_rank"])
        .join(
            prior.select(["gsis_id", "depth_chart_rank"]).rename({"depth_chart_rank": "prior_rank"}),
            on="gsis_id", how="inner",
        )
        .with_columns((pl.col("prior_rank") - pl.col("depth_chart_rank")).cast(pl.Int64).alias("depth_chart_delta"))
        .select(list(empty_schema.keys()))
    )


def compute_team_vegas_context(df_schedules: pl.DataFrame, week: int) -> pl.DataFrame:
    """
    Unpivots each scheduled game into a home row and an away row. spread_line is
    positive when the home team is favored, so home_implied_total = total/2 + spread/2
    and away_implied_total = total/2 - spread/2. spread_line is stored on each row from
    that row's own team's perspective (negated for the away row) so downstream consumers
    don't have to re-derive the sign convention. A bye-week team simply has no row here.
    """
    empty_schema = {
        "team": pl.Utf8, "opponent": pl.Utf8, "is_home": pl.Boolean,
        "spread_line": pl.Float64, "total_line": pl.Float64,
        "team_moneyline": pl.Int64, "opponent_moneyline": pl.Int64, "team_implied_total": pl.Float64,
    }
    if df_schedules.is_empty():
        return pl.DataFrame(schema=empty_schema)

    games = df_schedules.filter(pl.col("week") == week)
    if games.is_empty():
        return pl.DataFrame(schema=empty_schema)

    home = games.select([
        pl.col("home_team").map_elements(normalize_team, return_dtype=pl.Utf8).alias("team"),
        pl.col("away_team").map_elements(normalize_team, return_dtype=pl.Utf8).alias("opponent"),
        pl.lit(True).alias("is_home"),
        pl.col("spread_line"),
        pl.col("total_line"),
        pl.col("home_moneyline").cast(pl.Int64, strict=False).alias("team_moneyline"),
        pl.col("away_moneyline").cast(pl.Int64, strict=False).alias("opponent_moneyline"),
        (pl.col("total_line") / 2 + pl.col("spread_line") / 2).alias("team_implied_total"),
    ])
    away = games.select([
        pl.col("away_team").map_elements(normalize_team, return_dtype=pl.Utf8).alias("team"),
        pl.col("home_team").map_elements(normalize_team, return_dtype=pl.Utf8).alias("opponent"),
        pl.lit(False).alias("is_home"),
        (-pl.col("spread_line")).alias("spread_line"),
        pl.col("total_line"),
        pl.col("away_moneyline").cast(pl.Int64, strict=False).alias("team_moneyline"),
        pl.col("home_moneyline").cast(pl.Int64, strict=False).alias("opponent_moneyline"),
        (pl.col("total_line") / 2 - pl.col("spread_line") / 2).alias("team_implied_total"),
    ])
    return pl.concat([home, away]).select(list(empty_schema.keys()))


def assemble_weekly_rows(
    usage: pl.DataFrame,
    snaps: pl.DataFrame,
    routes: pl.DataFrame,
    ngs: pl.DataFrame,
    contested: pl.DataFrame,
    oline: pl.DataFrame,
    depth_rank: pl.DataFrame,
    depth_delta: pl.DataFrame,
    vegas: pl.DataFrame,
) -> pl.DataFrame:
    """
    Builds one row per player from a coalescing union of every player-keyed source's
    gsis_id (so a player who took zero touches but played snaps, or vice versa, still
    gets a row), then left-joins every other source. Missing supplementary data yields
    NULL columns rather than dropping the player - a dead feed degrades gracefully.
    """
    player_frames = [usage, snaps, routes, ngs, contested, depth_rank, depth_delta]
    spine = None
    for frame in player_frames:
        if frame.is_empty():
            continue
        ids = frame.select(["gsis_id", "team"] if "team" in frame.columns else ["gsis_id"])
        if "team" not in ids.columns:
            ids = ids.with_columns(pl.lit(None, dtype=pl.Utf8).alias("team"))
        spine = ids if spine is None else spine.join(ids, on="gsis_id", how="full", coalesce=True, suffix="_other")
        if "team_other" in (spine.columns if spine is not None else []):
            spine = spine.with_columns(pl.coalesce(["team", "team_other"]).alias("team")).drop("team_other")

    if spine is None:
        return pl.DataFrame(schema={"gsis_id": pl.Utf8, "team": pl.Utf8})

    spine = spine.unique(subset=["gsis_id"], keep="first")

    result = spine
    for frame, cols in [
        (usage, ["gsis_id", "targets", "carries", "target_share", "rush_share", "high_value_touches"]),
        (snaps, ["gsis_id", "snap_share"]),
        (routes, ["gsis_id", "route_rate"]),
        (ngs, ["gsis_id", "avg_separation", "avg_intended_air_yards", "avg_time_to_throw", "aggressiveness"]),
        (contested, ["gsis_id", "contested_ball_rate"]),
        (depth_rank, ["gsis_id", "depth_chart_rank"]),
        (depth_delta, ["gsis_id", "depth_chart_delta"]),
    ]:
        if frame.is_empty():
            continue
        result = result.join(frame.select(cols), on="gsis_id", how="left", coalesce=True)

    if not oline.is_empty():
        result = result.join(oline, on="team", how="left", coalesce=True)
    if not vegas.is_empty():
        result = result.join(vegas, on="team", how="left", coalesce=True)

    return result


def load_gsis_to_player_id(cursor) -> dict:
    """Loads the full gsis_id -> player_id crosswalk once, replacing a per-row
    correlated subquery that would otherwise insert a NULL player_id (and abort the
    whole batch) for any unmatched gsis_id."""
    cursor.execute("SELECT gsis_id, player_id FROM players WHERE gsis_id IS NOT NULL;")
    return {row[0]: row[1] for row in cursor.fetchall()}


def build_upsert_batch(
    assembled: pl.DataFrame, gsis_to_player_id: dict, season: int, week: int
) -> list[tuple]:
    """
    Pure. Maps gsis_id -> player_id via the supplied crosswalk, skipping (not crashing
    on) any row with no match, and emits fixed-order tuples matching UPSERT_COLUMNS,
    with None (not a computed 0.0/0) for any missing metric.
    """
    batch = []
    skipped = 0
    for row in assembled.iter_rows(named=True):
        player_id = gsis_to_player_id.get(row.get("gsis_id"))
        if player_id is None:
            skipped += 1
            continue
        batch.append((
            player_id, season, week,
            row.get("snap_share"), row.get("target_share"), row.get("route_rate"),
            row.get("high_value_touches"),
            row.get("rush_share"), row.get("targets"), row.get("carries"),
            row.get("avg_separation"), row.get("avg_intended_air_yards"),
            row.get("avg_time_to_throw"), row.get("aggressiveness"),
            row.get("contested_ball_rate"), row.get("pressure_rate_allowed"),
            row.get("pass_block_win_rate"),
            row.get("depth_chart_rank"), row.get("depth_chart_delta"),
            row.get("team"), row.get("opponent"), row.get("is_home"),
            row.get("spread_line"), row.get("total_line"),
            row.get("team_moneyline"), row.get("opponent_moneyline"), row.get("team_implied_total"),
        ))
    if skipped:
        logger.warning(f"Skipped {skipped} row(s) with a gsis_id not present in the players table.")
    return batch


def persist_weekly_metrics(conn, batch: list[tuple]) -> int:
    if not batch:
        return 0
    set_clause = ",\n            ".join(
        f"{col} = EXCLUDED.{col}" for col in UPSERT_COLUMNS if col not in ("player_id", "season", "week")
    )
    upsert_query = f"""
        INSERT INTO weekly_stats ({", ".join(UPSERT_COLUMNS)})
        VALUES ({", ".join(["%s"] * len(UPSERT_COLUMNS))})
        ON CONFLICT (player_id, season, week) DO UPDATE SET
            {set_clause};
    """
    with conn.cursor() as cursor:
        cursor.executemany(upsert_query, batch)
    return len(batch)


def _safe_load(loader, label: str, *args, **kwargs) -> pl.DataFrame:
    """Wraps an nflreadpy loader call; on failure, logs and returns an empty frame so one
    dead supplementary feed degrades to NULL columns instead of aborting the whole run."""
    try:
        return loader(*args, **kwargs)
    except Exception as e:
        logger.warning(f"Failed to load '{label}' from nflverse: {e}")
        return pl.DataFrame()


def extract_weekly_metrics(year: int, week: int) -> int:
    """Ingests play-by-play, NGS, FTN charting, depth charts, and schedule/Vegas data for
    a given season/week, computes leading-indicator utilization metrics, and upserts them
    into weekly_stats. Returns the number of rows written."""
    logger.info(f"Beginning nflverse advanced metrics extraction for {year} Week {week}...")

    try:
        df_pbp = nfl.load_pbp(year)
    except Exception as e:
        logger.error(f"Failed to load play-by-play data, aborting: {e}")
        return 0

    df_week = filter_week_plays(df_pbp, week)
    if df_week.is_empty():
        logger.warning(f"No offensive plays found for {year} Week {week}; nothing to write.")
        return 0

    df_players = _safe_load(nfl.load_players, "players")
    crosswalk = build_gsis_pfr_crosswalk(df_players)

    df_snaps = _safe_load(nfl.load_snap_counts, "snap_counts", year)
    # nflreadpy currently validates load_participation()'s season argument against a fixed
    # upper bound that lags the real current season by up to a year, so this call can raise
    # ValueError for the in-progress season. _safe_load converts that into an empty frame,
    # and compute_route_participation degrades route_rate to NULL rather than failing the
    # whole run - route_rate is a nice-to-have, not load-bearing for this ingestion.
    df_participation = _safe_load(nfl.load_participation, "participation", year)
    df_ngs_passing = _safe_load(nfl.load_nextgen_stats, "ngs_passing", year, stat_type="passing")
    df_ngs_receiving = _safe_load(nfl.load_nextgen_stats, "ngs_receiving", year, stat_type="receiving")
    df_ftn = _safe_load(nfl.load_ftn_charting, "ftn_charting", year)
    df_depth = _safe_load(nfl.load_depth_charts, "depth_charts", year)
    df_schedules = _safe_load(nfl.load_schedules, "schedules", year)

    usage = compute_usage_shares(df_week)
    snaps = compute_snap_share(df_snaps, week, crosswalk)
    routes = compute_route_participation(df_participation, df_week, week)
    ngs = normalize_nextgen_stats(df_ngs_passing, df_ngs_receiving, week)
    contested = compute_contested_ball_rate(df_ftn, df_week, week)
    oline = compute_oline_pressure_metrics(df_week, df_ftn, week)
    vegas = compute_team_vegas_context(df_schedules, week)

    if "week" in df_depth.columns:
        depth_rank = compute_depth_chart_ranks(df_depth, week=week)
        depth_prior = compute_depth_chart_ranks(df_depth, week=week - 1) if week > 1 else pl.DataFrame()
    else:
        game_date: str | None = None
        if not df_schedules.is_empty():
            week_games = df_schedules.filter(pl.col("week") == week)
            if not week_games.is_empty():
                game_date = week_games.select(pl.col("gameday").max()).item()
        depth_rank = pl.DataFrame()
        depth_prior = pl.DataFrame()
        if game_date:
            depth_rank = compute_depth_chart_ranks(df_depth, as_of=f"{game_date}T23:59:59Z")
            try:
                prior_date = (datetime.fromisoformat(game_date) - timedelta(days=7)).date().isoformat()
                depth_prior = compute_depth_chart_ranks(df_depth, as_of=f"{prior_date}T23:59:59Z")
            except ValueError:
                pass
    depth_delta = compute_depth_chart_delta(depth_rank, depth_prior)

    assembled = assemble_weekly_rows(usage, snaps, routes, ngs, contested, oline, depth_rank, depth_delta, vegas)

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            gsis_to_player_id = load_gsis_to_player_id(cursor)
        batch = build_upsert_batch(assembled, gsis_to_player_id, year, week)
        written = persist_weekly_metrics(conn, batch)

    logger.info(f"Successfully committed {written} weekly_stats row(s) for {year} Week {week}.")
    return written


def resolve_default_year_week() -> tuple[int, int]:
    """Resolves safe CLI defaults without letting nflreadpy "current" helpers crash startup."""
    fallback_year = datetime.now().year
    fallback_week = 1

    try:
        year = int(nfl.get_current_season())
    except Exception as exc:
        logger.warning(
            f"Failed to resolve current NFL season via nflreadpy ({exc}); defaulting --year to {fallback_year}."
        )
        year = fallback_year

    try:
        week = int(nfl.get_current_week())
    except Exception as exc:
        logger.warning(
            f"Failed to resolve current NFL week via nflreadpy ({exc}); defaulting --week to {fallback_week}."
        )
        week = fallback_week

    return year, week


if __name__ == "__main__":
    default_year, default_week = resolve_default_year_week()
    parser = argparse.ArgumentParser(description="Extract nflverse advanced weekly metrics into weekly_stats.")
    parser.add_argument("--year", type=int, default=default_year)
    parser.add_argument("--week", type=int, default=default_week)
    args = parser.parse_args()
    extract_weekly_metrics(year=args.year, week=args.week)
