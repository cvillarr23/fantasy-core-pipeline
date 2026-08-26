"""
Cold-start defensive-strength-vs-position proxy (#93).

Week 1 lineup weighting needs an opponent-defense signal before the current season has
produced any data at all, so this derives it from LAST season's weekly fantasy points
allowed by position, sourced via nflreadpy (already a pinned dependency, used by
metrics_extractor.py) - no new external dependency or credential surface. Has no dependency
on any current-season `weekly_stats` row: it only ever reads `season - 1` player stats, which
is exactly what makes it usable before Week 1 of `season` has any data.

Independent of the CORE-05 ingestion order (sync_players -> ... -> vorp_matrix): must simply
run at some point before a MATCHUP_WEIGHTING_ENABLED=true vorp_matrix.py pipeline run for
`season`, since that's the only consumer of defense_position_strength.
"""
import argparse
import logging

import nflreadpy as nfl
import polars as pl

from src.database import get_db_connection
from src.ingestion.metrics_extractor import normalize_team

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DefenseStrengthSync")

# No meaningful "points allowed" signal for K/D-ST facing a defense - mirrors
# compute_matchup_coefficient's own SKILL_POSITIONS-only _signal_raw branch in
# src/optimization/vorp_matrix.py.
SKILL_POSITIONS = ["QB", "RB", "WR", "TE"]
DEFENSE_Z_CLAMP = 2.0


def load_prior_season_positional_stats(source_season: int) -> pl.DataFrame:
    """Weekly per-player fantasy points against each opponent defense, regular season only,
    skill positions only. opponent_team is normalized through the same TEAM_ABBR_ALIASES map
    metrics_extractor.py already uses to populate weekly_stats.opponent, so the two agree on
    team codes at vorp_matrix.py's join."""
    df = nfl.load_player_stats(seasons=[source_season], summary_level="week")
    if df.is_empty():
        return df
    return (
        df.filter((pl.col("season_type") == "REG") & pl.col("position").is_in(SKILL_POSITIONS))
        .select(["position", "opponent_team", "week", "fantasy_points_ppr"])
        .with_columns(
            pl.col("opponent_team")
            .map_elements(normalize_team, return_dtype=pl.Utf8)
            .alias("defense_team")
        )
    )


def aggregate_points_allowed_by_position(df_stats: pl.DataFrame) -> pl.DataFrame:
    """Sums fantasy points across every player of a position who faced a given defense in a
    given week, then averages those weekly totals across the season -> points_allowed_per_game
    per (defense_team, position)."""
    if df_stats.is_empty():
        return pl.DataFrame(
            schema={"defense_team": pl.Utf8, "position": pl.Utf8, "points_allowed_per_game": pl.Float64}
        )
    weekly_totals = df_stats.group_by(["defense_team", "position", "week"]).agg(
        pl.col("fantasy_points_ppr").sum().alias("points_allowed")
    )
    return weekly_totals.group_by(["defense_team", "position"]).agg(
        pl.col("points_allowed").mean().alias("points_allowed_per_game")
    )


def compute_defense_strength_z(agg_df: pl.DataFrame) -> pl.DataFrame:
    """Z-scores points_allowed_per_game within each position group. Higher points allowed
    means a weaker defense, so a positive strength_z is deliberately "good for the offense
    facing them" - matches compute_defense_matchup_multiplier's sign convention in
    vorp_matrix.py (multiplier = 1 + W_DEFENSE * strength_z). A single-team or zero-variance
    position group contributes 0.0, not NaN - a missing/degenerate signal must stay neutral
    rather than poisoning the column."""
    if agg_df.is_empty():
        return agg_df.with_columns(pl.lit(None).cast(pl.Float64).alias("strength_z"))
    return agg_df.with_columns(
        pl.when(
            (pl.col("points_allowed_per_game").std().over("position").fill_null(0.0) == 0.0)
            | (pl.col("points_allowed_per_game").count().over("position") < 2)
        )
        .then(0.0)
        .otherwise(
            (
                (pl.col("points_allowed_per_game") - pl.col("points_allowed_per_game").mean().over("position"))
                / pl.col("points_allowed_per_game").std().over("position")
            ).clip(-DEFENSE_Z_CLAMP, DEFENSE_Z_CLAMP)
        )
        .alias("strength_z")
    )


def persist_defense_strength(conn, rows: list, season: int, source_season: int) -> int:
    if not rows:
        return 0
    upsert_query = """
        INSERT INTO defense_position_strength
            (defense_team, position, season, source_season, points_allowed_per_game, strength_z)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (defense_team, position, season) DO UPDATE SET
            source_season = EXCLUDED.source_season,
            points_allowed_per_game = EXCLUDED.points_allowed_per_game,
            strength_z = EXCLUDED.strength_z,
            updated_at = CURRENT_TIMESTAMP;
    """
    with conn.cursor() as cursor:
        cursor.executemany(upsert_query, rows)
    return len(rows)


def sync_defense_strength(season: int) -> dict:
    source_season = season - 1
    logger.info(f"Syncing defense-vs-position strength for season {season} from source season {source_season}...")

    df_stats = load_prior_season_positional_stats(source_season)
    agg_df = aggregate_points_allowed_by_position(df_stats)
    scored_df = compute_defense_strength_z(agg_df)

    rows = [
        (r["defense_team"], r["position"], season, source_season, r["points_allowed_per_game"], r["strength_z"])
        for r in scored_df.iter_rows(named=True)
    ]
    with get_db_connection() as conn:
        written = persist_defense_strength(conn, rows, season, source_season)

    logger.info(f"Defense-vs-position strength sync complete: {written} row(s) written for season {season}.")
    return {"season": season, "source_season": source_season, "rows_written": written}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync the cold-start defense-vs-position strength proxy (#93) for the upcoming season."
    )
    parser.add_argument(
        "--season", type=int, required=True,
        help="Upcoming season these rows are used FOR; source data is season - 1.",
    )
    args = parser.parse_args()
    sync_defense_strength(season=args.season)
