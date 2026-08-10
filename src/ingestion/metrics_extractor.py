import os
import logging
import nflreadpy as nfl
import polars as pl
from src.database import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("NFLVerseIngestor")

def extract_weekly_metrics(year: int, week: int):
    """
    Ingests official play-by-play blocks, runs utilization math via Polars,
    and commits volume indicators to PostgreSQL weekly tables.
    """
    logger.info(f"Beginning nflverse utilization parsing loop for {year} Week {week}...")
    
    try:
        # Load raw seasonal data from the active nflverse repo infrastructure
        pbp_raw = nfl.load_pbp_data(year)
        df_pbp = pl.from_pandas(pbp_raw)
        logger.info("Play-by-play data blocks loaded cleanly into Polars context.")
    except Exception as e:
        logger.error(f"Failed to stream datasets from nflverse repository mirrors: {e}")
        return

    # Filter down immediately to our targeted week and valid offensive plays
    df_week = df_pbp.filter((pl.col("week") == week) & (pl.col("play_type").is_in(["pass", "run"])))
    
    if df_week.height == 0:
        logger.warning(f"No active record data rows located for targeted frame: {year} Week {week}.")
        return

    logger.info("Computing advanced market share volume metrics...")
    
    # 1. Isolate total team-level offensive play counts to compute denominators
    team_plays = df_week.group_by("posteam").agg(
        pl.len().alias("total_team_plays"),
        pl.col("pass").sum().alias("total_team_passes")
    )

    # 2. Isolate target allocations and touch metrics per individual player tracking ID
    # Note: Using nflverse common tracking variables (receiver_id, rusher_id)
    targets = df_week.filter(pl.col("receiver_id").is_not_null()).group_by(["receiver_id", "posteam"]).agg(
        pl.len().alias("player_targets"),
        (pl.col("yardline_10") <= 10).fill_null(False).sum().alias("redzone_targets")
    ).rename({"receiver_id": "gsis_id"})

    carries = df_week.filter(pl.col("rusher_id").is_not_null()).group_by(["rusher_id", "posteam"]).agg(
        pl.len().alias("player_carries"),
        (pl.col("yardline_10") <= 10).fill_null(False).sum().alias("redzone_carries")
    ).rename({"rusher_id": "gsis_id"})

    # Combine data tracking layers together
    player_volume = targets.join(carries, on=["gsis_id", "posteam"], how="outer").join(team_plays, on="posteam", how="left")

    # 3. Compute structural share fields using lazy fill evaluations
    processed_stats = player_volume.with_columns([
        (pl.col("player_targets").fill_null(0) / pl.col("total_team_passes").fill_null(1)).alias("target_share"),
        (pl.col("redzone_targets").fill_null(0) + pl.col("redzone_carries").fill_null(0)).alias("high_value_touches")
    ])

    # Batch save metrics straight down into the PostgreSQL weekly tables
    logger.info("Syncing utilization metrics down to stateful storage...")
    
    # Map nfl gsis_ids back to local player IDs via players.gsis_id (populated by the
    # Sleeper sync - nflverse gsis_ids and Sleeper player_ids are different ID spaces,
    # so this can't be matched by name or by player_id directly).
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            upsert_query = """
                INSERT INTO weekly_stats (player_id, week, target_share, high_value_touches)
                VALUES (
                    (SELECT player_id FROM players WHERE gsis_id = %s LIMIT 1),
                    %s, %s, %s
                )
                ON CONFLICT (player_id, week) DO UPDATE SET
                    target_share = EXCLUDED.target_share,
                    high_value_touches = EXCLUDED.high_value_touches;
            """

            # Pack values cleanly into tuple lists for optimal execution performance
            batch_data = []
            for row in processed_stats.iter_rows(named=True):
                if row["gsis_id"]:
                    batch_data.append((
                        row["gsis_id"], week,
                        float(row["target_share"]), int(row["high_value_touches"])
                    ))

            cursor.executemany(upsert_query, batch_data)
            
    logger.info(f"Successfully processed and committed {len(batch_data)} metric rows for Week {week}.")

if __name__ == "__main__":
    # Default trigger context targeting baseline parameters
    extract_weekly_metrics(year=2026, week=1)

