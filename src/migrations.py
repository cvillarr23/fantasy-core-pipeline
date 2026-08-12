import logging
from src.database import get_db_cursor
from src.models import (
    CREATE_PLAYERS_TABLE,
    ADD_PLAYERS_GSIS_ID_COLUMN,
    ADD_PLAYERS_ESPN_ID_COLUMN,
    CREATE_WEEKLY_STATS_TABLE,
    ADD_WEEKLY_STATS_ADVANCED_COLUMNS,
    ADD_WEEKLY_STATS_VORP_COLUMNS,
    MIGRATE_WEEKLY_STATS_PK_ADD_SEASON,
    CREATE_LEAGUE_ROSTERS_TABLE,
    ADD_LEAGUE_ROSTERS_IS_STARTER_COLUMN,
    CREATE_LEAGUE_SETTINGS_TABLE,
    CREATE_EXECUTION_LOG_TABLE,
    CREATE_INDICES
)

logger = logging.getLogger("MigrationsEngine")

def run_migrations():
    """
    Validates connection pool liveness and executes table schemas 
    sequentially using IF NOT EXISTS guards on cluster boot.
    """
    logger.info("Starting structural database migrations...")
    try:
        with get_db_cursor() as cursor:
            # 1. Execute base master metadata tables
            logger.info("Validating 'players' metadata framework...")
            cursor.execute(CREATE_PLAYERS_TABLE)
            cursor.execute(ADD_PLAYERS_GSIS_ID_COLUMN)
            cursor.execute(ADD_PLAYERS_ESPN_ID_COLUMN)

            # 2. Execute relational time-series metric tables
            logger.info("Validating 'weekly_stats' telemetry tracking...")
            cursor.execute(CREATE_WEEKLY_STATS_TABLE)
            cursor.execute(ADD_WEEKLY_STATS_ADVANCED_COLUMNS)
            cursor.execute(ADD_WEEKLY_STATS_VORP_COLUMNS)
            cursor.execute(MIGRATE_WEEKLY_STATS_PK_ADD_SEASON)
            # MIGRATE_WEEKLY_STATS_PK_ADD_SEASON silently skips (via a Postgres NOTICE, not
            # an error) if pre-existing rows have a NULL season. If that happened, the CORE-04
            # extractor's `ON CONFLICT (player_id, season, week)` upsert has no matching
            # constraint to target and every insert would fail. Fail startup loudly here
            # instead of letting that surface later as an opaque psycopg2 error mid-ingestion.
            cursor.execute("""
                SELECT 1 FROM information_schema.key_column_usage
                WHERE table_name = 'weekly_stats' AND constraint_name = 'weekly_stats_pkey'
                AND column_name = 'season';
            """)
            if cursor.fetchone() is None:
                raise RuntimeError(
                    "weekly_stats has pre-existing rows with a NULL season, so its primary key "
                    "could not be migrated to (player_id, season, week). CORE-04 ingestion cannot "
                    "run until those rows are backfilled with a reviewed season value and this "
                    "migration is re-run."
                )

            # 3. Execute league active configurations roster tables
            logger.info("Validating 'league_rosters' target matrices...")
            cursor.execute(CREATE_LEAGUE_ROSTERS_TABLE)
            cursor.execute(ADD_LEAGUE_ROSTERS_IS_STARTER_COLUMN)

            logger.info("Validating 'league_settings' configuration table...")
            cursor.execute(CREATE_LEAGUE_SETTINGS_TABLE)

            # 4. Execute worker execution telemetry table (INFRA-06)
            logger.info("Validating 'execution_log' worker telemetry table...")
            cursor.execute(CREATE_EXECUTION_LOG_TABLE)

            # 5. Generate query speed optimizations
            logger.info("Optimizing relational query indices...")
            cursor.execute(CREATE_INDICES)
            
        logger.info("Database migrations executed cleanly. System is ready.")
    except Exception as e:
        logger.error(f"Migration sequence aborted due to a critical error: {e}")
        raise

if __name__ == "__main__":
    # Allows manual migration execution blocks during debugging phases
    run_migrations()

