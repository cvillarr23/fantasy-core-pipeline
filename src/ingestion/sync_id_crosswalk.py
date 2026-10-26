"""
Syncs the DynastyProcess ID crosswalk (sleeper_id / espn_id / gsis_id) onto the `players`
table. Sleeper's own `espn_id`/`gsis_id` fields are stale for a large share of live skill
players, so this crosswalk is the only reliable way to join Sleeper-keyed rows (`players`
is keyed on `player_id` = Sleeper ID) onto ESPN roster data and nflverse's gsis-keyed feeds.

Must run after `sync_players` (which creates the base player rows) and before
`sync_league` (needs espn_id) and `metrics_extractor` (needs a fresh gsis_id).
"""
import logging

import nflreadpy as nfl
import polars as pl

from src.database import get_db_connection
from src.services import run_log_service

from src.telemetry import configure_trace_logging

configure_trace_logging()
logger = logging.getLogger("IDCrosswalkSync")

CROSSWALK_COLUMNS = ["sleeper_id", "espn_id", "gsis_id"]


def load_crosswalk() -> pl.DataFrame:
    """Loads the DynastyProcess crosswalk, casts IDs to Utf8, drops rows with no
    sleeper_id (nothing to join to `players` on), and dedupes on sleeper_id."""
    df = nfl.load_ff_playerids().select(CROSSWALK_COLUMNS)
    return (
        df.with_columns([pl.col(c).cast(pl.Utf8) for c in CROSSWALK_COLUMNS])
        .drop_nulls(subset=["sleeper_id"])
        .unique(subset=["sleeper_id"], keep="first")
    )


def persist_crosswalk(conn, crosswalk: pl.DataFrame, existing_player_ids: set) -> int:
    """
    Updates only rows for player_ids already present in `players` (inserted by
    sync_players). COALESCE on gsis_id so a crosswalk miss (NULL) never erases a good
    existing value from Sleeper's own payload, and on espn_id so it never erases one
    sync_kicker_dst or sync_skill_position_crosswalk backfilled: the crosswalk lists
    kickers (as "PK") and some skill players with a sleeper_id but no espn_id. Returns the number of players actually
    matched, computed against `existing_player_ids` rather than cursor.rowcount, whose
    accumulation semantics across executemany() batches aren't guaranteed by psycopg2.
    """
    if crosswalk.is_empty():
        return 0
    update_query = """
        UPDATE players SET espn_id = COALESCE(%s, espn_id), gsis_id = COALESCE(%s, gsis_id)
        WHERE player_id = %s;
    """
    rows = [
        (row["espn_id"], row["gsis_id"], row["sleeper_id"])
        for row in crosswalk.iter_rows(named=True)
    ]
    matched = sum(1 for row in rows if row[2] in existing_player_ids)
    with conn.cursor() as cursor:
        cursor.executemany(update_query, rows)
    return matched


def sync_id_crosswalk() -> int:
    """Loads and persists the DynastyProcess crosswalk. Returns the number of `players`
    rows actually matched and updated."""
    logger.info("Loading DynastyProcess ID crosswalk (sleeper_id/espn_id/gsis_id)...")
    crosswalk = load_crosswalk()
    logger.info(f"Crosswalk loaded: {crosswalk.height} row(s) with a sleeper_id.")

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT player_id FROM players;")
            existing_player_ids = {row[0] for row in cursor.fetchall()}
        matched = persist_crosswalk(conn, crosswalk, existing_player_ids)

    total_players = len(existing_player_ids)
    match_rate = (matched / total_players * 100) if total_players else 0.0
    logger.info(
        f"ID crosswalk sync complete: matched {matched}/{total_players} players "
        f"({match_rate:.1f}%)."
    )
    return matched


if __name__ == "__main__":
    with run_log_service.run_scope("sync_id_crosswalk"):
        sync_id_crosswalk()
