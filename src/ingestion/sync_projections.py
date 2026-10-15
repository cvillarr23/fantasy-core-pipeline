"""
Syncs Sleeper's weekly fantasy projections into weekly_stats.projected_points. Nothing
else populated projected_points before this module existed, so every VORP score computed
against this column previously ran against the `0.0` DEFAULT.
"""
import argparse
import logging

import polars as pl
import requests

from src.database import get_db_connection
from src.services import run_log_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ProjectionsSync")

SLEEPER_PROJECTIONS_URL = "https://api.sleeper.com/projections/nfl/{season}/{week}"
SKILL_POSITIONS = ["QB", "RB", "WR", "TE"]

# The ESPN league this pipeline serves scores receptions at 1.0 (statId 53) - i.e. full
# PPR - confirmed live against .../mSettings. If the league's scoring settings ever
# change, this constant (and the ESPN scoring verification) both need to be re-checked.
SCORING_FIELD = "pts_ppr"

FETCH_RETRY_ATTEMPTS = 3
FETCH_CONNECT_TIMEOUT_SECONDS = 5
FETCH_READ_TIMEOUT_SECONDS = 30


def fetch_projections(season: int, week: int) -> list:
    """Fetches Sleeper's weekly projections, one request per skill position, retrying
    each on failure. Mirrors sync_players.fetch_sleeper_players's retry loop."""
    url = SLEEPER_PROJECTIONS_URL.format(season=season, week=week)
    rows = []
    for position in SKILL_POSITIONS:
        params = {"season_type": "regular", "position[]": position, "order_by": SCORING_FIELD}
        last_error = None
        for attempt in range(1, FETCH_RETRY_ATTEMPTS + 1):
            try:
                logger.info(
                    f"Fetching Sleeper {position} projections for {season} week {week} "
                    f"(attempt {attempt}/{FETCH_RETRY_ATTEMPTS})..."
                )
                response = requests.get(
                    url, params=params,
                    timeout=(FETCH_CONNECT_TIMEOUT_SECONDS, FETCH_READ_TIMEOUT_SECONDS),
                )
                response.raise_for_status()
                rows.extend(response.json())
                last_error = None
                break
            except requests.RequestException as e:
                last_error = e
                logger.warning(f"Sleeper projections fetch attempt {attempt} for {position} failed: {e}")
        if last_error is not None:
            logger.error(f"Exhausted all {FETCH_RETRY_ATTEMPTS} attempts fetching {position} projections.")
            raise last_error
    return rows


def _is_bye_row(row: dict) -> bool:
    """Sleeper lists a player whose team is on bye with his team but no opponent and no
    scoring stats. A teamless player (no NFL team at all) isn't on a bye."""
    return bool(row.get("team")) and row.get("opponent") is None


def bye_week_player_ids(raw: list) -> set:
    """Pure. Player ids Sleeper lists as on their team's bye for the fetched week."""
    return {row.get("player_id") for row in raw if _is_bye_row(row) and row.get("player_id")}


def build_projection_frame(raw: list) -> pl.DataFrame:
    """Pure. Shapes the raw Sleeper payload to (player_id, season, week,
    projected_points), defaulting a missing/null scoring field to 0.0.

    Bye-week players get no row, never a 0.0 one, matching sync_kicker_dst: lineup,
    waiver and trade code read a missing row as "on bye", while a 0.0 row looks like a
    startable player with a negative VORP."""
    schema = {"player_id": pl.Utf8, "season": pl.Int64, "week": pl.Int64, "projected_points": pl.Float64}
    records = []
    for row in raw:
        if _is_bye_row(row):
            continue
        stats = row.get("stats") or {}
        records.append({
            "player_id": row.get("player_id"),
            "season": int(row["season"]) if row.get("season") is not None else None,
            "week": row.get("week"),
            "projected_points": stats.get(SCORING_FIELD) if stats.get(SCORING_FIELD) is not None else 0.0,
        })
    return pl.DataFrame(records, schema=schema)


def persist_projections(conn, frame: pl.DataFrame, known_player_ids: set) -> int:
    """
    Upsert that touches only projected_points, so CORE-04's advanced-metric columns are
    never clobbered - the mirror image of metrics_extractor's UPSERT_COLUMNS discipline.
    Filtered to player_ids already present in `players` (FK + implicitly excludes K/DEF,
    which sync_players never inserts).
    """
    rows = [
        (r["player_id"], r["season"], r["week"], r["projected_points"])
        for r in frame.iter_rows(named=True)
        if r["player_id"] in known_player_ids
    ]
    if not rows:
        return 0
    upsert_query = """
        INSERT INTO weekly_stats (player_id, season, week, projected_points)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (player_id, season, week) DO UPDATE SET projected_points = EXCLUDED.projected_points;
    """
    with conn.cursor() as cursor:
        cursor.executemany(upsert_query, rows)
    return len(rows)


def delete_bye_week_projections(conn, season: int, week: int, player_ids: set) -> int:
    """Removes the 0.0 rows earlier runs wrote for players on bye in `week`. The refresher
    re-targets the same weeks daily, so without this a bye player keeps his stale row.
    Rows with synced actual scores are left alone."""
    if not player_ids:
        return 0
    with conn.cursor() as cursor:
        cursor.execute(
            """
            DELETE FROM weekly_stats
            WHERE player_id = ANY(%s) AND season = %s AND week = %s
              AND actual_points_source IS NULL;
            """,
            (sorted(player_ids), season, week),
        )
        return cursor.rowcount


def sync_projections(season: int, week: int) -> int:
    logger.info(f"Syncing Sleeper projections for {season} week {week} (scoring field={SCORING_FIELD})...")
    raw = fetch_projections(season, week)
    frame = build_projection_frame(raw)
    logger.info(f"Fetched {frame.height} projection row(s) across {SKILL_POSITIONS}.")

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT player_id FROM players;")
            known_player_ids = {row[0] for row in cursor.fetchall()}
        written = persist_projections(conn, frame, known_player_ids)
        removed = delete_bye_week_projections(conn, season, week, bye_week_player_ids(raw))
    if removed:
        logger.info(f"Removed {removed} stale projection row(s) for players on bye in week {week}.")

    logger.info(f"Persisted {written} projected_points row(s) for {season} week {week}.")
    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Sleeper weekly projections into weekly_stats.")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int, required=True)
    args = parser.parse_args()
    with run_log_service.run_scope("sync_projections", trigger={"season": args.season, "week": args.week}):
        sync_projections(season=args.season, week=args.week)
