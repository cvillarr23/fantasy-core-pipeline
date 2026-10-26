import json
import logging
import os
import sys
from datetime import datetime, timezone

import pika
import polars as pl
import redis
import requests

from src.queue_contracts import WAIVER_ACTIONS as WAIVER_ACTIONS_QUEUE
from src.database import get_db_connection, get_redis_client
from src.services import run_log_service

logger = logging.getLogger("SleeperPlayerSync")

# Default vhost must be the percent-encoded "%2f", not a bare trailing "/" - a bare "//"
# parses as vhost "" (empty string) in pika, not "/", and RabbitMQ then rejects the
# connection with "vhost  not found" instead of connecting to the default vhost.
AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@fantasy-broker.databases.svc.cluster.local:5672/%2f")

SLEEPER_PLAYERS_URL = "https://api.sleeper.app/v1/players/nfl"
# Sleeper labels team defenses "DEF"; everywhere else in this codebase (lineup_solver's
# DEFAULT_LINEUP_SLOT_COUNTS, sync_league's POSITION_MAP, league_settings.lineup_slot_counts)
# uses ESPN's "D/ST" label instead - SLEEPER_TO_INTERNAL_POSITION retags it at ingestion so
# every downstream position lookup (`lineup_slot_counts.get("D/ST")`, etc.) actually matches.
ROSTERABLE_POSITIONS = ["QB", "RB", "WR", "TE", "K", "DEF"]
SLEEPER_TO_INTERNAL_POSITION = {"DEF": "D/ST"}
ALERT_INJURY_STATUSES = {"Questionable", "Doubtful", "Out"}

SYNC_LOCK_KEY = "sleeper:sync:last_run"
SYNC_LOCK_TTL_SECONDS = 24 * 60 * 60
SYNC_IN_PROGRESS_TTL_SECONDS = 5 * 60
FETCH_RETRY_ATTEMPTS = 3
FETCH_CONNECT_TIMEOUT_SECONDS = 5
FETCH_READ_TIMEOUT_SECONDS = 30


def fetch_sleeper_players() -> dict:
    """Fetches the flat master player payload from the live Sleeper API, retrying on failure."""
    last_error = None
    for attempt in range(1, FETCH_RETRY_ATTEMPTS + 1):
        try:
            logger.info(f"Fetching Sleeper player matrix (attempt {attempt}/{FETCH_RETRY_ATTEMPTS})...")
            response = requests.get(
                SLEEPER_PLAYERS_URL,
                timeout=(FETCH_CONNECT_TIMEOUT_SECONDS, FETCH_READ_TIMEOUT_SECONDS),
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            last_error = e
            logger.warning(f"Sleeper API fetch attempt {attempt} failed: {e}")
    logger.error(f"Exhausted all {FETCH_RETRY_ATTEMPTS} attempts fetching Sleeper player matrix.")
    raise last_error


def acquire_sync_lock(r_client: "redis.Redis", force: bool) -> bool:
    """
    Once-per-day gate, claimed atomically via SET NX so overlapping/parallel runs can't
    both pass the check and double-fetch Sleeper. Grabs a short-lived "in progress" lock;
    mark_synced() extends it to the full 24h window once the run actually succeeds. If a
    run crashes before that, the short TTL expires and the next run can retry instead of
    being locked out for a day.
    """
    if force:
        r_client.delete(SYNC_LOCK_KEY)
    return bool(r_client.set(SYNC_LOCK_KEY, "in_progress", nx=True, ex=SYNC_IN_PROGRESS_TTL_SECONDS))


def mark_synced(r_client: "redis.Redis") -> None:
    r_client.set(SYNC_LOCK_KEY, datetime.now(timezone.utc).isoformat(), ex=SYNC_LOCK_TTL_SECONDS)


def filter_skill_position_players(raw_data: dict) -> pl.DataFrame:
    """Retains only active-roster rosterable-position players, shaped to match the
    `players` table. Sleeper's "DEF" position label is retagged to "D/ST" here (see
    SLEEPER_TO_INTERNAL_POSITION) so it matches this codebase's convention everywhere else."""
    records = []
    for player_id, info in raw_data.items():
        if not info:
            continue
        position = info.get("position")
        if position not in ROSTERABLE_POSITIONS:
            continue
        if not info.get("team"):
            continue
        records.append({
            "player_id": player_id,
            "name": f"{info.get('first_name', '')} {info.get('last_name', '')}".strip(),
            "position": SLEEPER_TO_INTERNAL_POSITION.get(position, position),
            "team": info.get("team"),
            "status": info.get("status") or "Unknown",
            "injury_status": info.get("injury_status") or "Healthy",
            "gsis_id": info.get("gsis_id"),
        })
    schema = {
        "player_id": pl.Utf8, "name": pl.Utf8, "position": pl.Utf8, "team": pl.Utf8,
        "status": pl.Utf8, "injury_status": pl.Utf8, "gsis_id": pl.Utf8,
    }
    return pl.DataFrame(records, schema=schema)


def assert_position_coverage(df: pl.DataFrame) -> None:
    """Fails loudly if any expected position class comes back empty from Sleeper - #83's
    fix for a missing position class (K/D-ST had zero rows) silently corrupting the
    undrafted pool instead of failing the ingestion job at sync time."""
    expected = {SLEEPER_TO_INTERNAL_POSITION.get(pos, pos) for pos in ROSTERABLE_POSITIONS}
    present = set(df["position"].unique().to_list()) if df.height else set()
    missing = expected - present
    if missing:
        raise RuntimeError(
            f"Sleeper sync returned zero players for position(s) {sorted(missing)} - "
            "refusing to persist a partial player set. Check the upstream payload before retrying."
        )


def load_existing_players(cursor) -> dict:
    """Loads the current DB state keyed by player_id, so the diff is O(1) per incoming player."""
    cursor.execute("SELECT player_id, status, injury_status FROM players;")
    return {row[0]: {"status": row[1], "injury_status": row[2]} for row in cursor.fetchall()}


def load_rostered_player_ids(cursor) -> set:
    """Players actually held on a fantasy roster - our proxy for 'starting-tier' relevance."""
    cursor.execute("SELECT DISTINCT player_id FROM league_rosters;")
    return {row[0] for row in cursor.fetchall()}


def diff_players(incoming_df: pl.DataFrame, existing_by_id: dict) -> list:
    """
    Returns only new or changed rows. `is_transition` is True only for an actual
    injury_status change (what the waiver alert cares about) - a status-only change
    (e.g. roster/active flag) is still persisted but won't trigger an alert.
    """
    changed = []
    for row in incoming_df.iter_rows(named=True):
        existing = existing_by_id.get(row["player_id"])
        if existing is None:
            changed.append({**row, "is_transition": False})
            continue
        status_changed = existing["status"] != row["status"]
        injury_changed = existing["injury_status"] != row["injury_status"]
        if status_changed or injury_changed:
            changed.append({**row, "is_transition": injury_changed})
    return changed


def persist_changes(conn, changed_rows: list) -> None:
    if not changed_rows:
        return
    upsert_query = """
        INSERT INTO players (player_id, name, position, team, status, injury_status, gsis_id, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (player_id) DO UPDATE SET
            name = EXCLUDED.name,
            position = EXCLUDED.position,
            team = EXCLUDED.team,
            status = EXCLUDED.status,
            injury_status = EXCLUDED.injury_status,
            gsis_id = EXCLUDED.gsis_id,
            updated_at = CURRENT_TIMESTAMP;
    """
    with conn.cursor() as cursor:
        cursor.executemany(upsert_query, [
            (row["player_id"], row["name"], row["position"], row["team"], row["status"], row["injury_status"], row["gsis_id"])
            for row in changed_rows
        ])
    logger.info(f"Persisted {len(changed_rows)} new/changed player rows to PostgreSQL.")


def publish_waiver_alerts(changed_rows: list, rostered_ids: set) -> None:
    alertable = [
        row for row in changed_rows
        if row["is_transition"]
        and row["player_id"] in rostered_ids
        and row["injury_status"] in ALERT_INJURY_STATUSES
    ]
    if not alertable:
        return

    logger.info(f"Publishing {len(alertable)} waiver alert(s) to '{WAIVER_ACTIONS_QUEUE}'...")
    params = pika.URLParameters(AMQP_URL)
    connection = pika.BlockingConnection(params)
    try:
        channel = connection.channel()
        channel.queue_declare(queue=WAIVER_ACTIONS_QUEUE, passive=True)

        for row in alertable:
            payload = {
                "event_type": "INJURY_STATUS_CHANGE",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "player_id": row["player_id"],
                "name": row["name"],
                "team": row["team"],
                "new_injury_status": row["injury_status"],
            }
            channel.basic_publish(
                exchange="",
                routing_key=WAIVER_ACTIONS_QUEUE,
                body=json.dumps(payload),
                properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
            )
            logger.info(f"CRITICAL STATUS DETECTED: alerted on {row['name']} ({row['team']}) -> {row['injury_status']}")
    finally:
        connection.close()


def sync_players(force: bool = False) -> None:
    try:
        r_client = get_redis_client()
        r_client.ping()
    except Exception as e:
        logger.error(f"Redis cache layer unavailable, aborting sync: {e}")
        return

    if not acquire_sync_lock(r_client, force):
        logger.info("Sleeper sync already ran within the last 24h (or is in progress); skipping fetch.")
        return

    try:
        raw_data = fetch_sleeper_players()
    except Exception:
        return

    incoming_df = filter_skill_position_players(raw_data)
    logger.info(f"Filtered Sleeper payload down to {incoming_df.height} active rosterable-position players.")
    assert_position_coverage(incoming_df)

    # Persist and publish inside the same DB transaction: if the RabbitMQ publish fails,
    # the DB changes roll back too, so the next run's diff sees the transition again
    # instead of silently losing the alert.
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            existing_by_id = load_existing_players(cursor)
            rostered_ids = load_rostered_player_ids(cursor)

        changed_rows = diff_players(incoming_df, existing_by_id)
        logger.info(f"Detected {len(changed_rows)} new/changed player rows.")

        persist_changes(conn, changed_rows)
        publish_waiver_alerts(changed_rows, rostered_ids)

    mark_synced(r_client)


if __name__ == "__main__":
    with run_log_service.run_scope("sync_players"):
        sync_players(force="--force" in sys.argv)
