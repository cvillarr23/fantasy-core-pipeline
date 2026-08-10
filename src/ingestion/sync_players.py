import json
import logging
import os
import sys
from datetime import datetime, timezone

import pika
import polars as pl
import redis
import requests

from src.database import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SleeperPlayerSync")

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@fantasy-broker.databases.svc.cluster.local:5672//")
REDIS_HOST = os.getenv("REDIS_HOST", "fantasy-cache.default.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

SLEEPER_PLAYERS_URL = "https://api.sleeper.app/v1/players/nfl"
SKILL_POSITIONS = ["QB", "RB", "WR", "TE"]
ALERT_INJURY_STATUSES = {"Questionable", "Doubtful", "Out"}

SYNC_LOCK_KEY = "sleeper:sync:last_run"
SYNC_LOCK_TTL_SECONDS = 24 * 60 * 60
FETCH_RETRY_ATTEMPTS = 3
FETCH_CONNECT_TIMEOUT_SECONDS = 5

WAIVER_ACTIONS_QUEUE = "waiver_actions"


def fetch_sleeper_players() -> dict:
    """Fetches the flat master player payload from the live Sleeper API, retrying on failure."""
    last_error = None
    for attempt in range(1, FETCH_RETRY_ATTEMPTS + 1):
        try:
            logger.info(f"Fetching Sleeper player matrix (attempt {attempt}/{FETCH_RETRY_ATTEMPTS})...")
            response = requests.get(SLEEPER_PLAYERS_URL, timeout=FETCH_CONNECT_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            last_error = e
            logger.warning(f"Sleeper API fetch attempt {attempt} failed: {e}")
    logger.error(f"Exhausted all {FETCH_RETRY_ATTEMPTS} attempts fetching Sleeper player matrix.")
    raise last_error


def should_run_master_sync(r_client: "redis.Redis", force: bool) -> bool:
    """Once-per-day gate: only fetch from Sleeper if forced or no recent sync is recorded."""
    if force:
        return True
    return not r_client.exists(SYNC_LOCK_KEY)


def mark_synced(r_client: "redis.Redis") -> None:
    r_client.setex(SYNC_LOCK_KEY, SYNC_LOCK_TTL_SECONDS, datetime.now(timezone.utc).isoformat())


def filter_skill_position_players(raw_data: dict) -> pl.DataFrame:
    """Retains only active-roster skill-position players, shaped to match the `players` table."""
    records = []
    for player_id, info in raw_data.items():
        if not info:
            continue
        if info.get("position") not in SKILL_POSITIONS:
            continue
        if not info.get("team"):
            continue
        records.append({
            "player_id": player_id,
            "name": f"{info.get('first_name', '')} {info.get('last_name', '')}".strip(),
            "position": info.get("position"),
            "team": info.get("team"),
            "status": info.get("status") or "Unknown",
            "injury_status": info.get("injury_status") or "Healthy",
        })
    schema = {"player_id": pl.Utf8, "name": pl.Utf8, "position": pl.Utf8, "team": pl.Utf8, "status": pl.Utf8, "injury_status": pl.Utf8}
    return pl.DataFrame(records, schema=schema)


def load_existing_players(cursor) -> dict:
    """Loads the current DB state keyed by player_id, so the diff is O(1) per incoming player."""
    cursor.execute("SELECT player_id, status, injury_status FROM players;")
    return {row[0]: {"status": row[1], "injury_status": row[2]} for row in cursor.fetchall()}


def load_rostered_player_ids(cursor) -> set:
    """Players actually held on a fantasy roster - our proxy for 'starting-tier' relevance."""
    cursor.execute("SELECT DISTINCT player_id FROM league_rosters;")
    return {row[0] for row in cursor.fetchall()}


def diff_players(incoming_df: pl.DataFrame, existing_by_id: dict) -> list:
    """Returns only new or changed rows, each tagged `is_transition` (False for brand-new players)."""
    changed = []
    for row in incoming_df.iter_rows(named=True):
        existing = existing_by_id.get(row["player_id"])
        if existing is None:
            changed.append({**row, "is_transition": False})
            continue
        if existing["status"] != row["status"] or existing["injury_status"] != row["injury_status"]:
            changed.append({**row, "is_transition": True})
    return changed


def persist_changes(conn, changed_rows: list) -> None:
    if not changed_rows:
        return
    upsert_query = """
        INSERT INTO players (player_id, name, position, team, status, injury_status, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (player_id) DO UPDATE SET
            name = EXCLUDED.name,
            position = EXCLUDED.position,
            team = EXCLUDED.team,
            status = EXCLUDED.status,
            injury_status = EXCLUDED.injury_status,
            updated_at = CURRENT_TIMESTAMP;
    """
    with conn.cursor() as cursor:
        cursor.executemany(upsert_query, [
            (row["player_id"], row["name"], row["position"], row["team"], row["status"], row["injury_status"])
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
    connection = None
    try:
        params = pika.URLParameters(AMQP_URL)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=WAIVER_ACTIONS_QUEUE, durable=True)

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
    except Exception as e:
        logger.error(f"Failed to publish waiver alerts to RabbitMQ: {e}")
    finally:
        if connection is not None:
            connection.close()


def sync_players(force: bool = False) -> None:
    try:
        r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r_client.ping()
    except Exception as e:
        logger.error(f"Redis cache layer unavailable, aborting sync: {e}")
        return

    if not should_run_master_sync(r_client, force):
        logger.info("Sleeper sync already ran within the last 24h; skipping fetch.")
        return

    try:
        raw_data = fetch_sleeper_players()
    except Exception:
        return

    incoming_df = filter_skill_position_players(raw_data)
    logger.info(f"Filtered Sleeper payload down to {incoming_df.height} active skill-position players.")

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
    sync_players(force="--force" in sys.argv)
