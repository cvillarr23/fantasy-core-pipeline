"""Durable rumor retry state; short transactions never span gateway calls.

The version check prevents overlapping HTTP/Job invocations overwriting progress.
Prepared actions are immutable, so overlapping deliveries use the same gateway key.
Redis is a compatibility mirror; PostgreSQL owns the cursor after initialization.
"""

from psycopg2.extras import Json

from src.database import get_db_connection


def load_checkpoint(team_id, initial_watermark=None):
    """Read durable state without consulting Redis after the first initialization."""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT version, state FROM rumor_action_checkpoints WHERE team_id = %s",
                (str(team_id),),
            )
            row = cursor.fetchone()
            if row is not None:
                return row

    # Preserve the legacy cursor only when creating a team's first durable checkpoint.
    if initial_watermark is None:
        from src.api.draft import _redis_client

        value = _redis_client().get("rumor_action:last_processed_id")
        initial_watermark = int(value) if value is not None else 0
    initial = {
        "watermark": initial_watermark,
        "signals": {},
        "actions": {},
        "failed_jobs": {},
    }
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO rumor_action_checkpoints (team_id, state) VALUES (%s, %s) "
                "ON CONFLICT (team_id) DO NOTHING",
                (str(team_id), Json(initial)),
            )
            cursor.execute(
                "SELECT version, state FROM rumor_action_checkpoints WHERE team_id = %s",
                (str(team_id),),
            )
            return cursor.fetchone()


def save_checkpoint(team_id, version, state):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE rumor_action_checkpoints SET state = %s, version = version + 1, "
                "updated_at = NOW() WHERE team_id = %s AND version = %s RETURNING version",
                (Json(state), str(team_id), version),
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(
                    "Concurrent rumor-action run changed the checkpoint; retry"
                )
            return row[0]
