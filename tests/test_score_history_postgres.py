"""Optional integration checks against an explicitly supplied local disposable DB."""

from contextlib import contextmanager
from copy import deepcopy
import os
from uuid import uuid4
from unittest.mock import patch

import psycopg2
from psycopg2 import sql
import pytest

from src import migrations
from src.ingestion.sync_actual_scores import persist_week, shape_week
from tests.test_sync_actual_scores import snapshot, box_for, scoreboard


@pytest.fixture
def postgres():
    dsn = os.getenv("SCORE_HISTORY_TEST_DSN")
    if not dsn:
        pytest.skip("Set SCORE_HISTORY_TEST_DSN to a disposable local Postgres")
    assert psycopg2.extensions.parse_dsn(dsn).get("host") in {"127.0.0.1", "localhost"}
    conn = psycopg2.connect(dsn)
    schema = "score_test_" + uuid4().hex
    with conn.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {};").format(sql.Identifier(schema)))
        cursor.execute(sql.SQL("SET search_path TO {};").format(sql.Identifier(schema)))
    conn.commit()

    @contextmanager
    def cursor_context():
        with conn, conn.cursor() as cursor:
            yield cursor

    with patch.object(migrations, "get_db_cursor", cursor_context):
        migrations.run_migrations()
        migrations.run_migrations()
    try:
        yield conn
    finally:
        conn.rollback()
        with conn, conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("DROP SCHEMA {} CASCADE;").format(sql.Identifier(schema))
            )
        conn.close()


def seed(conn):
    with conn, conn.cursor() as cursor:
        cursor.execute(
            """INSERT INTO players (player_id,name,position,team,status,espn_id)
            VALUES ('internal','Player','QB','KC','Active','100');
            INSERT INTO weekly_stats (player_id,season,week,projected_points,vorp_score,targets)
            VALUES ('internal',2026,4,19,7,3);"""
        )
    raw = snapshot.__wrapped__()
    return shape_week(raw, box_for(raw), scoreboard(), "123", 2026, 4)


def test_migrations_reruns_corrections_and_missing_mappings(postgres):
    data = seed(postgres)
    with postgres:
        persist_week(postgres, data)
        persist_week(postgres, data)
    with postgres.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM fantasy_team_week_scores;")
        assert cursor.fetchone() == (2,)
        cursor.execute(
            "SELECT count(*) FROM fantasy_player_week_scores WHERE player_id IS NULL;"
        )
        assert cursor.fetchone() == (1,)
        cursor.execute(
            "SELECT actual_points,projected_points,vorp_score,targets,actual_points_source FROM weekly_stats;"
        )
        assert cursor.fetchone() == (10, 19, 7, 3, "espn:123")
    data["players"][0]["actual_points"] = -2
    data["teams"][0]["score"] = -2
    with postgres:
        persist_week(postgres, data)
    with postgres.cursor() as cursor:
        cursor.execute("SELECT actual_points FROM weekly_stats;")
        assert cursor.fetchone() == (-2,)
    data["players"] = [p for p in data["players"] if p["espn_id"] != "100"]
    with postgres:
        persist_week(postgres, data)
    with postgres.cursor() as cursor:
        cursor.execute(
            "SELECT actual_points,actual_points_source,vorp_score FROM weekly_stats;"
        )
        assert cursor.fetchone() == (None, None, 7)


def test_failed_reingestion_rolls_back_deletes_and_actuals(postgres):
    data = seed(postgres)
    with postgres:
        persist_week(postgres, data)
    bad = deepcopy(data)
    bad["teams"][0]["period"] = 999
    with pytest.raises(psycopg2.IntegrityError):
        with postgres:
            persist_week(postgres, bad)
    with postgres.cursor() as cursor:
        cursor.execute("SELECT actual_points FROM weekly_stats;")
        assert cursor.fetchone() == (10,)
        cursor.execute("SELECT count(*) FROM fantasy_team_week_scores;")
        assert cursor.fetchone() == (2,)
