"""Optional integration checks of the consensus projection SQL (#462) against an explicitly
supplied local disposable DB."""

from contextlib import contextmanager
import os
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import psycopg2
from psycopg2 import sql
import pytest

from src import migrations
from src.ingestion import sync_espn_projections, sync_kicker_dst, sync_projections
from src.optimization import vorp_matrix

SEASON, WEEK = 2026, 13


@pytest.fixture
def postgres():
    dsn = os.getenv("ACTIVITY_TEST_DSN")
    if not dsn:
        pytest.skip("Set ACTIVITY_TEST_DSN to a disposable local Postgres")
    assert psycopg2.extensions.parse_dsn(dsn).get("host") in {"127.0.0.1", "localhost"}
    conn = psycopg2.connect(dsn)
    schema = "consensus_test_" + uuid4().hex
    with conn, conn.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {};").format(sql.Identifier(schema)))
        cursor.execute(sql.SQL("SET search_path TO {};").format(sql.Identifier(schema)))

    @contextmanager
    def cursor_context():
        with conn, conn.cursor() as cursor:
            yield cursor

    @contextmanager
    def connection_context():
        with conn:
            yield conn

    with patch.object(migrations, "get_db_cursor", cursor_context):
        migrations.run_migrations()
        migrations.run_migrations()
    with patch.object(vorp_matrix, "get_db_connection", connection_context):
        try:
            yield conn
        finally:
            conn.rollback()
            with conn, conn.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE;").format(sql.Identifier(schema)))
            conn.close()


def _seed_players(conn):
    with conn, conn.cursor() as cursor:
        cursor.execute(
            """INSERT INTO players (player_id, name, position, team, status, espn_id) VALUES
            ('qb1', 'QB One', 'QB', 'KC', 'Active', '100'),
            ('wr1', 'WR One', 'WR', 'SF', 'Active', '200'),
            ('wr2', 'WR Two', 'WR', 'SF', 'Active', '300'),
            ('k1', 'Kicker', 'K', 'SF', 'Active', '400');"""
        )


def _sleeper(conn, points_by_player):
    frame = sync_projections.build_projection_frame([
        {"player_id": pid, "season": str(SEASON), "week": WEEK, "team": "SF", "opponent": "LAR",
         "stats": {"pts_ppr": pts}}
        for pid, pts in points_by_player.items()
    ])
    with conn:
        sync_projections.persist_projections(conn, frame, set(points_by_player))


def _espn(conn, values):
    with conn:
        sync_espn_projections.persist_espn_projections(conn, values, SEASON, WEEK)


def _rows(conn):
    with conn, conn.cursor() as cursor:
        cursor.execute(
            "SELECT player_id, espn_projected_points, sleeper_projected_points, projected_points "
            "FROM weekly_stats WHERE season = %s AND week = %s ORDER BY player_id;",
            (SEASON, WEEK),
        )
        return {pid: (espn, sleeper, consensus) for pid, espn, sleeper, consensus in cursor.fetchall()}


def test_consensus_follows_the_sources_through_each_sync(postgres):
    _seed_players(postgres)

    # Sleeper only (the ESPN column is still empty): consensus is Sleeper's value.
    _sleeper(postgres, {"qb1": 20.0, "wr1": 10.0})
    assert _rows(postgres) == {"qb1": (None, 20.0, 20.0), "wr1": (None, 10.0, 10.0)}

    # Both sources: the mean. ESPN never inserts a row Sleeper didn't write (wr2).
    _espn(postgres, [("qb1", 18.0), ("wr1", None), ("wr2", 30.0)])
    assert _rows(postgres) == {"qb1": (18.0, 20.0, 19.0), "wr1": (None, 10.0, 10.0)}

    # A Sleeper re-run keeps the ESPN half.
    _sleeper(postgres, {"qb1": 22.0, "wr1": 10.0})
    assert _rows(postgres)["qb1"] == (18.0, 22.0, 20.0)

    # ESPN drops the projection: back to Sleeper alone, never null or 0.
    _espn(postgres, [("qb1", None)])
    assert _rows(postgres)["qb1"] == (None, 22.0, 22.0)


def test_kicker_consensus_is_espns_value(postgres):
    _seed_players(postgres)
    kicker = SimpleNamespace(proTeam="SF", playerId=400, stats={WEEK: {"projected_points": 8.5}})
    with postgres:
        sync_kicker_dst.persist_projections(postgres, [("k1", kicker)], SEASON, WEEK)
    assert _rows(postgres) == {"k1": (8.5, None, 8.5)}


def test_vorp_matrix_reads_the_consensus(postgres):
    _seed_players(postgres)
    _sleeper(postgres, {"qb1": 20.0, "wr1": 10.0, "wr2": 12.0})
    _espn(postgres, [("qb1", 16.0), ("wr1", 20.0), ("wr2", None)])

    vorp_matrix.run_vorp_pipeline(SEASON, WEEK, market_weight=0.0)

    with postgres, postgres.cursor() as cursor:
        cursor.execute(
            "SELECT player_id, adjusted_projected_points FROM weekly_stats "
            "WHERE season = %s AND week = %s AND player_id IN ('qb1', 'wr1', 'wr2') ORDER BY player_id;",
            (SEASON, WEEK),
        )
        adjusted = dict(cursor.fetchall())
    # No implied totals, so matchup_coefficient is 1.0 and adjusted == consensus.
    assert adjusted == {"qb1": 18.0, "wr1": 15.0, "wr2": 12.0}
