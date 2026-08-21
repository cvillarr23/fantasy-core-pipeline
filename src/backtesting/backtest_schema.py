"""
Isolated, wipeable Postgres schema provisioning for the historical backtesting harness
(CORE-15).

src.database.db_pool is a module-level ThreadedConnectionPool that get_db_connection/
get_db_cursor re-look-up from module globals on every call (not a captured closure value),
so a second, backtesting-owned pool whose connections are pinned to a different schema via
the `options=-c search_path=...` connect param can be swapped into src.database.db_pool for
the duration of a backtest run. Every existing function in vorp_matrix.py/lineup_solver.py/
draft_matrix.py then transparently operates against the backtest schema with zero code
changes to those modules, since they all import get_db_connection/get_db_cursor fresh on
each call rather than at import time.

This is deliberate, narrow, process-global mutable state - fine for a single-purpose batch
CLI job, but must never run concurrently with anything else touching src.database.db_pool in
the same process (e.g. never import this into the FastAPI app).
"""
import logging
from contextlib import contextmanager

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

import src.database as database
from src.migrations import run_migrations

logger = logging.getLogger("BacktestSchema")

BACKTEST_SCHEMA_PREFIX = "backtest"


def schema_name(season: int) -> str:
    return f"{BACKTEST_SCHEMA_PREFIX}_{season}"


def _connect_params() -> dict:
    return {
        "host": database.DB_HOST,
        "database": database.DB_NAME,
        "user": database.DB_USER,
        "password": database.DB_PASSWORD,
    }


def build_backtest_pool(schema: str) -> ThreadedConnectionPool:
    """A second, backtesting-owned pool (never src.database.db_pool directly) whose
    sessions pin search_path to the isolated backtest schema via psycopg2's `options`
    connect param, so every checked-out connection is already scoped before any
    backtesting code runs a query against it."""
    return ThreadedConnectionPool(
        minconn=1,
        maxconn=5,
        options=f"-c search_path={schema},public",
        **_connect_params(),
    )


def provision_schema(schema: str, wipe: bool = True) -> None:
    """DROP SCHEMA CASCADE (if wipe) + CREATE SCHEMA using a short-lived bootstrap
    connection with no search_path override (CREATE SCHEMA itself must run against the
    real default schema), then runs the existing, unmodified run_migrations() against a
    pool pinned to the new schema to lay down players/weekly_stats/league_settings/
    draft_picks/backtest_results via the same DDL the production pipeline uses."""
    conn = psycopg2.connect(**_connect_params())
    try:
        with conn.cursor() as cursor:
            if wipe:
                cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        conn.commit()
    finally:
        conn.close()

    with backtest_db(schema):
        run_migrations()
    logger.info(f"Provisioned backtest schema '{schema}'.")


@contextmanager
def backtest_db(schema: str):
    """For the duration of this block, src.database.get_db_connection/get_db_cursor
    transparently operate against `schema` instead of `public`. The original pool is
    restored on exit, even on exception, and the backtest pool is closed to avoid
    leaking connections across repeated CLI runs."""
    original_pool = database.db_pool
    backtest_pool = build_backtest_pool(schema)
    database.db_pool = backtest_pool
    try:
        yield
    finally:
        database.db_pool = original_pool
        backtest_pool.closeall()
