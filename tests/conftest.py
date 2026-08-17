"""
src.database eagerly opens a real ThreadedConnectionPool at import time, which
would otherwise require a live Postgres to even import src.api.routes. Stub the
pool class out before any test module imports that chain.
"""
import os
from unittest.mock import MagicMock
from unittest.mock import patch

import psycopg2.pool
import pytest
from fastapi.testclient import TestClient

psycopg2.pool.ThreadedConnectionPool = MagicMock(return_value=MagicMock())


@pytest.fixture(scope="session")
def client():
    """
    Session-scoped on purpose: src.api.main.app mounts the MCP server (CORE-07), whose
    StreamableHTTPSessionManager.run() is entered by the app's lifespan and can only be
    called once per process for the life of that FastMCP instance - a second
    `with TestClient(app) as ...` on the same (module-singleton) app would raise
    "StreamableHTTPSessionManager .run() can only be called once per instance." A
    function-scoped fixture would enter/exit that lifespan once per test function, so
    every test after the first would fail. All test-specific behavior (DB cursors, pika
    connections, GoogleTokenVerifier) is still patched per-test around individual
    client.get/post() calls, which is safe regardless of when the app/lifespan was
    constructed - patches resolve names at call time, not at import/construction time.
    """
    # Defense-in-depth (CORE-12): src.telemetry.configure_tracing() already defaults to
    # a true no-op when this is unset, but a stray value in a dev's shell shouldn't be
    # able to make the suite attempt a live OTLP export.
    os.environ.pop("OTEL_EXPORTER_OTLP_ENDPOINT", None)

    with patch("src.api.main.run_migrations"):
        from src.api.main import app

        with TestClient(app) as test_client:
            yield test_client
