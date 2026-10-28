from unittest.mock import patch

import psycopg2
import pytest

from src import database


def test_get_redis_client_bounds_its_socket_timeouts():
    """redis-py waits forever by default. A Redis that accepts the connection then stops
    replying otherwise parks a caller permanently - the same failure mode api/draft.py's
    synchronous endpoints already guarded against before this became the shared client."""
    with patch("src.database.redis.Redis") as mock_redis_cls:
        database.get_redis_client()

    kwargs = mock_redis_cls.call_args.kwargs
    assert kwargs["socket_connect_timeout"] > 0
    assert kwargs["socket_timeout"] > 0


def test_open_pool_retries_until_dns_resolves():
    """A pod started right after a node boot can run before cluster DNS is up. The first
    connects fail, and the pool opens once the name resolves instead of failing the job."""
    pool = object()
    failure = psycopg2.OperationalError("could not translate host name")
    with patch(
        "src.database.ThreadedConnectionPool", side_effect=[failure, failure, pool]
    ) as mock_pool_cls, patch("src.database.time.sleep") as mock_sleep:
        assert database._open_pool() is pool

    assert mock_pool_cls.call_count == 3
    assert [c.args[0] for c in mock_sleep.call_args_list] == [2, 4]


def test_open_pool_gives_up_after_retry_window():
    clock = iter(range(0, 10_000, 60))
    with patch(
        "src.database.ThreadedConnectionPool",
        side_effect=psycopg2.OperationalError("connection refused"),
    ), patch("src.database.time.sleep") as mock_sleep, patch(
        "src.database.time.monotonic", side_effect=lambda: next(clock)
    ):
        with pytest.raises(psycopg2.OperationalError):
            database._open_pool()

    assert 0 < mock_sleep.call_count <= database.DB_CONNECT_RETRY_SECONDS // 60


def test_open_pool_does_not_retry_other_errors():
    with patch(
        "src.database.ThreadedConnectionPool", side_effect=ValueError("bad dsn")
    ), patch("src.database.time.sleep") as mock_sleep:
        with pytest.raises(ValueError):
            database._open_pool()

    mock_sleep.assert_not_called()
