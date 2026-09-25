from unittest.mock import patch

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
