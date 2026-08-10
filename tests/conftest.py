"""
src.database eagerly opens a real ThreadedConnectionPool at import time, which
would otherwise require a live Postgres to even import src.api.routes. Stub the
pool class out before any test module imports that chain.
"""
from unittest.mock import MagicMock

import psycopg2.pool

psycopg2.pool.ThreadedConnectionPool = MagicMock(return_value=MagicMock())
