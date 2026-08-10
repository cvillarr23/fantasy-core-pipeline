import os
from contextlib import contextmanager
import logging
from psycopg2.pool import ThreadedConnectionPool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DatabaseEngine")

# Load cluster boundaries from environment secrets
DB_HOST = os.getenv("DB_HOST", "fantasy-db.default.svc.cluster.local")
DB_NAME = os.getenv("DB_NAME", "fantasy_football")
DB_USER = os.getenv("DB_USER", "bot_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not DB_PASSWORD:
    logger.error("DB_PASSWORD environment variable is missing!")

# Establish a connection pool with conservative caps to preserve 12GB node RAM
try:
    db_pool = ThreadedConnectionPool(
        minconn=2,
        maxconn=10,
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    logger.info("Database connection pool initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize database connection pool: {e}")
    raise

@contextmanager
def get_db_connection():
    """
    Context manager to handle checked-out pool connections safely.
    Automatically commits transactions on success or rolls them back on failure.
    """
    connection = db_pool.getconn()
    try:
        yield connection
        connection.commit()
    except Exception as e:
        connection.rollback()
        logger.error(f"Database transaction error; transaction rolled back: {e}")
        raise
    finally:
        db_pool.putconn(connection)

@contextmanager
def get_db_cursor():
    """
    Context manager providing a clean execution cursor.
    Guarantees the cursor object is destroyed immediately to prevent memory leaks.
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

