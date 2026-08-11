from unittest.mock import MagicMock, patch

import pytest

from src import migrations


def _mock_cursor(pk_check_result):
    """fetchone() is only meaningfully asserted on for the weekly_stats PK verification
    query; every other cursor.execute() call in run_migrations() doesn't read a result."""
    cursor = MagicMock()
    cursor.fetchone.return_value = pk_check_result
    return cursor


def test_run_migrations_succeeds_when_weekly_stats_pk_includes_season():
    cursor = _mock_cursor(pk_check_result=(1,))
    with patch("src.migrations.get_db_cursor") as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = cursor
        migrations.run_migrations()  # should not raise


def test_run_migrations_raises_when_weekly_stats_pk_migration_was_skipped():
    """
    MIGRATE_WEEKLY_STATS_PK_ADD_SEASON silently skips (a Postgres NOTICE, not an error) if
    pre-existing rows have a NULL season. If that happens, weekly_stats' primary key is
    still (player_id, week), but the CORE-04 extractor's upsert uses
    `ON CONFLICT (player_id, season, week)`, which Postgres rejects outright with no
    matching constraint. run_migrations() must fail loudly here instead of reporting
    success and leaving CORE-04 unable to write anything.
    """
    cursor = _mock_cursor(pk_check_result=None)
    with patch("src.migrations.get_db_cursor") as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = cursor
        with pytest.raises(RuntimeError, match="season"):
            migrations.run_migrations()
