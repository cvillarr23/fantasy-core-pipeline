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


def test_run_migrations_executes_core05_ddl_in_order():
    """CORE-05 adds espn_id, the VORP audit columns, league_settings, and
    league_rosters.is_starter. Lock in that they all execute, in the same relative order
    as their CORE-01/CORE-04 counterparts (players -> weekly_stats -> league_rosters ->
    league_settings -> indices)."""
    cursor = _mock_cursor(pk_check_result=(1,))
    with patch("src.migrations.get_db_cursor") as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = cursor
        migrations.run_migrations()

    executed = [call.args[0] for call in cursor.execute.call_args_list if call.args]
    assert any("espn_id" in stmt for stmt in executed)
    assert any("matchup_coefficient" in stmt for stmt in executed)
    assert any("league_settings" in stmt for stmt in executed)
    assert any("is_starter" in stmt for stmt in executed)

    espn_id_idx = next(i for i, s in enumerate(executed) if "ADD COLUMN IF NOT EXISTS espn_id" in s)
    vorp_cols_idx = next(i for i, s in enumerate(executed) if "matchup_coefficient" in s)
    league_settings_idx = next(i for i, s in enumerate(executed) if "CREATE TABLE IF NOT EXISTS league_settings" in s)
    indices_idx = next(i for i, s in enumerate(executed) if "CREATE INDEX" in s)
    assert espn_id_idx < vorp_cols_idx < league_settings_idx < indices_idx


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
