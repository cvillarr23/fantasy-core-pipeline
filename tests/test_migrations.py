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


def test_run_migrations_executes_llm_call_metrics_ddl_before_indices():
    """WORKER-12's llm_client.js inserts into llm_call_metrics on every provider-call
    attempt. A MagicMock cursor would let this table's CREATE statement silently go
    missing from run_migrations() and still report success, leaving those inserts to
    fail at runtime with "relation does not exist". Lock in that the table is created,
    and before CREATE_INDICES (which indexes it), same ordering convention as CORE-05's
    test above."""
    cursor = _mock_cursor(pk_check_result=(1,))
    with patch("src.migrations.get_db_cursor") as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = cursor
        migrations.run_migrations()

    executed = [call.args[0] for call in cursor.execute.call_args_list if call.args]
    llm_metrics_idx = next(
        i
        for i, s in enumerate(executed)
        if "CREATE TABLE IF NOT EXISTS llm_call_metrics" in s
    )
    indices_idx = next(i for i, s in enumerate(executed) if "CREATE INDEX" in s)
    assert llm_metrics_idx < indices_idx


def test_run_migrations_executes_defense_position_strength_ddl_before_indices():
    """#93 adds defense_matchup_multiplier to weekly_stats and the new
    defense_position_strength table. Lock in that both execute, and the table's CREATE runs
    before CREATE_INDICES, same ordering convention as the other ledger tables' tests above."""
    cursor = _mock_cursor(pk_check_result=(1,))
    with patch("src.migrations.get_db_cursor") as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = cursor
        migrations.run_migrations()

    executed = [call.args[0] for call in cursor.execute.call_args_list if call.args]
    assert any("defense_matchup_multiplier" in stmt for stmt in executed)
    defense_strength_idx = next(
        i
        for i, s in enumerate(executed)
        if "CREATE TABLE IF NOT EXISTS defense_position_strength" in s
    )
    indices_idx = next(i for i, s in enumerate(executed) if "CREATE INDEX" in s)
    assert defense_strength_idx < indices_idx


def test_run_migrations_executes_draft_picks_was_autodrafted_column_before_indices():
    """Issue #108 (Task F, fantasy-core-pipeline#110): draft_picks needs a nullable
    was_autodrafted boolean column so handle_pick_event can flag our own team's picks
    that landed on a different player than the last target we attempted to publish.
    Lock in that the ADD COLUMN runs, alongside the existing espn_player_id column
    addition, and before CREATE_INDICES like the other draft_picks column additions."""
    cursor = _mock_cursor(pk_check_result=(1,))
    with patch("src.migrations.get_db_cursor") as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = cursor
        migrations.run_migrations()

    executed = [call.args[0] for call in cursor.execute.call_args_list if call.args]
    was_autodrafted_idx = next(
        i
        for i, s in enumerate(executed)
        if "ADD COLUMN IF NOT EXISTS was_autodrafted" in s
    )
    indices_idx = next(i for i, s in enumerate(executed) if "CREATE INDEX" in s)
    assert was_autodrafted_idx < indices_idx


def test_run_migrations_executes_auto_publish_kill_switch_columns():
    """Issue #108/#109: draft_config gets auto_publish_enabled (boolean, defaults true so
    every pre-existing row keeps today's always-on behavior) and auto_publish_disabled_by
    (nullable text, recording 'manual' vs a future 'circuit_breaker' reason) - both must run
    after CREATE_DRAFT_CONFIG_TABLE, same ordering convention as mode/team_count above."""
    cursor = _mock_cursor(pk_check_result=(1,))
    with patch("src.migrations.get_db_cursor") as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = cursor
        migrations.run_migrations()

    executed = [call.args[0] for call in cursor.execute.call_args_list if call.args]
    enabled_idx = next(
        i
        for i, s in enumerate(executed)
        if "ADD COLUMN IF NOT EXISTS auto_publish_enabled BOOLEAN NOT NULL DEFAULT true" in s
    )
    disabled_by_idx = next(
        i
        for i, s in enumerate(executed)
        if "ADD COLUMN IF NOT EXISTS auto_publish_disabled_by VARCHAR(32)" in s
    )
    draft_config_idx = next(
        i for i, s in enumerate(executed) if "CREATE TABLE IF NOT EXISTS draft_config" in s
    )
    assert draft_config_idx < enabled_idx < disabled_by_idx


def test_run_migrations_executes_bot_decisions_ddl_before_indices():
    """CORE-17 (#35): the trigger endpoints in src/api/routes.py insert into bot_decisions
    on every successful lineup/waiver/draft-pick publish. Lock in that the table is
    created, and before CREATE_INDICES (which indexes it), same ordering convention as
    the other ledger tables' tests above."""
    cursor = _mock_cursor(pk_check_result=(1,))
    with patch("src.migrations.get_db_cursor") as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = cursor
        migrations.run_migrations()

    executed = [call.args[0] for call in cursor.execute.call_args_list if call.args]
    bot_decisions_idx = next(
        i
        for i, s in enumerate(executed)
        if "CREATE TABLE IF NOT EXISTS bot_decisions" in s
    )
    indices_idx = next(i for i, s in enumerate(executed) if "CREATE INDEX" in s)
    assert bot_decisions_idx < indices_idx


def test_run_migrations_executes_rumor_signals_ddl_before_indices():
    """WORKER-13 (#27, fantasy-browser-worker): rumor_digest_bot.js writes structured
    per-player verdicts into rumor_signals on every digest run. Lock in that the table is
    created, and before CREATE_INDICES (which indexes it), same ordering convention as
    the other ledger tables' tests above."""
    cursor = _mock_cursor(pk_check_result=(1,))
    with patch("src.migrations.get_db_cursor") as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = cursor
        migrations.run_migrations()

    executed = [call.args[0] for call in cursor.execute.call_args_list if call.args]
    rumor_signals_idx = next(
        i
        for i, s in enumerate(executed)
        if "CREATE TABLE IF NOT EXISTS rumor_signals" in s
    )
    indices_idx = next(i for i, s in enumerate(executed) if "CREATE INDEX" in s)
    assert rumor_signals_idx < indices_idx


def test_run_migrations_executes_execution_log_trade_context_columns_before_indices():
    """trade_bot.js's recordTradeNegotiation (fantasy-browser-worker) writes
    offering_team_id/counter_offer/dry_run on every negotiation run. Lock in that the
    columns are added, and before CREATE_INDICES, same ordering convention as the other
    execution_log column backfills above."""
    cursor = _mock_cursor(pk_check_result=(1,))
    with patch("src.migrations.get_db_cursor") as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = cursor
        migrations.run_migrations()

    executed = [call.args[0] for call in cursor.execute.call_args_list if call.args]
    trade_cols_idx = next(
        i for i, s in enumerate(executed) if "offering_team_id" in s
    )
    indices_idx = next(i for i, s in enumerate(executed) if "CREATE INDEX" in s)
    assert trade_cols_idx < indices_idx


def test_run_migrations_executes_league_teams_ddl_after_league_settings():
    """The matchup view / real-team-names feature needs league_teams (team_id ->
    team_name), synced alongside league_settings by sync_league.py. Lock in it's created,
    right after league_settings, matching this migration block's existing ordering."""
    cursor = _mock_cursor(pk_check_result=(1,))
    with patch("src.migrations.get_db_cursor") as mock_get_cursor:
        mock_get_cursor.return_value.__enter__.return_value = cursor
        migrations.run_migrations()

    executed = [call.args[0] for call in cursor.execute.call_args_list if call.args]
    league_settings_idx = next(i for i, s in enumerate(executed) if "CREATE TABLE IF NOT EXISTS league_settings" in s)
    league_teams_idx = next(i for i, s in enumerate(executed) if "CREATE TABLE IF NOT EXISTS league_teams" in s)
    assert league_settings_idx < league_teams_idx


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
