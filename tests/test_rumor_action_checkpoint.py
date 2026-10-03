from unittest.mock import MagicMock, patch

import pytest

from src import migrations, models
from src.services import rumor_action_checkpoint as checkpoint


def test_checkpoint_is_migrated():
    with patch.object(migrations, "get_db_cursor") as get_cursor:
        migrations.run_migrations()
    sql = [
        c.args[0]
        for c in get_cursor.return_value.__enter__.return_value.execute.call_args_list
    ]
    assert models.CREATE_RUMOR_ACTION_CHECKPOINTS_TABLE in sql


def test_load_initializes_once_and_reads_existing_state():
    conn = MagicMock()
    cursor = conn.__enter__.return_value.cursor.return_value.__enter__.return_value
    state = {"watermark": 42}
    cursor.fetchone.return_value = (3, state)
    with patch.object(checkpoint, "get_db_connection", return_value=conn):
        assert checkpoint.load_checkpoint("7", 0) == (3, state)
    assert (
        "ON CONFLICT (team_id) DO NOTHING" in cursor.execute.call_args_list[0].args[0]
    )
    conn.__exit__.assert_called_once_with(None, None, None)


def test_save_requires_matching_version_and_commits_new_state():
    conn = MagicMock()
    cursor = conn.__enter__.return_value.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (5,)
    with patch.object(checkpoint, "get_db_connection", return_value=conn):
        assert checkpoint.save_checkpoint("7", 4, {"watermark": 42}) == 5
    sql, params = cursor.execute.call_args.args
    assert "AND version = %s RETURNING version" in sql
    assert params[0].adapted == {"watermark": 42}
    assert params[1:] == ("7", 4)
    conn.__exit__.assert_called_once_with(None, None, None)


def test_concurrent_save_fails_instead_of_overwriting_progress():
    conn = MagicMock()
    cursor = conn.__enter__.return_value.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = None
    with patch.object(
        checkpoint, "get_db_connection", return_value=conn
    ), pytest.raises(RuntimeError, match="Concurrent"):
        checkpoint.save_checkpoint("7", 4, {})
    assert conn.__exit__.call_args.args[0] is RuntimeError
