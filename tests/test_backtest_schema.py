from unittest.mock import MagicMock, patch

import src.database as database
from src.backtesting import backtest_schema


def test_schema_name():
    assert backtest_schema.schema_name(2023) == "backtest_2023"


def test_build_backtest_pool_pins_search_path():
    with patch("src.backtesting.backtest_schema.ThreadedConnectionPool") as mock_pool_cls:
        backtest_schema.build_backtest_pool("backtest_2023")
        _, kwargs = mock_pool_cls.call_args
        assert kwargs["options"] == "-c search_path=backtest_2023,public"


def test_provision_schema_wipes_and_creates_schema():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    with patch("src.backtesting.backtest_schema.psycopg2.connect", return_value=mock_conn), \
         patch("src.backtesting.backtest_schema.backtest_db") as mock_backtest_db, \
         patch("src.backtesting.backtest_schema.run_migrations"):
        mock_backtest_db.return_value.__enter__.return_value = None
        backtest_schema.provision_schema("backtest_2023", wipe=True)

    executed = [call.args[0] for call in mock_cursor.execute.call_args_list]
    assert "DROP SCHEMA IF EXISTS backtest_2023 CASCADE;" in executed
    assert "CREATE SCHEMA IF NOT EXISTS backtest_2023;" in executed
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


def test_provision_schema_no_wipe_skips_drop():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    with patch("src.backtesting.backtest_schema.psycopg2.connect", return_value=mock_conn), \
         patch("src.backtesting.backtest_schema.backtest_db") as mock_backtest_db, \
         patch("src.backtesting.backtest_schema.run_migrations"):
        mock_backtest_db.return_value.__enter__.return_value = None
        backtest_schema.provision_schema("backtest_2023", wipe=False)

    executed = [call.args[0] for call in mock_cursor.execute.call_args_list]
    assert not any(e.startswith("DROP SCHEMA") for e in executed)
    assert "CREATE SCHEMA IF NOT EXISTS backtest_2023;" in executed


def test_backtest_db_swaps_and_restores_pool():
    original_pool = database.db_pool
    fake_pool = MagicMock()
    with patch("src.backtesting.backtest_schema.build_backtest_pool", return_value=fake_pool):
        with backtest_schema.backtest_db("backtest_2023"):
            assert database.db_pool is fake_pool
    assert database.db_pool is original_pool
    fake_pool.closeall.assert_called_once()


def test_backtest_db_restores_pool_on_exception():
    original_pool = database.db_pool
    fake_pool = MagicMock()
    with patch("src.backtesting.backtest_schema.build_backtest_pool", return_value=fake_pool):
        try:
            with backtest_schema.backtest_db("backtest_2023"):
                raise RuntimeError("boom")
        except RuntimeError:
            pass
    assert database.db_pool is original_pool
    fake_pool.closeall.assert_called_once()
