from unittest.mock import MagicMock, patch

import polars as pl

from src.ingestion import sync_id_crosswalk as sic


def _raw_crosswalk_frame(rows):
    schema = {"sleeper_id": pl.Int64, "espn_id": pl.Int64, "gsis_id": pl.Utf8}
    return pl.DataFrame(rows, schema=schema)


def test_load_crosswalk_drops_null_sleeper_id():
    with patch("src.ingestion.sync_id_crosswalk.nfl.load_ff_playerids") as mock_load:
        mock_load.return_value = _raw_crosswalk_frame([
            {"sleeper_id": 4046, "espn_id": 3117251, "gsis_id": "00-0033280"},
            {"sleeper_id": None, "espn_id": 999, "gsis_id": "00-0099999"},
        ])
        result = sic.load_crosswalk()

    assert result.height == 1
    assert result.row(0, named=True)["sleeper_id"] == "4046"


def test_load_crosswalk_dedupes_on_sleeper_id():
    with patch("src.ingestion.sync_id_crosswalk.nfl.load_ff_playerids") as mock_load:
        mock_load.return_value = _raw_crosswalk_frame([
            {"sleeper_id": 4046, "espn_id": 3117251, "gsis_id": "00-0033280"},
            {"sleeper_id": 4046, "espn_id": 3117251, "gsis_id": "00-0033280"},
        ])
        result = sic.load_crosswalk()

    assert result.height == 1


def test_load_crosswalk_casts_ids_to_utf8():
    with patch("src.ingestion.sync_id_crosswalk.nfl.load_ff_playerids") as mock_load:
        mock_load.return_value = _raw_crosswalk_frame([
            {"sleeper_id": 4046, "espn_id": 3117251, "gsis_id": "00-0033280"},
        ])
        result = sic.load_crosswalk()

    row = result.row(0, named=True)
    assert row["sleeper_id"] == "4046"
    assert row["espn_id"] == "3117251"
    assert row["gsis_id"] == "00-0033280"


def test_persist_crosswalk_coalesces_gsis_id_on_miss():
    """A crosswalk row with a NULL gsis_id must not erase an existing good value -
    the UPDATE's COALESCE(%s, gsis_id) is what enforces that; this test locks in the
    parameter order passed to it."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    crosswalk = pl.DataFrame(
        [{"sleeper_id": "4046", "espn_id": "3117251", "gsis_id": None}],
        schema={"sleeper_id": pl.Utf8, "espn_id": pl.Utf8, "gsis_id": pl.Utf8},
    )
    matched = sic.persist_crosswalk(mock_conn, crosswalk, existing_player_ids={"4046"})

    assert matched == 1
    call_args = mock_cursor.executemany.call_args
    query, rows = call_args.args
    assert "COALESCE" in query
    assert rows == [("3117251", None, "4046")]


def test_persist_crosswalk_matched_excludes_unknown_player_ids():
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = MagicMock()

    crosswalk = pl.DataFrame(
        [
            {"sleeper_id": "4046", "espn_id": "3117251", "gsis_id": "00-0033280"},
            {"sleeper_id": "9999", "espn_id": "1", "gsis_id": "00-0000001"},
        ],
        schema={"sleeper_id": pl.Utf8, "espn_id": pl.Utf8, "gsis_id": pl.Utf8},
    )
    matched = sic.persist_crosswalk(mock_conn, crosswalk, existing_player_ids={"4046"})

    assert matched == 1


def test_sync_id_crosswalk_logs_match_rate():
    with patch("src.ingestion.sync_id_crosswalk.load_crosswalk") as mock_load, \
         patch("src.ingestion.sync_id_crosswalk.get_db_connection") as mock_get_conn:
        mock_load.return_value = pl.DataFrame(
            [{"sleeper_id": "4046", "espn_id": "3117251", "gsis_id": "00-0033280"}],
            schema={"sleeper_id": pl.Utf8, "espn_id": pl.Utf8, "gsis_id": pl.Utf8},
        )
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("4046",)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        matched = sic.sync_id_crosswalk()

    assert matched == 1
