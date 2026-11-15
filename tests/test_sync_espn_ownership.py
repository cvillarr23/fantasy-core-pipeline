from unittest.mock import MagicMock, patch

import pytest

from src.ingestion import sync_espn_ownership as seo
from src.nfl_schedule import WeekResolutionError


def _entry(espn_id=3117251, owned=99.5, started=88.25, change=-1.5, rank=3, with_ownership=True):
    player = {"id": espn_id, "fullName": "Some Player"}
    if with_ownership:
        player["ownership"] = {"percentOwned": owned, "percentStarted": started, "percentChange": change}
    entry = {"id": espn_id, "player": player}
    if rank is not None:
        entry["ratings"] = {"0": {"positionalRanking": rank, "totalRanking": 10}}
    return entry


def test_ownership_row_reads_all_fields():
    assert seo._ownership_row(_entry()) == {
        "espn_id": "3117251",
        "percent_owned": 99.5,
        "percent_started": 88.25,
        "percent_change": -1.5,
        "pos_rank": 3,
    }


def test_ownership_row_maps_minus_one_to_none():
    row = seo._ownership_row(_entry(owned=-1, started=-1, change=-1, rank=-1))
    assert row["percent_owned"] is None
    assert row["percent_started"] is None
    assert row["percent_change"] is None
    assert row["pos_rank"] is None


def test_ownership_row_keeps_zero_and_negative_change():
    row = seo._ownership_row(_entry(owned=0.0, change=-12.0))
    assert row["percent_owned"] == 0.0
    assert row["percent_change"] == -12.0


def test_ownership_row_missing_ownership_block_is_all_none():
    row = seo._ownership_row(_entry(with_ownership=False, rank=None))
    assert row == {
        "espn_id": "3117251",
        "percent_owned": None,
        "percent_started": None,
        "percent_change": None,
        "pos_rank": None,
    }


def test_ownership_row_without_player_id_is_none():
    assert seo._ownership_row({"player": {}}) is None


def test_build_ownership_rows_skips_and_counts_unmatched():
    entries = [_entry(espn_id=1), _entry(espn_id=2)]
    rows, unmatched = seo.build_ownership_rows(entries, {"1": "4046"}, season=2026, week=10)
    assert unmatched == 1
    assert len(rows) == 1
    assert rows[0]["player_id"] == "4046"
    assert (rows[0]["season"], rows[0]["week"]) == (2026, 10)
    assert "espn_id" not in rows[0]


def test_fetch_ownership_entries_sorts_by_ownership_without_status_filter():
    league = MagicMock()
    league.espn_request.league_get.return_value = {"players": [_entry()]}
    assert seo.fetch_ownership_entries(league) == [_entry()]
    kwargs = league.espn_request.league_get.call_args.kwargs
    assert kwargs["params"] == {"view": "kona_player_info"}
    filters = kwargs["headers"]["x-fantasy-filter"]
    assert "sortPercOwned" in filters and "filterStatus" not in filters
    assert str(seo.OWNERSHIP_FETCH_LIMIT) in filters


def test_persist_ownership_upserts_in_one_batch():
    conn = MagicMock()
    rows = [
        {"player_id": "1", "season": 2026, "week": 10, "percent_owned": 1.0,
         "percent_started": None, "percent_change": 0.5, "pos_rank": 4},
        {"player_id": "2", "season": 2026, "week": 10, "percent_owned": None,
         "percent_started": None, "percent_change": None, "pos_rank": None},
    ]
    with patch("src.ingestion.sync_espn_ownership.execute_values") as mock_execute_values:
        assert seo.persist_ownership(conn, rows) == 2
    mock_execute_values.assert_called_once()
    _, query, values = mock_execute_values.call_args.args
    assert "ON CONFLICT (player_id, season, week) DO UPDATE" in query
    assert values[0] == ("1", 2026, 10, 1.0, None, 0.5, 4)


def test_persist_ownership_noop_on_empty():
    conn = MagicMock()
    assert seo.persist_ownership(conn, []) == 0
    conn.cursor.assert_not_called()


def _patched_sync(entries, id_map):
    cursor = MagicMock()
    cursor.fetchall.return_value = list(id_map.items())
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    stack = [
        patch("src.ingestion.sync_espn_ownership.build_league"),
        patch("src.ingestion.sync_espn_ownership.fetch_ownership_entries", return_value=entries),
        patch("src.ingestion.sync_espn_ownership.get_db_connection"),
        patch("src.ingestion.sync_espn_ownership.execute_values"),
    ]
    return stack, conn


def test_sync_espn_ownership_writes_matched_rows():
    stack, conn = _patched_sync([_entry(espn_id=1), _entry(espn_id=2)], {"1": "4046"})
    with stack[0], stack[1], stack[2] as mock_get_conn, stack[3]:
        mock_get_conn.return_value.__enter__.return_value = conn
        result = seo.sync_espn_ownership(season=2026, week=10)
    assert result == {"written": 1, "unmatched": 1}


def test_sync_espn_ownership_raises_when_espn_returns_nothing():
    stack, conn = _patched_sync([], {})
    with stack[0], stack[1], stack[2], stack[3]:
        with pytest.raises(RuntimeError):
            seo.sync_espn_ownership(season=2026, week=10)


def test_main_defaults_to_upcoming_week():
    with patch("src.ingestion.sync_espn_ownership.nfl.get_current_season", return_value=2026), \
         patch("src.ingestion.sync_espn_ownership.resolve_upcoming_week", return_value=11), \
         patch("src.ingestion.sync_espn_ownership.sync_espn_ownership") as mock_sync:
        assert seo.main([]) == 0
    mock_sync.assert_called_once_with(season=2026, week=11)


def test_main_explicit_args_skip_resolution():
    with patch("src.ingestion.sync_espn_ownership.resolve_upcoming_week") as mock_resolve, \
         patch("src.ingestion.sync_espn_ownership.sync_espn_ownership") as mock_sync:
        assert seo.main(["--season", "2025", "--week", "4"]) == 0
    mock_resolve.assert_not_called()
    mock_sync.assert_called_once_with(season=2025, week=4)


def test_main_season_over_is_a_noop():
    with patch("src.ingestion.sync_espn_ownership.nfl.get_current_season", return_value=2026), \
         patch("src.ingestion.sync_espn_ownership.resolve_upcoming_week", return_value=None), \
         patch("src.ingestion.sync_espn_ownership.sync_espn_ownership") as mock_sync:
        assert seo.main([]) == 0
    mock_sync.assert_not_called()


def test_main_week_resolution_failure_exits_one():
    with patch("src.ingestion.sync_espn_ownership.nfl.get_current_season", return_value=2026), \
         patch("src.ingestion.sync_espn_ownership.resolve_upcoming_week", side_effect=WeekResolutionError("x")), \
         patch("src.ingestion.sync_espn_ownership.sync_espn_ownership") as mock_sync:
        assert seo.main([]) == 1
    mock_sync.assert_not_called()
