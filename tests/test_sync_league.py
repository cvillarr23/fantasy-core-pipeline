from unittest.mock import MagicMock, patch

import pytest

from src.ingestion import sync_league as sl


RAW_MSETTINGS = {
    "settings": {
        "rosterSettings": {"lineupSlotCounts": {"0": 1, "2": 2, "4": 2, "6": 1, "23": 1, "20": 7, "21": 1}},
        "scoringSettings": {"scoringItems": [{"statId": 53, "points": 1.0}]},
    }
}

RAW_TEAMS = [
    {
        "id": 1,
        "roster": {
            "entries": [
                {"playerId": 3117251, "lineupSlotId": 2},
                {"playerId": 4360438, "lineupSlotId": 20},
                {"playerId": 999999, "lineupSlotId": 21},
            ]
        },
    },
    {
        "id": 2,
        "roster": {"entries": []},
    },
]

ESPN_TO_PLAYER_ID = {"3117251": "4046", "4360438": "8112"}


def _require_env_vars(monkeypatch):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "1248158344")
    monkeypatch.setenv("ESPN_S2", "fake_s2")
    monkeypatch.setenv("ESPN_SWID", "{fake-swid}")


def test_require_env_raises_clear_error_when_unset(monkeypatch):
    monkeypatch.delenv("ESPN_LEAGUE_ID", raising=False)
    with pytest.raises(RuntimeError, match="ESPN_LEAGUE_ID"):
        sl._require_env("ESPN_LEAGUE_ID")


def test_build_league_raises_when_credentials_missing(monkeypatch):
    monkeypatch.delenv("ESPN_LEAGUE_ID", raising=False)
    monkeypatch.delenv("ESPN_S2", raising=False)
    monkeypatch.delenv("ESPN_SWID", raising=False)
    with pytest.raises(RuntimeError):
        sl.build_league(season=2026)


def test_fetch_league_config_reads_lineup_slot_counts_from_raw_json():
    mock_league = MagicMock()
    mock_league.settings.team_count = 10
    mock_league.settings.scoring_type = "H2H_POINTS"
    mock_league.settings.name = "Test League"

    config = sl.fetch_league_config(mock_league, RAW_MSETTINGS)

    assert config["team_count"] == 10
    assert config["scoring_type"] == "H2H_POINTS"
    assert config["lineup_slot_counts"]["23"] == 1


def test_build_roster_rows_maps_espn_id_to_player_id():
    rows = sl.build_roster_rows(RAW_TEAMS, ESPN_TO_PLAYER_ID)

    assert ("1", "4046", "RB", True) in rows
    assert ("1", "8112", "BE", False) in rows


def test_build_roster_rows_skips_unmatched_players():
    rows = sl.build_roster_rows(RAW_TEAMS, ESPN_TO_PLAYER_ID)
    player_ids = {r[1] for r in rows}

    # 999999 has no crosswalk entry in ESPN_TO_PLAYER_ID and must be skipped, not crash.
    assert "999999" not in player_ids
    assert len(rows) == 2


def test_build_roster_rows_marks_bench_and_ir_as_non_starter():
    rows = sl.build_roster_rows(RAW_TEAMS, ESPN_TO_PLAYER_ID)
    bench_row = next(r for r in rows if r[1] == "8112")

    assert bench_row[2] == "BE"
    assert bench_row[3] is False


def test_build_roster_rows_handles_all_empty_rosters():
    rows = sl.build_roster_rows([{"id": 1, "roster": {"entries": []}}], ESPN_TO_PLAYER_ID)
    assert rows == []


def test_persist_rosters_deletes_before_insert_in_one_transaction():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    written = sl.persist_rosters(mock_conn, [("1", "4046", "RB", True)])

    assert written == 1
    calls = [c[0][0] for c in mock_cursor.execute.call_args_list]
    assert "DELETE FROM league_rosters" in calls[0]
    mock_cursor.executemany.assert_called_once()


def test_persist_rosters_empty_rows_still_deletes():
    """The pre-draft path: zero roster rows is a valid, logged outcome, not an error -
    but a stale prior-week snapshot must still be cleared."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    written = sl.persist_rosters(mock_conn, [])

    assert written == 0
    mock_cursor.execute.assert_called_once()
    mock_cursor.executemany.assert_not_called()
