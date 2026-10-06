from unittest.mock import MagicMock, patch

import pytest

from src.ingestion import sync_league as sl
# Captured at import, before conftest's autouse stub replaces the module attribute.
from src.ingestion.sync_league import refresh_rosters_if_stale as real_refresh_rosters_if_stale


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
                {"playerId": 4360438, "lineupSlotId": 20,
                 "playerPoolEntry": {"player": {"id": 4360438, "injuryStatus": "OUT"}}},
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
    assert config["lineup_slot_counts"]["RB/WR/TE"] == 1


def test_fetch_league_config_translates_slot_ids_to_position_labels():
    """Raw lineupSlotCounts is keyed by ESPN slot ID, not position name - persisting it
    as-is would make every downstream lineup_slot_counts.get('RB', 0) silently return 0."""
    mock_league = MagicMock()
    mock_league.settings.team_count = 10
    mock_league.settings.scoring_type = "H2H_POINTS"
    mock_league.settings.name = "Test League"

    config = sl.fetch_league_config(mock_league, RAW_MSETTINGS)

    assert config["lineup_slot_counts"] == {
        "QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 7, "IR": 1,
    }
    assert "2" not in config["lineup_slot_counts"]


def test_build_roster_rows_maps_espn_id_to_player_id():
    rows = sl.build_roster_rows(RAW_TEAMS, ESPN_TO_PLAYER_ID)

    assert ("1", "4046", "RB", True, None) in rows
    assert ("1", "8112", "BE", False, "OUT") in rows


def test_build_roster_rows_carries_espn_injury_status():
    """ESPN, not Sleeper, decides IR eligibility, so its status rides along on the roster row."""
    rows = sl.build_roster_rows(RAW_TEAMS, ESPN_TO_PLAYER_ID)
    by_player = {r[1]: r for r in rows}

    assert by_player["8112"][4] == "OUT"
    assert by_player["4046"][4] is None


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

    written = sl.persist_rosters(mock_conn, [("1", "4046", "RB", True, "OUT")])

    assert written == 1
    calls = [c[0][0] for c in mock_cursor.execute.call_args_list]
    assert "DELETE FROM league_rosters" in calls[0]
    mock_cursor.executemany.assert_called_once()
    insert_sql, insert_rows = mock_cursor.executemany.call_args.args
    assert "espn_injury_status" in insert_sql
    assert insert_rows == [("1", "4046", "RB", True, "OUT")]


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


RAW_TEAMS_WITH_NAMES = [
    {"id": 1, "name": "The Dynasty", "roster": {"entries": []}},
    {"id": 2, "name": "Unknown", "location": "Team", "nickname": "Two", "roster": {"entries": []}},
]


def test_extract_team_name_uses_name_field_when_present():
    assert sl._extract_team_name(RAW_TEAMS_WITH_NAMES[0]) == "The Dynasty"


def test_extract_team_name_falls_back_to_location_and_nickname_when_name_is_unknown():
    assert sl._extract_team_name(RAW_TEAMS_WITH_NAMES[1]) == "Team Two"


def test_build_team_name_rows_maps_all_teams():
    rows = sl.build_team_name_rows(RAW_TEAMS_WITH_NAMES)

    assert rows == [("1", "The Dynasty"), ("2", "Team Two")]


def test_persist_team_names_upserts_without_deleting():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    sl.persist_team_names(mock_conn, "1248158344", [("1", "The Dynasty")])

    execute_calls = [c[0][0] for c in mock_cursor.execute.call_args_list]
    assert not any("DELETE FROM league_teams" in c for c in execute_calls)
    mock_cursor.executemany.assert_called_once()
    upsert_sql = mock_cursor.executemany.call_args[0][0]
    assert "INSERT INTO league_teams" in upsert_sql
    assert "ON CONFLICT" in upsert_sql


def test_sync_league_persists_team_names_alongside_rosters(monkeypatch):
    _require_env_vars(monkeypatch)
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_league = MagicMock()
    mock_league.settings.team_count = 10
    mock_league.settings.scoring_type = "H2H_POINTS"
    mock_league.settings.name = "Test League"

    with patch("src.ingestion.sync_league.build_league", return_value=mock_league), \
         patch("src.ingestion.sync_league.fetch_raw_league_data",
               return_value={"teams": RAW_TEAMS_WITH_NAMES, "settings": RAW_MSETTINGS["settings"]}), \
         patch("src.ingestion.sync_league.get_db_connection") as mock_get_conn, \
         patch("src.ingestion.sync_league.persist_team_names") as mock_persist_names:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        sl.sync_league(season=2026)

    mock_persist_names.assert_called_once_with(mock_conn, "1248158344", [("1", "The Dynasty"), ("2", "Team Two")])



def test_roster_refresh_skips_when_another_request_refreshed_recently():
    redis = MagicMock()
    redis.set.return_value = None
    with (
        patch("src.ingestion.sync_league.get_redis_client", return_value=redis),
        patch("src.ingestion.sync_league.sync_rosters") as sync,
    ):
        assert real_refresh_rosters_if_stale(2026) is False
    sync.assert_not_called()


def test_roster_refresh_runs_once_per_window_and_survives_espn_failure():
    redis = MagicMock()
    redis.set.return_value = True
    with (
        patch("src.ingestion.sync_league.get_redis_client", return_value=redis),
        patch("src.ingestion.sync_league.sync_rosters", return_value=160) as sync,
    ):
        assert real_refresh_rosters_if_stale(2026) is True
    assert redis.set.call_args.kwargs == {"nx": True, "ex": 600}
    sync.assert_called_once_with(2026)

    with (
        patch("src.ingestion.sync_league.get_redis_client", return_value=redis),
        patch("src.ingestion.sync_league.sync_rosters", side_effect=TimeoutError("espn")),
    ):
        assert real_refresh_rosters_if_stale(2026) is False


def test_sync_rosters_never_wipes_rosters_on_an_empty_espn_response():
    league = MagicMock()
    league.espn_request.league_get.return_value = {"teams": []}
    with (
        patch("src.ingestion.sync_league.build_league", return_value=league),
        patch("src.ingestion.sync_league.persist_rosters") as persist,
    ):
        with pytest.raises(RuntimeError):
            sl.sync_rosters(2026)
    persist.assert_not_called()
