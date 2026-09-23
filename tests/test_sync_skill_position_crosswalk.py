from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.ingestion import sync_skill_position_crosswalk as sspc


def _espn_player(name, pro_team, position, player_id):
    return SimpleNamespace(name=name, proTeam=pro_team, position=position, playerId=player_id)


def test_match_skill_position_espn_ids_matches_exact_name_and_team():
    player_rows = [
        {"player_id": "4046", "position": "RB", "team": "SF", "name": "Christian McCaffrey"},
    ]
    espn_players = [_espn_player("Christian McCaffrey", "SF", "RB", 3117251)]

    matches, unmatched = sspc.match_skill_position_espn_ids(player_rows, espn_players)

    assert unmatched == []
    assert matches == [("4046", espn_players[0])]


def test_match_skill_position_espn_ids_falls_back_to_fuzzy_on_team_mismatch():
    # players.team is stale (a mid-season trade ESPN already reflects) so the exact tier
    # (name, team) misses; the fuzzy tier should still resolve it on name alone.
    player_rows = [
        {"player_id": "4046", "position": "RB", "team": "SF", "name": "Christian McCaffrey"},
    ]
    espn_players = [_espn_player("Christian McCaffrey", "LAR", "RB", 3117251)]

    matches, unmatched = sspc.match_skill_position_espn_ids(player_rows, espn_players)

    assert unmatched == []
    assert matches == [("4046", espn_players[0])]


def test_match_skill_position_espn_ids_stays_unmatched_below_fuzzy_threshold():
    player_rows = [
        {"player_id": "9999", "position": "WR", "team": "SF", "name": "Totally Different Guy"},
    ]
    espn_players = [_espn_player("Christian McCaffrey", "SF", "WR", 3117251)]

    matches, unmatched = sspc.match_skill_position_espn_ids(player_rows, espn_players)

    assert matches == []
    assert unmatched == ["9999"]


def test_match_skill_position_espn_ids_scopes_fuzzy_match_by_position():
    # Same name, wrong position - must not cross-match a WR candidate for a TE row.
    player_rows = [
        {"player_id": "1111", "position": "TE", "team": "SF", "name": "George Kittle"},
    ]
    espn_players = [_espn_player("George Kittle", "SF", "WR", 2222)]

    matches, unmatched = sspc.match_skill_position_espn_ids(player_rows, espn_players)

    assert matches == []
    assert unmatched == ["1111"]


def test_match_skill_position_espn_ids_reports_unmatched_with_no_candidates():
    player_rows = [{"player_id": "9999", "position": "QB", "team": "SF", "name": "Some Guy"}]

    matches, unmatched = sspc.match_skill_position_espn_ids(player_rows, espn_players=[])

    assert matches == []
    assert unmatched == ["9999"]


def test_persist_espn_ids_writes_string_player_id():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    espn_player = _espn_player("Christian McCaffrey", "SF", "RB", 3117251)
    written = sspc.persist_espn_ids(mock_conn, [("4046", espn_player)])

    assert written == 1
    query, rows = mock_cursor.executemany.call_args.args
    assert rows == [("3117251", "4046")]


def test_persist_espn_ids_noop_on_empty_matches():
    mock_conn = MagicMock()
    assert sspc.persist_espn_ids(mock_conn, []) == 0
    mock_conn.cursor.assert_not_called()


def test_fetch_espn_skill_position_players_wraps_each_raw_entry():
    mock_league = MagicMock()
    mock_league.year = 2026
    raw_player = {
        "player": {
            "id": 1,
            "fullName": "Some Player",
            "proTeamId": 0,
            "eligibleSlots": [2],
            "ownership": {},
        }
    }
    mock_league.espn_request.league_get.return_value = {"players": [raw_player]}

    result = sspc.fetch_espn_skill_position_players(mock_league, positions=["RB"])

    assert len(result) == 1
    assert result[0].name == "Some Player"
    # No filterStatus in the request - that's what would exclude owned/rostered players.
    _, kwargs = mock_league.espn_request.league_get.call_args
    filters = kwargs["headers"]["x-fantasy-filter"]
    assert "filterStatus" not in filters


def test_sync_skill_position_crosswalk_backfills_espn_ids():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("4046", "RB", "SF", "Christian McCaffrey")]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    espn_player = _espn_player("Christian McCaffrey", "SF", "RB", 3117251)

    with patch("src.ingestion.sync_skill_position_crosswalk.build_league") as mock_build_league, \
         patch("src.ingestion.sync_skill_position_crosswalk.fetch_espn_skill_position_players") as mock_fetch, \
         patch("src.ingestion.sync_skill_position_crosswalk.get_db_connection") as mock_get_conn:
        mock_build_league.return_value = MagicMock()
        mock_fetch.return_value = [espn_player]
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        result = sspc.sync_skill_position_crosswalk(season=2026)

    assert result["espn_id_written"] == 1
    assert result["unmatched"] == []
