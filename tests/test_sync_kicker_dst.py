from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.ingestion import sync_kicker_dst as skd


def _espn_player(name, pro_team, player_id, week=1, projected_points=8.5):
    return SimpleNamespace(
        name=name,
        proTeam=pro_team,
        playerId=player_id,
        stats={week: {"projected_points": projected_points}},
    )


def test_match_dst_espn_ids_matches_by_team_abbreviation():
    player_rows = [
        {"player_id": "HOU", "position": "D/ST", "team": "HOU", "name": "Houston Texans"},
        {"player_id": "SF", "position": "D/ST", "team": "SF", "name": "San Francisco 49ers"},
        {"player_id": "4046", "position": "RB", "team": "SF", "name": "Christian McCaffrey"},
    ]
    espn_dst = [_espn_player("Houston D/ST", "HOU", -16034), _espn_player("SF D/ST", "SF", -16025)]

    matches, unmatched = skd.match_dst_espn_ids(player_rows, espn_dst)

    assert unmatched == []
    matched_ids = {player_id: espn_player.playerId for player_id, espn_player in matches}
    assert matched_ids == {"HOU": -16034, "SF": -16025}


def test_match_dst_espn_ids_normalizes_washington_abbreviation():
    # ESPN's PRO_TEAM_MAP spells Washington "WSH"; Sleeper (and this repo's players.team
    # for D/ST) spells it "WAS" - the one mismatch between the two sources.
    player_rows = [{"player_id": "WAS", "position": "D/ST", "team": "WAS", "name": "Washington"}]
    espn_dst = [_espn_player("Washington D/ST", "WSH", -16028)]

    matches, unmatched = skd.match_dst_espn_ids(player_rows, espn_dst)

    assert unmatched == []
    assert matches == [("WAS", espn_dst[0])]


def test_match_dst_espn_ids_reports_unmatched_team():
    player_rows = [{"player_id": "KC", "position": "D/ST", "team": "KC", "name": "Kansas City Chiefs"}]

    matches, unmatched = skd.match_dst_espn_ids(player_rows, espn_dst_players=[])

    assert matches == []
    assert unmatched == ["KC"]


def test_match_kicker_espn_ids_matches_by_normalized_name_and_team():
    player_rows = [
        {"player_id": "9999", "position": "K", "team": "SF", "name": "  Some   Kicker "},
        {"player_id": "4046", "position": "RB", "team": "SF", "name": "Christian McCaffrey"},
    ]
    espn_k = [_espn_player("Some Kicker", "SF", 12345)]

    matches, unmatched = skd.match_kicker_espn_ids(player_rows, espn_k)

    assert unmatched == []
    assert matches == [("9999", espn_k[0])]


def test_match_kicker_espn_ids_reports_unmatched_kicker():
    player_rows = [{"player_id": "9999", "position": "K", "team": "SF", "name": "Some Kicker"}]

    matches, unmatched = skd.match_kicker_espn_ids(player_rows, espn_k_players=[])

    assert matches == []
    assert unmatched == ["9999"]


def test_persist_espn_ids_writes_string_player_id():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    espn_player = _espn_player("Houston D/ST", "HOU", -16034)
    written = skd.persist_espn_ids(mock_conn, [("HOU", espn_player)])

    assert written == 1
    query, rows = mock_cursor.executemany.call_args.args
    assert rows == [("-16034", "HOU")]


def test_persist_espn_ids_noop_on_empty_matches():
    mock_conn = MagicMock()
    assert skd.persist_espn_ids(mock_conn, []) == 0
    mock_conn.cursor.assert_not_called()


def test_persist_projections_writes_week_scoped_points():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    espn_player = _espn_player("Houston D/ST", "HOU", -16034, week=1, projected_points=7.25)
    written = skd.persist_projections(mock_conn, [("HOU", espn_player)], season=2026, week=1)

    assert written == 1
    query, rows = mock_cursor.executemany.call_args.args
    assert "ON CONFLICT (player_id, season, week)" in query
    assert rows == [("HOU", 2026, 1, 7.25)]


def test_persist_projections_defaults_missing_week_stats_to_zero():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    espn_player = _espn_player("Houston D/ST", "HOU", -16034, week=1, projected_points=7.25)
    # Requesting week 2 stats from a player object that only has week 1 populated.
    written = skd.persist_projections(mock_conn, [("HOU", espn_player)], season=2026, week=2)

    assert written == 1
    _, rows = mock_cursor.executemany.call_args.args
    assert rows == [("HOU", 2026, 2, 0.0)]


def test_fetch_espn_kicker_dst_tags_each_player_with_its_requested_position():
    mock_league = MagicMock()
    k_players = [_espn_player("Some Kicker", "SF", 1)]
    dst_players = [_espn_player("Houston D/ST", "HOU", -16034)]

    def fake_free_agents(week, size, position):
        return k_players if position == "K" else dst_players

    mock_league.free_agents.side_effect = fake_free_agents
    result = skd.fetch_espn_kicker_dst(mock_league, week=1)

    assert ("K", k_players[0]) in result
    assert ("D/ST", dst_players[0]) in result
    assert len(result) == 2


def test_sync_kicker_dst_backfills_espn_ids_and_projections():
    dst_player = _espn_player("Houston D/ST", "HOU", -16034, week=1, projected_points=6.0)
    k_player = _espn_player("Some Kicker", "SF", 12345, week=1, projected_points=8.0)

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("HOU", "D/ST", "HOU", "Houston Texans"),
        ("9999", "K", "SF", "Some Kicker"),
    ]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.ingestion.sync_kicker_dst.build_league") as mock_build_league, \
         patch("src.ingestion.sync_kicker_dst.get_db_connection") as mock_get_conn:
        mock_league = MagicMock()
        mock_league.free_agents.side_effect = (
            lambda week, size, position: [dst_player] if position == "D/ST" else [k_player]
        )
        mock_build_league.return_value = mock_league
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        result = skd.sync_kicker_dst(season=2026, week=1)

    assert result["espn_id_written"] == 2
    assert result["projections_written"] == 2
    assert result["dst_unmatched"] == []
    assert result["k_unmatched"] == []
