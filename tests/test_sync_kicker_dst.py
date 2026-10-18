import json
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
    assert rows == [("HOU", 2026, 1, 7.25, "HOU")]


def test_persist_projections_writes_team_so_the_pool_query_never_sees_a_null(): # noqa: E501
    """Regression test: draft_matrix.load_undrafted_pool selects s.team (not
    players.team), and publish_draft_target forwards it as the required pro_team field -
    leaving weekly_stats.team NULL here made every K/D-ST recommendation get rejected with
    a 422 the moment the worker tried to publish one (caught live verifying #83)."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    espn_player = _espn_player("Washington D/ST", "WSH", -16028, week=1, projected_points=6.5)
    skd.persist_projections(mock_conn, [("WAS", espn_player)], season=2026, week=1)

    query, rows = mock_cursor.executemany.call_args.args
    assert "team" in query
    # ESPN's "WSH" normalized to Sleeper/this repo's "WAS" - same mismatch match_dst_espn_ids handles.
    assert rows == [("WAS", 2026, 1, 6.5, "WAS")]


def test_persist_projections_skips_players_with_no_projection_for_the_week():
    """A player with no target-week projection (a bye) must get no row at all:
    lineup_solver.load_pregame_roster_pool reads "no target-week row" as "on bye", so a
    0.0 row would make a bye-week K/D-ST look startable (#183)."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    on_bye = _espn_player("Houston D/ST", "HOU", -16034, week=1, projected_points=7.25)
    playing = _espn_player("SF D/ST", "SF", -16025, week=2, projected_points=5.5)
    written = skd.persist_projections(mock_conn, [("HOU", on_bye), ("SF", playing)], season=2026, week=2)

    assert written == 1
    _, rows = mock_cursor.executemany.call_args.args
    assert rows == [("SF", 2026, 2, 5.5, "SF")]


def test_persist_projections_deletes_an_earlier_runs_row_once_espn_drops_the_projection():
    """The refresher re-targets the same week daily (Tue-Sun). If ESPN drops a K/D-ST's
    projection mid-week (e.g. a released kicker), the row an earlier run wrote must go too,
    or the pregame pool keeps treating him as playing on stale points."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    dropped = _espn_player("Released Kicker", "SF", 12345, week=1)

    written = skd.persist_projections(mock_conn, [("9999", dropped)], season=2026, week=2)

    assert written == 0
    mock_cursor.executemany.assert_not_called()
    query, params = mock_cursor.execute.call_args.args
    assert query.lstrip().startswith("DELETE FROM weekly_stats")
    # Scoped to K/D-ST rows: no other ingestion step writes those, so nothing else is lost.
    assert "position IN ('K', 'D/ST')" in query
    assert params == (["9999"], 2026, 2)


def test_persist_projections_noop_on_empty_matches():
    mock_conn = MagicMock()
    assert skd.persist_projections(mock_conn, [], season=2026, week=2) == 0
    mock_conn.cursor.assert_not_called()


def _raw_kona_player(player_id, name, pro_team_id, slot_id, week, projected_points=None, on_team_id=0):
    """A kona_player_info entry, shaped like ESPN's raw JSON so espn_api's Player parses it."""
    stats = []
    if projected_points is not None:
        stats.append({
            "seasonId": 2026, "scoringPeriodId": week, "statSourceId": 1, "statSplitTypeId": 1,
            "appliedTotal": projected_points,
        })
    return {
        "id": player_id,
        "onTeamId": on_team_id,
        "player": {
            "id": player_id, "fullName": name, "proTeamId": pro_team_id,
            "eligibleSlots": [slot_id], "ownership": {}, "stats": stats,
        },
    }


# espn_api POSITION_MAP slot ids / PRO_TEAM_MAP team ids.
K_SLOT, DST_SLOT = 17, 16
MIN_TEAM_ID, PHI_TEAM_ID = 16, 21


def _kona_league(raw_by_slot):
    mock_league = MagicMock()
    mock_league.year = 2026

    def league_get(params, headers):
        slot = json.loads(headers["x-fantasy-filter"])["players"]["filterSlotIds"]["value"][0]
        return {"players": raw_by_slot.get(slot, [])}

    mock_league.espn_request.league_get.side_effect = league_get
    return mock_league


def test_fetch_espn_kicker_dst_queries_target_week_with_no_ownership_filter():
    mock_league = _kona_league({})

    skd.fetch_espn_kicker_dst(mock_league, week=5)

    assert mock_league.espn_request.league_get.call_count == 2
    for call in mock_league.espn_request.league_get.call_args_list:
        params, headers = call.kwargs["params"], call.kwargs["headers"]
        assert params == {"view": "kona_player_info", "scoringPeriodId": 5}
        # filterStatus is what limits League.free_agents() to unowned players.
        assert "filterStatus" not in json.loads(headers["x-fantasy-filter"])["players"]
    mock_league.free_agents.assert_not_called()


def test_fetch_espn_kicker_dst_returns_rostered_and_free_agent_players_with_week_projections():
    mock_league = _kona_league({
        K_SLOT: [
            _raw_kona_player(4567104, "Will Reichard", MIN_TEAM_ID, K_SLOT, week=5, projected_points=8.4, on_team_id=8),
            _raw_kona_player(1, "Free Kicker", PHI_TEAM_ID, K_SLOT, week=5, projected_points=6.1),
        ],
        DST_SLOT: [
            _raw_kona_player(-16021, "Eagles D/ST", PHI_TEAM_ID, DST_SLOT, week=5, projected_points=7.0, on_team_id=8),
        ],
    })

    result = skd.fetch_espn_kicker_dst(mock_league, week=5)

    by_id = {player.playerId: (position, player) for position, player in result}
    assert set(by_id) == {4567104, 1, -16021}
    assert by_id[4567104][0] == "K"
    assert by_id[-16021][0] == "D/ST"
    assert by_id[-16021][1].proTeam == "PHI"
    assert by_id[4567104][1].stats[5]["projected_points"] == 8.4
    assert by_id[-16021][1].stats[5]["projected_points"] == 7.0


def test_match_kicker_espn_ids_prefers_existing_espn_id_over_a_stale_team():
    # players.team is stale (signed elsewhere since the last sync_players run), but the
    # row's espn_id still identifies the kicker.
    player_rows = [{"player_id": "11792", "position": "K", "team": "LV", "name": "Will Reichard", "espn_id": "4567104"}]
    espn_k = [_espn_player("Will Reichard", "MIN", 4567104)]

    matches, unmatched = skd.match_kicker_espn_ids(player_rows, espn_k)

    assert unmatched == []
    assert matches == [("11792", espn_k[0])]


def test_sync_kicker_dst_writes_target_week_projections_for_rostered_kicker_and_dst():
    week = 5
    mock_league = _kona_league({
        K_SLOT: [_raw_kona_player(4567104, "Will Reichard", MIN_TEAM_ID, K_SLOT, week, 8.4, on_team_id=8)],
        DST_SLOT: [_raw_kona_player(-16021, "Eagles D/ST", PHI_TEAM_ID, DST_SLOT, week, 7.0, on_team_id=8)],
    })

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("PHI", "D/ST", "PHI", "Philadelphia Eagles", "-16021"),
        ("11792", "K", "MIN", "Will Reichard", "4567104"),
    ]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.ingestion.sync_kicker_dst.build_league", return_value=mock_league), \
         patch("src.ingestion.sync_kicker_dst.get_db_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        result = skd.sync_kicker_dst(season=2026, week=week)

    assert result["projections_written"] == 2
    assert result["dst_unmatched"] == []
    assert result["k_unmatched"] == []
    espn_id_call, projection_call = mock_cursor.executemany.call_args_list
    assert sorted(espn_id_call.args[1]) == [("-16021", "PHI"), ("4567104", "11792")]
    assert sorted(projection_call.args[1]) == [
        ("11792", 2026, week, 8.4, "MIN"),
        ("PHI", 2026, week, 7.0, "PHI"),
    ]


def test_persist_projections_treats_espns_zero_point_bye_projection_as_none():
    """ESPN projects a kicker on bye at 0 instead of leaving the week out. The NFL bye
    map decides, so he gets no row and the old one is deleted."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    on_bye = _espn_player("Will Reichard", "MIN", 4567, week=6, projected_points=0.0)
    playing = _espn_player("SF D/ST", "SF", -16025, week=6, projected_points=5.5)

    written = skd.persist_projections(
        mock_conn, [("k1", on_bye), ("SF", playing)], season=2026, week=6, bye_weeks={"MIN": 6, "SF": 14},
    )

    assert written == 1
    _, rows = mock_cursor.executemany.call_args.args
    assert rows == [("SF", 2026, 6, 5.5, "SF")]
    _, params = mock_cursor.execute.call_args.args
    assert params == (["k1"], 2026, 6)
