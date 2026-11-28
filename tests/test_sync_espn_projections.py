import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion import sync_espn_projections as sep


def _espn_player(player_id, pro_team="SF", week=13, projected_points=15.0):
    stats = {week: {"projected_points": projected_points}} if projected_points is not None else {}
    return SimpleNamespace(playerId=player_id, proTeam=pro_team, stats=stats)


def _mock_conn():
    conn, cursor = MagicMock(), MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn, cursor


def _league_returning(counts):
    """A league whose kona_player_info call returns `counts[position]` players."""
    league = MagicMock()
    league.year = 2026
    slot_to_position = {sep.POSITION_MAP[p]: p for p in sep.SKILL_POSITIONS}

    def league_get(params, headers):
        slot = json.loads(headers["x-fantasy-filter"])["players"]["filterSlotIds"]["value"][0]
        return {"players": [{"id": i} for i in range(counts[slot_to_position[slot]])]}

    league.espn_request.league_get.side_effect = league_get
    return league


def test_build_values_matches_by_espn_id_only():
    """A player is matched by players.espn_id, never by anything else; ESPN players with no
    matching row are ignored."""
    rows = [{"player_id": "4046", "espn_id": "3139477"}, {"player_id": "6794", "espn_id": "4241389"}]
    entries = {"3139477": 18.5, "9999999": 30.0}

    values, not_returned = sep.build_espn_projection_values(rows, entries)

    assert values == [("4046", 18.5), ("6794", None)]
    assert not_returned == ["6794"]


def test_projection_entries_clear_a_missing_projection_and_a_schedule_bye():
    """ESPN projects some byes at 0; the NFL schedule decides."""
    entries = sep.projection_entries(
        [
            _espn_player(1, projected_points=None),
            _espn_player(2, pro_team="SF", projected_points=0.0),
            _espn_player(3, pro_team="KC", projected_points=17.0),
        ],
        week=13, bye_weeks={"SF": 13, "KC": 6},
    )
    assert entries == {"1": None, "2": None, "3": 17.0}


def test_persist_updates_only_rows_sleeper_projected_and_recomputes_the_consensus():
    conn, cursor = _mock_conn()

    with patch("src.ingestion.sync_espn_projections.recompute_consensus") as recompute:
        sep.persist_espn_projections(conn, [("4046", 18.5), ("6794", None)], season=2026, week=13)

    query, params = cursor.execute.call_args.args
    query = " ".join(query.split())
    assert query.startswith("UPDATE weekly_stats")
    assert "INSERT" not in query
    assert "SET espn_projected_points = v.points" in query
    # Never a row without a Sleeper value: that would make it ESPN-only, and covers rows from
    # before #462 and a schedule failure's empty bye map.
    assert "AND s.sleeper_projected_points IS NOT NULL" in query
    assert params == (["4046", "6794"], [18.5, None], 2026, 13)
    recompute.assert_called_once_with(cursor, 2026, 13, ["4046", "6794"])


def test_persist_noop_without_values():
    conn, _ = _mock_conn()
    assert sep.persist_espn_projections(conn, [], season=2026, week=13) == 0
    conn.cursor.assert_not_called()


def test_fetch_requests_each_skill_position_for_the_target_week_without_an_ownership_filter():
    league = _league_returning({"QB": 100, "RB": 200, "WR": 300, "TE": 100})

    with patch("src.ingestion.sync_espn_projections.Player",
               side_effect=lambda raw, year: _espn_player(raw["id"])):
        sep.fetch_projection_entries(league, week=13, bye_weeks={})

    calls = league.espn_request.league_get.call_args_list
    assert len(calls) == len(sep.SKILL_POSITIONS)
    for call in calls:
        assert call.kwargs["params"] == {"view": "kona_player_info", "scoringPeriodId": 13}
        assert "filterStatus" not in json.loads(call.kwargs["headers"]["x-fantasy-filter"])["players"]


@pytest.mark.parametrize("short_position, count", [("WR", 0), ("TE", 5)])
def test_an_empty_or_short_fetch_fails_and_writes_nothing(short_position, count):
    """A broken ESPN response must never null the stored ESPN values."""
    counts = {"QB": 100, "RB": 200, "WR": 300, "TE": 100, short_position: count}
    conn, cursor = _mock_conn()

    with patch("src.ingestion.sync_espn_projections.build_league", return_value=_league_returning(counts)), \
         patch("src.ingestion.sync_espn_projections.Player", side_effect=lambda raw, year: _espn_player(raw["id"])), \
         patch("src.ingestion.sync_espn_projections.bye_weeks_or_empty", return_value={}), \
         patch("src.ingestion.sync_espn_projections.get_db_connection") as db:
        db.return_value.__enter__.return_value = conn
        with pytest.raises(sep.IncompleteEspnFetchError, match=short_position):
            sep.sync_espn_projections(season=2026, week=13)

    db.assert_not_called()
    cursor.execute.assert_not_called()


def test_sync_uses_a_request_timeout():
    with patch("src.ingestion.sync_espn_projections.build_league", side_effect=RuntimeError("stop")) as build:
        with pytest.raises(RuntimeError):
            sep.sync_espn_projections(season=2026, week=13)
    build.assert_called_once_with(2026, request_timeout_seconds=sep.ESPN_REQUEST_TIMEOUT_SECONDS)
