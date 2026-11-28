import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from src.ingestion import sync_espn_projections as sep


def _espn_player(player_id, pro_team="SF", week=13, projected_points=15.0):
    stats = {week: {"projected_points": projected_points}} if projected_points is not None else {}
    return SimpleNamespace(playerId=player_id, proTeam=pro_team, stats=stats)


def _mock_conn():
    conn, cursor = MagicMock(), MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn, cursor


def test_build_values_matches_by_espn_id_only():
    """A player is matched by players.espn_id, never by anything else; ESPN players with no
    matching row are ignored."""
    rows = [{"player_id": "4046", "espn_id": "3139477"}, {"player_id": "6794", "espn_id": "4241389"}]
    espn = [_espn_player(3139477, projected_points=18.5), _espn_player(9999999, projected_points=30.0)]

    values, not_returned = sep.build_espn_projection_values(rows, espn, week=13, bye_weeks={})

    assert values == [("4046", 18.5), ("6794", None)]
    assert not_returned == ["6794"]


def test_build_values_clears_a_missing_projection():
    rows = [{"player_id": "4046", "espn_id": "1"}]
    values, not_returned = sep.build_espn_projection_values(
        rows, [_espn_player(1, projected_points=None)], week=13, bye_weeks={}
    )
    assert values == [("4046", None)]
    assert not_returned == []


def test_build_values_clears_a_schedule_bye_even_when_espn_projects_zero():
    rows = [{"player_id": "4046", "espn_id": "1"}]
    values, _ = sep.build_espn_projection_values(
        rows, [_espn_player(1, pro_team="SF", projected_points=0.0)], week=13, bye_weeks={"SF": 13}
    )
    assert values == [("4046", None)]


def test_persist_updates_existing_rows_only_and_recomputes_the_consensus():
    conn, cursor = _mock_conn()

    with patch("src.ingestion.sync_espn_projections.recompute_consensus") as recompute:
        sep.persist_espn_projections(conn, [("4046", 18.5), ("6794", None)], season=2026, week=13)

    query, params = cursor.execute.call_args.args
    assert query.lstrip().startswith("UPDATE weekly_stats")
    assert "INSERT" not in query
    assert "SET espn_projected_points = v.points" in query
    assert "sleeper_projected_points" not in query
    assert params == (["4046", "6794"], [18.5, None], 2026, 13)
    recompute.assert_called_once_with(cursor, 2026, 13, ["4046", "6794"])


def test_persist_noop_without_values():
    conn, _ = _mock_conn()
    assert sep.persist_espn_projections(conn, [], season=2026, week=13) == 0
    conn.cursor.assert_not_called()


def test_fetch_requests_each_skill_position_for_the_target_week_without_an_ownership_filter():
    league = MagicMock()
    league.year = 2026
    league.espn_request.league_get.return_value = {"players": []}

    sep.fetch_espn_skill_players(league, week=13)

    calls = league.espn_request.league_get.call_args_list
    assert len(calls) == len(sep.SKILL_POSITIONS)
    for call in calls:
        assert call.kwargs["params"] == {"view": "kona_player_info", "scoringPeriodId": 13}
        assert "filterStatus" not in json.loads(call.kwargs["headers"]["x-fantasy-filter"])["players"]
