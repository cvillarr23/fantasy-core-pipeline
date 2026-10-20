from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.api import mcp_server
from src.services.trade_context_service import TradeContextUnavailable

COVERAGE = {"crunch_weeks_before": [], "crunch_weeks_after": [], "fixed": [], "created": []}
COMPUTED_AT = datetime(2026, 10, 19, 10, 0, tzinfo=timezone.utc)
TEAMS = [
    {"team_id": "2", "name": "Team 2", "proposable": 1, "needs_drop": 2, "one_sided": 0},
    {"team_id": "3", "name": "Team 3", "proposable": 0, "needs_drop": 0, "one_sided": 4},
]
QUERY = {"league_id": "123", "season": 2026, "week": 5}


def candidate_player(espn_id, position):
    return {
        "espn_id": espn_id, "player_id": f"s{espn_id}", "name": f"Player {espn_id}",
        "position": position, "nfl_team": "KC", "vorp": 4.0, "nfl_bye_week": 7,
    }


def item_row(key="a" * 64, final_score=3.5):
    return (
        key, "2", "Team 2", "1for1",
        [candidate_player("105", "RB")], [candidate_player("203", "RB")],
        "proposable", [], final_score, 2.0, 2.5, 0.0, 1.0, 0.5,
        {"start_week": 5, "end_week": 8, "scored_weeks": [5, 6, 7], "complete_horizon": False},
        {"us": COVERAGE, "counterparty": COVERAGE},
    )


def fake_cursor(run=("run-1", COMPUTED_AT, 4, {"teams": TEAMS}), total=1, rows=None):
    cursor = MagicMock()
    cursor.fetchone.side_effect = [run, (total,)]
    cursor.fetchall.return_value = [item_row()] if rows is None else rows

    @contextmanager
    def get_cursor():
        yield cursor

    return cursor, get_cursor


def get(client, cursor_factory, **params):
    with patch("src.services.trade_candidate_service.get_db_cursor", cursor_factory):
        return client.get("/v1/trade/candidates", params={**QUERY, **params})


def test_candidates_route_shapes_the_stored_run(client):
    cursor, factory = fake_cursor()
    response = get(client, factory)
    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] == "run-1"
    assert body["computed_at"].startswith("2026-10-19T10:00:00")
    assert body["horizon"] == 4 and body["total"] == 1
    assert body["team_counts"] == TEAMS
    item = body["items"][0]
    assert item["counterparty"] == {"team_id": "2", "name": "Team 2"}
    assert item["send"][0]["espn_id"] == "105" and item["receive"][0]["espn_id"] == "203"
    assert item["receive"][0]["nfl_bye_week"] == 7 and item["receive"][0]["vorp"] == 4.0
    assert item["our_lineup_gain"] == 2.0 and item["counterparty_lineup_change"] == 0.0
    assert item["scored_weeks"] == [5, 6, 7]
    assert item["horizon_weeks"] == [5, 6, 7, 8]
    assert item["complete_horizon"] is False
    assert item["status"] == "proposable" and item["reasons"] == []
    assert item["candidate_key"] == "a" * 64


def test_candidates_route_defaults_to_proposable_by_final_score(client):
    cursor, factory = fake_cursor()
    get(client, factory)
    count_sql, count_params = cursor.execute.call_args_list[1].args
    assert "status = %s" in count_sql
    assert count_params == ["123", 2026, 5, "proposable"]
    items_sql, items_params = cursor.execute.call_args_list[2].args
    assert "ORDER BY final_score DESC NULLS LAST, candidate_key" in items_sql
    assert items_params[-2:] == [50, 0]


def test_candidates_route_passes_every_filter_sort_and_page(client):
    cursor, factory = fake_cursor()
    response = get(
        client, factory, counterparty_team_id="2", receive_position="RB", send_position="WR",
        shape="2for1", status="all", sort="their_benefit", limit=10, offset=20,
    )
    assert response.status_code == 200
    sql, params = cursor.execute.call_args_list[2].args
    assert "counterparty_team_id = %s" in sql
    assert "%s = ANY(receive_positions)" in sql and "%s = ANY(send_positions)" in sql
    assert "shape = %s" in sql
    assert "status = %s" not in sql
    assert "ORDER BY counterparty_lineup_change DESC NULLS LAST" in sql
    assert params == ["123", 2026, 5, "2", "RB", "WR", "2for1", 10, 20]


@pytest.mark.parametrize("sort,column", [("our_gain", "our_lineup_gain"), ("final_score", "final_score")])
def test_candidates_route_sort_columns(client, sort, column):
    cursor, factory = fake_cursor()
    get(client, factory, sort=sort)
    assert f"ORDER BY {column} DESC NULLS LAST" in cursor.execute.call_args_list[2].args[0]


@pytest.mark.parametrize(
    "params",
    [
        {"shape": "3for1"},
        {"status": "great"},
        {"sort": "name"},
        {"limit": 0},
        {"limit": 201},
        {"offset": -1},
        {"counterparty_team_id": "abc"},
        {"week": 19},
    ],
)
def test_candidates_route_rejects_bad_parameters(client, params):
    _, factory = fake_cursor()
    assert get(client, factory, **params).status_code == 422


def test_candidates_route_404s_before_the_first_run(client):
    _, factory = fake_cursor(run=None)
    response = get(client, factory)
    assert response.status_code == 404
    assert "trade_candidates job has not run" in response.json()["detail"]


def test_candidates_route_returns_an_empty_run(client):
    _, factory = fake_cursor(total=0, rows=[])
    response = get(client, factory)
    assert response.status_code == 200
    assert response.json()["items"] == [] and response.json()["total"] == 0


def test_candidates_route_maps_trade_read_errors(client):
    with patch(
        "src.services.trade_candidate_service.list_trade_candidates",
        side_effect=TradeContextUnavailable("nope"),
    ):
        response = client.get("/v1/trade/candidates", params=QUERY)
    assert response.status_code == 503


def test_list_trade_candidates_mcp_tool_uses_the_shared_service():
    with patch.object(mcp_server, "list_candidates", return_value={"items": []}) as service:
        result = mcp_server.list_trade_candidates("123", 2026, 5, shape="1for1", limit=500)
    assert result == {"items": []}
    service.assert_called_once_with(
        "123", 2026, 5, None, None, None, "1for1", "proposable", "final_score", 200, 0
    )
