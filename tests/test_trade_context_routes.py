from unittest.mock import patch

import pytest
import requests

from src.api import mcp_server
from src.services.trade_context_service import TradeContextUnavailable


@pytest.mark.parametrize(
    "query",
    [
        "",
        "?league_id=123&season=2026&week=0",
        "?league_id=123&season=2026&week=3&horizon=19",
        "?league_id=other&season=2026&week=3",
    ],
)
def test_context_route_validates_inputs(client, query):
    assert client.get("/v1/trade/context" + query).status_code == 422


@pytest.mark.parametrize(
    "error,status",
    [
        (ValueError("Wrong league"), 422),
        (TradeContextUnavailable("Missing schedule settings"), 503),
        (requests.Timeout("ESPN stalled"), 504),
    ],
)
def test_context_route_maps_service_failures(client, error, status):
    with patch("src.api.trade.get_league_trade_context", side_effect=error):
        response = client.get("/v1/trade/context?league_id=123&season=2026&week=3")
    assert response.status_code == status


def test_rest_and_mcp_share_the_same_context_service(client):
    payload = {
        "league_id": "123",
        "season": 2026,
        "as_of_week": 3,
        "start_week": 3,
        "end_week": 6,
        "regular_season_end_week": 6,
        "lineup_slot_counts": {"RB": 2},
        "roster_snapshot": {
            "kind": "current",
            "source": "ESPN mRoster",
            "fetched_at": "2026-10-05T12:00:00+00:00",
            "observed_week": 3,
            "matches_as_of_week": True,
        },
        "strength_metric": {
            "name": "completed_fantasy_points_per_week_z",
            "source": "ESPN completed regular-season matchup totals",
            "league_mean": None,
            "league_stddev": None,
            "teams_with_strength": 0,
        },
        "coverage": {"unmapped_espn_ids": [], "missing_metric_count": 0},
        "teams": {},
    }
    with patch("src.api.trade.get_league_trade_context", return_value=payload) as rest:
        response = client.get(
            "/v1/trade/context?league_id=123&season=2026&week=3&horizon=4"
        )
    assert response.status_code == 200
    assert response.json() == payload
    rest.assert_called_once_with("123", 2026, 3, 4)
    with patch(
        "src.api.mcp_server.get_league_trade_context", return_value=payload
    ) as mcp:
        assert mcp_server.get_league_trade_context_model("123", 2026, 3, 4) == payload
    mcp.assert_called_once_with("123", 2026, 3, 4)
