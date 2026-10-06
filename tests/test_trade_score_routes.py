from unittest.mock import patch

import pytest
import requests

from src.api import mcp_server
from src.services.trade_context_service import TradeContextUnavailable
from src.services.trade_score_service import TradeOwnershipError, shape_trade_score
from tests.test_trade_score_service import ALL_TEAMS, RAW, context  # noqa: F401

BODY = {
    "league_id": "123",
    "season": 2026,
    "week": 5,
    "horizon": 2,
    "our_team_id": "1",
    "counterparty_team_id": "2",
    "send_espn_ids": ["103"],
    "receive_espn_ids": ["203"],
}


def test_score_route_runs_the_real_scorer_over_the_shared_context(client, context):  # noqa: F811
    with patch("src.services.trade_score_service.get_league_trade_context", return_value=context), \
         patch("src.services.trade_score_service.evaluate_trade", return_value=dict(RAW)), \
         patch("src.services.trade_score_service.load_teams_playing", return_value=set(ALL_TEAMS)):
        response = client.post("/v1/trade/score", json=BODY)
    assert response.status_code == 200
    body = response.json()
    assert body["scoring_version"] == "trade-score-v1"
    assert body["raw_vorp"]["delta_vorp"] == 3.0
    assert body["score"]["our_lineup_gain"] == 6.0
    assert body["horizon"]["scored_weeks"] == [5, 6]


@pytest.mark.parametrize(
    "change",
    [
        {"extra": True},
        {"send_espn_ids": []},
        {"receive_espn_ids": ["s203"]},
        {"our_team_id": "0"},
        {"week": 19},
        {"horizon": 0},
    ],
)
def test_score_route_validates_request_shape(client, change):
    with patch("src.api.trade.score_trade") as scorer:
        response = client.post("/v1/trade/score", json={**BODY, **change})
    assert response.status_code == 422
    scorer.assert_not_called()


@pytest.mark.parametrize(
    "error,status",
    [
        (ValueError("Unknown ESPN team 9"), 422),
        (TradeOwnershipError("ESPN player 203 is on team 2"), 409),
        (TradeContextUnavailable("Missing schedule settings"), 503),
        (requests.Timeout("ESPN stalled"), 504),
    ],
)
def test_score_route_maps_service_failures_like_context(client, error, status):
    with patch("src.api.trade.score_trade", side_effect=error):
        response = client.post("/v1/trade/score", json=BODY)
    assert response.status_code == status


def test_rest_and_mcp_share_the_same_scorer(client, context):  # noqa: F811
    payload = shape_trade_score(
        context, "1", "2", ["103"], ["203"], dict(RAW), {5: set(ALL_TEAMS), 6: set(ALL_TEAMS)}
    )
    with patch("src.api.trade.score_trade", return_value=payload) as rest:
        response = client.post("/v1/trade/score", json=BODY)
    assert response.json() == payload
    args = ("123", 2026, 5, 2, "1", "2", ["103"], ["203"])
    rest.assert_called_once_with(*args)
    with patch("src.api.mcp_server.score_trade", return_value=payload) as mcp:
        assert mcp_server.score_trade_proposal(*args) == payload
    mcp.assert_called_once_with(*args)
