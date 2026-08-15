import asyncio
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from src.api import mcp_server

LEAGUE_SETTINGS_ROW = (10, {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 7})


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


# ---------------------------------------------------------------------------
# get_league_roster_deficits
# ---------------------------------------------------------------------------

def test_get_league_roster_deficits_flags_positions_below_starter_requirement():
    cursor = MagicMock()
    cursor.fetchone.return_value = LEAGUE_SETTINGS_ROW
    cursor.fetchall.return_value = [
        ("team1", "RB", 1, 12.5),
        ("team1", "QB", 1, 3.0),
        ("team2", "WR", 4, -2.0),
    ]

    with patch("src.api.mcp_server.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = mcp_server.get_league_roster_deficits(season=2026, week=3)

    assert result["season"] == 2026
    assert result["week"] == 3
    # Starting requirement for RB is 3 (2 starting slots + this league's FLEX share),
    # so a team with only 1 rostered RB is short by 2.
    assert result["teams"]["team1"]["RB"] == {
        "rostered": 1,
        "starters_required": 3,
        "deficit": 2,
        "net_vorp": 12.5,
    }
    # QB has no FLEX/bench share, so 1 rostered QB exactly meets the 1-QB requirement.
    assert result["teams"]["team1"]["QB"] == {
        "rostered": 1,
        "starters_required": 1,
        "deficit": 0,
        "net_vorp": 3.0,
    }
    # A team can be over-rostered (no deficit) while still carrying negative net VORP.
    assert result["teams"]["team2"]["WR"]["deficit"] == 0
    assert result["teams"]["team2"]["WR"]["net_vorp"] == -2.0


def test_get_league_roster_deficits_queries_scoped_to_season_and_week():
    cursor = MagicMock()
    cursor.fetchone.return_value = LEAGUE_SETTINGS_ROW
    cursor.fetchall.return_value = []

    with patch("src.api.mcp_server.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        mcp_server.get_league_roster_deficits(season=2026, week=5)

    roster_query_call = cursor.execute.call_args_list[-1]
    params = roster_query_call.args[1]
    assert params[0] == 2026
    assert params[1] == 5


# ---------------------------------------------------------------------------
# evaluate_proposed_trade
# ---------------------------------------------------------------------------

def test_evaluate_proposed_trade_computes_strict_delta():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("p1", 10.0), ("p2", 4.0), ("p3", 6.0)]

    with patch("src.api.mcp_server.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = mcp_server.evaluate_proposed_trade(
            incoming_player_ids=["p1"],
            outgoing_player_ids=["p2", "p3"],
            season=2026,
            week=3,
        )

    assert result["incoming_total"] == 10.0
    assert result["outgoing_total"] == 10.0
    assert result["delta_vorp"] == 0.0
    assert result["missing_player_ids"] == []


def test_evaluate_proposed_trade_reports_missing_players_instead_of_scoring_zero():
    cursor = MagicMock()
    # p2 has no weekly_stats row for this season/week (bye week, unsynced, or bad ID).
    cursor.fetchall.return_value = [("p1", 10.0)]

    with patch("src.api.mcp_server.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = mcp_server.evaluate_proposed_trade(
            incoming_player_ids=["p1"],
            outgoing_player_ids=["p2"],
            season=2026,
            week=3,
        )

    assert result["incoming_total"] == 10.0
    assert result["outgoing_total"] == 0.0
    assert result["delta_vorp"] == 10.0
    assert result["missing_player_ids"] == ["p2"]


# ---------------------------------------------------------------------------
# GoogleTokenVerifier
# ---------------------------------------------------------------------------

@pytest.fixture
def google_env():
    with patch.object(mcp_server, "MCP_GOOGLE_OAUTH_CLIENT_ID", "client-123"), \
         patch.object(mcp_server, "MCP_GOOGLE_ALLOWED_EMAIL", "owner@example.com"):
        yield


def test_google_token_verifier_accepts_matching_operator(google_env):
    response = MagicMock(status_code=200)
    response.json.return_value = {"aud": "client-123", "email": "owner@example.com", "email_verified": "true"}

    with patch("src.api.mcp_server.requests.get", return_value=response):
        result = asyncio.run(mcp_server.GoogleTokenVerifier().verify_token("sometoken"))

    assert result is not None
    assert result.subject == "owner@example.com"


def test_google_token_verifier_rejects_wrong_audience(google_env):
    response = MagicMock(status_code=200)
    response.json.return_value = {"aud": "someone-elses-client", "email": "owner@example.com", "email_verified": "true"}

    with patch("src.api.mcp_server.requests.get", return_value=response):
        result = asyncio.run(mcp_server.GoogleTokenVerifier().verify_token("sometoken"))

    assert result is None


def test_google_token_verifier_rejects_wrong_email(google_env):
    response = MagicMock(status_code=200)
    response.json.return_value = {"aud": "client-123", "email": "someone-else@example.com", "email_verified": "true"}

    with patch("src.api.mcp_server.requests.get", return_value=response):
        result = asyncio.run(mcp_server.GoogleTokenVerifier().verify_token("sometoken"))

    assert result is None


def test_google_token_verifier_rejects_expired_or_invalid_token(google_env):
    response = MagicMock(status_code=400)

    with patch("src.api.mcp_server.requests.get", return_value=response):
        result = asyncio.run(mcp_server.GoogleTokenVerifier().verify_token("expired"))

    assert result is None


# ---------------------------------------------------------------------------
# App mount smoke test
# ---------------------------------------------------------------------------

def test_mcp_app_mounted_and_v1_routes_still_resolve():
    with patch("src.api.main.run_migrations"), \
         patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(MagicMock())):
        from fastapi.testclient import TestClient
        from src.api.main import app

        with TestClient(app) as client:
            response = client.get("/v1/health")
            assert response.status_code in (200, 503)

            # Unauthenticated MCP calls must be rejected, not silently allowed through.
            mcp_response = client.post("/mcp", json={})
            assert mcp_response.status_code == 401
