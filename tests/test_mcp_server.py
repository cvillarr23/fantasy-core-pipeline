import asyncio
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.api import mcp_server

LEAGUE_SETTINGS_ROW = (10, {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 7})


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


class _FakeAsyncClient:
    """Stands in for httpx.AsyncClient so GoogleTokenVerifier's async call can be
    exercised without a real network request."""

    def __init__(self, *args, response=None, exc=None, **kwargs):
        self._response = response
        self._exc = exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, url, params=None):
        if self._exc:
            raise self._exc
        return self._response


def _mock_google_response(status_code=200, **claims):
    response = MagicMock(status_code=status_code)
    response.json.return_value = claims
    return response


# ---------------------------------------------------------------------------
# get_league_roster_deficits
# ---------------------------------------------------------------------------

def test_get_league_roster_deficits_flags_positions_below_starter_requirement():
    cursor = MagicMock()
    cursor.fetchone.return_value = LEAGUE_SETTINGS_ROW
    cursor.fetchall.side_effect = [
        [("team1",), ("team2",)],  # SELECT DISTINCT team_id
        [
            ("team1", "RB", 1, 12.5),
            ("team1", "QB", 1, 3.0),
            ("team2", "WR", 4, -2.0),
        ],
    ]

    with patch("src.api.mcp_server.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = mcp_server.get_league_roster_deficits(season=2026, week=3)

    assert result["season"] == 2026
    assert result["week"] == 3
    assert result["roster_snapshot"] == "current"
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


def test_get_league_roster_deficits_reports_zero_rostered_positions_as_full_deficit():
    # team2 has rostered players (it's in the distinct team_id list) but none at any
    # skill position - the grouped query returns nothing for it at all. Every position
    # must still appear for team2, showing the maximum possible deficit, not vanish.
    cursor = MagicMock()
    cursor.fetchone.return_value = LEAGUE_SETTINGS_ROW
    cursor.fetchall.side_effect = [
        [("team1",), ("team2",)],
        [("team1", "RB", 1, 12.5)],
    ]

    with patch("src.api.mcp_server.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = mcp_server.get_league_roster_deficits(season=2026, week=3)

    assert set(result["teams"]["team2"].keys()) == {"QB", "RB", "WR", "TE"}
    assert result["teams"]["team2"]["RB"] == {
        "rostered": 0,
        "starters_required": 3,
        "deficit": 3,
        "net_vorp": 0.0,
    }
    assert result["teams"]["team2"]["QB"]["deficit"] == 1


def test_get_league_roster_deficits_queries_scoped_to_season_and_week():
    cursor = MagicMock()
    cursor.fetchone.return_value = LEAGUE_SETTINGS_ROW
    cursor.fetchall.side_effect = [[], []]

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


def test_evaluate_proposed_trade_rejects_duplicate_incoming_ids():
    with pytest.raises(ValueError, match="duplicate"):
        mcp_server.evaluate_proposed_trade(
            incoming_player_ids=["p1", "p1"],
            outgoing_player_ids=["p2"],
            season=2026,
            week=3,
        )


def test_evaluate_proposed_trade_rejects_duplicate_outgoing_ids():
    with pytest.raises(ValueError, match="duplicate"):
        mcp_server.evaluate_proposed_trade(
            incoming_player_ids=["p1"],
            outgoing_player_ids=["p2", "p2"],
            season=2026,
            week=3,
        )


def test_evaluate_proposed_trade_rejects_player_on_both_sides():
    with pytest.raises(ValueError, match="both sides"):
        mcp_server.evaluate_proposed_trade(
            incoming_player_ids=["p1", "p2"],
            outgoing_player_ids=["p2", "p3"],
            season=2026,
            week=3,
        )


# ---------------------------------------------------------------------------
# GoogleTokenVerifier
# ---------------------------------------------------------------------------

@pytest.fixture
def google_env():
    with patch.object(mcp_server, "MCP_GOOGLE_OAUTH_CLIENT_ID", "client-123"), \
         patch.object(mcp_server, "MCP_GOOGLE_ALLOWED_EMAIL", "owner@example.com"):
        yield


def test_google_token_verifier_accepts_matching_operator(google_env):
    response = _mock_google_response(aud="client-123", email="owner@example.com", email_verified="true")

    with patch("src.api.mcp_server.httpx.AsyncClient", lambda **kw: _FakeAsyncClient(response=response)):
        result = asyncio.run(mcp_server.GoogleTokenVerifier().verify_token("sometoken"))

    assert result is not None
    assert result.subject == "owner@example.com"


def test_google_token_verifier_rejects_wrong_audience(google_env):
    response = _mock_google_response(aud="someone-elses-client", email="owner@example.com", email_verified="true")

    with patch("src.api.mcp_server.httpx.AsyncClient", lambda **kw: _FakeAsyncClient(response=response)):
        result = asyncio.run(mcp_server.GoogleTokenVerifier().verify_token("sometoken"))

    assert result is None


def test_google_token_verifier_rejects_wrong_email(google_env):
    response = _mock_google_response(aud="client-123", email="someone-else@example.com", email_verified="true")

    with patch("src.api.mcp_server.httpx.AsyncClient", lambda **kw: _FakeAsyncClient(response=response)):
        result = asyncio.run(mcp_server.GoogleTokenVerifier().verify_token("sometoken"))

    assert result is None


def test_google_token_verifier_rejects_expired_or_invalid_token(google_env):
    response = _mock_google_response(status_code=400)

    with patch("src.api.mcp_server.httpx.AsyncClient", lambda **kw: _FakeAsyncClient(response=response)):
        result = asyncio.run(mcp_server.GoogleTokenVerifier().verify_token("expired"))

    assert result is None


def test_google_token_verifier_does_not_log_bearer_token_on_network_error(google_env, caplog):
    secret_token = "super-secret-bearer-token-value"
    exc = httpx.ConnectError(f"failed to connect: ...?access_token={secret_token}")

    with patch("src.api.mcp_server.httpx.AsyncClient", lambda **kw: _FakeAsyncClient(exc=exc)):
        with caplog.at_level("ERROR"):
            result = asyncio.run(mcp_server.GoogleTokenVerifier().verify_token(secret_token))

    assert result is None
    assert secret_token not in caplog.text


# ---------------------------------------------------------------------------
# App mount / lifespan / authenticated handshake
# ---------------------------------------------------------------------------

def test_mcp_app_mounted_v1_routes_resolve_and_authenticated_handshake_succeeds(client):
    """Uses the shared session-scoped `client` fixture (tests/conftest.py) rather than
    its own TestClient(app): mcp.session_manager.run() can only be entered once per
    process for the life of that FastMCP instance, so a second independent
    `with TestClient(app)` block on the same (module-singleton) app would raise
    'StreamableHTTPSessionManager .run() can only be called once per instance.'

    Also a regression test for the session-manager lifespan wiring itself: app.mount()
    alone never runs a sub-app's own lifespan, so mcp.session_manager.run() must be
    entered explicitly by the parent app's lifespan (src/api/main.py). Without that, the
    authenticated request below fails with 'Task group is not initialized' instead of
    completing - the unauthenticated 401 check alone wouldn't catch that, since auth is
    resolved before the transport layer that needs the session manager.
    """
    from mcp.server.auth.provider import AccessToken

    async def _fake_verify(self, token):
        return AccessToken(token=token, client_id="test-client", scopes=["mcp:tools"], subject="owner@example.com")

    with patch("src.api.routes.get_db_cursor", lambda: _fake_db_cursor(MagicMock())):
        response = client.get("/v1/health")
    assert response.status_code in (200, 503)

    # Unauthenticated MCP calls must be rejected, not silently allowed through.
    unauth_response = client.post("/mcp", json={})
    assert unauth_response.status_code == 401

    with patch.object(mcp_server.GoogleTokenVerifier, "verify_token", _fake_verify):
        auth_response = client.post(
            "/mcp",
            headers={"Authorization": "Bearer faketoken", "Accept": "application/json, text/event-stream"},
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "1.0"},
                },
            },
        )

    assert auth_response.status_code == 200
    assert '"serverInfo"' in auth_response.text
    assert "fantasy-core-mcp" in auth_response.text
