import asyncio
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.api import mcp_server


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
# get_league_roster_deficits / evaluate_proposed_trade - thin delegation to
# src.services.roster_service / src.services.trade_service. The real behavior is
# covered by tests/test_roster_service.py and tests/test_trade_service.py; these just
# confirm the MCP tool still forwards to the shared service with the right args.
# ---------------------------------------------------------------------------

def test_get_league_roster_deficits_delegates_to_roster_service():
    with patch("src.api.mcp_server.compute_roster_deficits") as mock_compute:
        mock_compute.return_value = {"season": 2026, "week": 3, "roster_snapshot": "current", "teams": {}}
        result = mcp_server.get_league_roster_deficits(season=2026, week=3)

    mock_compute.assert_called_once_with(2026, 3)
    assert result["season"] == 2026


def test_get_team_roster_delegates_to_roster_service():
    with patch("src.api.mcp_server.get_team_roster_players") as mock_roster:
        mock_roster.return_value = {"team_id": "10", "season": 2026, "week": 3, "starters": [], "bench": []}
        result = mcp_server.get_team_roster(team_id="10", season=2026, week=3)

    mock_roster.assert_called_once_with("10", 2026, 3)
    assert result["team_id"] == "10"


def test_evaluate_proposed_trade_delegates_to_trade_service():
    with patch("src.api.mcp_server.evaluate_trade") as mock_evaluate:
        mock_evaluate.return_value = {
            "incoming_total": 10.0, "outgoing_total": 0.0, "delta_vorp": 10.0, "missing_player_ids": [],
        }
        result = mcp_server.evaluate_proposed_trade(
            incoming_player_ids=["p1"], outgoing_player_ids=["p2"], season=2026, week=3,
        )

    mock_evaluate.assert_called_once_with(["p1"], ["p2"], 2026, 3)
    assert result["delta_vorp"] == 10.0


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
