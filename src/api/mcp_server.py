"""
Model Context Protocol server infrastructure (CORE-07).

Exposes internal Postgres-backed VORP/roster data as MCP tools a remote LLM can call
directly instead of a human copy-pasting query results into a chat. Mounted as a
Starlette sub-app under the main FastAPI gateway (src/api/main.py) rather than a second
process, matching the issue's "additional router mount" instruction.

Auth: resource-server-only OAuth 2.1 (RFC 9728), delegating the actual authorization
server role to Google - the same Google OAuth Cloud client homelab-deployment already
uses for OpenBao's OIDC login (that repo's own convention is "add a redirect URI to the
existing client rather than standing up a new one"). GoogleTokenVerifier only validates
bearer tokens already issued by Google; it never runs an authorization/token exchange
itself. MCP_GOOGLE_OAUTH_CLIENT_ID/MCP_GOOGLE_ALLOWED_EMAIL/MCP_RESOURCE_URL are sourced
from OpenBao in deployed environments, same as ESPN credentials.
"""
import logging
import os

import requests
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP

from src.database import get_db_cursor
from src.optimization.draft_matrix import compute_position_caps
from src.optimization.vorp_matrix import SKILL_POSITIONS, load_league_config

logger = logging.getLogger("MCPEngine")

GOOGLE_TOKENINFO_URL = "https://oauth2.googleapis.com/tokeninfo"
MCP_GOOGLE_OAUTH_CLIENT_ID = os.getenv("MCP_GOOGLE_OAUTH_CLIENT_ID")
MCP_GOOGLE_ALLOWED_EMAIL = os.getenv("MCP_GOOGLE_ALLOWED_EMAIL")
MCP_RESOURCE_URL = os.getenv("MCP_RESOURCE_URL", "http://fantasy-core-pipeline.default.svc.cluster.local:8000/mcp")


class GoogleTokenVerifier(TokenVerifier):
    """
    Verifies bearer tokens as Google OAuth access tokens via Google's tokeninfo
    endpoint - this server is a Resource Server only (RFC 8707/9728 split), it never
    issues or stores tokens itself. Mirrors OpenBao's own OIDC setup: a single allowed
    email (bound_claims-style), not a real multi-user client registry, since this
    replaces one operator's manual data lookups, not a shared login system.
    """

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            response = requests.get(GOOGLE_TOKENINFO_URL, params={"access_token": token}, timeout=5)
        except requests.RequestException as e:
            logger.error(f"Google tokeninfo lookup failed: {e}")
            return None

        if response.status_code != 200:
            return None

        claims = response.json()
        if claims.get("aud") != MCP_GOOGLE_OAUTH_CLIENT_ID:
            logger.warning("Rejected MCP bearer token: audience does not match this server's OAuth client.")
            return None
        if claims.get("email") != MCP_GOOGLE_ALLOWED_EMAIL or claims.get("email_verified") not in ("true", True):
            logger.warning("Rejected MCP bearer token: email not on the allowed operator list.")
            return None

        return AccessToken(
            token=token,
            client_id=claims["aud"],
            scopes=["mcp:tools"],
            subject=claims["email"],
        )


mcp = FastMCP(
    name="fantasy-core-mcp",
    instructions="Structured tools for reasoning about this fantasy football league's roster construction and trade math.",
    token_verifier=GoogleTokenVerifier(),
    auth=AuthSettings(
        issuer_url="https://accounts.google.com",
        resource_server_url=MCP_RESOURCE_URL,
    ),
)


@mcp.tool()
def get_league_roster_deficits(season: int, week: int) -> dict:
    """
    Computes net VORP and starter-slot deficits per team/position across the whole
    league, so a negotiating LLM can see exactly which managers are structurally thin
    at a position before proposing a trade.
    """
    with get_db_cursor() as cursor:
        league_config = load_league_config(cursor)
        _, starting_requirements = compute_position_caps(league_config, drafted_by_us={})

        cursor.execute(
            """
            SELECT r.team_id, p.position, COUNT(*) AS player_count,
                   COALESCE(SUM(s.vorp_score), 0.0) AS net_vorp
            FROM league_rosters r
            JOIN players p ON r.player_id = p.player_id
            LEFT JOIN weekly_stats s
                ON s.player_id = r.player_id AND s.season = %s AND s.week = %s
            WHERE p.position = ANY(%s)
            GROUP BY r.team_id, p.position;
            """,
            (season, week, SKILL_POSITIONS),
        )
        rows = cursor.fetchall()

    deficits: dict = {}
    for team_id, position, player_count, net_vorp in rows:
        required = starting_requirements.get(position, 0)
        deficits.setdefault(team_id, {})[position] = {
            "rostered": player_count,
            "starters_required": required,
            "deficit": max(0, required - player_count),
            "net_vorp": net_vorp,
        }
    return {"season": season, "week": week, "teams": deficits}


@mcp.tool()
def evaluate_proposed_trade(
    incoming_player_ids: list[str],
    outgoing_player_ids: list[str],
    season: int,
    week: int,
) -> dict:
    """
    Sums weekly_stats.vorp_score for each side of a proposed trade and returns the
    strict delta (incoming - outgoing) - real database numbers only, never an LLM
    estimate. Any player_id with no weekly_stats row for this season/week (bye week,
    not yet synced, bad ID) is reported separately rather than silently scored as 0.
    """
    all_ids = list(set(incoming_player_ids) | set(outgoing_player_ids))
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT player_id, vorp_score FROM weekly_stats
            WHERE season = %s AND week = %s AND player_id = ANY(%s);
            """,
            (season, week, all_ids),
        )
        vorp_by_player = dict(cursor.fetchall())

    missing_player_ids = [pid for pid in all_ids if pid not in vorp_by_player]
    incoming_total = sum(vorp_by_player.get(pid, 0.0) for pid in incoming_player_ids)
    outgoing_total = sum(vorp_by_player.get(pid, 0.0) for pid in outgoing_player_ids)

    return {
        "incoming_total": incoming_total,
        "outgoing_total": outgoing_total,
        "delta_vorp": incoming_total - outgoing_total,
        "missing_player_ids": missing_player_ids,
    }


mcp_asgi_app = mcp.streamable_http_app()
