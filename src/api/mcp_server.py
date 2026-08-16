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
from urllib.parse import urlparse

import httpx
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from src.database import get_db_cursor
from src.optimization.draft_matrix import compute_position_caps
from src.optimization.vorp_matrix import SKILL_POSITIONS, load_league_config

logger = logging.getLogger("MCPEngine")

GOOGLE_TOKENINFO_URL = "https://oauth2.googleapis.com/tokeninfo"
MCP_GOOGLE_OAUTH_CLIENT_ID = os.getenv("MCP_GOOGLE_OAUTH_CLIENT_ID")
MCP_GOOGLE_ALLOWED_EMAIL = os.getenv("MCP_GOOGLE_ALLOWED_EMAIL")
MCP_RESOURCE_URL = os.getenv("MCP_RESOURCE_URL", "http://fantasy-core-pipeline.default.svc.cluster.local:8000/mcp")


def _allowed_hosts() -> list[str]:
    """
    Host header allow-list for the SDK's built-in DNS-rebinding protection. Always
    includes MCP_RESOURCE_URL's own host, plus anything in MCP_ALLOWED_HOSTS (comma
    separated - e.g. the Tailscale ingress hostname in deployed environments, which
    differs from the in-cluster Service DNS name MCP_RESOURCE_URL defaults to).
    "testserver" is Starlette TestClient's fixed Host header - allowing it doesn't
    weaken real deployments (nothing reachable over the network can make its Host
    header literally equal that string matter, since routing there depends on
    MCP_RESOURCE_URL/MCP_ALLOWED_HOSTS, not on the literal value "testserver" being
    special) and lets tests exercise the real MCP transport instead of only ever
    hitting the pre-transport 401 short-circuit.
    """
    hosts = {urlparse(MCP_RESOURCE_URL).netloc, "testserver"}
    hosts.update(h.strip() for h in os.getenv("MCP_ALLOWED_HOSTS", "").split(",") if h.strip())
    return sorted(hosts)


class GoogleTokenVerifier(TokenVerifier):
    """
    Verifies bearer tokens as Google OAuth access tokens via Google's tokeninfo
    endpoint - this server is a Resource Server only (RFC 8707/9728 split), it never
    issues or stores tokens itself. Mirrors OpenBao's own OIDC setup: a single allowed
    email (bound_claims-style), not a real multi-user client registry, since this
    replaces one operator's manual data lookups, not a shared login system.

    Known residual risk (accepted, not fixed here): checking `aud` against
    MCP_GOOGLE_OAUTH_CLIENT_ID proves the token was issued to homelab-deployment's
    shared Google OAuth client and belongs to the allowed operator - it does NOT prove
    the token was requested specifically for MCP access as opposed to, say, OpenBao's
    browser OIDC login, since both flows share the same client and (per Google's
    standard OIDC scopes) request the same openid/email scopes. True per-resource
    audience binding (RFC 8707) isn't something Google's generic OAuth client supports,
    and standing up a dedicated authorization server for real scope separation was
    explicitly rejected for this homelab (see homelab-deployment's CLAUDE.md on why
    OpenBao itself isn't a centralized OIDC provider for other apps). Given this
    reduces to "the single trusted operator's own already-authenticated Google session
    can reach MCP," not a privilege escalation for anyone else, that's accepted for now
    rather than building bespoke OAuth infrastructure to close a gap with no realistic
    attacker on the other side of it.
    """

    async def verify_token(self, token: str) -> AccessToken | None:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(GOOGLE_TOKENINFO_URL, params={"access_token": token})
        except httpx.HTTPError:
            # Deliberately not logging the exception body: the access token travels as
            # a URL query parameter, and some httpx/httpcore error messages echo the
            # request URL back verbatim - logging {e} here would write live bearer
            # tokens into application logs.
            logger.error("Google tokeninfo lookup failed (network or timeout error).")
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
    transport_security=TransportSecuritySettings(allowed_hosts=_allowed_hosts()),
)


@mcp.tool()
def get_league_roster_deficits(season: int, week: int) -> dict:
    """
    Computes net VORP and starter-slot deficits per team/position across the whole
    league, so a negotiating LLM can see exactly which managers are structurally thin
    at a position before proposing a trade.

    Roster composition (`rostered`/`deficit`) always reflects the CURRENT league_rosters
    snapshot, not a historical one as of `season`/`week` - persist_rosters() fully
    replaces that table on every sync, so there is no historical roster data to key by
    week. Only `net_vorp` is actually scoped to the requested season/week (via
    weekly_stats). Requesting a past week's VORP still combines it with today's rosters.
    """
    with get_db_cursor() as cursor:
        league_config = load_league_config(cursor)
        _, starting_requirements = compute_position_caps(league_config, drafted_by_us={})

        cursor.execute("SELECT DISTINCT team_id FROM league_rosters;")
        team_ids = [row[0] for row in cursor.fetchall()]

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

    # Every team/position pair is initialized up front (zero rostered = full deficit)
    # before overlaying real rows, so a team with zero players at a position - the exact
    # structural gap this tool exists to surface - doesn't just silently disappear from
    # the grouped query's output instead of showing up as a deficit.
    deficits: dict = {
        team_id: {
            position: {
                "rostered": 0,
                "starters_required": starting_requirements.get(position, 0),
                "deficit": starting_requirements.get(position, 0),
                "net_vorp": 0.0,
            }
            for position in SKILL_POSITIONS
        }
        for team_id in team_ids
    }
    for team_id, position, player_count, net_vorp in rows:
        required = starting_requirements.get(position, 0)
        deficits[team_id][position] = {
            "rostered": player_count,
            "starters_required": required,
            "deficit": max(0, required - player_count),
            "net_vorp": net_vorp,
        }
    return {"season": season, "week": week, "roster_snapshot": "current", "teams": deficits}


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

    Rejects duplicate player_ids within either side and any player_id appearing on both
    sides - an LLM-generated malformed trade proposal must fail loudly here rather than
    silently produce a valid-looking but impossible score.
    """
    incoming_set, outgoing_set = set(incoming_player_ids), set(outgoing_player_ids)
    if len(incoming_set) != len(incoming_player_ids):
        raise ValueError("incoming_player_ids contains duplicate player_id(s).")
    if len(outgoing_set) != len(outgoing_player_ids):
        raise ValueError("outgoing_player_ids contains duplicate player_id(s).")
    overlap = incoming_set & outgoing_set
    if overlap:
        raise ValueError(f"player_id(s) {sorted(overlap)} cannot appear on both sides of a trade.")

    all_ids = list(incoming_set | outgoing_set)
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
