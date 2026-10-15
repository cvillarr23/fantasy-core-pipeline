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

from src.services.free_agent_service import browse_free_agents
from src.services.roster_outlook_service import get_bye_outlook
from src.services.roster_service import compute_roster_deficits, get_team_roster_players
from src.services.trade_service import evaluate_trade
from src.services.trade_context_service import get_league_trade_context
from src.services.trade_score_service import score_trade

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
    instructions=(
        "Structured tools for reasoning about this fantasy football league's roster construction "
        "and trade math, including one team's roster player by player (get_team_roster), the "
        "free-agent pool (get_free_agents) and a deterministic schedule-aware trade score (score_trade_proposal)."
    ),
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
    return compute_roster_deficits(season, week)


@mcp.tool()
def get_team_roster(team_id: str, season: int, week: int) -> dict:
    """
    One team's roster player by player - name, position, NFL team, lineup slot, injury
    status, and that week's vorp_score/projected_points - split into starters and bench,
    each sorted by vorp_score descending. Use this for player-level questions (weakest
    starters, who to bench); get_league_roster_deficits only has per-position aggregates.

    Roster composition is the CURRENT league_rosters snapshot; only vorp_score/
    projected_points are scoped to `season`/`week`. A null vorp_score/projected_points
    means no weekly_stats row for that week (bye, not synced yet), not a zero projection.
    """
    return get_team_roster_players(team_id, season, week)


@mcp.tool()
def get_free_agents(
    season: int,
    week: int,
    position: str | None = None,
    q: str | None = None,
    limit: int = 25,
) -> dict:
    """
    Unrostered players in the league, sorted by vorp_score descending - name, ESPN id,
    position, NFL team, injury status, projected_points and vorp_score, plus the bot team's
    weakest same-position bench player and the VORP delta of swapping them. Same service as
    GET /v1/waiver/free-agents. Filter by `position` (e.g. "RB") or a name substring `q`.

    Before the week's projections land, metrics are the mean of up to 3 prior weeks
    (`source_mode` "rolling_prior_weeks"); players on bye then have null metrics, not zero.
    Raises when neither source has usable data.
    """
    return browse_free_agents(season, week, position, q, "vorp", max(1, min(limit, 200)))


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
    return evaluate_trade(incoming_player_ids, outgoing_player_ids, season, week)


@mcp.tool()
def get_league_trade_context_model(
    league_id: str, season: int, week: int, horizon: int = 4
) -> dict:
    """Current rosters, exact-week VORP/projections, and fantasy-opponent schedule strength.

    Player identities include ESPN and internal IDs separately. Strength uses completed
    fantasy matchup totals before the selected week; positive z means tougher opponents.
    Unknown pairings, identity mappings and metrics remain explicit. This is current
    ownership, not a historical roster reconstruction. NFL matchup difficulty is separate.
    """
    return get_league_trade_context(league_id, season, week, horizon)


@mcp.tool()
def get_team_bye_outlook(
    league_id: str, season: int, week: int, team_id: str, horizon: int = 4
) -> dict:
    """Which upcoming weeks NFL byes leave one team's lineup short.

    Per horizon week: the best lineup VORP the roster can field, players on an NFL bye
    (ESPN ids), empty starter seats, below-replacement starters, and crunch (byes leave a
    seat empty or force a below-replacement starter). Each player carries nfl_bye_week.
    Use it to find bye-complementary trade partners or fill-ins; never as a reason to
    drop or trade a good player just because he has a bye - judge moves over the whole
    horizon with score_trade_proposal.
    """
    return get_bye_outlook(league_id, season, week, horizon, team_id)


@mcp.tool()
def score_trade_proposal(
    league_id: str,
    season: int,
    week: int,
    horizon: int,
    our_team_id: str,
    counterparty_team_id: str,
    send_espn_ids: list[str],
    receive_espn_ids: list[str],
) -> dict:
    """Deterministic both-team trade score; quote its numbers, never estimate your own.

    All player ids are ESPN ids (D/ST ids are negative) and must be on the sending team's
    current ESPN roster. Returns raw single-week VORP (same as evaluate_proposed_trade),
    each team's before/after optimal-lineup VORP per horizon week, the fantasy-opponent
    schedule weights, and final_score = our weighted gain + the weighted benefit of
    weakening the counterparty. Positive is good for our_team_id. Weeks without exact
    metrics are unscored, and unknown opponent strength leaves final_score null with a
    reason - neither is ever treated as zero.
    """
    return score_trade(
        league_id, season, week, horizon, our_team_id, counterparty_team_id,
        send_espn_ids, receive_espn_ids,
    )


mcp_asgi_app = mcp.streamable_http_app()
