"""
Shared roster-deficit computation, used by both the MCP tool (src/api/mcp_server.py) and
the REST endpoint (src/api/roster.py) so the two transport layers stay thin controllers
over one implementation.
"""
from src.database import get_db_cursor
from src.optimization.draft_matrix import compute_position_caps
from src.optimization.vorp_matrix import SKILL_POSITIONS, load_league_config


def compute_roster_deficits(season: int, week: int) -> dict:
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
