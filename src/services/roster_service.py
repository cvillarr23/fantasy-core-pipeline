"""
Shared roster-deficit and per-team roster computation, used by the MCP tools
(src/api/mcp_server.py), the chat endpoint (src/services/chat_service.py), and
the REST endpoint (src/api/roster.py) so each transport layer stays a thin controller
over one implementation.
"""
from src.database import get_db_cursor
from src.optimization.draft_matrix import compute_position_caps
from src.optimization.vorp_matrix import SKILL_POSITIONS, load_league_config
from src.nfl_byes import bye_week_for, bye_weeks_or_empty


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


BENCH_AND_IR_SLOTS = ("BE", "IR")


def _vorp_sort_key(player: dict):
    # Highest VORP first; players with no weekly_stats row for the week sink to the bottom
    # rather than sorting as if they were 0.0.
    return (player["vorp_score"] is None, -(player["vorp_score"] or 0.0))


def get_team_roster_players(team_id: str, season: int, week: int) -> dict:
    """
    One team's roster player by player - name, position, NFL team, lineup slot, injury
    status, and that week's vorp_score/projected_points - split into starters and bench,
    each sorted by vorp_score descending. Backs both the get_team_roster MCP tool and the
    chat endpoint's player-level grounding (compute_roster_deficits only has per-position
    aggregates, so it can't answer "who are my weakest starters?").

    Same caveat as compute_roster_deficits: roster composition and starter/bench status are
    the CURRENT league_rosters snapshot; only vorp_score/projected_points are scoped to
    `season`/`week`. A player with no weekly_stats row for that week (bye, not synced yet)
    gets None for both, never 0.0. An unknown team_id returns empty lists, not an error.
    """
    bye_weeks = bye_weeks_or_empty(season)
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT p.player_id, p.name, p.position, p.team, p.injury_status,
                   r.slot_position, r.is_starter, s.vorp_score, s.projected_points
            FROM league_rosters r
            JOIN players p ON r.player_id = p.player_id
            LEFT JOIN weekly_stats s
                ON s.player_id = r.player_id AND s.season = %s AND s.week = %s
            WHERE r.team_id = %s;
            """,
            (season, week, team_id),
        )
        rows = cursor.fetchall()

    starters, bench = [], []
    for player_id, name, position, nfl_team, injury_status, slot, is_starter, vorp, projected in rows:
        # is_starter was added after league_rosters existed, so older rows may hold NULL until
        # the next sync - fall back to the slot itself, the same rule the column is derived by.
        if is_starter is None:
            is_starter = slot not in BENCH_AND_IR_SLOTS
        player = {
            "player_id": player_id,
            "name": name,
            "position": position,
            "nfl_team": nfl_team,
            "bye_week": bye_week_for(nfl_team, bye_weeks),
            "slot": slot,
            "injury_status": injury_status,
            "vorp_score": vorp,
            "projected_points": projected,
        }
        (starters if is_starter else bench).append(player)

    starters.sort(key=_vorp_sort_key)
    bench.sort(key=_vorp_sort_key)
    return {
        "team_id": team_id,
        "season": season,
        "week": week,
        "roster_snapshot": "current",
        "starters": starters,
        "bench": bench,
    }
