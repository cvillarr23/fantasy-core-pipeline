"""
Minimal weekly waiver rule (CORE-15). No waiver-decision logic exists anywhere else in this
repo - sync_players.py only publishes injury-transition alerts - so this is new logic,
built specifically to be scorable against a static (no-moves) baseline. No FAAB/budget
modeling.
"""
import logging

logger = logging.getLogger("BacktestWaiverStrategy")

MARGIN_THRESHOLD = 3.0


def load_bench_by_position(cursor, team_id: str, season: int, week: int) -> dict:
    """Bench players (is_starter is not TRUE) for a team/season/week, grouped by position."""
    cursor.execute(
        """
        SELECT p.player_id, p.position, COALESCE(s.vorp_score, 0.0) AS vorp_score
        FROM players p
        JOIN league_rosters r ON p.player_id = r.player_id
        JOIN weekly_stats s ON p.player_id = s.player_id
        WHERE r.team_id = %s AND s.season = %s AND s.week = %s
          AND (r.is_starter IS NULL OR r.is_starter = FALSE);
        """,
        (team_id, season, week),
    )
    by_position: dict = {}
    for player_id, position, vorp_score in cursor.fetchall():
        by_position.setdefault(position, []).append({"player_id": player_id, "vorp_score": vorp_score})
    return by_position


def load_best_free_agent_by_position(cursor, season: int, week: int) -> dict:
    """Best (highest vorp_score) player at each position not on ANY team's roster - a free
    agent here means unrostered league-wide, unlike draft_matrix.load_undrafted_pool which
    checks against draft_picks (pre-draft has no league_rosters rows yet)."""
    cursor.execute(
        """
        SELECT p.player_id, p.position, COALESCE(s.vorp_score, 0.0) AS vorp_score
        FROM players p
        JOIN weekly_stats s ON p.player_id = s.player_id
        WHERE s.season = %s AND s.week = %s
          AND NOT EXISTS (SELECT 1 FROM league_rosters r WHERE r.player_id = p.player_id);
        """,
        (season, week),
    )
    best_by_position: dict = {}
    for player_id, position, vorp_score in cursor.fetchall():
        current = best_by_position.get(position)
        if current is None or vorp_score > current["vorp_score"]:
            best_by_position[position] = {"player_id": player_id, "vorp_score": vorp_score}
    return best_by_position


def propose_waiver_moves(bench_by_position: dict, free_agents_by_position: dict, margin: float = MARGIN_THRESHOLD) -> list:
    """Pure: for each position with both a bench player and a free agent, proposes a swap
    if the free agent beats the worst bench player at that position by more than `margin`."""
    moves = []
    for position, free_agent in free_agents_by_position.items():
        bench = bench_by_position.get(position)
        if not bench:
            continue
        worst_bench = min(bench, key=lambda p: p["vorp_score"])
        if free_agent["vorp_score"] - worst_bench["vorp_score"] > margin:
            moves.append({
                "position": position,
                "drop_player_id": worst_bench["player_id"],
                "add_player_id": free_agent["player_id"],
                # Carried through for CORE-17 (#35)'s bot_decisions audit trail -
                # resolve_live_waiver_moves folds these into decision_inputs.
                "add_vorp_score": free_agent["vorp_score"],
                "drop_vorp_score": worst_bench["vorp_score"],
            })
    return moves


def apply_waiver_moves(conn, team_id: str, moves: list) -> int:
    if not moves:
        return 0
    with conn.cursor() as cursor:
        for move in moves:
            cursor.execute(
                "DELETE FROM league_rosters WHERE team_id = %s AND player_id = %s;",
                (team_id, move["drop_player_id"]),
            )
            cursor.execute(
                """
                INSERT INTO league_rosters (team_id, player_id, slot_position, is_starter)
                VALUES (%s, %s, 'BE', FALSE)
                ON CONFLICT (team_id, player_id) DO NOTHING;
                """,
                (team_id, move["add_player_id"]),
            )
    return len(moves)


def run_weekly_waivers(conn, team_id: str, season: int, week: int) -> list:
    with conn.cursor() as cursor:
        bench_by_position = load_bench_by_position(cursor, team_id, season, week)
        free_agents_by_position = load_best_free_agent_by_position(cursor, season, week)
    moves = propose_waiver_moves(bench_by_position, free_agents_by_position)
    apply_waiver_moves(conn, team_id, moves)
    return moves
