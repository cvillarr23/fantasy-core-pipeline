"""
Minimal weekly waiver rule (CORE-15). No waiver-decision logic exists anywhere else in this
repo - sync_players.py only publishes injury-transition alerts - so this is new logic,
built specifically to be scorable against a static (no-moves) baseline. No FAAB/budget
modeling.
"""
import json
import logging

from src.ir_eligibility import is_ir_eligible

logger = logging.getLogger("BacktestWaiverStrategy")

MARGIN_THRESHOLD = 3.0


def load_bench_by_position(cursor, team_id: str, season: int, week: int) -> dict:
    """Bench players (is_starter is not TRUE) for a team/season/week, grouped by position.
    IR is also a non-starter slot, but a stashed player is never a drop candidate, so IR
    rows are left out. espn_injury_status rides along for split_ir_moves."""
    cursor.execute(
        """
        SELECT p.player_id, p.position, COALESCE(s.vorp_score, 0.0) AS vorp_score, r.espn_injury_status
        FROM players p
        JOIN league_rosters r ON p.player_id = r.player_id
        JOIN weekly_stats s ON p.player_id = s.player_id
        WHERE r.team_id = %s AND s.season = %s AND s.week = %s
          AND (r.is_starter IS NULL OR r.is_starter = FALSE)
          AND r.slot_position <> 'IR';
        """,
        (team_id, season, week),
    )
    by_position: dict = {}
    for player_id, position, vorp_score, espn_injury_status in cursor.fetchall():
        by_position.setdefault(position, []).append(
            {"player_id": player_id, "vorp_score": vorp_score, "espn_injury_status": espn_injury_status}
        )
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


def load_open_roster_slots(cursor, team_id: str, season: int) -> int:
    """Non-IR capacity minus current occupancy; unknown capacity never permits adds."""
    cursor.execute(
        """
        SELECT lineup_slot_counts FROM league_settings
        WHERE season = %s ORDER BY updated_at DESC LIMIT 1;
        """,
        (season,),
    )
    row = cursor.fetchone()
    counts = row[0] if row else None
    if isinstance(counts, str):
        counts = json.loads(counts)
    if not isinstance(counts, dict):
        return 0
    capacity = sum(int(count or 0) for slot, count in counts.items() if slot != "IR")
    if capacity <= 0:
        return 0
    cursor.execute(
        """SELECT COUNT(*) FROM league_rosters
        WHERE team_id = %s AND slot_position IS DISTINCT FROM 'IR';""",
        (str(team_id),),
    )
    return max(0, capacity - int(cursor.fetchone()[0]))


def load_add_candidates(cursor, season: int, week: int) -> list:
    """All projected free agents, so multiple open slots can use the same position."""
    cursor.execute(
        """
        SELECT p.player_id, p.position, COALESCE(s.vorp_score, 0.0)
        FROM players p JOIN weekly_stats s ON p.player_id = s.player_id
        WHERE s.season = %s AND s.week = %s
          AND NOT EXISTS (SELECT 1 FROM league_rosters r WHERE r.player_id = p.player_id);
        """,
        (season, week),
    )
    return [
        {"player_id": pid, "position": position, "vorp_score": score}
        for pid, position, score in cursor.fetchall()
    ]


def plan_add_only_moves(candidates: list, open_roster_slots: int,
                        margin: float = MARGIN_THRESHOLD) -> tuple:
    """Reserve the best worthwhile adds, then return best remaining agents for swaps/IR."""
    ranked = sorted(candidates, key=lambda p: (-p["vorp_score"], str(p["player_id"])))
    unique = {}
    for player in ranked:
        unique.setdefault(player["player_id"], player)
    ranked = list(unique.values())
    selected = [p for p in ranked if p["vorp_score"] > margin][:max(0, open_roster_slots)]
    adds = [
        {"kind": "add", "position": p["position"], "drop_player_id": None,
         "add_player_id": p["player_id"], "add_vorp_score": p["vorp_score"]}
        for p in selected
    ]
    claimed = {p["player_id"] for p in selected}
    remaining = {}
    for p in ranked:
        if p["player_id"] not in claimed:
            remaining.setdefault(p["position"], {"player_id": p["player_id"], "vorp_score": p["vorp_score"]})
    return adds, remaining


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


def ir_eligible_bench(bench_by_position: dict) -> dict:
    """player_id -> bench entry (plus its position) for every IR-eligible bench player."""
    return {
        player["player_id"]: {"position": position, **player}
        for position, players in bench_by_position.items()
        for player in players
        if is_ir_eligible(player.get("espn_injury_status"))
    }


def ir_stash_candidates(bench_by_position: dict, open_ir_slots: int) -> list:
    """Pure: the IR-eligible bench players to stash, most valuable first, at most one per
    open IR slot."""
    if open_ir_slots <= 0:
        return []
    eligible = sorted(ir_eligible_bench(bench_by_position).values(), key=lambda p: p["vorp_score"], reverse=True)
    return eligible[:open_ir_slots]


def plan_waiver_moves(bench_by_position: dict, free_agents_by_position: dict, open_ir_slots: int,
                      margin: float = MARGIN_THRESHOLD) -> tuple:
    """Pure: (swaps, ir_and_adds), IR-aware propose_waiver_moves.

    While an IR slot is open no IR-eligible player is dropped, wherever they rank on the
    bench. Each stash frees a roster spot, so the best free agents (any position, worth more
    than `margin` on their own since nobody leaves) become "ir_and_add" moves first. Only the
    remaining positions' free agents are weighed against healthy bench players as swaps.
    Eligible players beyond the open slots are neither stashed nor dropped: dropping them is
    only right once the stashes have filled IR."""
    eligible = ir_eligible_bench(bench_by_position) if open_ir_slots > 0 else {}
    if not eligible:
        return propose_waiver_moves(bench_by_position, free_agents_by_position, margin=margin), []

    ranked_agents = sorted(
        ((position, agent) for position, agent in free_agents_by_position.items() if agent["vorp_score"] > margin),
        key=lambda item: item[1]["vorp_score"], reverse=True,
    )
    ir_and_adds = [
        {
            "kind": "ir_and_add",
            "position": position,
            "ir_player_id": stashed["player_id"],
            "ir_position": stashed["position"],
            "drop_player_id": None,
            "add_player_id": agent["player_id"],
            "add_vorp_score": agent["vorp_score"],
            "ir_vorp_score": stashed["vorp_score"],
        }
        for stashed, (position, agent) in zip(ir_stash_candidates(bench_by_position, open_ir_slots), ranked_agents)
    ]
    claimed_positions = {move["position"] for move in ir_and_adds}
    healthy_bench = {
        position: [p for p in players if p["player_id"] not in eligible]
        for position, players in bench_by_position.items()
    }
    remaining_agents = {
        position: agent for position, agent in free_agents_by_position.items() if position not in claimed_positions
    }
    swaps = [
        move for move in propose_waiver_moves(healthy_bench, remaining_agents, margin=margin)
        if move["drop_player_id"] not in eligible
    ]
    return swaps, ir_and_adds


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
