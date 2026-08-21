"""
ADP-based snake draft simulator (CORE-15). The other 9 teams always take the best
remaining player by historical ADP (see historical_data.resolve_adp); our team calls
draft_matrix.py's real recommendation logic verbatim on its own turns.

compute_snake_order is computed locally rather than via draft_matrix.compute_on_the_clock_team,
which depends on live Redis round-1-pick-arrival-order inference we don't have (and don't
need) in a simulation where the full turn order is known up front.
"""
import logging

from src.backtesting.historical_data import HARDCODED_LEAGUE_CONFIG, SYNTHETIC_LEAGUE_ID
from src.optimization import draft_matrix

logger = logging.getLogger("BacktestDraftSimulator")

OUR_TEAM_ID = "backtest-team-1"
OPPONENT_TEAM_IDS = [f"backtest-team-{n}" for n in range(2, 11)]
ALL_TEAM_IDS = [OUR_TEAM_ID] + OPPONENT_TEAM_IDS


def compute_default_rounds() -> int:
    """Total draftable roster size (starters + bench), derived from the same hardcoded
    league config vorp_matrix/draft_matrix use via draft_matrix.compute_draftable_roster_size
    - a round count that doesn't match this exactly makes our solver's real pick budget
    (bounded by compute_position_caps) diverge from how many players the ADP control-arm
    drafts, so score_draft would compare unequal roster sizes."""
    starting_total, bench_total = draft_matrix.compute_draftable_roster_size(HARDCODED_LEAGUE_CONFIG)
    return starting_total + bench_total


DEFAULT_ROUNDS = compute_default_rounds()


def compute_snake_order(team_count: int, total_rounds: int) -> list:
    """Returns [(pick_number, slot_number), ...] for a standard snake draft: round 1 goes
    slot 1 -> team_count, round 2 reverses, and so on."""
    order = []
    pick_number = 1
    for round_num in range(1, total_rounds + 1):
        slots = range(1, team_count + 1) if round_num % 2 == 1 else range(team_count, 0, -1)
        for slot in slots:
            order.append((pick_number, slot))
            pick_number += 1
    return order


def pick_best_available_by_adp(cursor, adp: dict, league_id: str, season: int) -> dict | None:
    """The opponent-bot rule: top remaining player by ADP rank, anti-joined against
    draft_picks. `adp` is {player_id: adp_rank}, ascending (rank 1 = first off the board)."""
    cursor.execute(
        "SELECT player_id FROM draft_picks WHERE league_id = %s AND season = %s;",
        (league_id, season),
    )
    drafted_ids = {row[0] for row in cursor.fetchall()}
    remaining = {pid: rank for pid, rank in adp.items() if pid not in drafted_ids}
    if not remaining:
        return None
    best_player_id = min(remaining, key=lambda pid: remaining[pid])

    cursor.execute("SELECT name, position, team FROM players WHERE player_id = %s;", (best_player_id,))
    row = cursor.fetchone()
    if row is None:
        return None
    name, position, team = row
    return {"player_id": best_player_id, "name": name, "position": position, "team": team}


def run_draft(conn, adp: dict, season: int, rounds: int = DEFAULT_ROUNDS, week: int = 1) -> list:
    """Runs the full simulated snake draft. Every pick (ours and opponents') is persisted
    immediately before the next turn - compute_draft_recommendation_candidates needs
    draft_picks to reflect all earlier picks to correctly compute the undrafted pool."""
    order = compute_snake_order(len(ALL_TEAM_IDS), rounds)
    picks = []
    for pick_number, slot in order:
        team_id = ALL_TEAM_IDS[slot - 1]
        round_ = (pick_number - 1) // len(ALL_TEAM_IDS) + 1

        with conn.cursor() as cursor:
            if team_id == OUR_TEAM_ID:
                candidates = draft_matrix.compute_draft_recommendation_candidates(
                    cursor, SYNTHETIC_LEAGUE_ID, season, week, OUR_TEAM_ID, limit=1
                )
                target = candidates[0] if candidates else None
            else:
                target = pick_best_available_by_adp(cursor, adp, SYNTHETIC_LEAGUE_ID, season)

        if target is None:
            logger.warning(f"No feasible pick for team {team_id} at pick {pick_number}; skipping.")
            continue

        draft_matrix.persist_draft_pick(
            conn, SYNTHETIC_LEAGUE_ID, season, pick_number, round_, team_id, target["player_id"]
        )
        picks.append({"pick_number": pick_number, "round": round_, "team_id": team_id, **target})

    return picks


def simulate_pure_adp_draft(adp: dict, team_count: int = 10, rounds: int = DEFAULT_ROUNDS) -> dict:
    """Pure, in-memory-only control-arm draft: every team (including ours) always takes
    best-available-by-ADP. Not persisted to draft_picks - this exists purely to score our
    system's real draft picks against "what if we'd drafted by ADP too", independent of the
    real simulated draft's DB state."""
    order = compute_snake_order(team_count, rounds)
    ranked_ids = [pid for pid, _rank in sorted(adp.items(), key=lambda kv: kv[1])]
    rosters: dict = {f"backtest-team-{n}": [] for n in range(1, team_count + 1)}
    drafted: set = set()
    cursor_idx = 0
    for _pick_number, slot in order:
        while cursor_idx < len(ranked_ids) and ranked_ids[cursor_idx] in drafted:
            cursor_idx += 1
        if cursor_idx >= len(ranked_ids):
            break
        player_id = ranked_ids[cursor_idx]
        drafted.add(player_id)
        rosters[f"backtest-team-{slot}"].append(player_id)
    return rosters


def seed_rosters_from_draft(conn, picks: list) -> int:
    """Synthesizes league_rosters rows for every drafted player on every team from the
    draft's results - the real pipeline gets this from ESPN sync, which the backtest has no
    equivalent of. Every player starts on the bench; season_replay's weekly lineup step
    marks starters."""
    if not picks:
        return 0
    rows = [(p["team_id"], p["player_id"], "BE", False) for p in picks]
    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO league_rosters (team_id, player_id, slot_position, is_starter)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (team_id, player_id) DO NOTHING;
            """,
            rows,
        )
    return len(rows)
