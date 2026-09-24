"""
Waiver-recommendation read endpoint backing the REST controller in src/api/waiver.py.

Reuses src/backtesting/waiver_strategy.py's pure decision functions unchanged - the same
ones season_replay.py backtests and waiver_evaluator.py publishes live from - rather than
re-deriving the VORP-delta math. margin=0.0 here, not waiver_strategy.MARGIN_THRESHOLD:
that threshold gates live auto-publish, not what's worth showing a manager to decide on.
"""
import logging
import os

from src.backtesting import waiver_strategy
from src.database import get_db_cursor

logger = logging.getLogger("WaiverService")


def _load_player_display(cursor, player_ids: set) -> dict:
    """player_id -> {espn_id, name, pro_team, injury_status}, one batched query."""
    if not player_ids:
        return {}
    cursor.execute(
        """
        SELECT player_id, espn_id, name, team, injury_status
        FROM players
        WHERE player_id = ANY(%s);
        """,
        (list(player_ids),),
    )
    return {
        player_id: {"espn_id": espn_id, "name": name, "pro_team": team, "injury_status": injury_status}
        for player_id, espn_id, name, team, injury_status in cursor.fetchall()
    }


def _shape_recommendation(move: dict, display_by_player_id: dict) -> dict:
    add_display = display_by_player_id[move["add_player_id"]]
    drop_display = display_by_player_id[move["drop_player_id"]]
    return {
        "position": move["position"],
        "vorp_delta": move["add_vorp_score"] - move["drop_vorp_score"],
        "free_agent": {**add_display, "net_vorp": move["add_vorp_score"]},
        "bench_player": {**drop_display, "net_vorp": move["drop_vorp_score"]},
        # Placeholders until a future ticket wires the rumor-signal feed into this
        # endpoint, mirroring how it was added to GET /v1/matchup/roster.
        "rumor_sentiment": None,
        "rumor_confidence": None,
        "rumor_suggested_action": None,
    }


def compute_waiver_recommendations(season: int, week: int) -> dict:
    our_team_id = os.getenv("ESPN_TEAM_ID")
    with get_db_cursor() as cursor:
        bench_by_position = waiver_strategy.load_bench_by_position(cursor, our_team_id, season, week)
        free_agents_by_position = waiver_strategy.load_best_free_agent_by_position(cursor, season, week)
        moves = waiver_strategy.propose_waiver_moves(bench_by_position, free_agents_by_position, margin=0.0)

        player_ids = {m["add_player_id"] for m in moves} | {m["drop_player_id"] for m in moves}
        display_by_player_id = _load_player_display(cursor, player_ids)

    recommendations = [_shape_recommendation(m, display_by_player_id) for m in moves]
    recommendations.sort(key=lambda r: r["vorp_delta"], reverse=True)
    return {"season": season, "week": week, "recommendations": recommendations}
