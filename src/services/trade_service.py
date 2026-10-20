"""
Shared trade-evaluation logic, used by both the MCP tool (src/api/mcp_server.py) and the
REST endpoint (src/api/trade.py).
"""
from src.database import get_db_cursor


def evaluate_trade(
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

    return summarize_raw_vorp(
        load_vorp_by_player(season, week, list(incoming_set | outgoing_set)),
        incoming_player_ids,
        outgoing_player_ids,
    )


def load_vorp_by_player(season: int, week: int, player_ids: list[str]) -> dict:
    """{player_id: vorp_score} for the ids with a weekly_stats row that season/week. One
    query, so a caller scoring many packages can read every rostered player at once."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT player_id, vorp_score FROM weekly_stats
            WHERE season = %s AND week = %s AND player_id = ANY(%s);
            """,
            (season, week, list(player_ids)),
        )
        return dict(cursor.fetchall())


def summarize_raw_vorp(
    vorp_by_player: dict,
    incoming_player_ids: list[str],
    outgoing_player_ids: list[str],
) -> dict:
    """evaluate_trade's totals over an already-loaded {player_id: vorp_score} map."""
    all_ids = list(set(incoming_player_ids) | set(outgoing_player_ids))
    missing_player_ids = [pid for pid in all_ids if pid not in vorp_by_player]
    incoming_total = sum(vorp_by_player.get(pid, 0.0) for pid in incoming_player_ids)
    outgoing_total = sum(vorp_by_player.get(pid, 0.0) for pid in outgoing_player_ids)

    return {
        "incoming_total": incoming_total,
        "outgoing_total": outgoing_total,
        "delta_vorp": incoming_total - outgoing_total,
        "missing_player_ids": missing_player_ids,
    }
