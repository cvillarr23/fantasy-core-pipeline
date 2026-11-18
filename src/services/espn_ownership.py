"""Batch loader that attaches ESPN's market signal to player payloads.

Each service collects its player ids and calls `load_espn_ownership` once, instead of
querying per player.
"""
from src.schemas.espn_ownership import EspnOwnership


def _percent_bench(percent_owned, percent_started):
    """Owned minus started, floored at 0 and rounded to ESPN's 2 decimals (the subtraction
    leaves float noise); None when ESPN gave either figure as unknown."""
    if percent_owned is None or percent_started is None:
        return None
    return round(max(percent_owned - percent_started, 0.0), 2)


def _ownership(row) -> EspnOwnership:
    _, pos_rank, percent_owned, percent_started, percent_change = row
    return EspnOwnership(
        pos_rank=pos_rank,
        percent_owned=percent_owned,
        percent_started=percent_started,
        percent_bench=_percent_bench(percent_owned, percent_started),
        percent_change=percent_change,
    )


def load_espn_ownership(cur, player_ids, season: int, week: int) -> dict:
    """player_id -> EspnOwnership from the player's latest row at or before `week` this season.

    Players with no row are absent from the result. One query for the whole id set."""
    ids = sorted({pid for pid in player_ids if pid is not None})
    if not ids:
        return {}
    cur.execute(
        """
        SELECT DISTINCT ON (player_id)
               player_id, pos_rank, percent_owned, percent_started, percent_change
        FROM espn_player_ownership
        WHERE player_id = ANY(%s) AND season = %s AND week <= %s
        ORDER BY player_id, week DESC;
        """,
        (ids, season, week),
    )
    return {row[0]: _ownership(row) for row in cur.fetchall()}


def load_latest_espn_ownership(cur, player_ids) -> dict:
    """Same as `load_espn_ownership` for payloads with no season/week of their own (player
    search and summary): uses the newest season and week that has any ownership row."""
    cur.execute("SELECT season, week FROM espn_player_ownership ORDER BY season DESC, week DESC LIMIT 1;")
    latest = cur.fetchone()
    if latest is None:
        return {}
    return load_espn_ownership(cur, player_ids, latest[0], latest[1])
