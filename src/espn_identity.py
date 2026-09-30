"""
Sleeper -> ESPN player identity translation, shared by every publisher that hands a player
id to fantasy-browser-worker (waiver claims, lineup swaps).

The optimizers key everything by players.player_id (Sleeper-namespaced), but the worker
acts on ESPN's id space, and the two overlap numerically - publishing the wrong one can
silently act on a different player (#187). Always translate through players.espn_id; a
player with no espn_id yet (the DynastyProcess crosswalk covers ~82%) has no ESPN identity
to safely act on, so callers skip it rather than publish.
"""
from typing import Optional


def resolve_espn_identity(cursor, player_id: str) -> Optional[dict]:
    cursor.execute("SELECT espn_id, name, team FROM players WHERE player_id = %s;", (player_id,))
    row = cursor.fetchone()
    if not row or not row[0]:
        return None
    return {"espn_id": row[0], "name": row[1], "team": row[2]}
