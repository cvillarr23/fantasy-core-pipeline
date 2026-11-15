"""
VORP browsing read logic, split out of src/api/stats.py so the router stays a thin HTTP
controller over this module doing the actual DB work.
"""
from typing import Optional

from src.database import get_db_cursor
from src.schemas.stats import VorpSort
from src.services.espn_ownership import load_espn_ownership

SORT_CLAUSES = {
    VorpSort.vorp: "s.vorp_score DESC NULLS LAST, s.season DESC, s.week DESC, p.name",
    VorpSort.projected_points: "s.projected_points DESC NULLS LAST, s.season DESC, s.week DESC, p.name",
    VorpSort.actual_points: "s.actual_points DESC NULLS LAST, s.season DESC, s.week DESC, p.name",
    VorpSort.week: "s.season, s.week, p.name",
}


def _espn_by_period(cursor, rows) -> dict:
    """(season, week) -> {player_id: EspnOwnership}. Each row carries the market signal as of
    its own week, so this is one batched query per distinct (season, week) in the result."""
    ids_by_period: dict = {}
    for row in rows:
        ids_by_period.setdefault((row[4], row[5]), []).append(row[0])
    return {
        (season, week): load_espn_ownership(cursor, ids, season, week)
        for (season, week), ids in ids_by_period.items()
    }


def browse_vorp(
    season: Optional[int],
    week: Optional[int],
    position: Optional[str],
    sort: VorpSort,
    limit: int,
) -> list:
    filters = []
    params = []
    if season is not None:
        filters.append("s.season = %s")
        params.append(season)
    if week is not None:
        filters.append("s.week = %s")
        params.append(week)
    if position is not None:
        filters.append("p.position = %s")
        params.append(position.upper())

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    params.append(limit)
    with get_db_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT s.player_id, p.name, p.position, s.team, s.season, s.week,
                   s.projected_points, s.actual_points, s.matchup_coefficient,
                   s.adjusted_projected_points, s.vorp_base, s.scarcity_multiplier,
                   s.defense_matchup_multiplier, s.vorp_score
            FROM weekly_stats s
            JOIN players p ON p.player_id = s.player_id
            {where_clause}
            ORDER BY {SORT_CLAUSES[sort]}
            LIMIT %s;
        """,
            tuple(params),
        )
        rows = cursor.fetchall()
        espn_by_period = _espn_by_period(cursor, rows)

    return [
        {
            "player_id": row[0], "name": row[1], "position": row[2], "team": row[3],
            "season": row[4], "week": row[5], "projected_points": row[6], "actual_points": row[7],
            "matchup_coefficient": row[8], "adjusted_projected_points": row[9], "vorp_base": row[10],
            "scarcity_multiplier": row[11], "defense_matchup_multiplier": row[12], "vorp_score": row[13],
            "espn": espn_by_period.get((row[4], row[5]), {}).get(row[0]),
        }
        for row in rows
    ]
