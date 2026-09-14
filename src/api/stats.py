import logging
from enum import Enum
from typing import List, Optional

from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from pydantic import BaseModel

from src.database import get_db_cursor

logger = logging.getLogger("StatsRoutes")
stats_router = APIRouter()


class VorpSort(str, Enum):
    vorp = "vorp"
    projected_points = "projected_points"
    actual_points = "actual_points"
    week = "week"


class VorpStat(BaseModel):
    player_id: str
    name: str
    position: str
    team: Optional[str] = None
    season: int
    week: int
    projected_points: Optional[float] = None
    actual_points: Optional[float] = None
    matchup_coefficient: Optional[float] = None
    adjusted_projected_points: Optional[float] = None
    vorp_base: Optional[float] = None
    scarcity_multiplier: Optional[float] = None
    defense_matchup_multiplier: Optional[float] = None
    vorp_score: Optional[float] = None


SORT_CLAUSES = {
    VorpSort.vorp: "s.vorp_score DESC NULLS LAST, s.season DESC, s.week DESC, p.name",
    VorpSort.projected_points: "s.projected_points DESC NULLS LAST, s.season DESC, s.week DESC, p.name",
    VorpSort.actual_points: "s.actual_points DESC NULLS LAST, s.season DESC, s.week DESC, p.name",
    VorpSort.week: "s.season, s.week, p.name",
}

# Every filter is optional, so an unscoped request must still be bounded - weekly_stats
# grows every player/week/season and has no other cap on rows returned.
DEFAULT_VORP_LIMIT = 200
MAX_VORP_LIMIT = 1000


@stats_router.get("/vorp", response_model=List[VorpStat])
def browse_vorp(
    season: Optional[int] = None,
    week: Optional[int] = None,
    position: Optional[str] = None,
    sort: VorpSort = VorpSort.vorp,
    limit: int = Query(DEFAULT_VORP_LIMIT, ge=1, le=MAX_VORP_LIMIT),
):
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
    try:
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
    except Exception as e:
        logger.error(f"Failed to browse VORP stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to browse VORP stats.")

    return [
        {
            "player_id": row[0],
            "name": row[1],
            "position": row[2],
            "team": row[3],
            "season": row[4],
            "week": row[5],
            "projected_points": row[6],
            "actual_points": row[7],
            "matchup_coefficient": row[8],
            "adjusted_projected_points": row[9],
            "vorp_base": row[10],
            "scarcity_multiplier": row[11],
            "defense_matchup_multiplier": row[12],
            "vorp_score": row[13],
        }
        for row in rows
    ]
