import os
from typing import Literal
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from src.services import league_dashboard as service

dashboard_router = APIRouter()


@dashboard_router.get("/league/standings")
def standings(season: int):
    return service.standings(season)


@dashboard_router.get("/league/player-leaders")
def leaders(
    season: int,
    week: int = Query(..., ge=1, le=18),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    q: str = "",
    position: str = "",
    owner: str = "",
    totals: bool = False,
):
    data = service.leaders(season, week, totals)
    rows = [
        r
        for r in data["items"]
        if q.lower() in r["name"].lower()
        and (not position or r["position"] == position)
    ]
    if owner == "mine":
        rows = [r for r in rows if r["owner_id"] == os.getenv("ESPN_TEAM_ID")]
    elif owner == "free":
        rows = [r for r in rows if not r["owner"]]
    elif owner == "rostered":
        rows = [r for r in rows if r["owner"]]
    return {**data, "items": rows[offset : offset + limit], "total": len(rows)}


class MoveRequest(BaseModel):
    kind: Literal["waiver", "trade"]
    season: int = Field(ge=2000, le=2100)
    start_week: int = Field(ge=1, le=18)
    end_week: int = Field(ge=1, le=18)
    incoming_espn_ids: list[str] = Field(min_length=1, max_length=20)
    outgoing_espn_ids: list[str] = Field(default_factory=list, max_length=20)


@dashboard_router.post("/roster/move-preview")
def preview(payload: MoveRequest):
    try:
        return service.move_preview(payload)
    except ValueError as exc:
        raise HTTPException(422, str(exc))
