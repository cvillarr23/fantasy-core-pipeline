"""Live game REST endpoints: thin controller over src.services.live_service. Reads only the
poller's Redis snapshot, never ESPN, and each request tells the poller someone is watching."""
from typing import List

from fastapi import APIRouter, HTTPException, Query

from src.schemas.live import LivePlayersResponse
from src.services import live_service

live_router = APIRouter()

MAX_PLAYER_IDS = 100


def _parse_ids(ids: str) -> List[str]:
    parsed = list(dict.fromkeys(i.strip() for i in ids.split(",") if i.strip()))
    if not parsed:
        raise HTTPException(status_code=422, detail="ids must list at least one player id")
    if len(parsed) > MAX_PLAYER_IDS:
        raise HTTPException(status_code=422, detail=f"at most {MAX_PLAYER_IDS} ids")
    return parsed


@live_router.get("/players", response_model=LivePlayersResponse)
def get_live_players(ids: str = Query(..., description="Comma-separated players.player_id values")):
    parsed = _parse_ids(ids)
    live_service.touch_last_viewed()
    return {"players": live_service.load_live_blocks(parsed)}
