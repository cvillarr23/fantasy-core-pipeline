"""One team's NFL bye outlook over a horizon (src/services/roster_outlook_service)."""

from pydantic import BaseModel

from src.schemas.trade_context import ByeOutlookWeek


class ByeOutlookPlayer(BaseModel):
    espn_id: str
    name: str | None
    position: str | None
    nfl_team: str | None
    nfl_bye_week: int | None


class TeamByeOutlook(BaseModel):
    team_id: str
    team_name: str | None
    season: int
    start_week: int
    end_week: int
    # False when the NFL schedule couldn't be read: byes are then unknown, not absent.
    schedule_known: bool
    median_lineup_value: float | None
    crunch_weeks: list[int]
    weeks: list[ByeOutlookWeek]
    players: list[ByeOutlookPlayer]
