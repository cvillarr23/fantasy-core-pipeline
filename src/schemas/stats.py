"""Pydantic response model + sort enum for the VORP browse REST endpoint (src/api/stats.py)."""
from enum import Enum
from typing import Optional

from pydantic import BaseModel


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
