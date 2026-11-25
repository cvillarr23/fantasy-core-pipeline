"""Pydantic response models for `GET /v1/scoring/rules` (src/api/scoring.py)."""
from typing import List, Optional

from pydantic import BaseModel


class ScoringRule(BaseModel):
    """One league rule: `points_per` points per unit of the stat, or, for a range stat like
    "7–13 points allowed", once when it happens."""
    stat_key: str
    label: str
    # Passing, Rushing, Receiving, Turnovers, Kicking, Defense or Returns.
    category: str
    points_per: float
    # What the stat counts (`yds`, `TD`, `rec`, ...); null for a range stat.
    unit: Optional[str] = None
    # Plain English, e.g. "1 pt per 10 rushing yds", "−2 pts per fumble lost".
    rule_label: str
    # Null when the rule applies to every player; otherwise the one position it applies to
    # (ESPN's per-position overrides, e.g. every D/ST stat).
    position: Optional[str] = None


class ScoringRulesResponse(BaseModel):
    season: int
    rules: List[ScoringRule]
