"""The per-source projections behind a line's consensus `projected_points` (#462)."""
from typing import Optional

from pydantic import BaseModel


class ProjectionSources(BaseModel):
    """weekly_stats' stored per-source projections; `projected_points` is the mean of the
    non-null ones. None means that source has no projection for the week (or hasn't synced)."""
    espn: Optional[float] = None
    sleeper: Optional[float] = None
