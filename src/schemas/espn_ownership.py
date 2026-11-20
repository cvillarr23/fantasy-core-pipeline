"""ESPN's league-wide market signal for a player, shared by every player payload."""
from typing import Optional

from pydantic import BaseModel


class EspnOwnership(BaseModel):
    pos_rank: Optional[int] = None
    percent_owned: Optional[float] = None
    percent_started: Optional[float] = None
    # Owned but not started: max(percent_owned - percent_started, 0).
    percent_bench: Optional[float] = None
    # ESPN's 24h change in percent owned (checked against consecutive daily snapshots: slope 1.00).
    percent_change: Optional[float] = None
