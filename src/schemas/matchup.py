"""Shapes shared by the matchup and roster lines."""
from typing import List, Optional

from pydantic import BaseModel


class SuggestedSwapMove(BaseModel):
    """A starter who must change slots so the swap's add fits. ESPN ids and ESPN slot names."""
    player_id: str
    name: str
    from_slot: str
    to_slot: str


class SuggestedSwap(BaseModel):
    """The solver's lineup hint on the line of the starter it would bench.

    `bench_for` is the added player's name, `add_player_id` his ESPN id, `slot` the ESPN slot
    he ends in, and `moves` the displaced starters (empty for a plain swap). A chain swap is
    applied by benching this line's player, starting the add in `slot`, and re-slotting each
    move. `slot` is null only for a hint built before the field existed."""
    bench_for: str
    add_player_id: Optional[str] = None
    slot: Optional[str] = None
    moves: List[SuggestedSwapMove] = []
