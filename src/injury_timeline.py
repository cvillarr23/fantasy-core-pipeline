"""
Per-week availability of an injured player, for recommenders that judge a move over a
horizon (trade scoring today).

Status is ESPN's live fantasy `injuryStatus` (the trade context's `injury_status`); the
expected return date comes from ESPN's public injuries feed (`player_injuries`, written by
src/ingestion/sync_injuries) and is mapped to the first week whose game for the player's
NFL team falls on or after it, so a bye is skipped rather than counted as the return.

A roster entry opts in by carrying an `injury` key, which the trade context sets only when
it has injury data for the player: a player_injuries row, and an NFL schedule to map its
return date onto. Entries without the key keep the old rule, Out only in the as-of week, so
other callers of roster_move_scoring (and a trade context with no feed or schedule) are
unchanged.

Rules, for a scored week (the horizon, then any rest-of-season weeks):
- Out, on injured reserve or suspended (or in an IR slot): out in the as-of week, and in
  every week before the expected return week. With no return week, INJURY_RESERVE (or an
  injured player in an IR slot) stays out through end_week, the horizon's last week, since
  he can't start until activated; rest-of-season weeks count him. A plain Out, or a healthy
  player still in an IR slot, is back the next week, since nothing better is known.
- Questionable or Doubtful in the as-of week only: a positive VORP is scaled by
  QUESTIONABLE_AVAILABILITY or DOUBTFUL_AVAILABILITY, the expected share of the game he
  plays. A negative VORP is left alone so a tag never makes a player look better. Later
  weeks count in full, since the tag is about this week's game.
"""
from datetime import date

OUT_STATUSES = {"OUT", "INJURY_RESERVE"}
# Statuses a return date applies to. SUSPENSION is ESPN's label for a suspended player.
TIMELINE_STATUSES = OUT_STATUSES | {"SUSPENSION"}
# Out for the whole horizon when no return week is known.
UNTIL_ACTIVATED_STATUSES = {"INJURY_RESERVE"}
QUESTIONABLE_AVAILABILITY = 0.75
DOUBTFUL_AVAILABILITY = 0.25
AS_OF_WEEK_AVAILABILITY = {
    "QUESTIONABLE": QUESTIONABLE_AVAILABILITY,
    "DOUBTFUL": DOUBTFUL_AVAILABILITY,
}


def expected_return_week(return_date, game_dates):
    """The first regular-season week whose game is on or after return_date, given the
    team's [(week, date)] games. A date after the team's last game maps to the week after
    it, so he's out for the rest of the season. None when either input is unknown."""
    if return_date is None or not game_dates:
        return None
    if isinstance(return_date, str):
        return_date = date.fromisoformat(return_date)
    games = sorted(game_dates)
    for week, game_day in games:
        if game_day >= return_date:
            return week
    return games[-1][0] + 1


def availability(player, week, as_of_week, end_week=None):
    """(factor, reason): 0.0 with "out" when the player can't play this week, a factor
    below 1 with the status it came from, or (1.0, None). end_week is the horizon's last
    week, which bounds an undated injured-reserve stay (None: no bound)."""
    status = player.get("injury_status")
    in_ir_slot = player.get("slot") == "IR"
    timeline_aware = "injury" in player
    if not timeline_aware:
        if week == as_of_week and (status in OUT_STATUSES or in_ir_slot):
            return 0.0, "out"
        return 1.0, None
    if status in TIMELINE_STATUSES or in_ir_slot:
        if week == as_of_week:
            return 0.0, "out"
        return_week = (player.get("injury") or {}).get("expected_return_week")
        if return_week is not None:
            return (0.0, "out") if week < return_week else (1.0, None)
        # A healthy player still in an IR slot is activated next week, like a plain Out.
        until_activated = status in UNTIL_ACTIVATED_STATUSES or (
            in_ir_slot and status in TIMELINE_STATUSES
        )
        if until_activated and (end_week is None or week <= end_week):
            return 0.0, "out"
        return 1.0, None
    if week == as_of_week and status in AS_OF_WEEK_AVAILABILITY:
        return AS_OF_WEEK_AVAILABILITY[status], status.lower()
    return 1.0, None


def weeks_out(player, weeks, as_of_week, end_week=None):
    """How many of `weeks` the player is out."""
    return sum(1 for week in weeks if availability(player, week, as_of_week, end_week)[0] == 0.0)


def without_timeline(player):
    """The entry without its `injury` key, so it follows the old as-of-week-only rule."""
    return {key: value for key, value in player.items() if key != "injury"}
