"""
Where each NFL team's game stands this week (not started, in progress, final), read from
ESPN's public NFL scoreboard. It is the only source here that knows a game is live:
nflverse has scores and kickoff times, and ESPN's `statsOfficial` flag only says "final".

Display-only, like `nfl_byes.bye_weeks_or_empty`: a failed read means unknown (None), never
a failed response.
"""
import logging

from src.ingestion.sync_actual_scores import fetch_scoreboard
from src.nfl_byes import to_nflverse_team

logger = logging.getLogger("NflGameStatus")

NOT_STARTED, IN_PROGRESS, FINAL = "not_started", "in_progress", "final"

# ESPN's `status.type.state` for an event.
_STATE_TO_STATUS = {"pre": NOT_STARTED, "in": IN_PROGRESS, "post": FINAL}


def _is_requested_week(board: dict, season: int, week: int) -> bool:
    return board.get("season", {}).get("year") == season and board.get("week", {}).get("number") == week


def _event_status(event: dict) -> str | None:
    return _STATE_TO_STATUS.get(event.get("status", {}).get("type", {}).get("state"))


def _event_teams(event: dict) -> list[str]:
    competitors = (event.get("competitions") or [{}])[0].get("competitors", [])
    return [to_nflverse_team(c["team"]["abbreviation"]) for c in competitors]


def _statuses_by_team(events: list[dict]) -> dict[str, str]:
    statuses = {}
    for event in events:
        status = _event_status(event)
        if status is None:
            continue
        for team in _event_teams(event):
            statuses[team] = status
    return statuses


def load_team_game_status(season: int, week: int) -> dict[str, str] | None:
    """nflverse team code -> "not_started" | "in_progress" | "final" for every game on the
    week's scoreboard. None when the scoreboard can't be read or is for another week, so a
    caller shows "unknown" instead of guessing."""
    try:
        board = fetch_scoreboard(season, week)
        if not _is_requested_week(board, season, week):
            raise ValueError("scoreboard is not for the requested season/week")
        return _statuses_by_team(board.get("events") or [])
    except Exception as exc:
        logger.warning(f"No game status for Season {season} Week {week}: could not read the NFL scoreboard: {exc}")
        return None


def game_status_for(pro_team: str | None, statuses: dict[str, str] | None) -> str | None:
    """The status of `pro_team`'s game (an ESPN pro-team code), or None when unknown."""
    if statuses is None or pro_team is None:
        return None
    return statuses.get(to_nflverse_team(pro_team))
