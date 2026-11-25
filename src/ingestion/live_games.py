"""
Keeps a live snapshot of every NFL game in this week's fantasy matchup in Redis, so the live
API reads it without calling ESPN on the request path. Runs as a launcher service job for one
game window (`python -m src.ingestion.live_games`).

Sources:
- Game state comes from ESPN's public site API (no auth). The scoreboard lists the week's games,
  with score, clock and `situation` (possession, yard line, red zone) while a game is live.
  `summary?event=` adds the play-by-play.
- Fantasy points come from the league's own `mMatchupScore` payload, the request
  `sync_actual_scores.fetch_week` makes: each roster entry's actual stat row
  (`statSourceId` 0) carries ESPN's live `appliedTotal`, which matches the matchup score, plus
  `appliedStats` and the raw `stats` the stat line is read from. Points are never recomputed
  here.

Each tick writes, with a 6h TTL so stale data ages out:
- `live:game:{event_id}` for each tracked game (a team with a player on either matchup roster).
- `live:player:{player_id}` (our `players.player_id`, mapped through `players.espn_id`). It keeps
  ESPN's `appliedStats` (points per stat id) and `applied_qty` (the raw count behind each), which
  the live API itemizes into a scoring breakdown (`src/espn_scoring.py`).
- `live:plays:{event_id}`, the plays that name a matchup player, newest first, capped at 300.
  ESPN's plays carry no athlete ids, so players are matched by name in the play text, scoped to
  the play's teams (`_play_player_ids`). Each play also carries `recorded_at` (the tick that first
  saw it, since ESPN gives no wall time) and `fantasy_delta` (`player_id` -> change in that
  player's `live_points` since the previous tick). A delta is null when one player has more than
  one new play in the same tick (the change can't be split) or when the player had no earlier
  `live:player` row (first tick, or after a restart with the keys expired), or when the previous
  tick didn't refresh that game's plays (its summary or stored plays couldn't be read), since
  `live:player` moved on without them. Both are carried forward unchanged on later ticks
  (`attach_play_deltas`). When the stored plays can't be read, that game's list is left as it is
  for the tick rather than rewritten.
- `live:player_ids`, a JSON list of this tick's `live:player` ids, so readers needn't scan.
- `live:updated_at`.

`live:health` is the launcher's health key for this service job. It is rewritten every tick,
including a failed one, with a 10 minute TTL: it says the loop is alive, not that ESPN answered.
It carries the `season` and `week` being polled, so readers can tell whose snapshot it is.

Cadence: a 30s tick while `live:last_viewed` (epoch seconds, written by the live API) is under
5 minutes old, otherwise 180s. The wait rechecks it every 10s, so a new viewer speeds polling
up at once. The poller exits once no tracked game is in progress and none kicks off within 30
minutes. A failed tick logs and the loop carries on.
"""
import argparse
import json
import logging
import os
import re
import signal
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import nflreadpy as nfl
import requests
from espn_api.football.constant import PRO_TEAM_MAP

from src.database import get_db_cursor, get_redis_client
from src.espn_positions import DEFAULT_POSITIONS
from src.ingestion.sync_league import build_espn_requests
from src.nfl_byes import to_nflverse_team
from src.nfl_schedule import WeekResolutionError, resolve_upcoming_week
from src.services import run_log_service

logger = logging.getLogger("LiveGames")

SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
SUMMARY_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary"

FETCH_RETRY_ATTEMPTS = 3
FETCH_CONNECT_TIMEOUT_SECONDS = 5
FETCH_READ_TIMEOUT_SECONDS = 30
ESPN_LEAGUE_TIMEOUT_SECONDS = 30

LIVE_TTL_SECONDS = 6 * 3600
HEALTH_KEY = "live:health"
HEALTH_TTL_SECONDS = 10 * 60
LAST_VIEWED_KEY = "live:last_viewed"
UPDATED_AT_KEY = "live:updated_at"
GAME_KEY = "live:game:{event_id}"
PLAYER_KEY = "live:player:{player_id}"
PLAYER_IDS_KEY = "live:player_ids"
PLAYS_KEY = "live:plays:{event_id}"
MAX_PLAYS = 300

FAST_TICK_SECONDS = 30
SLOW_TICK_SECONDS = 180
VIEWED_WINDOW_SECONDS = 5 * 60
WAIT_SLICE_SECONDS = 10
KICKOFF_LOOKAHEAD = timedelta(minutes=30)

NOT_STARTED, IN_PROGRESS, FINAL = "not_started", "in_progress", "final"
_STATE_TO_STATUS = {"pre": NOT_STARTED, "in": IN_PROGRESS, "post": FINAL}

# ESPN fantasy stat ids (espn_api's PLAYER_STATS_MAP) -> the stat line's keys. Two-point
# conversions are summed across passing, rushing and receiving.
STAT_LINE_IDS = {
    3: "passYd", 4: "passTD", 20: "int",
    24: "rushYd", 25: "rushTD",
    53: "rec", 42: "recYd", 43: "recTD",
    72: "fumLost",
    83: "fgMade", 85: "fgMissed", 86: "xpMade", 88: "xpMissed",
    99: "sacks", 95: "defInt", 96: "fumRec", 98: "safeties", 97: "blockedKicks",
    105: "defTD", 120: "ptsAllowed", 127: "ydsAllowed",
}
TWO_POINT_IDS = (19, 26, 44)

# ESPN's lineupSlotId for a D/ST (POSITION_MAP 16).
DST_SLOT_ID = 16
NAME_SUFFIX = re.compile(r"\s+(Jr\.?|Sr\.?|II|III|IV|V)$")


class LiveGamesSetupError(RuntimeError):
    """The poller can't start: a required environment variable is unset."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_time(value):
    """ESPN's `2026-11-24T01:15Z` (or any ISO-8601) as aware UTC; None when unparsable."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------- fetching


def _get_json(url: str, params: dict | None = None):
    response = requests.get(
        url, params=params, timeout=(FETCH_CONNECT_TIMEOUT_SECONDS, FETCH_READ_TIMEOUT_SECONDS)
    )
    response.raise_for_status()
    return response.json()


def _with_retries(label: str, fetch):
    """Calls `fetch` up to FETCH_RETRY_ATTEMPTS times, like sync_rumors' fetch helpers.
    Returns None once every attempt has failed."""
    for attempt in range(1, FETCH_RETRY_ATTEMPTS + 1):
        try:
            return fetch()
        except Exception as e:
            logger.warning(f"{label} fetch attempt {attempt}/{FETCH_RETRY_ATTEMPTS} failed: {e}")
    logger.error(f"Exhausted all {FETCH_RETRY_ATTEMPTS} attempts fetching {label}.")
    return None


def fetch_scoreboard(season: int, week: int):
    """The week's NFL scoreboard, or None when ESPN can't be read or returns another week."""
    params = {"dates": season, "seasontype": 2, "week": week, "limit": 100}
    board = _with_retries("NFL scoreboard", lambda: _get_json(SCOREBOARD_URL, params))
    if board is None:
        return None
    if board.get("season", {}).get("year") != season or board.get("week", {}).get("number") != week:
        logger.error(f"The NFL scoreboard is not for Season {season} Week {week}; ignoring it.")
        return None
    return board


def fetch_summary(event_id: str):
    """One game's summary (plays), or None when it is unavailable."""
    return _with_retries(f"summary {event_id}", lambda: _get_json(SUMMARY_URL, {"event": event_id}))


def fetch_matchup_payload(espn_requests, week: int, period: int):
    """The league's mMatchupScore payload for our matchup period: both rosters with live
    points. None when ESPN can't be read."""
    filters = {"schedule": {"filterMatchupPeriodIds": {"value": [period]}}}
    return _with_retries(
        "league matchup payload",
        lambda: espn_requests.league_get(
            params={"view": ["mMatchupScore", "mScoreboard"], "scoringPeriodId": week},
            headers={"x-fantasy-filter": json.dumps(filters)},
        ),
    )


# ---------------------------------------------------------------- games


def _competitors(event: dict) -> list:
    return ((event.get("competitions") or [{}])[0]).get("competitors") or []


def _event_status(event: dict) -> str | None:
    return _STATE_TO_STATUS.get(event.get("status", {}).get("type", {}).get("state"))


def _score(competitor: dict, status: str):
    """A team's score; None before kickoff, since a 0-0 that hasn't started is unknown."""
    if status == NOT_STARTED or competitor.get("score") in (None, ""):
        return None
    try:
        return int(competitor["score"])
    except (TypeError, ValueError):
        return None


def _side(competitors: list, home_away: str, status: str) -> dict:
    for competitor in competitors:
        if competitor.get("homeAway") == home_away:
            return {
                "abbr": to_nflverse_team((competitor.get("team") or {}).get("abbreviation")),
                "score": _score(competitor, status),
            }
    return {"abbr": None, "score": None}


def _team_abbr_by_id(competitors: list) -> dict:
    return {
        str((c.get("team") or {}).get("id")): to_nflverse_team((c.get("team") or {}).get("abbreviation"))
        for c in competitors
    }


def _situation(event: dict) -> dict:
    """The live down-and-distance block. Only the scoreboard carries it: a live `summary` has
    no `situation`."""
    return ((event.get("competitions") or [{}])[0]).get("situation") or {}


def _possession(situation: dict, abbr_by_id: dict):
    possession = situation.get("possession")
    if isinstance(possession, dict):
        possession = possession.get("id")
    return abbr_by_id.get(str(possession)) if possession is not None else None


def shape_game(event: dict, now: datetime) -> dict:
    """`live:game:{event_id}`. Clock, period and field position are null unless the game is
    in progress."""
    status = _event_status(event) or NOT_STARTED
    competitors = _competitors(event)
    event_status = event.get("status") or {}
    live = status == IN_PROGRESS
    situation = _situation(event) if live else {}
    return {
        "event_id": str(event.get("id")),
        "status": status,
        "detail": event_status.get("type", {}).get("shortDetail"),
        "period": (event_status.get("period") or None) if status != NOT_STARTED else None,
        "clock": event_status.get("displayClock") if live else None,
        "home": _side(competitors, "home", status),
        "away": _side(competitors, "away", status),
        "possession": _possession(situation, _team_abbr_by_id(competitors)),
        "yard_line": situation.get("possessionText"),
        "red_zone": bool(situation.get("isRedZone")) if live else False,
        "kickoff": event.get("date"),
        "updated_at": _iso(now),
    }


def tracked_events(board: dict, pro_team_ids: set) -> dict:
    """Event id -> scoreboard event, for every game with a team in `pro_team_ids` (ESPN pro
    team ids, shared by the fantasy API and the site API)."""
    events = {}
    for event in board.get("events") or []:
        team_ids = {str((c.get("team") or {}).get("id")) for c in _competitors(event)}
        if team_ids & pro_team_ids:
            events[str(event.get("id"))] = event
    return events


def _kicks_off_soon(game: dict, now: datetime) -> bool:
    kickoff = _parse_time(game.get("kickoff"))
    return kickoff is not None and kickoff <= now + KICKOFF_LOOKAHEAD


def should_exit(games: list, now: datetime) -> bool:
    """True once no tracked game is in progress and none kicks off within 30 minutes: every
    game is final, or the window is empty."""
    if any(game["status"] == IN_PROGRESS for game in games):
        return False
    return not any(game["status"] == NOT_STARTED and _kicks_off_soon(game, now) for game in games)


# ---------------------------------------------------------------- players


def _actual_stat_row(player: dict, season: int, week: int):
    for stat in player.get("stats") or []:
        if (
            stat.get("seasonId") == season
            and stat.get("scoringPeriodId") == week
            and stat.get("statSplitTypeId") == 1
            and stat.get("statSourceId") == 0
        ):
            return stat
    return None


def stat_line(raw_stats: dict) -> dict:
    """ESPN's raw stat counts as named keys; a stat ESPN didn't report is left out."""
    line = {}
    for stat_id, key in STAT_LINE_IDS.items():
        value = raw_stats.get(str(stat_id))
        if value is not None:
            line[key] = value
    two_points = [raw_stats[str(i)] for i in TWO_POINT_IDS if raw_stats.get(str(i)) is not None]
    if two_points:
        line["twoPt"] = sum(two_points)
    return line


def applied_quantities(row: dict) -> dict:
    """The raw count behind each scored stat (`appliedStats` holds points, `stats` the counts),
    so a breakdown can say "98 yds × 0.1". A stat with no count is left out."""
    raw = row.get("stats") or {}
    return {stat_id: raw[stat_id] for stat_id in row.get("appliedStats") or {} if raw.get(stat_id) is not None}


def roster_entries(payload: dict, team_ids: set) -> list:
    """`(fantasy team id, roster entry)` for both sides of our matchup, bench included."""
    entries = []
    for game in payload.get("schedule") or []:
        for side in ("home", "away"):
            own = game.get(side) or {}
            team_id = str(own.get("teamId"))
            if team_id not in team_ids:
                continue
            roster = own.get("rosterForCurrentScoringPeriod") or {}
            entries.extend((team_id, entry) for entry in roster.get("entries") or [])
    return entries


def _entry_player(entry: dict) -> dict:
    return (entry.get("playerPoolEntry") or {}).get("player") or entry.get("player") or {}


def shape_roster_player(team_id, entry, season, week, player_id, event_by_pro_team, now) -> dict:
    """One matchup roster player: identity for play matching plus the `live:player` value."""
    player = _entry_player(entry)
    pro_team_id = str(player.get("proTeamId"))
    row = _actual_stat_row(player, season, week)
    return {
        "player_id": player_id,
        "espn_id": str(entry.get("playerId", player.get("id"))),
        "full_name": player.get("fullName") or "",
        "first_name": player.get("firstName") or "",
        "last_name": player.get("lastName") or "",
        "pro_team_id": pro_team_id,
        "is_dst": entry.get("lineupSlotId") == DST_SLOT_ID or str(entry.get("playerId", "")).startswith("-"),
        "value": {
            "event_id": event_by_pro_team.get(pro_team_id),
            "team": to_nflverse_team(_pro_team_abbr(player)),
            "fantasy_team_id": team_id,
            "lineup_slot_id": entry.get("lineupSlotId"),
            "stat_line": stat_line(row.get("stats") or {}) if row else {},
            "live_points": row.get("appliedTotal") if row else None,
            "applied_stats": (row.get("appliedStats") or {}) if row else {},
            "applied_qty": applied_quantities(row) if row else {},
            "position": DEFAULT_POSITIONS.get(player.get("defaultPositionId")),
            "updated_at": _iso(now),
        },
    }


def _pro_team_abbr(player: dict):
    return PRO_TEAM_MAP.get(player.get("proTeamId"))


def shape_roster(payload, team_ids, season, week, espn_to_player, event_by_pro_team, now):
    """Matchup roster players we can map to a `players` row. Unmapped ESPN ids are logged and
    skipped: `live:player` is keyed by our id."""
    players, unmapped = [], []
    for team_id, entry in roster_entries(payload, team_ids):
        espn_id = str(entry.get("playerId", _entry_player(entry).get("id")))
        player_id = espn_to_player.get(espn_id)
        if player_id is None:
            unmapped.append(espn_id)
            continue
        players.append(shape_roster_player(team_id, entry, season, week, player_id, event_by_pro_team, now))
    if unmapped:
        logger.warning(f"{len(unmapped)} matchup roster player(s) have no players.espn_id match: {unmapped}")
    return players


# ---------------------------------------------------------------- plays


def _name_pattern(player: dict):
    """Matches ESPN's play-by-play form (`J.Gibbs`, `A.St. Brown`, or `Ja.Chase` where ESPN
    lengthens the initial to tell teammates apart) and the full name scoring plays use
    (`Jahmyr Gibbs`), without a Jr./III suffix."""
    first = player["first_name"].strip()
    last = NAME_SUFFIX.sub("", player["last_name"].strip())
    if not first or not last:
        return None
    initial = rf"{re.escape(first[0])}[a-z']{{0,2}}\."
    forms = [rf"{initial}\s?{re.escape(last)}", rf"{re.escape(first)}\s+{re.escape(last)}"]
    return re.compile(rf"(?<![\w.])(?:{'|'.join(forms)})(?![\w'])")


def _play_teams(play: dict) -> dict:
    """Team id -> 'offense' | 'defense' for the play's participants."""
    return {str(t.get("id")): t.get("type") for t in play.get("teamParticipants") or []}


def _dst_involved(play: dict, teams: dict, pro_team_id: str) -> bool:
    """A D/ST is involved when its team is the play's defense for a turnover, a sack or a
    score (a score against it counts toward points allowed), or when it returns a kick or
    punt for a score. Its own offense's plays are not its business."""
    kind = (play.get("type") or {}).get("text") or ""
    if teams.get(pro_team_id) == "defense":
        return bool(play.get("isTurnover") or play.get("scoringPlay") or "Sack" in kind or "Safety" in kind)
    return pro_team_id in teams and bool(play.get("scoringPlay")) and "Return" in kind


def _play_player_ids(play: dict, roster: list, patterns: dict) -> list:
    """Our ids of the matchup players a play involves. A play without team participants is
    matched against every roster player; otherwise only that play's teams' players."""
    teams = _play_teams(play)
    text = play.get("text") or ""
    ids = []
    for player in roster:
        if teams and player["pro_team_id"] not in teams:
            continue
        if player["is_dst"]:
            if _dst_involved(play, teams, player["pro_team_id"]):
                ids.append(player["player_id"])
            continue
        pattern = patterns.get(player["player_id"])
        if pattern is not None and pattern.search(text):
            ids.append(player["player_id"])
    return ids


def _all_plays(summary: dict) -> list:
    """Every play in game order, deduped by id (the current drive can repeat the last one)."""
    drives = summary.get("drives") or {}
    groups = list(drives.get("previous") or [])
    if drives.get("current"):
        groups.append(drives["current"])
    plays, seen = [], set()
    for drive in groups:
        for play in drive.get("plays") or []:
            play_id = str(play.get("id"))
            if play_id not in seen:
                seen.add(play_id)
                plays.append(play)
    return plays


def shape_plays(summary: dict, roster: list) -> list:
    """`live:plays:{event_id}`: plays that involve a matchup player, newest first, capped."""
    patterns = {p["player_id"]: _name_pattern(p) for p in roster if not p["is_dst"]}
    shaped = []
    for play in reversed(_all_plays(summary)):
        player_ids = _play_player_ids(play, roster, patterns)
        if player_ids:
            shaped.append({
                "play_id": str(play.get("id")),
                "period": (play.get("period") or {}).get("number"),
                "clock": (play.get("clock") or {}).get("displayValue"),
                "text": play.get("text"),
                "player_ids": player_ids,
                "scoring": bool(play.get("scoringPlay")),
            })
        if len(shaped) >= MAX_PLAYS:
            break
    return shaped


def _play_delta(player_id, new_counts: Counter, previous_points: dict, current_points: dict):
    """One player's fantasy-point change attributed to a new play, or None when it can't be
    told: several new plays this tick, no earlier snapshot of the player, or no points now."""
    if new_counts[player_id] != 1 or player_id not in previous_points:
        return None
    current = current_points.get(player_id)
    if current is None:
        return None
    return round(current - (previous_points[player_id] or 0.0), 2)


def _carried_play(play: dict, earlier: dict) -> dict:
    """A play an earlier tick stored keeps the stamp and deltas it got then: the baseline has
    moved on since, so recomputing would give zero."""
    return {**play, "recorded_at": earlier.get("recorded_at"), "fantasy_delta": earlier.get("fantasy_delta") or {}}


def attach_play_deltas(plays, previous_plays, previous_points, current_points, now, baseline_stale=False) -> list:
    """Stamps each play with `recorded_at` and `fantasy_delta`. A play stored by an earlier
    tick keeps what it had; a new one is stamped now, with each of its players' point change
    since the previous tick. `previous_points` / `current_points` map our player id to
    `live_points`; a player missing from `previous_points` had no earlier snapshot.
    `baseline_stale` means the previous tick didn't refresh this game's plays, so
    `previous_points` already counts plays this tick sees as new: their deltas are null."""
    known = {play["play_id"]: play for play in previous_plays}
    new_counts = Counter(
        player_id
        for play in plays
        if play["play_id"] not in known
        for player_id in play["player_ids"]
    )
    baseline = {} if baseline_stale else previous_points
    stamped = []
    for play in plays:
        earlier = known.get(play["play_id"])
        if earlier is not None:
            stamped.append(_carried_play(play, earlier))
            continue
        deltas = {
            player_id: _play_delta(player_id, new_counts, baseline, current_points)
            for player_id in play["player_ids"]
        }
        stamped.append({**play, "recorded_at": _iso(now), "fantasy_delta": deltas})
    return stamped


# ---------------------------------------------------------------- cadence


def _last_viewed(value):
    """`live:last_viewed` as aware UTC: epoch seconds, or an ISO-8601 string."""
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return _parse_time(value)


def tick_interval(r_client, now: datetime) -> int:
    """30s while someone viewed a live page in the last 5 minutes, otherwise 180s. A Redis
    error reads as "not viewed"."""
    try:
        viewed = _last_viewed(r_client.get(LAST_VIEWED_KEY))
    except Exception as e:
        logger.warning(f"Could not read {LAST_VIEWED_KEY}: {e}")
        viewed = None
    if viewed is not None and (now - viewed).total_seconds() < VIEWED_WINDOW_SECONDS:
        return FAST_TICK_SECONDS
    return SLOW_TICK_SECONDS


def wait_for_next_tick(r_client, stop: threading.Event, started: float, monotonic=time.monotonic, now=_now) -> None:
    """Sleeps until the tick interval since `started` has passed, rechecking the interval each
    slice so a new viewer cuts a slow wait short. Returns early once `stop` is set."""
    while not stop.is_set():
        remaining = tick_interval(r_client, now()) - (monotonic() - started)
        if remaining <= 0:
            return
        stop.wait(min(WAIT_SLICE_SECONDS, remaining))


# ---------------------------------------------------------------- the poller


class LiveGamesPoller:
    """One game window's poll state. Only `tick` touches ESPN and Redis."""

    def __init__(self, r_client, espn_requests, season, week, period, team_ids, espn_to_player):
        self.r = r_client
        self.espn = espn_requests
        self.season = season
        self.week = week
        self.period = period
        self.team_ids = team_ids
        self.espn_to_player = espn_to_player
        self.pro_team_ids = None
        self.finalized = set()
        self.last_success_at = None
        self.ticks = 0
        self.failed_ticks = 0
        self.tracked = set()
        self.unrefreshed = set()

    def tick(self, now: datetime | None = None):
        """One poll. Returns the tracked games' shapes, or None when the tick failed (ESPN
        down, no rosters yet). `live:health` is written either way."""
        now = now or _now()
        self.ticks += 1
        try:
            games = self._poll(now)
        except Exception as e:
            logger.exception(f"Live games tick failed: {e}")
            games = None
        if games is None:
            self.failed_ticks += 1
        else:
            self.last_success_at = now
        self._write_health(now)
        return games

    def _poll(self, now):
        board = fetch_scoreboard(self.season, self.week)
        payload = fetch_matchup_payload(self.espn, self.week, self.period)
        if payload is not None:
            self.pro_team_ids = self._pro_team_ids(payload)
        if board is None or self.pro_team_ids is None:
            return None
        events = tracked_events(board, self.pro_team_ids)
        self.tracked = set(events)
        event_by_pro_team = self._event_by_pro_team(events)
        roster = (
            shape_roster(payload, self.team_ids, self.season, self.week, self.espn_to_player, event_by_pro_team, now)
            if payload is not None
            else []
        )
        needed = {event_id for event_id, event in events.items() if self._needs_summary(event_id, event)}
        games, plays = self._games_and_plays(events, roster, now)
        plays = self._stamp_plays(plays, roster, now)
        self.unrefreshed = self._unrefreshed(needed, roster, plays)
        self._write(games, roster, plays, now)
        return games

    def _unrefreshed(self, needed: set, roster: list, plays: dict) -> set:
        """Games whose plays this tick should have refreshed but didn't, while `live:player`
        still moved on: the next tick can't attribute their point changes. Without a roster
        no `live:player` is written, so the baseline holds and the earlier answer stands."""
        return needed - set(plays) if roster else self.unrefreshed

    def _stamp_plays(self, plays: dict, roster: list, now) -> dict:
        """Adds `recorded_at` and `fantasy_delta` from what the previous tick left in Redis.
        Read before `_write` replaces it, so a restart doesn't lose the baseline. A game whose
        stored plays can't be read is dropped for this tick, so `_write` keeps its list."""
        if not plays:
            return plays
        previous_points = self._previous_points(roster)
        current_points = {p["player_id"]: p["value"]["live_points"] for p in roster}
        stamped = {}
        for event_id, event_plays in plays.items():
            previous_plays = self._previous_plays(event_id)
            if previous_plays is not None:
                stamped[event_id] = attach_play_deltas(
                    event_plays, previous_plays, previous_points, current_points, now,
                    baseline_stale=event_id in self.unrefreshed,
                )
        return stamped

    def _previous_points(self, roster: list) -> dict:
        """Our player id -> `live_points` for every player with a `live:player` row; empty on a
        Redis error, which only costs this tick its deltas."""
        if not roster:
            return {}
        try:
            raw = self.r.mget([PLAYER_KEY.format(player_id=p["player_id"]) for p in roster])
            return {
                player["player_id"]: json.loads(value).get("live_points")
                for player, value in zip(roster, raw)
                if value
            }
        except Exception as e:
            logger.warning(f"Could not read previous live players: {e}")
            return {}

    def _previous_plays(self, event_id: str) -> list | None:
        """The game's stored plays, or None when they can't be read: an empty list would make
        every stored play look new and rewrite its stamp."""
        try:
            return [json.loads(p) for p in self.r.lrange(PLAYS_KEY.format(event_id=event_id), 0, -1)]
        except Exception as e:
            logger.warning(f"Could not read previous plays for {event_id}; keeping them this tick: {e}")
            return None

    def _pro_team_ids(self, payload) -> set:
        return {
            str(_entry_player(entry).get("proTeamId"))
            for _, entry in roster_entries(payload, self.team_ids)
        }

    @staticmethod
    def _event_by_pro_team(events: dict) -> dict:
        return {
            str((c.get("team") or {}).get("id")): event_id
            for event_id, event in events.items()
            for c in _competitors(event)
        }

    def _needs_summary(self, event_id, event) -> bool:
        """In-progress games, and one last fetch when a game turns final."""
        status = _event_status(event)
        return status == IN_PROGRESS or (status == FINAL and event_id not in self.finalized)

    def _games_and_plays(self, events, roster, now):
        games, plays = [], {}
        for event_id, event in events.items():
            summary = fetch_summary(event_id) if self._needs_summary(event_id, event) else None
            games.append(shape_game(event, now))
            if summary is not None and roster:
                plays[event_id] = shape_plays(summary, roster)
                if _event_status(event) == FINAL:
                    self.finalized.add(event_id)
        return games, plays

    def _write(self, games, roster, plays, now):
        pipe = self.r.pipeline()
        for game in games:
            pipe.set(GAME_KEY.format(event_id=game["event_id"]), json.dumps(game), ex=LIVE_TTL_SECONDS)
        for player in roster:
            pipe.set(PLAYER_KEY.format(player_id=player["player_id"]), json.dumps(player["value"]), ex=LIVE_TTL_SECONDS)
        if roster:
            pipe.set(PLAYER_IDS_KEY, json.dumps(sorted(p["player_id"] for p in roster)), ex=LIVE_TTL_SECONDS)
        for event_id, event_plays in plays.items():
            key = PLAYS_KEY.format(event_id=event_id)
            pipe.delete(key)
            if event_plays:
                pipe.rpush(key, *(json.dumps(p) for p in event_plays))
                pipe.expire(key, LIVE_TTL_SECONDS)
        pipe.set(UPDATED_AT_KEY, _iso(now), ex=LIVE_TTL_SECONDS)
        pipe.execute()
        logger.info(
            f"Wrote {len(games)} game(s), {len(roster)} player(s), "
            f"{sum(len(p) for p in plays.values())} play(s) for Week {self.week}."
        )

    def _write_health(self, now):
        health = {
            "updated_at": _iso(now),
            "last_success_at": _iso(self.last_success_at) if self.last_success_at else None,
            "season": self.season,
            "week": self.week,
            "tracked_events": sorted(self.tracked),
        }
        try:
            self.r.set(HEALTH_KEY, json.dumps(health), ex=HEALTH_TTL_SECONDS)
        except Exception as e:
            logger.warning(f"Could not write {HEALTH_KEY}: {e}")


def run_loop(poller, stop: threading.Event, once: bool = False, monotonic=time.monotonic, now=_now) -> str:
    """Polls until the window ends, `stop` is set or (with `once`) after one tick. Returns
    why it stopped."""
    while not stop.is_set():
        started = monotonic()
        games = poller.tick(now())
        if games is not None and should_exit(games, now()):
            return "window_over"
        if once:
            return "once"
        wait_for_next_tick(poller.r, stop, started, monotonic=monotonic, now=now)
    return "stopped"


# ---------------------------------------------------------------- startup


def _current_season() -> int:
    return int(os.getenv("CURRENT_YEAR") or nfl.get_current_season())


def load_matchup(cursor, league_id: str, season: int, week: int, team_id: str):
    """`(matchup_period, {our team id, opponent's})` for the week, or None when we have no
    scheduled matchup (a bye, or the schedule hasn't been synced)."""
    cursor.execute(
        """SELECT matchup_period, opponent_team_id, status FROM fantasy_matchups
            WHERE league_id = %s AND season = %s AND team_id = %s AND %s = ANY(scoring_weeks);""",
        (league_id, season, team_id, week),
    )
    row = cursor.fetchone()
    if not row or row[2] != "scheduled":
        return None
    return row[0], {team_id} | ({str(row[1])} if row[1] else set())


def load_espn_to_player(cursor) -> dict:
    cursor.execute("SELECT espn_id, player_id FROM players WHERE espn_id IS NOT NULL;")
    return {str(espn_id): player_id for espn_id, player_id in cursor.fetchall()}


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise LiveGamesSetupError(f"{name} environment variable is required.")
    return value


def _summarize(poller, week, reason):
    run_log_service.add_summary(
        headline=f"Week {week}: {poller.ticks} tick(s), {poller.failed_ticks} failed ({reason})",
        week=week,
        ticks=poller.ticks,
        failed_ticks=poller.failed_ticks,
        tracked_events=sorted(poller.tracked),
        stopped=reason,
    )


def _skip(headline: str) -> int:
    logger.info(headline)
    run_log_service.add_summary(headline=headline)
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--season", type=int)
    parser.add_argument("--week", type=int)
    parser.add_argument("--once", action="store_true", help="Run one tick and exit.")
    args = parser.parse_args(argv)

    season = args.season or _current_season()
    try:
        week = args.week or resolve_upcoming_week(season)
    except WeekResolutionError as e:
        logger.error(f"Could not resolve this week: {e}")
        return 1
    if week is None:
        return _skip(f"The {season} regular season is over; nothing to poll.")

    league_id, team_id = _require_env("ESPN_LEAGUE_ID"), _require_env("ESPN_TEAM_ID")
    with get_db_cursor() as cursor:
        matchup = load_matchup(cursor, league_id, season, week, team_id)
        espn_to_player = load_espn_to_player(cursor) if matchup else {}
    if matchup is None:
        return _skip(f"No scheduled Season {season} Week {week} matchup for team {team_id}; nothing to poll.")
    period, team_ids = matchup

    r_client = get_redis_client()
    r_client.ping()
    espn_requests = build_espn_requests(season, request_timeout_seconds=ESPN_LEAGUE_TIMEOUT_SECONDS)
    poller = LiveGamesPoller(r_client, espn_requests, season, week, period, team_ids, espn_to_player)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    logger.info(f"Polling live games for Season {season} Week {week} (matchup period {period}, teams {sorted(team_ids)}).")
    reason = run_loop(poller, stop, once=args.once)
    logger.info(f"Live games poller stopping: {reason}.")
    _summarize(poller, week, reason)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    sys.exit(run_log_service.run_main("live_games", main))
