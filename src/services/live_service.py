"""
Reads the live snapshot `src.ingestion.live_games` keeps in Redis: the `live` block on matchup
and summary players, `/v1/live/players` and the matchup feed. Nothing here calls ESPN.

Every reader fails soft: a Redis error reads as "no snapshot" (null blocks, an empty feed), so a
Redis outage never breaks the page around it.

The snapshot counts only while the poller is running: `live:health` (10 minute TTL, rewritten
every tick) must exist and name a `season`. The `live:game`/`live:player` keys outlive the poller
by hours, so without health every reader shows nothing. The matchup overlay and the feed also
require health's season and week to be the ones asked for, so a past season's week never gets
this week's games. Health from a poller that predates the `season` key counts as not live.

`_touch_last_viewed` is what speeds the poller up (epoch seconds, kept 10 minutes; under 5
minutes old means 30s ticks). Each reader calls it only when it served live data, so a page for
another week, or one opened while no poller runs, doesn't count as watching.

Keys are keyed by our `players.player_id`; matchup lines carry ESPN ids, so `overlay_matchup`
maps them through `players.espn_id` (the two id spaces overlap numerically, never substitute).
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from espn_api.football.constant import POSITION_MAP

from src.database import get_db_cursor, get_redis_client
from src.espn_scoring import score_breakdown
from src.ingestion.live_games import (
    GAME_KEY, HEALTH_KEY, LAST_VIEWED_KEY, NOT_STARTED, PLAYER_IDS_KEY, PLAYER_KEY, PLAYS_KEY,
)
from src.services import news_service, scoring_service

logger = logging.getLogger("LiveService")

LAST_VIEWED_TTL_SECONDS = 10 * 60
NEWS_WINDOW = timedelta(hours=2)
NEWS_CURSOR_PREFIX = "news:"
EARLIEST = datetime.min.replace(tzinfo=timezone.utc)
# ESPN lineup slots whose points don't count toward the matchup score.
NON_SCORING_SLOT_IDS = {20, 21}


class LiveGameNotFoundError(LookupError):
    """The poller has no snapshot of that game for the requested week."""


class LiveSnapshotUnavailableError(RuntimeError):
    """Redis can't be read, so whether the game exists is unknown."""


# ---------------------------------------------------------------- redis access


def _touch_last_viewed(r) -> None:
    """Marks the live pages as in use. Never raises."""
    try:
        r.set(LAST_VIEWED_KEY, str(int(time.time())), ex=LAST_VIEWED_TTL_SECONDS)
    except Exception as e:
        logger.warning(f"Could not write {LAST_VIEWED_KEY}: {e}")


def _redis():
    """The Redis client, or None when it can't be built."""
    try:
        return get_redis_client()
    except Exception as e:
        logger.warning(f"Live snapshot unavailable: {e}")
        return None


def _loads(values) -> list:
    return [json.loads(value) if value else None for value in values]


def _live_health(r, season: int | None = None, week: int | None = None) -> dict | None:
    """The poller's health while it is running (and, given a season and week, polling that
    one), else None. Health without `season` came from a poller older than that key; it can't
    say which season it polls, so it counts as not live."""
    raw = r.get(HEALTH_KEY)
    health = json.loads(raw) if raw else None
    if not health or health.get("season") is None:
        return None
    if season is not None and (health.get("season"), health.get("week")) != (season, week):
        return None
    return health


def _games(r, event_ids: list) -> dict:
    if not event_ids:
        return {}
    return dict(zip(event_ids, _loads(r.mget([GAME_KEY.format(event_id=e) for e in event_ids]))))


def _snapshots(r, player_ids: list) -> dict:
    """player id -> `(live:player value, its live:game value or None)`; missing players are
    left out."""
    if not player_ids:
        return {}
    players = dict(zip(player_ids, _loads(r.mget([PLAYER_KEY.format(player_id=p) for p in player_ids]))))
    games = _games(r, sorted({p["event_id"] for p in players.values() if p and p.get("event_id")}))
    return {
        player_id: (player, games.get(player.get("event_id")))
        for player_id, player in players.items()
        if player
    }


# ---------------------------------------------------------------- scoring breakdown


def _load_rule_points() -> dict | None:
    """The league's rule index for the breakdown's labels, or None (no breakdown) when the
    settings can't be read. Its own cursor, so a failure never aborts the caller's transaction."""
    try:
        with get_db_cursor() as cursor:
            return scoring_service.load_rule_points(cursor)
    except Exception as e:
        logger.warning(f"Scoring rules unavailable; live breakdowns left out: {e}")
        return None


def _breakdown(player: dict, rules: dict | None):
    return score_breakdown(player, rules) if rules is not None else None


# ---------------------------------------------------------------- the live block


def _live_block(player: dict, game: dict | None, rules: dict | None) -> dict:
    game = game or {}
    home, away = game.get("home") or {}, game.get("away") or {}
    return {
        "status": game.get("status"),
        "period": game.get("period"),
        "clock": game.get("clock"),
        "home_abbr": home.get("abbr"),
        "home_score": home.get("score"),
        "away_abbr": away.get("abbr"),
        "away_score": away.get("score"),
        "possession": game.get("possession"),
        "yard_line": game.get("yard_line"),
        "red_zone": game.get("red_zone"),
        "stat_line": player.get("stat_line"),
        "live_points": player.get("live_points"),
        "breakdown": _breakdown(player, rules),
        "updated_at": player.get("updated_at"),
    }


def _game_label(game: dict | None) -> str | None:
    """`TB @ ATL` before kickoff, `TB 14 – 17 ATL` once there is a score."""
    if not game:
        return None
    home, away = game.get("home") or {}, game.get("away") or {}
    if not home.get("abbr") or not away.get("abbr"):
        return None
    if home.get("score") is None or away.get("score") is None:
        return f"{away['abbr']} @ {home['abbr']}"
    return f"{away['abbr']} {away['score']} – {home['score']} {home['abbr']}"


def _live_blocks(r, player_ids: list) -> dict:
    """player id -> live block, for the players with a snapshot. The league's rules are read
    once, and only when there is a snapshot to break down."""
    snapshots = _snapshots(r, player_ids)
    rules = _load_rule_points() if snapshots else None
    return {player_id: _live_block(player, game, rules) for player_id, (player, game) in snapshots.items()}


def load_live_blocks(player_ids: list) -> dict:
    """`GET /v1/live/players`: player id -> live block, or None when the player has no
    snapshot or the poller isn't running."""
    r = _redis()
    blocks = {player_id: None for player_id in player_ids}
    if r is None or not player_ids:
        return blocks
    try:
        if _live_health(r) is None:
            return blocks
        _touch_last_viewed(r)
        blocks.update(_live_blocks(r, player_ids))
    except Exception as e:
        logger.warning(f"Could not read live players: {e}")
    return blocks


# ---------------------------------------------------------------- matchup overlay


def _matchup_lines(matchup: dict) -> list:
    return [
        line
        for side in ("our_team", "opponent")
        for group in ("starters", "bench")
        for line in matchup[side][group]
    ]


def _player_ids_by_espn_id(espn_ids: list) -> dict:
    with get_db_cursor() as cursor:
        cursor.execute("SELECT espn_id, player_id FROM players WHERE espn_id = ANY(%s);", (espn_ids,))
        return {str(espn_id): player_id for espn_id, player_id in cursor.fetchall()}


def _line_with_live(line: dict, blocks_by_espn_id: dict) -> dict:
    """Attaches `live`, and takes `game_status` from it so the two always agree: the matchup is
    cached for a minute and the snapshot is newer."""
    block = blocks_by_espn_id.get(line["espn_id"])
    if block is None:
        return {**line, "live": None}
    return {**line, "live": block, "game_status": block["status"] or line.get("game_status")}


def _side_with_live(side: dict, blocks_by_espn_id: dict) -> dict:
    return {
        **side,
        "starters": [_line_with_live(line, blocks_by_espn_id) for line in side["starters"]],
        "bench": [_line_with_live(line, blocks_by_espn_id) for line in side["bench"]],
    }


def _blocks_by_espn_id(r, matchup: dict) -> dict:
    espn_ids = sorted({line["espn_id"] for line in _matchup_lines(matchup)})
    player_by_espn = _player_ids_by_espn_id(espn_ids)
    blocks = _live_blocks(r, sorted(set(player_by_espn.values())))
    return {espn_id: blocks.get(player_id) for espn_id, player_id in player_by_espn.items()}


def overlay_matchup(matchup: dict) -> dict:
    """The matchup with a `live` block on every player the poller has a snapshot for, when the
    poller is running the matchup's season and week. Applied after the matchup cache, since
    the snapshot moves faster. The input is not mutated, and any failure returns it as it
    came."""
    r = _redis()
    if r is None:
        return matchup
    try:
        if _live_health(r, matchup["season"], matchup["week"]) is None:
            return matchup
        _touch_last_viewed(r)
        blocks = _blocks_by_espn_id(r, matchup)
        return {
            **matchup,
            "our_team": _side_with_live(matchup["our_team"], blocks),
            "opponent": _side_with_live(matchup["opponent"], blocks),
        }
    except Exception as e:
        logger.warning(f"Could not add live data to the matchup: {e}")
        return matchup


# ---------------------------------------------------------------- player summary


def player_live_summary(cursor, player_id: str) -> dict | None:
    """The `live` field of `GET /v1/players/{id}/summary`, or None without a snapshot or
    while the poller isn't running."""
    r = _redis()
    if r is None:
        return None
    try:
        health = _live_health(r)
        snapshot = _snapshots(r, [player_id]).get(player_id) if health else None
    except Exception as e:
        logger.warning(f"Could not read the live snapshot for {player_id}: {e}")
        return None
    if snapshot is None:
        return None
    _touch_last_viewed(r)
    player, game = snapshot
    return {
        "live_points": player.get("live_points"),
        "projected_points": _projected_points(cursor, player_id, health["season"], health.get("week")),
        "game_label": _game_label(game),
        "game_status": (game or {}).get("status"),
        "event_id": player.get("event_id"),
        "breakdown": _breakdown(player, _load_rule_points()),
    }


def _projected_points(cursor, player_id: str, season: int, week: int | None):
    """The player's projection for the polled week, or None. Runs in a savepoint on the
    caller's cursor, so a failed query rolls back only itself: the summary's transaction
    carries on and the field is null rather than the request failing."""
    if week is None:
        return None
    try:
        cursor.execute("SAVEPOINT live_projected_points;")
        cursor.execute(
            "SELECT projected_points FROM weekly_stats WHERE player_id = %s AND season = %s AND week = %s;",
            (player_id, season, week),
        )
        row = cursor.fetchone()
        cursor.execute("RELEASE SAVEPOINT live_projected_points;")
        return row[0] if row else None
    except Exception as e:
        logger.warning(f"Could not read the live projection for {player_id}: {e}")
        _rollback_to_savepoint(cursor)
        return None


def _rollback_to_savepoint(cursor) -> None:
    try:
        cursor.execute("ROLLBACK TO SAVEPOINT live_projected_points;")
    except Exception as e:
        logger.warning(f"Could not roll back the live projection savepoint: {e}")


# ---------------------------------------------------------------- matchup feed


def _indexed_live_players(r) -> dict:
    """player id -> `live:player` value for every matchup player the poller wrote, found
    through its `live:player_ids` index rather than a keyspace scan."""
    raw = r.get(PLAYER_IDS_KEY)
    player_ids = json.loads(raw) if raw else []
    if not player_ids:
        return {}
    values = _loads(r.mget([PLAYER_KEY.format(player_id=p) for p in player_ids]))
    return {player_id: value for player_id, value in zip(player_ids, values) if value}


def _player_row(row) -> tuple:
    """`(player_id, {name, position, team, espn_id})` from a players row."""
    return row[0], {"name": row[1], "position": row[2], "team": row[3], "espn_id": row[4]}


def _player_rows(player_ids: list) -> dict:
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT player_id, name, position, team, espn_id FROM players WHERE player_id = ANY(%s);",
            (player_ids,),
        )
        return dict(_player_row(row) for row in cursor.fetchall())


def _side(player: dict) -> str:
    return "mine" if str(player.get("fantasy_team_id")) == str(os.getenv("ESPN_TEAM_ID")) else "theirs"


def _who(player_id: str, live_player: dict, rows: dict) -> dict:
    row = rows.get(player_id, {})
    return {
        "player_id": player_id,
        "espn_id": row.get("espn_id"),
        "player_name": row.get("name"),
        "position": row.get("position"),
        "team": row.get("team") or live_player.get("team"),
        "side": _side(live_player),
    }


def _play_item(event_id: str, label: str | None, play: dict, player_id: str, who: dict) -> dict:
    """One feed item: a play as one of its matchup players saw it."""
    return {
        "type": "play",
        "cursor": f"play:{event_id}:{play['play_id']}:{player_id}",
        "at": play.get("recorded_at"),
        **who,
        "play_id": play["play_id"],
        "event_id": event_id,
        "game_label": label,
        "period": play.get("period"),
        "clock": play.get("clock"),
        "text": play.get("text"),
        "scoring": bool(play.get("scoring")),
        "fantasy_delta": (play.get("fantasy_delta") or {}).get(player_id),
    }


def _play_items(r, event_ids: list, players: dict, rows: dict) -> list:
    games = _games(r, event_ids)
    items = []
    for event_id in event_ids:
        label = _game_label(games.get(event_id))
        for play in _loads(r.lrange(PLAYS_KEY.format(event_id=event_id), 0, -1)):
            items.extend(
                _play_item(event_id, label, play, player_id, _who(player_id, players[player_id], rows))
                for player_id in play.get("player_ids") or []
                if player_id in players
            )
    return items


def _news_item(story: dict, who: dict) -> dict:
    return {
        "type": "news",
        "cursor": f"{NEWS_CURSOR_PREFIX}{story['id']}",
        "at": story["published_at"].replace(microsecond=0).isoformat() + "Z",
        **who,
        "news_id": story["id"],
        "source": story["source"],
        "headline": story["headline"],
        "url": story.get("url"),
    }


def _news_items(players: dict, rows: dict, now: datetime, limit: int, until: datetime | None) -> list:
    """News on matchup players from the last two hours, at or before `until` (the page's
    cursor time) when paging. Two more than `limit` are fetched: one for the cursor's own
    story and one so the page can tell more follow. Extra to the feed, so a failure leaves it
    out rather than failing the feed."""
    since = (now - NEWS_WINDOW).astimezone(timezone.utc).replace(tzinfo=None)
    until = until.astimezone(timezone.utc).replace(tzinfo=None) if until else None
    try:
        stories = news_service.get_news(list(players), since, limit + 2, until=until)
    except Exception as e:
        logger.warning(f"Could not load news for the matchup feed: {e}")
        return []
    return [
        _news_item(story, _who(story["player_id"], players[story["player_id"]], rows))
        for story in stories
        if story["player_id"] in players
    ]


def _moment(at: str | None) -> datetime:
    """A feed item's `at` as aware UTC, for ordering; EARLIEST when missing or unparsable."""
    if not at:
        return EARLIEST
    try:
        parsed = datetime.fromisoformat(at.replace("Z", "+00:00"))
    except ValueError:
        return EARLIEST
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _news_cursor_time(cursor: str) -> datetime | None:
    """When a `news:{id}` cursor's story was published, even after it left the news window;
    None when it can't be told."""
    try:
        published = news_service.story_published_at(int(cursor[len(NEWS_CURSOR_PREFIX):]))
    except Exception as e:
        logger.warning(f"Could not resolve the feed cursor {cursor}: {e}")
        return None
    return published.replace(tzinfo=timezone.utc) if published else None


def _cursor_time(before: str | None, plays: list) -> datetime | None:
    """The time a `before` cursor stands for: its play's `at`, or its story's publish time.
    None without a cursor, or for a play cursor that aged out of the poller's cap."""
    if before is None:
        return None
    play = next((item for item in plays if item["cursor"] == before), None)
    if play is not None:
        return _moment(play["at"])
    return _news_cursor_time(before) if before.startswith(NEWS_CURSOR_PREFIX) else None


def _page_start(items: list, before: str | None, before_at: datetime | None) -> int:
    """Index of the first item after the cursor. A cursor that isn't in the list (a story that
    left the news window) starts at the first older item; one with no known time ends the
    feed."""
    if before is None:
        return 0
    cursors = [item["cursor"] for item in items]
    if before in cursors:
        return cursors.index(before) + 1
    if before_at is None:
        return len(items)
    return next((i for i, item in enumerate(items) if _moment(item["at"]) < before_at), len(items))


def _page(items: list, limit: int, before: str | None, before_at: datetime | None) -> dict:
    start = _page_start(items, before, before_at)
    page = items[start:start + limit]
    more = start + limit < len(items)
    return {"items": page, "next_before": page[-1]["cursor"] if page and more else None}


def load_feed(season: int, week: int, limit: int, before: str | None = None, now: datetime | None = None) -> dict:
    """Plays (and recent news) for the matchup's players, newest first, one item per player
    involved. Empty when the poller isn't running this season and week. An unknown play
    cursor (its play aged out of the poller's cap) returns an empty page; a news cursor whose
    story left the window resumes from that story's time."""
    empty = {"items": [], "next_before": None}
    r = _redis()
    if r is None:
        return empty
    now = now or datetime.now(timezone.utc)
    try:
        health = _live_health(r, season, week)
        if health is None:
            return empty
        _touch_last_viewed(r)
        players = _indexed_live_players(r)
        rows = _player_rows(list(players))
        plays = _play_items(r, health.get("tracked_events") or [], players, rows)
    except Exception as e:
        logger.warning(f"Could not build the matchup feed: {e}")
        return empty
    before_at = _cursor_time(before, plays)
    items = plays + _news_items(players, rows, now, limit, before_at)
    # Stable, so plays from one tick keep the poller's newest-first order.
    items.sort(key=lambda item: _moment(item["at"]), reverse=True)
    return _page(items, limit, before, before_at)


# ---------------------------------------------------------------- game detail


def _game_header(game: dict) -> dict:
    return {**game, "game_label": _game_label(game)}


def _projections(player_ids: list, season: int, week: int) -> dict:
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT player_id, projected_points FROM weekly_stats WHERE player_id = ANY(%s) AND season = %s AND week = %s;",
            (player_ids, season, week),
        )
        return dict(cursor.fetchall())


def _is_starter(live_player: dict) -> bool:
    slot_id = live_player.get("lineup_slot_id")
    return slot_id is not None and slot_id not in NON_SCORING_SLOT_IDS


def _game_player(player_id, live_player, rows, projections, rules) -> dict:
    slot_id = live_player.get("lineup_slot_id")
    return {
        **_who(player_id, live_player, rows),
        "slot": POSITION_MAP.get(slot_id) if slot_id is not None else None,
        "starter": _is_starter(live_player),
        "live_points": live_player.get("live_points"),
        "projected_points": projections.get(player_id),
        "stat_line": live_player.get("stat_line"),
        "breakdown": _breakdown(live_player, rules),
    }


def _player_order(player: dict):
    """Ours first, starters before bench, most points first."""
    points = player["live_points"]
    return (player["side"] != "mine", not player["starter"], points is None, -(points or 0))


def _side_points(players: list, side: str, status: str | None):
    """What one side's starters scored in this game; bench points never count. Null before
    kickoff, when nothing is known yet; a starter with no points yet counts 0."""
    if status == NOT_STARTED:
        return None
    return round(sum(p["live_points"] or 0 for p in players if p["side"] == side and p["starter"]), 2)


def _game_snapshot(r, event_id: str, season: int, week: int):
    """`(live:game value, {player id: live:player value in that game})`, while the poller runs
    this season and week."""
    if _live_health(r, season, week) is None:
        raise LiveGameNotFoundError(f"No live snapshot for {season} week {week}.")
    raw_game = r.get(GAME_KEY.format(event_id=event_id))
    if not raw_game:
        raise LiveGameNotFoundError(f"No live snapshot of game {event_id}.")
    in_game = {pid: p for pid, p in _indexed_live_players(r).items() if p.get("event_id") == event_id}
    return json.loads(raw_game), in_game


def _read_game_snapshot(r, event_id: str, season: int, week: int):
    """`_game_snapshot`, with a Redis failure raised as LiveSnapshotUnavailableError."""
    if r is None:
        raise LiveSnapshotUnavailableError("The live snapshot can't be read.")
    try:
        return _game_snapshot(r, event_id, season, week)
    except LiveGameNotFoundError:
        raise
    except Exception as e:
        raise LiveSnapshotUnavailableError(f"The live snapshot can't be read: {e}") from e


def load_game_detail(event_id: str, season: int, week: int) -> dict:
    """One NFL game in this week's matchup: its header, each side's starters' points from it,
    every matchup player in it with their breakdown, and its plays (the feed's item shape,
    newest first)."""
    r = _redis()
    game, in_game = _read_game_snapshot(r, event_id, season, week)
    _touch_last_viewed(r)
    ids = sorted(in_game)
    rows = _player_rows(ids) if ids else {}
    projections = _projections(ids, season, week) if ids else {}
    rules = _load_rule_points() if ids else None
    players = sorted(
        (_game_player(pid, in_game[pid], rows, projections, rules) for pid in ids),
        key=_player_order,
    )
    plays = _play_items(r, [event_id], in_game, rows)
    plays.sort(key=lambda item: _moment(item["at"]), reverse=True)
    return {
        "season": season,
        "week": week,
        "game": _game_header(game),
        "mine_points": _side_points(players, "mine", game.get("status")),
        "theirs_points": _side_points(players, "theirs", game.get("status")),
        "players": players,
        "plays": plays,
    }
