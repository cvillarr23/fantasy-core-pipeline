"""
Reads the live snapshot `src.ingestion.live_games` keeps in Redis: the `live` block on matchup
and summary players, `/v1/live/players` and the matchup feed. Nothing here calls ESPN.

Every reader fails soft: a Redis error reads as "no snapshot" (null blocks, an empty feed), so a
Redis outage never breaks the page around it. `touch_last_viewed` is what speeds the poller up:
its value is epoch seconds, kept 10 minutes, and under 5 minutes old means 30s ticks.

Keys are keyed by our `players.player_id`; matchup lines carry ESPN ids, so `overlay_matchup`
maps them through `players.espn_id` (the two id spaces overlap numerically, never substitute).
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from src.database import get_db_cursor, get_redis_client
from src.ingestion.live_games import GAME_KEY, HEALTH_KEY, LAST_VIEWED_KEY, PLAYER_KEY, PLAYS_KEY
from src.services import news_service

logger = logging.getLogger("LiveService")

LAST_VIEWED_TTL_SECONDS = 10 * 60
NEWS_WINDOW = timedelta(hours=2)
PLAYER_KEY_PATTERN = PLAYER_KEY.format(player_id="*")


# ---------------------------------------------------------------- redis access


def touch_last_viewed() -> None:
    """Marks the live pages as in use. Never raises."""
    try:
        get_redis_client().set(LAST_VIEWED_KEY, str(int(time.time())), ex=LAST_VIEWED_TTL_SECONDS)
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


def _read_health(r) -> dict | None:
    raw = r.get(HEALTH_KEY)
    return json.loads(raw) if raw else None


def _live_week_health(r, week: int) -> dict | None:
    """The poller's health if it is polling `week`, else None: a snapshot for another week is
    not this week's."""
    health = _read_health(r)
    return health if health and health.get("week") == week else None


def _snapshots(r, player_ids: list) -> dict:
    """player id -> `(live:player value, its live:game value or None)`; missing players are
    left out."""
    if not player_ids:
        return {}
    players = dict(zip(player_ids, _loads(r.mget([PLAYER_KEY.format(player_id=p) for p in player_ids]))))
    event_ids = sorted({p["event_id"] for p in players.values() if p and p.get("event_id")})
    games = dict(zip(event_ids, _loads(r.mget([GAME_KEY.format(event_id=e) for e in event_ids])))) if event_ids else {}
    return {
        player_id: (player, games.get(player.get("event_id")))
        for player_id, player in players.items()
        if player
    }


# ---------------------------------------------------------------- the live block


def _live_block(player: dict, game: dict | None) -> dict:
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


def load_live_blocks(player_ids: list) -> dict:
    """player id -> live block, or None when the player has no snapshot."""
    r = _redis()
    blocks = {player_id: None for player_id in player_ids}
    if r is None or not player_ids:
        return blocks
    try:
        for player_id, (player, game) in _snapshots(r, player_ids).items():
            blocks[player_id] = _live_block(player, game)
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


def _blocks_by_espn_id(matchup: dict) -> dict:
    espn_ids = sorted({line["espn_id"] for line in _matchup_lines(matchup)})
    player_by_espn = _player_ids_by_espn_id(espn_ids)
    blocks = load_live_blocks(sorted(set(player_by_espn.values())))
    return {espn_id: blocks.get(player_id) for espn_id, player_id in player_by_espn.items()}


def overlay_matchup(matchup: dict) -> dict:
    """The matchup with a `live` block on every player the poller has a snapshot for. Applied
    after the matchup cache, since the snapshot moves faster. The input is not mutated, and
    any failure returns it as it came."""
    r = _redis()
    if r is None:
        return matchup
    try:
        if _live_week_health(r, matchup["week"]) is None:
            return matchup
        blocks = _blocks_by_espn_id(matchup)
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
    """The `live` field of `GET /v1/players/{id}/summary`, or None without a snapshot."""
    r = _redis()
    if r is None:
        return None
    try:
        health = _read_health(r)
        player, game = _snapshots(r, [player_id]).get(player_id, (None, None))
    except Exception as e:
        logger.warning(f"Could not read the live snapshot for {player_id}: {e}")
        return None
    if player is None:
        return None
    return {
        "live_points": player.get("live_points"),
        "projected_points": _projected_points(cursor, player_id, (health or {}).get("week")),
        "game_label": _game_label(game),
        "game_status": (game or {}).get("status"),
        "event_id": player.get("event_id"),
    }


def _projected_points(cursor, player_id: str, week: int | None):
    if week is None:
        return None
    cursor.execute(
        "SELECT projected_points FROM weekly_stats WHERE player_id = %s AND week = %s ORDER BY season DESC LIMIT 1;",
        (player_id, week),
    )
    row = cursor.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------- matchup feed


def _all_live_players(r) -> dict:
    """player id -> `live:player` value for every matchup player the poller wrote."""
    keys = sorted(r.scan_iter(match=PLAYER_KEY_PATTERN))
    prefix = PLAYER_KEY.format(player_id="")
    return {
        key[len(prefix):]: value
        for key, value in zip(keys, _loads(r.mget(keys)) if keys else [])
        if value
    }


def _player_rows(player_ids: list) -> dict:
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT player_id, name, position, team, espn_id FROM players WHERE player_id = ANY(%s);",
            (player_ids,),
        )
        return {row[0]: {"name": row[1], "position": row[2], "team": row[3], "espn_id": row[4]} for row in cursor.fetchall()}


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


def _play_items(r, event_ids: list, games: dict, players: dict, rows: dict) -> list:
    items = []
    for event_id in event_ids:
        label = _game_label(games.get(event_id))
        for play in _loads(r.lrange(PLAYS_KEY.format(event_id=event_id), 0, -1)):
            deltas = play.get("fantasy_delta") or {}
            for player_id in play.get("player_ids") or []:
                if player_id not in players:
                    continue
                items.append({
                    "type": "play",
                    "cursor": f"play:{event_id}:{play['play_id']}:{player_id}",
                    "at": play.get("recorded_at"),
                    **_who(player_id, players[player_id], rows),
                    "play_id": play["play_id"],
                    "event_id": event_id,
                    "game_label": label,
                    "period": play.get("period"),
                    "clock": play.get("clock"),
                    "text": play.get("text"),
                    "scoring": bool(play.get("scoring")),
                    "fantasy_delta": deltas.get(player_id),
                })
    return items


def _news_items(players: dict, rows: dict, now: datetime, limit: int) -> list:
    """News on matchup players from the last two hours. Extra to the feed, so a failure
    leaves it out rather than failing the feed."""
    try:
        stories = news_service.get_news(list(players), (now - NEWS_WINDOW).replace(tzinfo=None), limit)
    except Exception as e:
        logger.warning(f"Could not load news for the matchup feed: {e}")
        return []
    return [
        {
            "type": "news",
            "cursor": f"news:{story['id']}",
            "at": story["published_at"].replace(microsecond=0).isoformat() + "Z",
            **_who(story["player_id"], players[story["player_id"]], rows),
            "news_id": story["id"],
            "source": story["source"],
            "headline": story["headline"],
            "url": story.get("url"),
        }
        for story in stories
        if story["player_id"] in players
    ]


def _page(items: list, limit: int, before: str | None) -> dict:
    start = 0
    if before is not None:
        cursors = [item["cursor"] for item in items]
        start = cursors.index(before) + 1 if before in cursors else len(items)
    page = items[start:start + limit]
    more = start + limit < len(items)
    return {"items": page, "next_before": page[-1]["cursor"] if page and more else None}


def load_feed(week: int, limit: int, before: str | None = None, now: datetime | None = None) -> dict:
    """Plays (and recent news) for the matchup's players, newest first, one item per player
    involved. Empty when the poller isn't running this week. An unknown `before` cursor (its
    play aged out of the poller's cap) returns an empty page."""
    empty = {"items": [], "next_before": None}
    r = _redis()
    if r is None:
        return empty
    now = now or datetime.now(timezone.utc)
    try:
        health = _live_week_health(r, week)
        if health is None:
            return empty
        players = _all_live_players(r)
        event_ids = health.get("tracked_events") or []
        games = dict(zip(event_ids, _loads(r.mget([GAME_KEY.format(event_id=e) for e in event_ids])))) if event_ids else {}
        rows = _player_rows(list(players))
        items = _play_items(r, event_ids, games, players, rows) + _news_items(players, rows, now, limit)
    except Exception as e:
        logger.warning(f"Could not build the matchup feed: {e}")
        return empty
    # Stable, so plays from one tick keep the poller's newest-first order.
    items.sort(key=lambda item: item["at"] or "", reverse=True)
    return _page(items, limit, before)
