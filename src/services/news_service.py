"""
Merged, de-duplicated player news feed over raw_signals.

Backs GET /v1/news and GET /v1/news/recent. ESPN and Bluesky rows are display-only
(see sync_rumors); this service only reads. ``player_id`` is the internal
``players.player_id``, like /v1/players/{id}/summary.

Dedupe: for one player, items whose normalized headlines match (rapidfuzz ratio >=
``DEDUPE_RATIO``) within ``DEDUPE_WINDOW`` of the story's first item are one story.
The earliest item is kept and the other sources are listed in ``also_reported_by``.
ESPN writes one row per tagged athlete, so one article is never two rows for the same
player; headline matching covers it.

Bluesky matches the player by fuzzy name, with false positives, so rows under
``BLUESKY_MIN_CONFIDENCE`` are hidden.
"""
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from rapidfuzz import fuzz

from src.database import get_db_cursor

SOURCE_BY_DB_VALUE = {
    "rotowire_rss": "rotowire",
    "reddit_fantasyfootball": "reddit",
    "espn_news": "espn_news",
    "bluesky": "bluesky",
}
DEDUPE_RATIO = 90
DEDUPE_WINDOW = timedelta(hours=6)
HEADLINE_FALLBACK_CHARS = 200
BLUESKY_MIN_CONFIDENCE = 0.9
# Rows fetched per requested item, so dedupe still leaves ``limit`` stories.
FETCH_MULTIPLIER = 5
RECENT_WINDOW = timedelta(hours=24)

_NON_WORD = re.compile(r"[^a-z0-9\s]")


def get_news(
    player_ids: List[str], since: Optional[datetime], limit: int, until: Optional[datetime] = None
) -> List[dict]:
    """Newest-first news for the given players, one entry per story. `until` (inclusive) lets a
    pager ask for the stories at or before its cursor rather than always the newest."""
    rows = _fetch_rows(player_ids, since, limit * FETCH_MULTIPLIER, until)
    stories = []
    for player_id in sorted({row["player_id"] for row in rows}):
        stories.extend(_merge_stories([r for r in rows if r["player_id"] == player_id]))
    stories.sort(key=lambda item: item["published_at"], reverse=True)
    return stories[:limit]


def has_recent_news(player_ids: List[str], now: Optional[datetime] = None) -> Dict[str, bool]:
    """Whether each player has a news item in the last 24 hours (unknown ids are false)."""
    cutoff = (now or datetime.utcnow()) - RECENT_WINDOW
    with get_db_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT DISTINCT player_id
            FROM raw_signals
            WHERE player_id = ANY(%s) AND published_at >= %s AND {_VISIBLE_SQL};
            """,
            (player_ids, cutoff, *_visible_params()),
        )
        recent = {row[0] for row in cursor.fetchall()}
    return {player_id: player_id in recent for player_id in player_ids}


_VISIBLE_SQL = "source = ANY(%s) AND (source <> 'bluesky' OR match_confidence >= %s)"


def _visible_params() -> tuple:
    return list(SOURCE_BY_DB_VALUE), BLUESKY_MIN_CONFIDENCE


def story_published_at(news_id: int) -> Optional[datetime]:
    """When one raw_signals row (a story's id) was published; None for an unknown id."""
    with get_db_cursor() as cursor:
        cursor.execute("SELECT published_at FROM raw_signals WHERE id = %s;", (news_id,))
        row = cursor.fetchone()
    return row[0] if row else None


def _fetch_rows(
    player_ids: List[str], since: Optional[datetime], cap: int, until: Optional[datetime] = None
) -> List[dict]:
    with get_db_cursor() as cursor:
        cursor.execute(
            f"""
            SELECT id, player_id, source, author, headline, raw_text, url, published_at
            FROM raw_signals
            WHERE player_id = ANY(%s)
              AND published_at IS NOT NULL
              AND (%s::timestamp IS NULL OR published_at >= %s)
              AND (%s::timestamp IS NULL OR published_at <= %s)
              AND {_VISIBLE_SQL}
            ORDER BY published_at DESC, id DESC
            LIMIT %s;
            """,
            (player_ids, since, since, until, until, *_visible_params(), cap),
        )
        return [_row_to_item(row) for row in cursor.fetchall()]


def _row_to_item(row) -> dict:
    return {
        "id": row[0],
        "player_id": row[1],
        "source": SOURCE_BY_DB_VALUE[row[2]],
        "author": row[3],
        "headline": _headline(row[4], row[5]),
        "url": row[6],
        "published_at": row[7],
        "score": None,
        "also_reported_by": [],
    }


def _headline(headline: Optional[str], raw_text: str) -> str:
    return headline or raw_text[:HEADLINE_FALLBACK_CHARS]


def _normalize_headline(headline: str) -> str:
    return " ".join(_NON_WORD.sub("", headline.lower()).split())


def _same_story(story: dict, item: dict) -> bool:
    within_window = item["published_at"] - story["published_at"] <= DEDUPE_WINDOW
    return within_window and (
        fuzz.ratio(_normalize_headline(story["headline"]), _normalize_headline(item["headline"]))
        >= DEDUPE_RATIO
    )


def _merge_stories(items: List[dict]) -> List[dict]:
    """Collapse one player's items into stories, keeping each story's earliest item."""
    stories: List[dict] = []
    for item in sorted(items, key=lambda i: (i["published_at"], i["id"])):
        story = next((s for s in stories if _same_story(s, item)), None)
        if story is None:
            stories.append(item)
        elif item["source"] != story["source"] and item["source"] not in story["also_reported_by"]:
            story["also_reported_by"].append(item["source"])
    return stories
