"""
Polls a small, fixed set of trusted free news/rumor sources (currently just RotoWire's NFL
news RSS feed), fuzzy-matches each item to a `players` row, and persists the raw text +
source + timestamp + matched player id to `raw_signals`.

Deliberately deterministic, fetch-normalize-store only - no sentiment/confidence
interpretation happens here (that's a downstream LLM concern, WORKER-13 in
fantasy-browser-worker). Has no "must run after X" ordering dependency on the rest of this
pipeline, but does depend on `players` already being populated by `sync_players` for
matching to be useful - an empty player table just means every item goes unmatched, not a
crash.

r/fantasyfootball was originally planned as a second source, but its unauthenticated
`.json` endpoint returns HTTP 403 (Reddit disabled anonymous listing access) - adding it
properly needs an OAuth client against oauth.reddit.com plus a quality/relevance filter to
avoid flooding `raw_signals` with low-effort posts, tracked as a follow-up (CORE-14, #38).
"""
import hashlib
import logging
import os
import sys
from datetime import datetime, timezone

import feedparser
import redis
import requests
from rapidfuzz import fuzz, process

from src.database import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RumorSignalSync")

REDIS_HOST = os.getenv("REDIS_HOST", "fantasy-cache.default.svc.cluster.local")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

ROTOWIRE_NEWS_RSS_URL = "https://www.rotowire.com/rss/news.php?sport=NFL"

SYNC_LOCK_KEY = "rumors:sync:last_run"
SYNC_LOCK_TTL_SECONDS = 15 * 60
SYNC_IN_PROGRESS_TTL_SECONDS = 5 * 60
FETCH_RETRY_ATTEMPTS = 3
FETCH_CONNECT_TIMEOUT_SECONDS = 5
FETCH_READ_TIMEOUT_SECONDS = 30

MATCH_CONFIDENCE_THRESHOLD = 90


def fetch_rotowire_rss() -> list:
    """Fetches and parses the RotoWire NFL news RSS feed, retrying on failure."""
    last_error = None
    for attempt in range(1, FETCH_RETRY_ATTEMPTS + 1):
        try:
            logger.info(f"Fetching RotoWire news RSS (attempt {attempt}/{FETCH_RETRY_ATTEMPTS})...")
            response = requests.get(
                ROTOWIRE_NEWS_RSS_URL,
                timeout=(FETCH_CONNECT_TIMEOUT_SECONDS, FETCH_READ_TIMEOUT_SECONDS),
            )
            response.raise_for_status()
            parsed = feedparser.parse(response.content)
            return list(parsed.entries)
        except requests.RequestException as e:
            last_error = e
            logger.warning(f"RotoWire RSS fetch attempt {attempt} failed: {e}")
    logger.error(f"Exhausted all {FETCH_RETRY_ATTEMPTS} attempts fetching RotoWire RSS; skipping this source.")
    return []


def normalize_rotowire_item(entry) -> dict:
    """
    Maps a feedparser RSS entry to the common intermediate shape. Reads `id` first, not
    `guid` - feedparser 6.x's FeedParserDict aliases "guid" to "id" internally for plain
    attribute/`.get()` access, but reading `id` directly is the documented, literal key
    (visible in `entry.keys()`) rather than relying on that alias, so this doesn't silently
    break if a future feedparser version drops the alias. `guid` is kept as a fallback for
    any parser/feed variant that doesn't expose `id`.
    """
    published_at = None
    published_parsed = entry.get("published_parsed")
    if published_parsed:
        published_at = datetime(*published_parsed[:6], tzinfo=timezone.utc)
    return {
        "source_item_id": entry.get("id") or entry.get("guid") or entry.get("link"),
        "raw_text": entry.get("title", "") + (f" - {entry.get('summary', '')}" if entry.get("summary") else ""),
        "url": entry.get("link"),
        "published_at": published_at,
    }


def _sources():
    """
    Looks up the fetch/normalize functions by module-global name at call time (rather
    than binding them into a module-level constant at import time) so tests can
    `patch("src.ingestion.sync_rumors.fetch_rotowire_rss", ...)` and have sync_rumors()
    actually see the patched version - a tuple of direct function references captured at
    import time would still hold the original, unpatched functions.

    Only one source for now - see the module docstring for why r/fantasyfootball was
    dropped (CORE-14, #38 tracks adding it back via OAuth).
    """
    return [
        ("rotowire_rss", fetch_rotowire_rss, normalize_rotowire_item),
    ]


def acquire_sync_lock(r_client: "redis.Redis", force: bool) -> bool:
    if force:
        r_client.delete(SYNC_LOCK_KEY)
    return bool(r_client.set(SYNC_LOCK_KEY, "in_progress", nx=True, ex=SYNC_IN_PROGRESS_TTL_SECONDS))


def mark_synced(r_client: "redis.Redis") -> None:
    r_client.set(SYNC_LOCK_KEY, datetime.now(timezone.utc).isoformat(), ex=SYNC_LOCK_TTL_SECONDS)


def load_player_lookup(cursor) -> list:
    """Loads (player_id, name) pairs to fuzzy-match rumor text against."""
    cursor.execute("SELECT player_id, name FROM players;")
    return [(row[0], row[1]) for row in cursor.fetchall()]


def match_player(raw_text: str, player_lookup: list):
    """
    Pure, deterministic fuzzy match of `raw_text` against `player_lookup` - no
    interpretation, just a mechanical string-similarity score. Returns
    (player_id, confidence) on a confident match, or (None, None) otherwise; no
    below-threshold "best guess" is ever returned, since a false match would be worse than
    no match at all for a table meant to be a faithful ingestion record.

    Ties above the threshold prefer an exact case-insensitive full-name match if one
    exists among the tied candidates; otherwise rapidfuzz's own stable (input-order)
    tiebreak applies - an arbitrary but deterministic choice, not an editorial judgment.
    """
    if not raw_text or not player_lookup:
        return None, None

    names = [name for _, name in player_lookup]
    matches = process.extract(
        raw_text, names, scorer=fuzz.token_set_ratio, score_cutoff=MATCH_CONFIDENCE_THRESHOLD
    )
    if not matches:
        return None, None

    top_score = matches[0][1]
    tied = [m for m in matches if m[1] == top_score]
    if len(tied) > 1:
        for name, score, idx in tied:
            if name.lower() in raw_text.lower():
                player_id = player_lookup[idx][0]
                return player_id, score / 100.0

    name, score, idx = tied[0]
    player_id = player_lookup[idx][0]
    return player_id, score / 100.0


def persist_signals(conn, rows: list) -> None:
    if not rows:
        return
    insert_query = """
        INSERT INTO raw_signals
            (source, source_item_id, content_hash, raw_text, url, published_at, player_id, match_confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, source_item_id) DO NOTHING;
    """
    with conn.cursor() as cursor:
        cursor.executemany(insert_query, [
            (
                row["source"], row["source_item_id"], row["content_hash"], row["raw_text"],
                row["url"], row["published_at"], row["player_id"], row["match_confidence"],
            )
            for row in rows
        ])
    logger.info(f"Persisted {len(rows)} raw signal row(s) to PostgreSQL (dupes silently skipped).")


def sync_rumors(force: bool = False) -> None:
    try:
        r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r_client.ping()
    except Exception as e:
        logger.error(f"Redis cache layer unavailable, aborting sync: {e}")
        return

    if not acquire_sync_lock(r_client, force):
        logger.info("Rumor sync already ran recently (or is in progress); skipping fetch.")
        return

    sources = _sources()
    items = []
    for source_name, fetch_fn, normalize_fn in sources:
        try:
            raw_items = fetch_fn()
        except Exception as e:
            logger.error(f"Source '{source_name}' fetch raised unexpectedly: {e}")
            continue
        for raw_item in raw_items:
            normalized = normalize_fn(raw_item)
            if not normalized.get("source_item_id") or not normalized.get("raw_text"):
                continue
            normalized["source"] = source_name
            items.append(normalized)

    logger.info(f"Fetched {len(items)} total item(s) across {len(sources)} source(s).")

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            player_lookup = load_player_lookup(cursor)

        rows = []
        for item in items:
            player_id, confidence = match_player(item["raw_text"], player_lookup)
            content_hash = hashlib.sha256(f"{item['source']}:{item['raw_text']}".encode("utf-8")).hexdigest()
            rows.append({
                **item,
                "content_hash": content_hash,
                "player_id": player_id,
                "match_confidence": confidence,
            })

        persist_signals(conn, rows)

    mark_synced(r_client)


if __name__ == "__main__":
    sync_rumors(force="--force" in sys.argv)
