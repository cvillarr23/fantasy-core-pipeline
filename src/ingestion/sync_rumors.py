"""
Polls a small, fixed set of trusted free news/rumor sources (RotoWire's NFL news RSS feed
and r/fantasyfootball via Reddit's OAuth API), fuzzy-matches each item to a `players` row,
and persists the raw text + source + timestamp + matched player id to `raw_signals`.

Deliberately deterministic, fetch-normalize-store only - no sentiment/confidence
interpretation happens here (that's a downstream LLM concern, WORKER-13 in
fantasy-browser-worker). Has no "must run after X" ordering dependency on the rest of this
pipeline, but does depend on `players` already being populated by `sync_players` for
matching to be useful - an empty player table just means every item goes unmatched, not a
crash.

Reddit is noisier than RotoWire's editorial feed, so `fetch_reddit_fantasyfootball` applies
a deterministic score/recency quality filter before returning items - still no sentiment/LLM
scoring here, consistent with the fetch-normalize-store mandate above.
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

from src.database import get_db_connection, get_redis_client
from src.services import run_log_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RumorSignalSync")

ROTOWIRE_NEWS_RSS_URL = "https://www.rotowire.com/rss/news.php?sport=NFL"

REDDIT_OAUTH_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
REDDIT_LISTING_URL = "https://oauth.reddit.com/r/fantasyfootball/new"
REDDIT_WATERMARK_KEY = "rumors:reddit_fantasyfootball:watermark"
REDDIT_PAGE_LIMIT = 100
REDDIT_MAX_PAGES = 10
REDDIT_MIN_SCORE = int(os.getenv("REDDIT_MIN_SCORE", "20"))
REDDIT_MAX_POST_AGE_HOURS = int(os.getenv("REDDIT_MAX_POST_AGE_HOURS", "72"))

SYNC_LOCK_KEY = "rumors:sync:last_run"
SYNC_LOCK_TTL_SECONDS = 15 * 60
SYNC_IN_PROGRESS_TTL_SECONDS = 5 * 60
FETCH_RETRY_ATTEMPTS = 3
FETCH_CONNECT_TIMEOUT_SECONDS = 5
FETCH_READ_TIMEOUT_SECONDS = 30

MATCH_CONFIDENCE_THRESHOLD = 90


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} environment variable is required for Reddit ingestion but is not set.")
    return value


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


def get_reddit_access_token() -> str:
    """
    App-only (client_credentials) OAuth against oauth.reddit.com - no Reddit user account
    needed since this only reads a public subreddit's listings. Raises RuntimeError if
    REDDIT_CLIENT_ID/REDDIT_CLIENT_SECRET/REDDIT_USER_AGENT aren't set (OpenBao-injected in
    deployed environments, no defaults, no credentials committed here) - the caller treats
    that the same as any other fetch failure and soft-fails to an empty list.
    """
    client_id = _require_env("REDDIT_CLIENT_ID")
    client_secret = _require_env("REDDIT_CLIENT_SECRET")
    user_agent = _require_env("REDDIT_USER_AGENT")
    response = requests.post(
        REDDIT_OAUTH_TOKEN_URL,
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials"},
        headers={"User-Agent": user_agent},
        timeout=(FETCH_CONNECT_TIMEOUT_SECONDS, FETCH_READ_TIMEOUT_SECONDS),
    )
    response.raise_for_status()
    return response.json()["access_token"]


def _passes_reddit_quality_filter(post: dict) -> bool:
    """Deterministic numeric threshold only - no sentiment/relevance interpretation."""
    score = post.get("score") or 0
    if score < REDDIT_MIN_SCORE:
        return False
    created_utc = post.get("created_utc")
    if created_utc is None:
        return False
    age_hours = (datetime.now(timezone.utc) - datetime.fromtimestamp(created_utc, tz=timezone.utc)).total_seconds() / 3600
    return age_hours <= REDDIT_MAX_POST_AGE_HOURS


def fetch_reddit_fantasyfootball() -> list:
    """
    Fetches new r/fantasyfootball posts via OAuth, paginating with `limit=100` pages via
    the `after` cursor until either the stored watermark (the newest post fullname already
    ingested last run, kept in Redis) is reached, `REDDIT_MAX_PAGES` is hit, or Reddit runs
    out of pages - a single un-paginated 25-item page would silently miss posts once
    ingestion falls behind the sub's volume. Retries mirror `fetch_rotowire_rss`: log and
    return `[]` on exhaustion rather than raising, so one bad source doesn't crash the sync.
    The quality filter (`_passes_reddit_quality_filter`) is applied per-post before it's
    added to the returned list, upstream of `normalize_reddit_item`.
    """
    last_error = None
    for attempt in range(1, FETCH_RETRY_ATTEMPTS + 1):
        try:
            logger.info(f"Fetching r/fantasyfootball via Reddit OAuth (attempt {attempt}/{FETCH_RETRY_ATTEMPTS})...")
            access_token = get_reddit_access_token()
            user_agent = _require_env("REDDIT_USER_AGENT")
            r_client = get_redis_client()
            watermark = r_client.get(REDDIT_WATERMARK_KEY)

            posts = []
            newest_id_seen = None
            after = None
            for _ in range(REDDIT_MAX_PAGES):
                response = requests.get(
                    REDDIT_LISTING_URL,
                    headers={"Authorization": f"Bearer {access_token}", "User-Agent": user_agent},
                    params={"limit": REDDIT_PAGE_LIMIT, "after": after} if after else {"limit": REDDIT_PAGE_LIMIT},
                    timeout=(FETCH_CONNECT_TIMEOUT_SECONDS, FETCH_READ_TIMEOUT_SECONDS),
                )
                response.raise_for_status()
                payload = response.json()
                children = payload.get("data", {}).get("children", [])
                if not children:
                    break

                hit_watermark = False
                for child in children:
                    post = child.get("data", {})
                    fullname = post.get("name")
                    if newest_id_seen is None:
                        newest_id_seen = fullname
                    if watermark and fullname == watermark:
                        hit_watermark = True
                        break
                    if _passes_reddit_quality_filter(post):
                        posts.append(post)

                if hit_watermark:
                    break
                after = payload.get("data", {}).get("after")
                if not after:
                    break

            if newest_id_seen:
                r_client.set(REDDIT_WATERMARK_KEY, newest_id_seen)
            return posts
        except (requests.RequestException, RuntimeError, KeyError) as e:
            last_error = e
            logger.warning(f"Reddit fetch attempt {attempt} failed: {e}")
    logger.error(f"Exhausted all {FETCH_RETRY_ATTEMPTS} attempts fetching r/fantasyfootball; skipping this source.")
    return []


def normalize_reddit_item(post: dict) -> dict:
    """Maps a Reddit post's raw API dict to the common intermediate shape."""
    created_utc = post.get("created_utc")
    published_at = datetime.fromtimestamp(created_utc, tz=timezone.utc) if created_utc else None
    permalink = post.get("permalink")
    return {
        "source_item_id": post.get("name"),
        "raw_text": post.get("title", "") + (f" - {post.get('selftext')}" if post.get("selftext") else ""),
        "url": f"https://www.reddit.com{permalink}" if permalink else post.get("url"),
        "published_at": published_at,
    }


def _sources():
    """
    Looks up the fetch/normalize functions by module-global name at call time (rather
    than binding them into a module-level constant at import time) so tests can
    `patch("src.ingestion.sync_rumors.fetch_rotowire_rss", ...)` and have sync_rumors()
    actually see the patched version - a tuple of direct function references captured at
    import time would still hold the original, unpatched functions.
    """
    return [
        ("rotowire_rss", fetch_rotowire_rss, normalize_rotowire_item),
        ("reddit_fantasyfootball", fetch_reddit_fantasyfootball, normalize_reddit_item),
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


def persist_signals(conn, rows: list) -> list:
    """Inserts new rows and returns their ids; a dupe returns nothing, so the ids are only
    what this run actually ingested."""
    if not rows:
        return []
    insert_query = """
        INSERT INTO raw_signals
            (source, source_item_id, content_hash, raw_text, url, published_at, player_id, match_confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, source_item_id) DO NOTHING
        RETURNING id;
    """
    new_ids = []
    with conn.cursor() as cursor:
        for row in rows:
            cursor.execute(insert_query, (
                row["source"], row["source_item_id"], row["content_hash"], row["raw_text"],
                row["url"], row["published_at"], row["player_id"], row["match_confidence"],
            ))
            inserted = cursor.fetchone()
            if inserted:
                new_ids.append(inserted[0])
    logger.info(f"Persisted {len(new_ids)} new raw signal row(s) of {len(rows)} fetched (dupes skipped).")
    return new_ids


def sync_rumors(force: bool = False) -> None:
    try:
        r_client = get_redis_client()
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

        new_ids = persist_signals(conn, rows)

    run_log_service.link("raw_signal", new_ids, "output")
    mark_synced(r_client)


if __name__ == "__main__":
    with run_log_service.run_scope("sync_rumors"):
        sync_rumors(force="--force" in sys.argv)
