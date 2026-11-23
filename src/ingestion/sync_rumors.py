"""
Polls a small, fixed set of trusted free news/rumor sources, matches each item to a
`players` row, and persists the raw text + source + timestamp + matched player id to
`raw_signals`:

- `rotowire_rss`: RotoWire's NFL news RSS feed (fuzzy-matched).
- `reddit_fantasyfootball`: r/fantasyfootball via Reddit's OAuth API (fuzzy-matched).
- `espn_news`: ESPN's NFL news feed, plus each watched player's own ESPN news when the
  general feed has nothing on them. Matched exactly on the article's athlete tags through
  `players.espn_id`, one row per tagged athlete.
- `bluesky`: the original posts of the handles in `NEWS_BLUESKY_HANDLES`, read from the
  public AppView without auth. Matched only when the player's full name appears as a
  contiguous phrase in the post, since `token_set_ratio` pairs tokens of different people.

`espn_news` and `bluesky` are display-only, for the player news feed. The worker's rumor
digest reads only an allowlist of sources (`AUTOMATION_SOURCES` in fantasy-browser-worker's
`src/db/rumor_signals.js`), so they never drive a waiver or lineup move. Adding a source to
that allowlist is a separate decision.

Each source fails on its own: a fetcher that can't reach its source logs and returns `[]`.

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
import re
import sys
import unicodedata
from datetime import datetime, timezone

import feedparser
import redis
import requests
from rapidfuzz import fuzz, process

import nflreadpy as nfl

from src.database import get_db_connection, get_db_cursor, get_redis_client
from src.nfl_schedule import WeekResolutionError, resolve_upcoming_week
from src.services import run_log_service

logger = logging.getLogger("RumorSignalSync")

ROTOWIRE_NEWS_RSS_URL = "https://www.rotowire.com/rss/news.php?sport=NFL"

REDDIT_OAUTH_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
REDDIT_LISTING_URL = "https://oauth.reddit.com/r/fantasyfootball/new"
REDDIT_WATERMARK_KEY = "rumors:reddit_fantasyfootball:watermark"
REDDIT_PAGE_LIMIT = 100
REDDIT_MAX_PAGES = 10
REDDIT_MIN_SCORE = int(os.getenv("REDDIT_MIN_SCORE", "20"))
REDDIT_MAX_POST_AGE_HOURS = int(os.getenv("REDDIT_MAX_POST_AGE_HOURS", "72"))

ESPN_NEWS_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/news"
ESPN_NEWS_LIMIT = 50
# The athlete page's own news list. The general feed's `athletes=` parameter is ignored, and
# the fantasy news/players endpoint returns RotoWire blurbs that duplicate rotowire_rss.
ESPN_ATHLETE_OVERVIEW_URL = (
    "https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{espn_id}/overview"
)

# An article tagging more athletes than this (a recap, a weekly roundup) still gets a row per
# athlete, but doesn't count as news about them, so it doesn't save their per-athlete call.
ESPN_COVERAGE_MAX_ATHLETES = 3

BLUESKY_AUTHOR_FEED_URL = "https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed"
BLUESKY_FEED_LIMIT = 50

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


def _get_json(url: str, params: dict | None = None):
    """One GET, parsed as JSON; raises on a network error or a non-2xx status."""
    response = requests.get(
        url, params=params, timeout=(FETCH_CONNECT_TIMEOUT_SECONDS, FETCH_READ_TIMEOUT_SECONDS)
    )
    response.raise_for_status()
    return response.json()


def _get_json_with_retries(label: str, url: str, params: dict | None = None):
    """`_get_json` with the same retry loop as the RSS fetch. Returns None once every
    attempt has failed, so the caller can skip its source."""
    for attempt in range(1, FETCH_RETRY_ATTEMPTS + 1):
        try:
            logger.info(f"Fetching {label} (attempt {attempt}/{FETCH_RETRY_ATTEMPTS})...")
            return _get_json(url, params)
        except (requests.RequestException, ValueError) as e:
            logger.warning(f"{label} fetch attempt {attempt} failed: {e}")
    logger.error(f"Exhausted all {FETCH_RETRY_ATTEMPTS} attempts fetching {label}; skipping it.")
    return None


def _parse_timestamp(value):
    """An ISO-8601 timestamp (ESPN's `...Z` or `...+00:00`, Bluesky's `...123Z`) as aware UTC."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _current_season() -> int:
    return int(os.getenv("CURRENT_YEAR") or nfl.get_current_season())


def _opponent_team_id(cursor, team_id: str):
    """This week's fantasy opponent, from the schedule sync_actual_scores stores. None (with
    a warning) when the league, the week or the matchup row is missing, which narrows the
    per-athlete calls to our own roster."""
    league_id = os.getenv("ESPN_LEAGUE_ID")
    if not league_id:
        logger.warning("ESPN_LEAGUE_ID is not set; fetching ESPN athlete news for our roster only.")
        return None
    season = _current_season()
    try:
        week = resolve_upcoming_week(season, attempts=1)
    except WeekResolutionError as e:
        logger.warning(f"Could not resolve this week ({e}); fetching ESPN athlete news for our roster only.")
        return None
    if week is None:
        return None
    cursor.execute(
        """SELECT opponent_team_id FROM fantasy_matchups
            WHERE league_id = %s AND season = %s AND team_id = %s AND %s = ANY(scoring_weeks);""",
        (league_id, season, team_id, week),
    )
    row = cursor.fetchone()
    if not row or not row[0]:
        logger.warning(f"No {season} week {week} opponent stored for team {team_id}; fetching ESPN athlete news for our roster only.")
        return None
    return row[0]


def _watched_espn_ids() -> list:
    """ESPN ids of the players on our roster and this week's opponent's roster: the players
    whose own ESPN news is fetched when the general feed misses them."""
    team_id = os.getenv("ESPN_TEAM_ID")
    if not team_id:
        logger.warning("ESPN_TEAM_ID is not set; skipping per-athlete ESPN news.")
        return []
    with get_db_cursor() as cursor:
        team_ids = [team_id]
        opponent = _opponent_team_id(cursor, team_id)
        if opponent:
            team_ids.append(opponent)
        cursor.execute(
            """SELECT DISTINCT p.espn_id FROM league_rosters r
                 JOIN players p ON p.player_id = r.player_id
                WHERE r.team_id = ANY(%s) AND p.espn_id IS NOT NULL
                ORDER BY p.espn_id;""",
            (team_ids,),
        )
        return [row[0] for row in cursor.fetchall()]


def _tagged_athlete_ids(article: dict) -> list:
    return [
        str(category["athleteId"])
        for category in article.get("categories") or []
        if category.get("type") == "athlete" and category.get("athleteId")
    ]


def _espn_items(articles: list) -> list:
    """One item per (article, tagged athlete), so each player's feed gets the article. An
    article tagged with no athlete is kept once, unmatched, like any unmatched rumor."""
    items = []
    for article in articles:
        athlete_ids = _tagged_athlete_ids(article) or [None]
        items.extend({"article": article, "espn_athlete_id": athlete_id} for athlete_id in athlete_ids)
    return items


def _covered_athlete_ids(articles: list) -> set:
    """Athletes the general feed has news about: tagged by an article that isn't a roundup."""
    covered = set()
    for article in articles:
        athlete_ids = _tagged_athlete_ids(article)
        if len(athlete_ids) <= ESPN_COVERAGE_MAX_ATHLETES:
            covered.update(athlete_ids)
    return covered


def _athlete_news(espn_id: str) -> list:
    """One watched player's own ESPN articles, attributed to that player only. A single
    attempt: an HTTP error or bad body skips this player. A timeout or connection error
    raises, so the caller stops calling a slow ESPN instead of waiting out the job deadline."""
    try:
        overview = _get_json(ESPN_ATHLETE_OVERVIEW_URL.format(espn_id=espn_id))
    except (requests.HTTPError, ValueError) as e:
        logger.warning(f"ESPN athlete news fetch for {espn_id} failed: {e}")
        return []
    return [{"article": article, "espn_athlete_id": espn_id} for article in overview.get("news") or []]


def _missed_athlete_news(espn_ids: list) -> list:
    """`_athlete_news` for each id, stopping at the first timeout or connection error."""
    items = []
    for espn_id in espn_ids:
        try:
            items.extend(_athlete_news(espn_id))
        except requests.RequestException as e:
            logger.warning(f"ESPN athlete news unreachable at {espn_id} ({e}); skipping the rest this run.")
            break
    return items


def _watched_ids_or_empty() -> list:
    """`_watched_espn_ids`, but a database error only costs the per-athlete calls."""
    try:
        return _watched_espn_ids()
    except Exception as e:
        logger.warning(f"Could not load watched players for ESPN athlete news: {e}")
        return []


def fetch_espn_athlete_news() -> list:
    """ESPN's general NFL news feed, then each watched player's own news when the general
    feed has none about them. Returns `{article, espn_athlete_id}` items; [] when the
    general feed is down, since the per-athlete calls would hit the same API."""
    general = _get_json_with_retries("ESPN NFL news", ESPN_NEWS_URL, {"limit": ESPN_NEWS_LIMIT})
    if general is None:
        return []
    articles = general.get("articles") or []
    items = _espn_items(articles)
    covered = _covered_athlete_ids(articles)
    items.extend(_missed_athlete_news([espn_id for espn_id in _watched_ids_or_empty() if espn_id not in covered]))
    return items


def normalize_espn_item(item: dict) -> dict:
    """Maps one `{article, espn_athlete_id}` item to the common shape. `espn_athlete_id`
    rides along so `sync_rumors` matches on it exactly instead of fuzzing the text."""
    article = item["article"]
    athlete_id = item.get("espn_athlete_id")
    headline = article.get("headline") or ""
    description = article.get("description") or ""
    article_id = article.get("id")
    return {
        "source_item_id": f"{article_id}:{athlete_id}" if athlete_id else (str(article_id) if article_id else None),
        "raw_text": headline + (f" - {description}" if description and description != headline else ""),
        "headline": headline or None,
        "author": article.get("byline") or "ESPN",
        "url": ((article.get("links") or {}).get("web") or {}).get("href"),
        "published_at": _parse_timestamp(article.get("published")),
        "espn_athlete_id": athlete_id,
    }


def _bluesky_handles() -> list:
    return [handle.strip() for handle in os.getenv("NEWS_BLUESKY_HANDLES", "").split(",") if handle.strip()]


def _is_original_post(item: dict, handle: str) -> bool:
    """The account's own top-level post: not a repost or pin (both carry a `reason`), not a
    reply, and written by that handle."""
    post = item.get("post") or {}
    return (
        not item.get("reason")
        and not (post.get("record") or {}).get("reply")
        and (post.get("author") or {}).get("handle") == handle
    )


def fetch_bluesky_accounts() -> list:
    """Original posts from each `NEWS_BLUESKY_HANDLES` account, via the public AppView's
    getAuthorFeed (no auth). Each handle fails on its own; no handles means no fetch."""
    handles = _bluesky_handles()
    if not handles:
        logger.info("NEWS_BLUESKY_HANDLES is empty; skipping Bluesky.")
        return []
    posts = []
    for handle in handles:
        payload = _get_json_with_retries(
            f"Bluesky @{handle}",
            BLUESKY_AUTHOR_FEED_URL,
            {"actor": handle, "limit": BLUESKY_FEED_LIMIT, "filter": "posts_no_replies"},
        )
        if payload is None:
            continue
        posts.extend(item["post"] for item in payload.get("feed") or [] if _is_original_post(item, handle))
    return posts


def normalize_bluesky_item(post: dict) -> dict:
    """Maps a Bluesky post view to the common shape. The web URL is built from the handle
    and the record key, the AT URI's last segment."""
    uri = post.get("uri") or ""
    author = post.get("author") or {}
    handle = author.get("handle")
    record = post.get("record") or {}
    return {
        "source_item_id": uri or None,
        "raw_text": record.get("text", ""),
        "headline": None,
        "author": author.get("displayName") or handle,
        "url": f"https://bsky.app/profile/{handle}/post/{uri.rsplit('/', 1)[-1]}" if uri and handle else None,
        "published_at": _parse_timestamp(record.get("createdAt")),
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
        ("espn_news", fetch_espn_athlete_news, normalize_espn_item),
        ("bluesky", fetch_bluesky_accounts, normalize_bluesky_item),
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


def load_espn_id_lookup(cursor) -> dict:
    """players.espn_id -> player_id, for the exact match of ESPN's athlete tags."""
    cursor.execute("SELECT espn_id, player_id FROM players WHERE espn_id IS NOT NULL;")
    return {row[0]: row[1] for row in cursor.fetchall()}


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


NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv"}


def _words(text: str) -> list:
    """Lowercased, accent-free alphanumeric words; punctuation is a separator."""
    plain = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return re.findall(r"[a-z0-9]+", plain.lower())


def _name_phrase(name: str) -> str:
    """A player's name as words with trailing suffixes (Jr., Sr., II) dropped, so the
    name matches a post whether or not it spells the suffix out."""
    words = _words(name)
    while words and words[-1] in NAME_SUFFIXES:
        words.pop()
    return " ".join(words)


def match_player_phrase(raw_text: str, player_lookup: list):
    """
    Strict match for free-form posts: a player matches only when their normalized name is a
    whole-word, contiguous phrase of the post. `token_set_ratio` scores 100 when a name's
    tokens come from two different people ("Brandon Staley ... Ben Johnson" -> Brandon
    Johnson), so it isn't used here. When several players match, the one named first in the post
    wins (the subject leads), then the longest name, then lookup order. Returns (player_id, 1.0) or (None, None).
    """
    post = f" {' '.join(_words(raw_text or ''))} "
    best = None
    for player_id, name in player_lookup:
        phrase = _name_phrase(name or "")
        position = post.find(f" {phrase} ") if phrase else -1
        if position < 0:
            continue
        rank = (position, -len(phrase))
        if best is None or rank < best[0]:
            best = (rank, player_id)
    return (best[1], 1.0) if best else (None, None)


def _match_item(item: dict, player_lookup: list, espn_id_lookup: dict):
    """An ESPN item matches only on its athlete tag, exactly, and stays unmatched when that
    id isn't in `players`; it is never fuzzed. A bluesky item goes through
    `match_player_phrase`, every other item through `match_player`."""
    if "espn_athlete_id" in item:
        player_id = espn_id_lookup.get(item["espn_athlete_id"]) if item["espn_athlete_id"] else None
        return (player_id, 1.0) if player_id else (None, None)
    if item.get("source") == "bluesky":
        return match_player_phrase(item["raw_text"], player_lookup)
    return match_player(item["raw_text"], player_lookup)


def persist_signals(conn, rows: list) -> list:
    """Inserts new rows and returns their ids; a dupe returns nothing, so the ids are only
    what this run actually ingested."""
    if not rows:
        return []
    insert_query = """
        INSERT INTO raw_signals
            (source, source_item_id, content_hash, raw_text, url, published_at, player_id, match_confidence,
             headline, author)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source, source_item_id) DO NOTHING
        RETURNING id;
    """
    new_ids = []
    with conn.cursor() as cursor:
        for row in rows:
            cursor.execute(insert_query, (
                row["source"], row["source_item_id"], row["content_hash"], row["raw_text"],
                row["url"], row["published_at"], row["player_id"], row["match_confidence"],
                row.get("headline"), row.get("author"),
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
        run_log_service.add_summary(headline="Skipped: another sync ran recently")
        return

    sources = _sources()
    items = []
    # Items per source for the run log; a source whose fetch failed shows 0 alongside the
    # warning its fetcher logged.
    per_source = {}
    for source_name, fetch_fn, normalize_fn in sources:
        per_source[source_name] = 0
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
            per_source[source_name] += 1

    logger.info(f"Fetched {len(items)} total item(s) across {len(sources)} source(s).")

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            player_lookup = load_player_lookup(cursor)
            espn_id_lookup = load_espn_id_lookup(cursor)

        rows = []
        for item in items:
            player_id, confidence = _match_item(item, player_lookup, espn_id_lookup)
            content_hash = hashlib.sha256(f"{item['source']}:{item['raw_text']}".encode("utf-8")).hexdigest()
            rows.append({
                **item,
                "content_hash": content_hash,
                "player_id": player_id,
                "match_confidence": confidence,
            })

        new_ids = persist_signals(conn, rows)

    run_log_service.link("raw_signal", new_ids, "output")
    empty = sorted(name for name, count in per_source.items() if count == 0)
    run_log_service.add_summary(
        headline=f"{len(items)} fetched, {len(new_ids)} new"
        + (f" ({', '.join(empty)} returned nothing)" if empty else ""),
        fetched=len(items),
        new=len(new_ids),
        per_source=per_source,
    )
    mark_synced(r_client)


if __name__ == "__main__":
    with run_log_service.run_scope("sync_rumors"):
        sync_rumors(force="--force" in sys.argv)
