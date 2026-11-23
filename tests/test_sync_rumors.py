import json
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.ingestion import sync_rumors


class _FakeEntry(dict):
    """Mimics a feedparser entry (dict-like with .get())."""
    def get(self, key, default=None):
        return dict.get(self, key, default)


def test_normalize_rotowire_item_maps_fields():
    entry = _FakeEntry({
        "id": "rotowire-12345",
        "guid": "rotowire-12345",
        "title": "Christian McCaffrey (knee) questionable for Sunday",
        "summary": "McCaffrey was limited in practice.",
        "link": "https://www.rotowire.com/football/news/12345",
        "published_parsed": (2026, 8, 15, 12, 0, 0, 5, 227, 0),
    })

    result = sync_rumors.normalize_rotowire_item(entry)

    assert result["source_item_id"] == "rotowire-12345"
    assert "Christian McCaffrey" in result["raw_text"]
    assert "limited in practice" in result["raw_text"]
    assert result["url"] == "https://www.rotowire.com/football/news/12345"
    assert result["published_at"].year == 2026


def test_normalize_rotowire_item_prefers_id_over_guid():
    """feedparser 6.x exposes the RSS <guid> under the `id` key; `guid` is only a
    best-effort fallback for parser/feed variants that don't set `id`."""
    entry = _FakeEntry({
        "id": "nfl634024",
        "guid": "some-other-value",
        "title": "Some news",
        "link": "https://www.rotowire.com/football/news/999",
        "published_parsed": None,
    })

    result = sync_rumors.normalize_rotowire_item(entry)

    assert result["source_item_id"] == "nfl634024"


def test_normalize_rotowire_item_falls_back_to_link_when_id_and_guid_missing():
    entry = _FakeEntry({
        "title": "Some news",
        "link": "https://www.rotowire.com/football/news/999",
        "published_parsed": None,
    })

    result = sync_rumors.normalize_rotowire_item(entry)

    assert result["source_item_id"] == "https://www.rotowire.com/football/news/999"
    assert result["published_at"] is None


def test_fetch_rotowire_rss_soft_fails_after_retries():
    with patch("src.ingestion.sync_rumors.requests.get", side_effect=requests.RequestException("boom")):
        result = sync_rumors.fetch_rotowire_rss()

    assert result == []


PLAYER_LOOKUP = [
    ("4046", "Christian McCaffrey"),
    ("6794", "Justin Jefferson"),
]


def test_match_player_returns_confident_match_above_threshold():
    text = "Christian McCaffrey (knee) is questionable for Sunday's game"

    player_id, confidence = sync_rumors.match_player(text, PLAYER_LOOKUP)

    assert player_id == "4046"
    assert confidence >= 0.9


def test_match_player_returns_none_below_threshold():
    text = "The weather forecast for Sunday's games looks clear across the league"

    player_id, confidence = sync_rumors.match_player(text, PLAYER_LOOKUP)

    assert player_id is None
    assert confidence is None


def test_match_player_tie_break_prefers_exact_name():
    lookup = [
        ("1", "Michael Thomas"),
        ("2", "Michael Thomass"),
    ]
    text = "Michael Thomas is active for Sunday"

    player_id, confidence = sync_rumors.match_player(text, lookup)

    assert player_id == "1"


def test_match_player_handles_empty_lookup():
    player_id, confidence = sync_rumors.match_player("Christian McCaffrey update", [])

    assert player_id is None
    assert confidence is None


def _signal_row(source_item_id="rotowire-1", player_id="4046"):
    return {
        "source": "rotowire_rss",
        "source_item_id": source_item_id,
        "content_hash": "deadbeef",
        "raw_text": "Christian McCaffrey questionable",
        "url": "https://www.rotowire.com/football/news/1",
        "published_at": None,
        "player_id": player_id,
        "match_confidence": 1.0,
    }


def test_persist_signals_dedupes_via_on_conflict():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.fetchone.return_value = (42,)

    assert sync_rumors.persist_signals(mock_conn, [_signal_row()]) == [42]

    query, params = mock_cursor.execute.call_args.args
    assert "ON CONFLICT (source, source_item_id) DO NOTHING" in query
    assert "RETURNING id" in query
    assert params == (
        "rotowire_rss", "rotowire-1", "deadbeef", "Christian McCaffrey questionable",
        "https://www.rotowire.com/football/news/1", None, "4046", 1.0, None, None,
    )


def test_persist_signals_returns_only_new_ids():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.side_effect = [(7,), None]

    assert sync_rumors.persist_signals(mock_conn, [_signal_row(), _signal_row()]) == [7]


def test_persist_signals_noop_on_empty_list():
    mock_conn = MagicMock()

    assert sync_rumors.persist_signals(mock_conn, []) == []

    mock_conn.cursor.assert_not_called()


def test_sync_rumors_skips_when_lock_held():
    mock_redis_instance = MagicMock()
    mock_redis_instance.set.return_value = None  # NX failed: lock already held

    with patch("src.ingestion.sync_rumors.get_redis_client", return_value=mock_redis_instance), \
         patch("src.ingestion.sync_rumors.fetch_rotowire_rss") as mock_fetch_rss:
        sync_rumors.sync_rumors(force=False)

    mock_fetch_rss.assert_not_called()


def test_sync_rumors_soft_fails_when_source_raises():
    mock_redis_instance = MagicMock()
    mock_redis_instance.set.return_value = True

    with patch("src.ingestion.sync_rumors.get_redis_client", return_value=mock_redis_instance), \
         patch("src.ingestion.sync_rumors.fetch_rotowire_rss", side_effect=Exception("rss down")), \
         patch("src.ingestion.sync_rumors.fetch_reddit_fantasyfootball", return_value=[]), \
         patch("src.ingestion.sync_rumors.fetch_espn_athlete_news", return_value=[]), \
         patch("src.ingestion.sync_rumors.fetch_bluesky_accounts", return_value=[]), \
         patch("src.ingestion.sync_rumors.get_db_connection") as mock_get_conn, \
         patch("src.ingestion.sync_rumors.persist_signals") as mock_persist:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        sync_rumors.sync_rumors(force=True)  # should not raise

    mock_persist.assert_called_once_with(mock_conn, [])
    mock_redis_instance.set.assert_called()  # lock acquire + mark_synced


REDDIT_ENV = {
    "REDDIT_CLIENT_ID": "test-client-id",
    "REDDIT_CLIENT_SECRET": "test-client-secret",
    "REDDIT_USER_AGENT": "test-agent/1.0 (by /u/testuser)",
}


def _reddit_post(name="t3_abc123", score=50, age_hours=1, title="McCaffrey injury update", selftext="", permalink="/r/fantasyfootball/comments/abc123/mccaffrey/"):
    return {
        "name": name,
        "score": score,
        "created_utc": time.time() - age_hours * 3600,
        "title": title,
        "selftext": selftext,
        "permalink": permalink,
        "url": "https://www.reddit.com" + permalink,
    }


def _reddit_listing_response(posts, after=None):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": {"children": [{"data": post} for post in posts], "after": after}
    }
    mock_response.raise_for_status.return_value = None
    return mock_response


def test_normalize_reddit_item_maps_fields():
    post = _reddit_post(selftext="He's expected to play.")

    result = sync_rumors.normalize_reddit_item(post)

    assert result["source_item_id"] == "t3_abc123"
    assert "McCaffrey injury update" in result["raw_text"]
    assert "expected to play" in result["raw_text"]
    assert result["url"] == "https://www.reddit.com/r/fantasyfootball/comments/abc123/mccaffrey/"
    assert result["published_at"].tzinfo is not None


def test_normalize_reddit_item_falls_back_to_url_without_permalink():
    post = _reddit_post()
    post["permalink"] = None
    post["url"] = "https://external-site.example/article"

    result = sync_rumors.normalize_reddit_item(post)

    assert result["url"] == "https://external-site.example/article"


def test_get_reddit_access_token_raises_when_env_missing():
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(RuntimeError):
            sync_rumors.get_reddit_access_token()


def test_get_reddit_access_token_returns_token():
    mock_response = MagicMock()
    mock_response.json.return_value = {"access_token": "fake-token"}
    mock_response.raise_for_status.return_value = None

    with patch.dict("os.environ", REDDIT_ENV, clear=True), \
         patch("src.ingestion.sync_rumors.requests.post", return_value=mock_response) as mock_post:
        token = sync_rumors.get_reddit_access_token()

    assert token == "fake-token"
    assert mock_post.call_args.kwargs["auth"] == ("test-client-id", "test-client-secret")


def test_fetch_reddit_fantasyfootball_soft_fails_after_retries():
    with patch.dict("os.environ", REDDIT_ENV, clear=True), \
         patch("src.ingestion.sync_rumors.get_reddit_access_token", side_effect=requests.RequestException("boom")):
        result = sync_rumors.fetch_reddit_fantasyfootball()

    assert result == []


def test_fetch_reddit_fantasyfootball_soft_fails_when_env_missing():
    with patch.dict("os.environ", {}, clear=True):
        result = sync_rumors.fetch_reddit_fantasyfootball()

    assert result == []


def test_fetch_reddit_fantasyfootball_applies_quality_filter():
    good_post = _reddit_post(name="t3_good", score=50, age_hours=1)
    low_score_post = _reddit_post(name="t3_lowscore", score=1, age_hours=1)
    stale_post = _reddit_post(name="t3_stale", score=999, age_hours=200)

    mock_redis_instance = MagicMock()
    mock_redis_instance.get.return_value = None
    response = _reddit_listing_response([good_post, low_score_post, stale_post], after=None)

    with patch.dict("os.environ", REDDIT_ENV, clear=True), \
         patch("src.ingestion.sync_rumors.get_reddit_access_token", return_value="fake-token"), \
         patch("src.ingestion.sync_rumors.get_redis_client", return_value=mock_redis_instance), \
         patch("src.ingestion.sync_rumors.requests.get", return_value=response):
        result = sync_rumors.fetch_reddit_fantasyfootball()

    assert [p["name"] for p in result] == ["t3_good"]


def test_fetch_reddit_fantasyfootball_paginates_via_after_cursor():
    page1 = [_reddit_post(name=f"t3_p1_{i}") for i in range(3)]
    page2 = [_reddit_post(name=f"t3_p2_{i}") for i in range(2)]

    mock_redis_instance = MagicMock()
    mock_redis_instance.get.return_value = None
    response_page1 = _reddit_listing_response(page1, after="cursor-1")
    response_page2 = _reddit_listing_response(page2, after=None)

    with patch.dict("os.environ", REDDIT_ENV, clear=True), \
         patch("src.ingestion.sync_rumors.get_reddit_access_token", return_value="fake-token"), \
         patch("src.ingestion.sync_rumors.get_redis_client", return_value=mock_redis_instance), \
         patch("src.ingestion.sync_rumors.requests.get", side_effect=[response_page1, response_page2]) as mock_get:
        result = sync_rumors.fetch_reddit_fantasyfootball()

    assert len(result) == 5
    assert mock_get.call_count == 2
    assert mock_get.call_args_list[1].kwargs["params"]["after"] == "cursor-1"


def test_fetch_reddit_fantasyfootball_stops_at_watermark():
    seen_post = _reddit_post(name="t3_seen")
    new_post = _reddit_post(name="t3_new")

    mock_redis_instance = MagicMock()
    mock_redis_instance.get.return_value = "t3_seen"
    response = _reddit_listing_response([new_post, seen_post], after="cursor-1")

    with patch.dict("os.environ", REDDIT_ENV, clear=True), \
         patch("src.ingestion.sync_rumors.get_reddit_access_token", return_value="fake-token"), \
         patch("src.ingestion.sync_rumors.get_redis_client", return_value=mock_redis_instance), \
         patch("src.ingestion.sync_rumors.requests.get", return_value=response) as mock_get:
        result = sync_rumors.fetch_reddit_fantasyfootball()

    assert [p["name"] for p in result] == ["t3_new"]
    mock_get.assert_called_once()


def test_fetch_reddit_fantasyfootball_updates_watermark():
    newest_post = _reddit_post(name="t3_newest")

    mock_redis_instance = MagicMock()
    mock_redis_instance.get.return_value = None
    response = _reddit_listing_response([newest_post], after=None)

    with patch.dict("os.environ", REDDIT_ENV, clear=True), \
         patch("src.ingestion.sync_rumors.get_reddit_access_token", return_value="fake-token"), \
         patch("src.ingestion.sync_rumors.get_redis_client", return_value=mock_redis_instance), \
         patch("src.ingestion.sync_rumors.requests.get", return_value=response):
        sync_rumors.fetch_reddit_fantasyfootball()

    mock_redis_instance.set.assert_called_once_with(sync_rumors.REDDIT_WATERMARK_KEY, "t3_newest")


def test_sources_includes_reddit_fantasyfootball():
    names = [name for name, _, _ in sync_rumors._sources()]

    assert "rotowire_rss" in names
    assert "reddit_fantasyfootball" in names


def test_sync_rumors_matches_and_persists_fetched_items():
    mock_redis_instance = MagicMock()
    mock_redis_instance.set.return_value = True

    rss_entry = _FakeEntry({
        "id": "nfl634024",
        "title": "Christian McCaffrey full participant in practice",
        "link": "https://www.rotowire.com/football/news/1",
        "published_parsed": None,
    })

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("4046", "Christian McCaffrey")]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.ingestion.sync_rumors.get_redis_client", return_value=mock_redis_instance), \
         patch("src.ingestion.sync_rumors.fetch_rotowire_rss", return_value=[rss_entry]), \
         patch("src.ingestion.sync_rumors.get_db_connection") as mock_get_conn, \
         patch("src.ingestion.sync_rumors.persist_signals", return_value=[91]) as mock_persist, \
         patch("src.ingestion.sync_rumors.fetch_reddit_fantasyfootball", return_value=[]), \
         patch("src.ingestion.sync_rumors.fetch_espn_athlete_news", return_value=[]), \
         patch("src.ingestion.sync_rumors.fetch_bluesky_accounts", return_value=[]), \
         patch("src.ingestion.sync_rumors.run_log_service") as run_log:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        sync_rumors.sync_rumors(force=True)

    run_log.link.assert_called_once_with("raw_signal", [91], "output")
    summary = run_log.add_summary.call_args.kwargs
    assert summary["headline"].startswith("1 fetched, 1 new (")
    assert "returned nothing" in summary["headline"]
    assert summary["per_source"]["rotowire_rss"] == 1
    mock_persist.assert_called_once()
    persisted_rows = mock_persist.call_args.args[1]
    assert len(persisted_rows) == 1
    assert persisted_rows[0]["source"] == "rotowire_rss"
    assert persisted_rows[0]["player_id"] == "4046"
    mock_redis_instance.set.assert_called()  # lock acquire + mark_synced


# --- espn_news and bluesky, against recorded fixtures ---

NEWS_FIXTURES = Path(__file__).parent / "fixtures" / "news"
BOSA, METCALF, DEJEAN, GIBBS = "4040605", "4047650", "4682618", "4429795"


def _fixture(name):
    return json.loads((NEWS_FIXTURES / name).read_text())


def _response(payload=None, error=None):
    response = MagicMock()
    if error:
        response.raise_for_status.side_effect = error
    response.json.return_value = payload
    return response


def _espn_get(general=None, overview=None, general_error=None):
    """A requests.get stand-in serving the ESPN fixtures by URL."""
    def get(url, params=None, timeout=None):
        if url == sync_rumors.ESPN_NEWS_URL:
            if general_error:
                raise general_error
            return _response(general if general is not None else _fixture("espn_news.json"))
        return _response(overview if overview is not None else _fixture("espn_athlete_overview.json"))
    return get


def test_fetch_espn_athlete_news_expands_one_item_per_tagged_athlete():
    with patch("src.ingestion.sync_rumors.requests.get", side_effect=_espn_get()), \
         patch("src.ingestion.sync_rumors._watched_espn_ids", return_value=[]):
        items = sync_rumors.fetch_espn_athlete_news()

    assert [(item["article"]["id"], item["espn_athlete_id"]) for item in items] == [
        (50568613, BOSA), (50564475, METCALF), (50564475, DEJEAN), (50567915, None),
    ]


def test_fetch_espn_athlete_news_calls_only_watched_players_the_feed_missed():
    get = MagicMock(side_effect=_espn_get())
    with patch("src.ingestion.sync_rumors.requests.get", get), \
         patch("src.ingestion.sync_rumors._watched_espn_ids", return_value=[BOSA, GIBBS]):
        items = sync_rumors.fetch_espn_athlete_news()

    urls = [c.args[0] for c in get.call_args_list]
    assert urls == [sync_rumors.ESPN_NEWS_URL, sync_rumors.ESPN_ATHLETE_OVERVIEW_URL.format(espn_id=GIBBS)]
    gibbs_items = [item for item in items if item["espn_athlete_id"] == GIBBS]
    assert [item["article"]["id"] for item in gibbs_items] == [50565334, 50564717]


def test_fetch_espn_athlete_news_roundup_tags_dont_count_as_coverage():
    tags = [{"type": "athlete", "athleteId": int(GIBBS)}] + [
        {"type": "athlete", "athleteId": 100 + i} for i in range(sync_rumors.ESPN_COVERAGE_MAX_ATHLETES)
    ]
    roundup = {**_fixture("espn_news.json")["articles"][2], "categories": tags}
    get = MagicMock(side_effect=_espn_get(general={"articles": [roundup]}))
    with patch("src.ingestion.sync_rumors.requests.get", get), \
         patch("src.ingestion.sync_rumors._watched_espn_ids", return_value=[GIBBS]):
        items = sync_rumors.fetch_espn_athlete_news()

    assert get.call_args.args[0] == sync_rumors.ESPN_ATHLETE_OVERVIEW_URL.format(espn_id=GIBBS)
    assert len([item for item in items if item["espn_athlete_id"] == GIBBS]) == 3


def test_fetch_espn_athlete_news_skips_an_athlete_whose_page_errors():
    def get(url, params=None, timeout=None):
        if url == sync_rumors.ESPN_NEWS_URL:
            return _response(_fixture("espn_news.json"))
        if "/athletes/999/" in url:
            return _response(error=requests.HTTPError("404"))
        return _response(_fixture("espn_athlete_overview.json"))

    with patch("src.ingestion.sync_rumors.requests.get", side_effect=get), \
         patch("src.ingestion.sync_rumors._watched_espn_ids", return_value=["999", GIBBS]):
        items = sync_rumors.fetch_espn_athlete_news()

    assert [item["espn_athlete_id"] for item in items[4:]] == [GIBBS, GIBBS]


def test_fetch_espn_athlete_news_stops_athlete_calls_when_espn_times_out():
    get = MagicMock()

    def respond(url, params=None, timeout=None):
        if url == sync_rumors.ESPN_NEWS_URL:
            return _response(_fixture("espn_news.json"))
        raise requests.Timeout("slow")

    get.side_effect = respond
    with patch("src.ingestion.sync_rumors.requests.get", get), \
         patch("src.ingestion.sync_rumors._watched_espn_ids", return_value=["998", "999", GIBBS]):
        items = sync_rumors.fetch_espn_athlete_news()

    assert get.call_count == 2
    assert len(items) == 4


def test_fetch_espn_athlete_news_soft_fails_when_espn_is_down():
    get = MagicMock(side_effect=requests.ConnectionError("espn down"))
    with patch("src.ingestion.sync_rumors.requests.get", get), \
         patch("src.ingestion.sync_rumors._watched_espn_ids") as watched:
        assert sync_rumors.fetch_espn_athlete_news() == []

    assert get.call_count == sync_rumors.FETCH_RETRY_ATTEMPTS
    watched.assert_not_called()


def test_normalize_espn_item_maps_fields():
    article = _fixture("espn_news.json")["articles"][1]
    item = sync_rumors.normalize_espn_item({"article": article, "espn_athlete_id": METCALF})

    assert item["source_item_id"] == f"50564475:{METCALF}"
    assert item["headline"] == article["headline"]
    assert item["raw_text"] == f"{article['headline']} - {article['description']}"
    assert item["author"] == "Brooke Pryor"
    assert item["url"] == article["links"]["web"]["href"]
    assert item["published_at"] == datetime(2026, 11, 23, 0, 43, 19, tzinfo=timezone.utc)
    assert item["espn_athlete_id"] == METCALF


def test_normalize_espn_item_untagged_article_and_missing_byline():
    article = _fixture("espn_news.json")["articles"][0]
    item = sync_rumors.normalize_espn_item({"article": article, "espn_athlete_id": None})

    assert item["source_item_id"] == "50568613"
    assert item["author"] == "ESPN"


def _watched_cursor(opponent_row=("7",), roster=((BOSA,), (GIBBS,))):
    cursor = MagicMock()
    cursor.fetchone.return_value = opponent_row
    cursor.fetchall.return_value = list(roster)
    return cursor


def _patch_db_cursor(cursor):
    cm = MagicMock()
    cm.__enter__.return_value = cursor
    return patch("src.ingestion.sync_rumors.get_db_cursor", return_value=cm)


def test_watched_espn_ids_covers_our_roster_and_this_weeks_opponent(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "3")
    monkeypatch.setenv("ESPN_LEAGUE_ID", "999")
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    cursor = _watched_cursor()
    with _patch_db_cursor(cursor), \
         patch("src.ingestion.sync_rumors.resolve_upcoming_week", return_value=12) as week:
        assert sync_rumors._watched_espn_ids() == [BOSA, GIBBS]

    week.assert_called_once_with(2026, attempts=1)
    matchup_params = cursor.execute.call_args_list[0].args[1]
    assert matchup_params == ("999", 2026, "3", 12)
    roster_params = cursor.execute.call_args_list[1].args[1]
    assert roster_params == (["3", "7"],)


def test_watched_espn_ids_falls_back_to_our_roster_when_the_week_is_unknown(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "3")
    monkeypatch.setenv("ESPN_LEAGUE_ID", "999")
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    cursor = _watched_cursor()
    with _patch_db_cursor(cursor), \
         patch("src.ingestion.sync_rumors.resolve_upcoming_week",
               side_effect=sync_rumors.WeekResolutionError("nflverse down")):
        sync_rumors._watched_espn_ids()

    assert cursor.execute.call_args.args[1] == (["3"],)


def test_watched_espn_ids_falls_back_when_no_matchup_row(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "3")
    monkeypatch.setenv("ESPN_LEAGUE_ID", "999")
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    cursor = _watched_cursor(opponent_row=None)
    with _patch_db_cursor(cursor), \
         patch("src.ingestion.sync_rumors.resolve_upcoming_week", return_value=12):
        sync_rumors._watched_espn_ids()

    assert cursor.execute.call_args.args[1] == (["3"],)


def test_watched_espn_ids_is_empty_without_a_team(monkeypatch):
    monkeypatch.delenv("ESPN_TEAM_ID", raising=False)
    with patch("src.ingestion.sync_rumors.get_db_cursor") as get_cursor:
        assert sync_rumors._watched_espn_ids() == []

    get_cursor.assert_not_called()


def _bluesky_get(feeds, failing=()):
    def get(url, params=None, timeout=None):
        if params["actor"] in failing:
            raise requests.ConnectionError("bluesky down")
        return _response(_fixture(feeds[params["actor"]]))
    return get


GREGG, RAP = "greggrosenthal.bsky.social", "rapsheet.bsky.social"
FEEDS = {GREGG: "bluesky_author_feed.json", RAP: "bluesky_author_feed_second.json"}


def test_fetch_bluesky_accounts_keeps_only_original_posts(monkeypatch):
    monkeypatch.setenv("NEWS_BLUESKY_HANDLES", f" {GREGG} ")
    get = MagicMock(side_effect=_bluesky_get(FEEDS))
    with patch("src.ingestion.sync_rumors.requests.get", get):
        posts = sync_rumors.fetch_bluesky_accounts()

    assert [p["record"]["text"][:20] for p in posts] == ["Patrick Mahomes is b", "the Titans spent a l"]
    assert get.call_args.kwargs["params"] == {"actor": GREGG, "limit": 50, "filter": "posts_no_replies"}


def test_fetch_bluesky_accounts_skips_without_handles(monkeypatch):
    monkeypatch.setenv("NEWS_BLUESKY_HANDLES", " , ")
    with patch("src.ingestion.sync_rumors.requests.get") as get:
        assert sync_rumors.fetch_bluesky_accounts() == []

    get.assert_not_called()


def test_fetch_bluesky_accounts_one_handle_down_keeps_the_others(monkeypatch):
    monkeypatch.setenv("NEWS_BLUESKY_HANDLES", f"{GREGG},{RAP}")
    with patch("src.ingestion.sync_rumors.requests.get", side_effect=_bluesky_get(FEEDS, failing={GREGG})):
        posts = sync_rumors.fetch_bluesky_accounts()

    assert [p["author"]["handle"] for p in posts] == [RAP]


def test_fetch_bluesky_accounts_soft_fails_when_all_handles_are_down(monkeypatch):
    monkeypatch.setenv("NEWS_BLUESKY_HANDLES", f"{GREGG},{RAP}")
    with patch("src.ingestion.sync_rumors.requests.get", side_effect=_bluesky_get(FEEDS, failing={GREGG, RAP})):
        assert sync_rumors.fetch_bluesky_accounts() == []


def test_normalize_bluesky_item_maps_fields():
    post = _fixture("bluesky_author_feed_second.json")["feed"][0]["post"]
    item = sync_rumors.normalize_bluesky_item(post)

    assert item["source_item_id"] == post["uri"]
    assert item["url"] == "https://bsky.app/profile/rapsheet.bsky.social/post/3n2wvevktc22b"
    assert item["author"] == "Ian Rapoport"
    assert item["headline"] is None
    assert item["raw_text"].startswith("Chargers QB Justin Herbert")
    assert item["published_at"] == datetime(2026, 11, 23, 0, 40, 4, 629000, tzinfo=timezone.utc)
    assert "espn_athlete_id" not in item


PLAYERS = [("4046", "Christian McCaffrey"), ("6904", "Patrick Mahomes"), ("5001", "Nick Bosa"),
           ("5002", "DK Metcalf"), ("5003", "Justin Herbert")]
ESPN_IDS = [(BOSA, "5001"), (METCALF, "5002")]


def _sync_db():
    """get_db_connection stand-in: players for the fuzzy match, then the espn_id lookup."""
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.side_effect = [PLAYERS, ESPN_IDS]
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn


def _run_sync(espn=None, bluesky=None, rotowire=None, persist=None):
    """sync_rumors with every fetcher stubbed. Returns the rows handed to persist_signals."""
    redis_client = MagicMock()
    redis_client.set.return_value = True
    persist = persist or MagicMock(return_value=[])
    with patch("src.ingestion.sync_rumors.get_redis_client", return_value=redis_client), \
         patch("src.ingestion.sync_rumors.fetch_rotowire_rss", **(rotowire or {"return_value": []})), \
         patch("src.ingestion.sync_rumors.fetch_reddit_fantasyfootball", return_value=[]), \
         patch("src.ingestion.sync_rumors.fetch_espn_athlete_news", **(espn or {"return_value": []})), \
         patch("src.ingestion.sync_rumors.fetch_bluesky_accounts", **(bluesky or {"return_value": []})), \
         patch("src.ingestion.sync_rumors.get_db_connection") as get_conn, \
         patch("src.ingestion.sync_rumors.persist_signals", persist), \
         patch("src.ingestion.sync_rumors.run_log_service"):
        get_conn.return_value.__enter__.return_value = _sync_db()
        sync_rumors.sync_rumors(force=True)
    return persist.call_args.args[1]


def _espn_fixture_items():
    with patch("src.ingestion.sync_rumors.requests.get", side_effect=_espn_get()), \
         patch("src.ingestion.sync_rumors._watched_espn_ids", return_value=[]):
        return sync_rumors.fetch_espn_athlete_news()


def _bluesky_fixture_posts(monkeypatch):
    monkeypatch.setenv("NEWS_BLUESKY_HANDLES", f"{GREGG},{RAP}")
    with patch("src.ingestion.sync_rumors.requests.get", side_effect=_bluesky_get(FEEDS)):
        return sync_rumors.fetch_bluesky_accounts()


def test_sync_rumors_matches_espn_exactly_and_bluesky_by_name(monkeypatch):
    espn_items, posts = _espn_fixture_items(), _bluesky_fixture_posts(monkeypatch)
    rows = _run_sync(espn={"return_value": espn_items}, bluesky={"return_value": posts})

    matched = {(r["source"], r["source_item_id"]): (r["player_id"], r["match_confidence"]) for r in rows}
    assert matched[("espn_news", f"50568613:{BOSA}")] == ("5001", 1.0)
    assert matched[("espn_news", f"50564475:{METCALF}")] == ("5002", 1.0)
    # DeJean's ESPN id isn't in players, and the untagged article has no athlete: both stay
    # unmatched rather than falling back to the fuzzy match.
    assert matched[("espn_news", f"50564475:{DEJEAN}")] == (None, None)
    assert matched[("espn_news", "50567915")] == (None, None)

    by_text = {r["raw_text"][:15]: r for r in rows if r["source"] == "bluesky"}
    assert by_text["Patrick Mahomes"]["player_id"] == "6904"
    assert by_text["the Titans spen"]["player_id"] is None
    assert by_text["Chargers QB Jus"]["player_id"] == "5003"
    assert {r["author"] for r in rows if r["source"] == "bluesky"} == {"Gregg Rosenthal", "Ian Rapoport"}


def test_sync_rumors_rerun_of_the_same_items_inserts_nothing_new(monkeypatch):
    """The second run computes the same (source, source_item_id) and content_hash, so the
    ON CONFLICT in persist_signals returns no new ids."""
    espn_items, posts = _espn_fixture_items(), _bluesky_fixture_posts(monkeypatch)
    keys = {}
    runs = []

    def persist(conn, rows):
        """Mimics ON CONFLICT (source, source_item_id) DO NOTHING ... RETURNING id."""
        fresh = []
        for row in rows:
            key = (row["source"], row["source_item_id"])
            if key in keys:
                assert keys[key] == row["content_hash"]
                continue
            keys[key] = row["content_hash"]
            fresh.append(len(keys))
        runs.append(fresh)
        return fresh

    for _ in range(2):
        _run_sync(espn={"return_value": espn_items}, bluesky={"return_value": posts}, persist=MagicMock(side_effect=persist))

    assert len(runs[0]) == 7
    assert runs[1] == []


def test_sync_rumors_one_source_outage_keeps_the_others(monkeypatch):
    posts = _bluesky_fixture_posts(monkeypatch)
    rss_entry = _FakeEntry({"id": "nfl1", "title": "Christian McCaffrey full participant", "link": "https://x"})

    rows = _run_sync(
        espn={"side_effect": RuntimeError("espn exploded")},
        bluesky={"return_value": posts},
        rotowire={"return_value": [rss_entry]},
    )

    assert {r["source"] for r in rows} == {"rotowire_rss", "bluesky"}

    espn_items = _espn_fixture_items()
    rows = _run_sync(espn={"return_value": espn_items}, bluesky={"side_effect": RuntimeError("bsky exploded")})
    assert {r["source"] for r in rows} == {"espn_news"}


def test_persist_signals_writes_headline_and_author():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchone.return_value = (5,)
    row = {**_signal_row(), "source": "espn_news", "headline": "Bosa booed", "author": "ESPN"}

    sync_rumors.persist_signals(conn, [row])

    query, params = cursor.execute.call_args.args
    assert "headline, author" in query
    assert params[-2:] == ("Bosa booed", "ESPN")


def test_sources_includes_display_only_news_sources():
    names = [name for name, _, _ in sync_rumors._sources()]

    assert names[-2:] == ["espn_news", "bluesky"]
