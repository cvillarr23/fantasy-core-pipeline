import time
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
        "https://www.rotowire.com/football/news/1", None, "4046", 1.0,
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
         patch("src.ingestion.sync_rumors.persist_signals") as mock_persist:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        sync_rumors.sync_rumors(force=True)

    mock_persist.assert_called_once()
    persisted_rows = mock_persist.call_args.args[1]
    assert len(persisted_rows) == 1
    assert persisted_rows[0]["source"] == "rotowire_rss"
    assert persisted_rows[0]["player_id"] == "4046"
    mock_redis_instance.set.assert_called()  # lock acquire + mark_synced
