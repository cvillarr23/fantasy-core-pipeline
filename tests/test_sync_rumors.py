from unittest.mock import MagicMock, patch

import requests

from src.ingestion import sync_rumors


class _FakeEntry(dict):
    """Mimics a feedparser entry (dict-like with .get())."""
    def get(self, key, default=None):
        return dict.get(self, key, default)


def test_normalize_rotowire_item_maps_fields():
    entry = _FakeEntry({
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


def test_normalize_rotowire_item_falls_back_to_link_when_guid_missing():
    entry = _FakeEntry({
        "title": "Some news",
        "link": "https://www.rotowire.com/football/news/999",
        "published_parsed": None,
    })

    result = sync_rumors.normalize_rotowire_item(entry)

    assert result["source_item_id"] == "https://www.rotowire.com/football/news/999"
    assert result["published_at"] is None


def test_normalize_reddit_item_maps_fields():
    post = {
        "name": "t3_abc123",
        "title": "Beat writer: RB1 expected to play Sunday",
        "selftext": "Source says he's a full go.",
        "permalink": "/r/fantasyfootball/comments/abc123/rb1_update/",
        "created_utc": 1755280800.0,
    }

    result = sync_rumors.normalize_reddit_item(post)

    assert result["source_item_id"] == "t3_abc123"
    assert "RB1 expected to play" in result["raw_text"]
    assert "full go" in result["raw_text"]
    assert result["url"] == "https://reddit.com/r/fantasyfootball/comments/abc123/rb1_update/"
    assert result["published_at"] is not None


def test_fetch_rotowire_rss_soft_fails_after_retries():
    with patch("src.ingestion.sync_rumors.requests.get", side_effect=requests.RequestException("boom")):
        result = sync_rumors.fetch_rotowire_rss()

    assert result == []


def test_fetch_reddit_new_sets_user_agent_header():
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": {"children": []}}

    with patch("src.ingestion.sync_rumors.requests.get", return_value=mock_response) as mock_get:
        sync_rumors.fetch_reddit_new()

    headers = mock_get.call_args.kwargs["headers"]
    assert "User-Agent" in headers
    assert headers["User-Agent"] != ""


def test_fetch_reddit_new_soft_fails_after_retries():
    with patch("src.ingestion.sync_rumors.requests.get", side_effect=requests.RequestException("boom")):
        result = sync_rumors.fetch_reddit_new()

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

    sync_rumors.persist_signals(mock_conn, [_signal_row()])

    args, kwargs = mock_cursor.executemany.call_args
    query, param_rows = args
    assert "ON CONFLICT (source, source_item_id) DO NOTHING" in query
    assert param_rows[0] == (
        "rotowire_rss", "rotowire-1", "deadbeef", "Christian McCaffrey questionable",
        "https://www.rotowire.com/football/news/1", None, "4046", 1.0,
    )


def test_persist_signals_noop_on_empty_list():
    mock_conn = MagicMock()

    sync_rumors.persist_signals(mock_conn, [])

    mock_conn.cursor.assert_not_called()


def test_sync_rumors_skips_when_lock_held():
    mock_redis_instance = MagicMock()
    mock_redis_instance.set.return_value = None  # NX failed: lock already held

    with patch("src.ingestion.sync_rumors.redis.Redis", return_value=mock_redis_instance), \
         patch("src.ingestion.sync_rumors.fetch_rotowire_rss") as mock_fetch_rss, \
         patch("src.ingestion.sync_rumors.fetch_reddit_new") as mock_fetch_reddit:
        sync_rumors.sync_rumors(force=False)

    mock_fetch_rss.assert_not_called()
    mock_fetch_reddit.assert_not_called()


def test_sync_rumors_continues_when_one_source_fails():
    mock_redis_instance = MagicMock()
    mock_redis_instance.set.return_value = True

    reddit_item = {
        "name": "t3_abc123",
        "title": "Christian McCaffrey full participant in practice",
        "selftext": "",
        "permalink": "/r/fantasyfootball/comments/abc123/",
        "created_utc": 1755280800.0,
    }

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("4046", "Christian McCaffrey")]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.ingestion.sync_rumors.redis.Redis", return_value=mock_redis_instance), \
         patch("src.ingestion.sync_rumors.fetch_rotowire_rss", side_effect=Exception("rss down")), \
         patch("src.ingestion.sync_rumors.fetch_reddit_new", return_value=[reddit_item]), \
         patch("src.ingestion.sync_rumors.get_db_connection") as mock_get_conn, \
         patch("src.ingestion.sync_rumors.persist_signals") as mock_persist:
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        sync_rumors.sync_rumors(force=True)

    mock_persist.assert_called_once()
    persisted_rows = mock_persist.call_args.args[1]
    assert len(persisted_rows) == 1
    assert persisted_rows[0]["source"] == "reddit_fantasyfootball"
    assert persisted_rows[0]["player_id"] == "4046"
    mock_redis_instance.set.assert_called()  # lock acquire + mark_synced
