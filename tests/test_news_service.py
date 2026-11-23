from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from src.services import news_service

T0 = datetime(2026, 11, 22, 12, 0, 0)


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


def _row(id, source, headline, at, player="p1", raw_text="body", author=None, url=None):
    return (id, player, source, author, headline, raw_text, url, at)


def _news(rows, player_ids=("p1",), since=None, limit=20):
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    with patch("src.services.news_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        return news_service.get_news(list(player_ids), since, limit), cursor


def test_merges_sources_newest_first_with_mapped_names():
    items, _ = _news([
        _row(1, "rotowire_rss", None, T0, raw_text="Rotowire says X"),
        _row(2, "reddit_fantasyfootball", None, T0 + timedelta(hours=8), raw_text="Reddit thread Y"),
        _row(3, "espn_news", "ESPN headline Z", T0 + timedelta(hours=16), author="ESPN"),
    ])
    assert [i["source"] for i in items] == ["espn_news", "reddit", "rotowire"]
    assert items[0]["author"] == "ESPN"
    assert items[0]["score"] is None


def test_headline_falls_back_to_first_200_chars_of_raw_text():
    items, _ = _news([_row(1, "rotowire_rss", None, T0, raw_text="x" * 300)])
    assert items[0]["headline"] == "x" * 200


def test_dedupes_across_sources_keeping_earliest():
    items, _ = _news([
        _row(1, "espn_news", "Some Back ruled OUT for Sunday!", T0 + timedelta(hours=1)),
        _row(2, "rotowire_rss", "some back ruled out for sunday", T0),
        _row(3, "bluesky", "Some Back ruled out for Sunday.", T0 + timedelta(hours=2)),
        _row(4, "bluesky", "Some Back ruled out for Sunday", T0 + timedelta(hours=3)),
    ])
    assert len(items) == 1
    assert items[0]["id"] == 2 and items[0]["source"] == "rotowire"
    assert items[0]["also_reported_by"] == ["espn_news", "bluesky"]


def test_same_headline_beyond_six_hours_is_a_new_story():
    items, _ = _news([
        _row(1, "rotowire_rss", "Some Back is questioned", T0),
        _row(2, "espn_news", "Some Back is questioned", T0 + timedelta(hours=7)),
    ])
    assert len(items) == 2
    assert all(i["also_reported_by"] == [] for i in items)


def test_same_headline_for_different_players_is_not_merged():
    items, _ = _news([
        _row(1, "rotowire_rss", "Backfield shake-up", T0, player="p1"),
        _row(2, "espn_news", "Backfield shake-up", T0, player="p2"),
    ], player_ids=("p1", "p2"))
    assert {i["player_id"] for i in items} == {"p1", "p2"}


def test_limit_applies_after_dedupe():
    rows = [_row(i, "rotowire_rss", f"unique story number {i * 7919}", T0 + timedelta(hours=i)) for i in range(1, 6)]
    rows += [_row(10, "espn_news", "unique story number 7919", T0 + timedelta(hours=1, minutes=5))]
    items, cursor = _news(rows, limit=3)
    assert len(items) == 3
    assert cursor.execute.call_args[0][1][-1] == 3 * news_service.FETCH_MULTIPLIER


def test_since_and_confidence_are_passed_to_the_query():
    _, cursor = _news([], since=T0)
    params = cursor.execute.call_args[0][1]
    assert params[:3] == (["p1"], T0, T0)
    assert news_service.BLUESKY_MIN_CONFIDENCE in params


def test_empty_result_and_unknown_player():
    items, _ = _news([], player_ids=("nobody",))
    assert items == []


def test_has_recent_news_marks_unknown_ids_false():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("p1",)]
    with patch("src.services.news_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        flags = news_service.has_recent_news(["p1", "ghost"], now=T0)
    assert flags == {"p1": True, "ghost": False}
    assert cursor.execute.call_args[0][1][1] == T0 - timedelta(hours=24)


def test_normalize_headline():
    assert news_service._normalize_headline("  Some-Back: OUT!!  for  Sunday ") == "someback out for sunday"
