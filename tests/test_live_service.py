import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.services import live_service

NOW = datetime(2026, 11, 23, 18, 0, 0, tzinfo=timezone.utc)
LIVE_EVENT, FINAL_EVENT = "401", "402"

LIVE_GAME = {
    "event_id": LIVE_EVENT, "status": "in_progress", "period": 3, "clock": "8:42",
    "home": {"abbr": "ATL", "score": 17}, "away": {"abbr": "TB", "score": 14},
    "possession": "ATL", "yard_line": "TB 12", "red_zone": True,
}
FINAL_GAME = {
    "event_id": FINAL_EVENT, "status": "final", "period": 4, "clock": None,
    "home": {"abbr": "BUF", "score": 31}, "away": {"abbr": "MIA", "score": 20},
    "possession": None, "yard_line": None, "red_zone": False,
}


def live_player(event_id, team_id, points, **extra):
    return {
        "event_id": event_id, "team": "ATL", "fantasy_team_id": team_id, "lineup_slot_id": 2,
        "stat_line": {"rushYd": 78}, "live_points": points, "updated_at": "2026-11-23T17:59:30Z", **extra,
    }


def play(play_id, player_ids, at, deltas, **extra):
    return json.dumps({
        "play_id": play_id, "period": 3, "clock": "8:42", "text": f"play {play_id}", "player_ids": player_ids,
        "scoring": False, "recorded_at": at, "fantasy_delta": deltas, **extra,
    })


@pytest.fixture
def seeded(live_redis, monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    data = live_redis.data
    data["live:health"] = json.dumps({"season": 2026, "week": 11, "tracked_events": [LIVE_EVENT, FINAL_EVENT]})
    data["live:player_ids"] = json.dumps(["allen", "bijan", "bye", "evans"])
    data[f"live:game:{LIVE_EVENT}"] = json.dumps(LIVE_GAME)
    data[f"live:game:{FINAL_EVENT}"] = json.dumps(FINAL_GAME)
    data["live:player:bijan"] = json.dumps(live_player(LIVE_EVENT, "8", 12.4))
    data["live:player:evans"] = json.dumps(live_player(LIVE_EVENT, "7", 9.0))
    data["live:player:allen"] = json.dumps(live_player(FINAL_EVENT, "8", 23.8))
    data["live:player:bye"] = json.dumps(live_player(None, "8", None, stat_line={}))
    data[f"live:plays:{LIVE_EVENT}"] = [
        play("12", ["bijan"], "2026-11-23T17:59:30Z", {"bijan": 1.2}),
        play("11", ["evans", "bijan"], "2026-11-23T17:57:00Z", {"evans": 6.0, "bijan": None}, scoring=True),
    ]
    data[f"live:plays:{FINAL_EVENT}"] = [play("90", ["allen"], "2026-11-23T17:58:00Z", {"allen": 4.0})]
    return live_redis


@pytest.fixture(autouse=True)
def _db(monkeypatch):
    rows = {
        "bijan": {"name": "Bijan Robinson", "position": "RB", "team": "ATL", "espn_id": "4429795"},
        "evans": {"name": "Mike Evans", "position": "WR", "team": "TB", "espn_id": "15795"},
        "allen": {"name": "Josh Allen", "position": "QB", "team": "BUF", "espn_id": "3918298"},
    }
    monkeypatch.setattr(live_service, "_player_rows", lambda ids: {i: rows[i] for i in ids if i in rows})
    monkeypatch.setattr(live_service.news_service, "get_news", lambda *a, **k: [])


# ---------------------------------------------------------------- live blocks


def test_live_player_merges_player_and_game(seeded):
    block = live_service.load_live_blocks(["bijan"])["bijan"]

    assert block == {
        "status": "in_progress", "period": 3, "clock": "8:42",
        "home_abbr": "ATL", "home_score": 17, "away_abbr": "TB", "away_score": 14,
        "possession": "ATL", "yard_line": "TB 12", "red_zone": True,
        "stat_line": {"rushYd": 78}, "live_points": 12.4, "updated_at": "2026-11-23T17:59:30Z",
    }


def test_final_player_has_final_status_and_no_clock(seeded):
    block = live_service.load_live_blocks(["allen"])["allen"]

    assert (block["status"], block["clock"], block["home_score"], block["live_points"]) == ("final", None, 31, 23.8)


def test_player_without_a_snapshot_is_null(seeded):
    assert live_service.load_live_blocks(["bijan", "ghost"]) == {
        "bijan": live_service.load_live_blocks(["bijan"])["bijan"], "ghost": None,
    }


def test_bye_player_has_a_block_with_a_null_game(seeded):
    block = live_service.load_live_blocks(["bye"])["bye"]

    assert block["status"] is None and block["live_points"] is None and block["stat_line"] == {}


def test_redis_down_reads_as_no_snapshot():
    assert live_service.load_live_blocks(["bijan"]) == {"bijan": None}
    assert live_service.load_feed(2026, 11, 50) == {"items": [], "next_before": None}


# ---------------------------------------------------------------- matchup overlay


def matchup(week=11, season=2026):
    def line(espn_id, status="not_started"):
        return {"espn_id": espn_id, "player_id": espn_id, "game_status": status}

    return {
        "season": season, "week": week,
        "our_team": {"team": {}, "starters": [line("4429795", "in_progress")], "bench": [line("999")]},
        "opponent": {"team": {}, "starters": [line("15795", "not_started")], "bench": []},
    }


def test_overlay_adds_live_and_aligns_game_status(seeded, monkeypatch):
    monkeypatch.setattr(live_service, "_player_ids_by_espn_id", lambda ids: {"4429795": "bijan", "15795": "evans"})
    original = matchup()

    result = live_service.overlay_matchup(original)

    ours = result["our_team"]["starters"][0]
    assert ours["live"]["live_points"] == 12.4
    # The cached line said not_started for Evans; the fresher snapshot wins, so the two agree.
    theirs = result["opponent"]["starters"][0]
    assert theirs["game_status"] == theirs["live"]["status"] == "in_progress"
    assert result["our_team"]["bench"][0]["live"] is None
    assert "live" not in original["our_team"]["starters"][0]


def test_overlay_skips_another_weeks_snapshot(seeded, monkeypatch):
    monkeypatch.setattr(live_service, "_player_ids_by_espn_id", lambda ids: {"4429795": "bijan"})
    original = matchup(week=12)

    assert live_service.overlay_matchup(original) is original


def test_overlay_skips_the_same_week_of_another_season(seeded, monkeypatch):
    monkeypatch.setattr(live_service, "_player_ids_by_espn_id", lambda ids: {"4429795": "bijan"})
    original = matchup(season=2025)

    assert live_service.overlay_matchup(original) is original


def test_health_without_a_season_is_not_live(seeded, monkeypatch):
    # A poller from before `season` was written can't say which season it polls.
    monkeypatch.setattr(live_service, "_player_ids_by_espn_id", lambda ids: {"4429795": "bijan"})
    seeded.data["live:health"] = json.dumps({"week": 11, "tracked_events": [LIVE_EVENT]})
    original = matchup()

    assert live_service.overlay_matchup(original) is original
    assert live_service.load_feed(2026, 11, 50, now=NOW) == {"items": [], "next_before": None}
    assert live_service.load_live_blocks(["bijan"]) == {"bijan": None}
    assert live_service.player_live_summary(MagicMock(), "bijan") is None


def test_overlay_survives_a_lookup_failure(seeded, monkeypatch):
    def boom(ids):
        raise RuntimeError("db down")

    monkeypatch.setattr(live_service, "_player_ids_by_espn_id", boom)
    original = matchup()

    assert live_service.overlay_matchup(original) is original


# ---------------------------------------------------------------- feed


def test_feed_is_newest_first_with_sides_and_deltas(seeded):
    feed = live_service.load_feed(2026, 11, 50, now=NOW)

    assert [(i["play_id"], i["player_id"], i["side"], i["fantasy_delta"]) for i in feed["items"]] == [
        ("12", "bijan", "mine", 1.2),
        ("90", "allen", "mine", 4.0),
        ("11", "evans", "theirs", 6.0),
        ("11", "bijan", "mine", None),
    ]
    first = feed["items"][0]
    assert first["type"] == "play" and first["game_label"] == "TB 14 – 17 ATL"
    assert (first["player_name"], first["position"], first["espn_id"]) == ("Bijan Robinson", "RB", "4429795")
    assert feed["items"][2]["scoring"] is True and feed["items"][0]["scoring"] is False and feed["next_before"] is None


def test_feed_pages_with_before(seeded):
    first = live_service.load_feed(2026, 11, 2, now=NOW)
    second = live_service.load_feed(2026, 11, 2, before=first["next_before"], now=NOW)

    assert [i["play_id"] for i in first["items"]] == ["12", "90"] and first["next_before"]
    assert [i["play_id"] for i in second["items"]] == ["11", "11"] and second["next_before"] is None


def test_feed_unknown_cursor_is_an_empty_page(seeded):
    assert live_service.load_feed(2026, 11, 5, before="play:1:2:3", now=NOW)["items"] == []


def test_feed_is_empty_for_a_week_the_poller_is_not_on(seeded):
    assert live_service.load_feed(2026, 12, 50, now=NOW) == {"items": [], "next_before": None}
    assert live_service.load_feed(2025, 11, 50, now=NOW) == {"items": [], "next_before": None}


def test_feed_players_come_from_the_pollers_index(seeded):
    # A live:player key from a roster the poller no longer writes (still inside its TTL).
    seeded.data["live:player:dropped"] = json.dumps(live_player(LIVE_EVENT, "8", 3.0))
    seeded.data[f"live:plays:{LIVE_EVENT}"].insert(0, play("13", ["dropped"], "2026-11-23T17:59:50Z", {}))

    feed = live_service.load_feed(2026, 11, 50, now=NOW)

    assert "dropped" not in {i["player_id"] for i in feed["items"]}


def _story(story_id, player_id, published_at):
    return {
        "id": story_id, "player_id": player_id, "source": "espn_news", "headline": f"story {story_id}",
        "url": None, "published_at": published_at,
    }


class FakeNews:
    """news_service.get_news over a fixed story list, honoring `until`, newest first."""

    def __init__(self, stories):
        self.stories, self.calls = stories, []

    def __call__(self, ids, since, limit, until=None):
        self.calls.append({"since": since, "until": until, "limit": limit})
        kept = [s for s in self.stories if s["published_at"] >= since and (until is None or s["published_at"] <= until)]
        return sorted(kept, key=lambda s: s["published_at"], reverse=True)[:limit]


def test_feed_pages_reach_older_news(seeded, monkeypatch):
    # Five stories newer than every play, then one older story between plays: paging by two
    # must still reach that older story, not drop it behind the newest five.
    stories = [_story(i, "evans", datetime(2026, 11, 23, 17, 59, 50 - i)) for i in range(1, 6)]
    stories.append(_story(9, "allen", datetime(2026, 11, 23, 17, 57, 30)))
    monkeypatch.setattr(live_service.news_service, "get_news", FakeNews(stories))
    published = {s["id"]: s["published_at"] for s in stories}
    monkeypatch.setattr(live_service.news_service, "story_published_at", published.get)

    cursors, before = [], None
    while True:
        page = live_service.load_feed(2026, 11, 2, before=before, now=NOW)
        cursors += [i["cursor"] for i in page["items"]]
        before = page["next_before"]
        if before is None:
            break

    assert cursors == [
        "news:1", "news:2", "news:3", "news:4", "news:5", "play:401:12:bijan", "play:402:90:allen",
        "news:9", "play:401:11:evans", "play:401:11:bijan",
    ]


def test_news_on_a_later_page_is_bounded_by_the_cursor_time(seeded, monkeypatch):
    news = FakeNews([])
    monkeypatch.setattr(live_service.news_service, "get_news", news)

    live_service.load_feed(2026, 11, 2, before="play:402:90:allen", now=NOW)

    assert news.calls[-1]["until"] == datetime(2026, 11, 23, 17, 58, 0)


def test_news_cursor_that_left_the_window_resumes_from_its_time(seeded, monkeypatch):
    monkeypatch.setattr(live_service.news_service, "get_news", FakeNews([]))
    monkeypatch.setattr(
        live_service.news_service, "story_published_at",
        lambda news_id: datetime(2026, 11, 23, 17, 58, 30) if news_id == 7 else None,
    )

    page = live_service.load_feed(2026, 11, 5, before="news:7", now=NOW)

    assert [i["cursor"] for i in page["items"]] == ["play:402:90:allen", "play:401:11:evans", "play:401:11:bijan"]
    assert live_service.load_feed(2026, 11, 5, before="news:8", now=NOW)["items"] == []


def test_feed_merges_recent_news_by_time(seeded, monkeypatch):
    story = {
        "id": 7, "player_id": "evans", "source": "espn_news", "headline": "Evans limps off", "url": "https://e",
        "published_at": datetime(2026, 11, 23, 17, 58, 30),
    }
    seen = {}

    def get_news(ids, since, limit, until=None):
        seen.update(ids=sorted(ids), since=since, until=until)
        return [story]

    monkeypatch.setattr(live_service.news_service, "get_news", get_news)

    feed = live_service.load_feed(2026, 11, 50, now=NOW)

    assert [i["cursor"] for i in feed["items"]][:3] == [
        "play:401:12:bijan", "news:7", "play:402:90:allen",
    ]
    news = feed["items"][1]
    assert (news["type"], news["side"], news["headline"], news["at"]) == (
        "news", "theirs", "Evans limps off", "2026-11-23T17:58:30Z",
    )
    assert seen["ids"] == ["allen", "bijan", "bye", "evans"]
    assert seen["since"] == datetime(2026, 11, 23, 16, 0, 0) and seen["until"] is None


def test_feed_survives_a_news_failure(seeded, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("db")

    monkeypatch.setattr(live_service.news_service, "get_news", boom)

    assert len(live_service.load_feed(2026, 11, 50, now=NOW)["items"]) == 4


# ---------------------------------------------------------------- last_viewed


def test_touch_last_viewed_writes_epoch_seconds_with_a_ten_minute_ttl(live_redis):
    with patch("src.services.live_service.time.time", return_value=1_700_000_000.9):
        live_service._touch_last_viewed(live_redis)

    assert live_redis.data["live:last_viewed"] == "1700000000"
    assert live_redis.ttl["live:last_viewed"] == 600


def test_touch_last_viewed_never_raises():
    class Down:
        def set(self, *a, **k):
            raise ConnectionError("down")

    live_service._touch_last_viewed(Down())


def test_live_readers_mark_the_page_viewed(seeded, monkeypatch):
    monkeypatch.setattr(live_service, "_player_ids_by_espn_id", lambda ids: {"4429795": "bijan"})
    for read in (
        lambda: live_service.load_live_blocks(["bijan"]),
        lambda: live_service.overlay_matchup(matchup()),
        lambda: live_service.load_feed(2026, 11, 50, now=NOW),
    ):
        seeded.data.pop("live:last_viewed", None)
        read()
        assert seeded.data["live:last_viewed"].isdigit()


def test_another_season_or_week_is_not_a_view(seeded, monkeypatch):
    monkeypatch.setattr(live_service, "_player_ids_by_espn_id", lambda ids: {"4429795": "bijan"})

    live_service.overlay_matchup(matchup(season=2025))
    live_service.overlay_matchup(matchup(week=12))
    live_service.load_feed(2025, 11, 50, now=NOW)

    assert "live:last_viewed" not in seeded.data


def test_nothing_is_a_view_once_the_poller_has_stopped(seeded, monkeypatch):
    monkeypatch.setattr(live_service, "_player_ids_by_espn_id", lambda ids: {"4429795": "bijan"})
    del seeded.data["live:health"]

    live_service.load_live_blocks(["bijan"])
    live_service.overlay_matchup(matchup())
    live_service.player_live_summary(MagicMock(), "bijan")

    assert "live:last_viewed" not in seeded.data


# ---------------------------------------------------------------- player summary


def test_summary_projects_the_polled_season_and_week(seeded):
    cursor = MagicMock()
    cursor.fetchone.return_value = (14.5,)

    live = live_service.player_live_summary(cursor, "bijan")

    assert live == {
        "live_points": 12.4, "projected_points": 14.5, "game_label": "TB 14 – 17 ATL",
        "game_status": "in_progress", "event_id": LIVE_EVENT,
    }
    select = next(c for c in cursor.execute.call_args_list if "weekly_stats" in c.args[0])
    assert select.args[1] == ("bijan", 2026, 11)
    assert seeded.data["live:last_viewed"].isdigit()


def test_summary_projection_failure_is_null_and_rolls_back_only_itself(seeded):
    cursor = MagicMock()

    def execute(sql, params=None):
        if "weekly_stats" in sql:
            raise RuntimeError("statement timeout")

    cursor.execute.side_effect = execute

    live = live_service.player_live_summary(cursor, "bijan")

    assert live["projected_points"] is None and live["live_points"] == 12.4
    statements = [c.args[0] for c in cursor.execute.call_args_list]
    assert statements[0].startswith("SAVEPOINT") and statements[-1].startswith("ROLLBACK TO SAVEPOINT")


def test_summary_is_null_once_the_poller_has_stopped(seeded):
    del seeded.data["live:health"]

    assert live_service.player_live_summary(MagicMock(), "bijan") is None
    assert live_service.load_live_blocks(["bijan"]) == {"bijan": None}
