"""Tests for the live_games poller, against ESPN payloads recorded in tests/fixtures/espn_live/.

`scoreboard_pre_and_final.json` and `summary_final.json`/`summary_pre.json` are week 11 of 2026
(MIN @ SF final, CIN @ WSH not started); `league_matchup.json` is that week's mMatchupScore
payload for fantasy teams 8 and 7, trimmed to the fields the poller reads. CIN @ WSH was then
recorded live: `scoreboard_in_progress.json` and `summary_in_progress.json` at 1:30 of the 2nd
(CIN 10-0, WSH ball at its 39), `scoreboard_red_zone.json` at 7:13 of the 2nd (CIN 3rd & 4 at
the WSH 20).
"""
import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion import live_games as lg
from src.nfl_schedule import WeekResolutionError

FIXTURES = Path(__file__).parent / "fixtures" / "espn_live"
NOW = datetime(2026, 11, 23, 18, 0, tzinfo=timezone.utc)
TEAMS = {"8", "7"}
MNF = "401873078"  # CIN @ WSH, not started in the recorded board
SNF = "401873077"  # MIN @ SF, final
CHASE_BROWN = "p4362238"


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def board():
    return fixture("scoreboard_pre_and_final.json")


@pytest.fixture
def league():
    return fixture("league_matchup.json")


def espn_map(league):
    """Every roster player mapped to `p<espn id>`, standing in for players.player_id."""
    return {str(e["playerId"]): f"p{e['playerId']}" for _, e in lg.roster_entries(league, TEAMS)}


def roster(league, event_by_pro_team=None):
    return lg.shape_roster(league, TEAMS, 2026, 11, espn_map(league), event_by_pro_team or {}, NOW)


def event(board, event_id):
    return next(e for e in board["events"] if e["id"] == event_id)


class FakeRedis:
    """Enough of redis-py for the poller: strings and lists with TTLs, and pipelines."""

    def __init__(self):
        self.data, self.ttl = {}, {}

    def ping(self):
        return True

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ex=None):
        self.data[key] = value
        self.ttl[key] = ex

    def mget(self, keys):
        return [self.data.get(key) for key in keys]

    def lrange(self, key, start, end):
        values = self.data.get(key, [])
        return values[start:] if end == -1 else values[start:end + 1]

    def delete(self, key):
        self.data.pop(key, None)
        self.ttl.pop(key, None)

    def rpush(self, key, *values):
        self.data.setdefault(key, []).extend(values)

    def expire(self, key, seconds):
        self.ttl[key] = seconds

    def pipeline(self):
        return FakePipeline(self)

    def json(self, key):
        return json.loads(self.data[key])


class FakePipeline:
    def __init__(self, redis):
        self.redis, self.ops = redis, []

    def __getattr__(self, name):
        return lambda *args, **kwargs: self.ops.append((name, args, kwargs))

    def execute(self):
        for name, args, kwargs in self.ops:
            getattr(self.redis, name)(*args, **kwargs)


# ---------------------------------------------------------------- games


def test_pre_game_has_kickoff_but_no_score_or_clock(board):
    game = lg.shape_game(event(board, MNF), NOW)

    assert game == {
        "event_id": MNF,
        "status": "not_started",
        "detail": "11/23 - 8:15 PM EST",
        "period": None,
        "clock": None,
        "home": {"abbr": "WAS", "score": None},
        "away": {"abbr": "CIN", "score": None},
        "possession": None,
        "yard_line": None,
        "red_zone": False,
        "kickoff": "2026-11-24T01:15Z",
        "updated_at": "2026-11-23T18:00:00Z",
    }


def test_final_game_has_score_and_no_clock(board):
    game = lg.shape_game(event(board, SNF), NOW)

    assert game["status"] == "final"
    assert game["home"] == {"abbr": "SF", "score": 35}
    assert game["away"] == {"abbr": "MIN", "score": 10}
    assert (game["period"], game["clock"], game["possession"], game["red_zone"]) == (4, None, None, False)


def test_in_progress_game_has_clock_score_and_possession():
    game = lg.shape_game(event(fixture("scoreboard_in_progress.json"), MNF), NOW)

    assert (game["status"], game["detail"]) == ("in_progress", "1:30 - 2nd")
    assert (game["period"], game["clock"]) == (2, "1:30")
    assert (game["home"], game["away"]) == ({"abbr": "WAS", "score": 0}, {"abbr": "CIN", "score": 10})
    assert (game["possession"], game["yard_line"], game["red_zone"]) == ("WAS", "WSH 39", False)


def test_red_zone_comes_from_the_scoreboard():
    game = lg.shape_game(event(fixture("scoreboard_red_zone.json"), MNF), NOW)

    assert (game["possession"], game["yard_line"], game["red_zone"]) == ("CIN", "WSH 20", True)


def test_live_summary_has_no_situation():
    """Why the scoreboard is the only field-position source."""
    assert "situation" not in fixture("summary_in_progress.json")


def test_tracks_only_games_with_a_matchup_player(board, league):
    poller = lg.LiveGamesPoller(None, None, 2026, 11, 11, TEAMS, {})

    events = lg.tracked_events(board, poller._pro_team_ids(league))

    assert MNF in events and SNF in events
    # The recorded board has 13 games; nobody on either roster plays for HOU/IND or LAC/NYJ.
    assert len(events) == 11


# ---------------------------------------------------------------- players


def test_player_carries_espns_live_points_and_stat_line(league):
    gibbs = next(p for p in roster(league, {"8": "401873072"}) if p["full_name"] == "Jahmyr Gibbs")

    assert gibbs["player_id"] == "p4429795"
    assert gibbs["value"]["event_id"] == "401873072"
    assert gibbs["value"]["team"] == "DET"
    assert gibbs["value"]["fantasy_team_id"] == "8"
    assert gibbs["value"]["live_points"] == 27.8
    assert gibbs["value"]["stat_line"] == {"rushYd": 98, "rushTD": 2, "rec": 4, "recYd": 20}
    assert gibbs["value"]["applied_stats"] == {"53": 4, "24": 9.8, "25": 12, "42": 2}
    assert gibbs["value"]["applied_qty"] == {"53": 4, "24": 98, "25": 2, "42": 20}
    assert gibbs["value"]["position"] == "RB"


def test_player_who_hasnt_played_has_unknown_points_not_zero(league):
    brown = next(p for p in roster(league) if p["full_name"] == "Chase Brown")

    assert brown["value"]["live_points"] is None
    assert brown["value"]["stat_line"] == {}
    assert brown["value"]["applied_qty"] == {}


def test_dst_stat_line(league):
    jags = next(p for p in roster(league) if p["full_name"] == "Jaguars D/ST")

    assert jags["is_dst"] and jags["player_id"] == "p-16030"
    assert jags["value"]["stat_line"] == {"sacks": 1, "fumRec": 1, "ptsAllowed": 23, "ydsAllowed": 238}
    assert jags["value"]["live_points"] == 5


def test_unmapped_players_are_skipped(league):
    mapping = espn_map(league)
    del mapping["4429795"]

    players = lg.shape_roster(league, TEAMS, 2026, 11, mapping, {}, NOW)

    assert "Jahmyr Gibbs" not in {p["full_name"] for p in players}
    assert len(players) == len(mapping)


def test_stat_line_sums_two_point_conversions():
    assert lg.stat_line({"3": 250, "19": 1, "26": 1}) == {"passYd": 250, "twoPt": 2}


# ---------------------------------------------------------------- plays


def test_plays_name_matchup_players_newest_first(league):
    plays = lg.shape_plays(fixture("summary_final.json"), roster(league))

    tagged = {pid for p in plays for pid in p["player_ids"]}
    # Cousins and Reichard (MIN) are on team 8; Evans (SF) on team 8; Deebo Samuel Sr. on team 7.
    assert tagged == {"p14880", "p4567104", "p16737", "p3126486"}
    assert (plays[0]["period"], plays[-1]["period"]) == (4, 1)
    kickoff = plays[-1]
    assert kickoff["text"].startswith("W.Reichard kicks")
    assert set(kickoff["player_ids"]) == {"p4567104", "p3126486"}
    assert set(kickoff) == {"play_id", "period", "clock", "text", "player_ids", "scoring"}


def _stamped_play(play_id, *player_ids):
    return {"play_id": play_id, "period": 1, "clock": "5:00", "text": "t", "player_ids": list(player_ids), "scoring": False}


def test_new_play_gets_its_players_point_change_and_recorded_at():
    plays = [_stamped_play("2", "a", "b")]

    stamped = lg.attach_play_deltas(plays, [], {"a": 4.0, "b": None}, {"a": 10.0, "b": 0.8}, NOW)

    assert stamped[0]["recorded_at"] == "2026-11-23T18:00:00Z"
    assert stamped[0]["fantasy_delta"] == {"a": 6.0, "b": 0.8}


def test_delta_is_null_when_a_player_has_several_new_plays_or_no_earlier_snapshot():
    plays = [_stamped_play("3", "a"), _stamped_play("2", "a", "c")]

    stamped = lg.attach_play_deltas(plays, [], {"a": 1.0}, {"a": 5.0, "c": 3.0}, NOW)

    assert stamped[0]["fantasy_delta"] == {"a": None}
    assert stamped[1]["fantasy_delta"] == {"a": None, "c": None}


def test_stored_play_keeps_its_stamp_and_delta():
    stored = {**_stamped_play("1", "a"), "recorded_at": "2026-11-23T17:00:00Z", "fantasy_delta": {"a": 2.0}}

    stamped = lg.attach_play_deltas([_stamped_play("1", "a")], [stored], {"a": 9.0}, {"a": 9.0}, NOW)

    assert stamped[0]["recorded_at"] == "2026-11-23T17:00:00Z"
    assert stamped[0]["fantasy_delta"] == {"a": 2.0}


def test_tick_stamps_new_plays_from_the_previous_snapshot(espn, board, league):
    espn.board = fixture("scoreboard_in_progress.json")
    espn.summaries[MNF] = fixture("summary_in_progress.json")
    redis = FakeRedis()
    poller = make_poller(league, redis)

    poller.tick(NOW)
    first = [json.loads(p) for p in redis.data[f"live:plays:{MNF}"]]
    poller.tick(NOW.replace(minute=1))
    second = [json.loads(p) for p in redis.data[f"live:plays:{MNF}"]]

    assert all("recorded_at" in p and "fantasy_delta" in p for p in first)
    assert [p["recorded_at"] for p in second] == [p["recorded_at"] for p in first]


def _summary_without(summary: dict, play_id: str) -> dict:
    """The summary as an earlier tick saw it: before `play_id` happened."""
    trimmed = json.loads(json.dumps(summary))
    drives = trimmed["drives"]
    for drive in [*drives.get("previous", []), drives.get("current") or {}]:
        drive["plays"] = [p for p in drive.get("plays") or [] if str(p.get("id")) != play_id]
    return trimmed


def _set_live_points(league, espn_id: str, points: float) -> None:
    """Gives one roster player an actual stat row worth `points` this week."""
    entry = next(e for _, e in lg.roster_entries(league, TEAMS) if str(e["playerId"]) == espn_id)
    player = lg._entry_player(entry)
    stats = [s for s in player.get("stats") or [] if s.get("statSourceId") != 0]
    player["stats"] = stats + [{
        "seasonId": 2026, "scoringPeriodId": 11, "statSplitTypeId": 1, "statSourceId": 0,
        "appliedTotal": points, "stats": {}, "appliedStats": {},
    }]


@pytest.fixture
def live_mnf(espn):
    """MNF in progress, with its newest matchup play (a Chase Brown run) not yet seen on the
    first tick, when Brown has 5.0 points."""
    espn.board = fixture("scoreboard_in_progress.json")
    full = fixture("summary_in_progress.json")
    newest = lg.shape_plays(full, roster(espn.league))[0]
    assert newest["player_ids"] == [CHASE_BROWN]
    espn.summaries[MNF] = _summary_without(full, newest["play_id"])
    espn.full_summary, espn.newest = full, newest
    _set_live_points(espn.league, CHASE_BROWN[1:], 5.0)
    return espn


def _stored_play(redis, play_id):
    return next(json.loads(p) for p in redis.data[f"live:plays:{MNF}"] if json.loads(p)["play_id"] == play_id)


def test_new_play_on_the_next_tick_gets_a_delta(live_mnf, league):
    redis = FakeRedis()
    poller = make_poller(league, redis)
    poller.tick(NOW)
    live_mnf.summaries[MNF] = live_mnf.full_summary
    _set_live_points(league, CHASE_BROWN[1:], 5.5)

    poller.tick(NOW + timedelta(minutes=1))

    assert _stored_play(redis, live_mnf.newest["play_id"])["fantasy_delta"] == {CHASE_BROWN: 0.5}


def test_delta_is_null_when_the_previous_tick_skipped_the_events_plays(live_mnf, league):
    # The middle tick's summary fails while live:player keeps moving, so the next tick's
    # baseline already includes the play's points: its delta would read +0.0.
    redis = FakeRedis()
    poller = make_poller(league, redis)
    poller.tick(NOW)
    live_mnf.summaries[MNF] = None
    _set_live_points(league, CHASE_BROWN[1:], 5.5)
    poller.tick(NOW + timedelta(minutes=1))
    live_mnf.summaries[MNF] = live_mnf.full_summary

    poller.tick(NOW + timedelta(minutes=2))

    assert _stored_play(redis, live_mnf.newest["play_id"])["fantasy_delta"] == {CHASE_BROWN: None}


class FlakyListRedis(FakeRedis):
    """A FakeRedis whose LRANGE fails while `fail_lrange` is set."""

    fail_lrange = False

    def lrange(self, key, start, end):
        if self.fail_lrange:
            raise ConnectionError("lrange timed out")
        return super().lrange(key, start, end)


def test_failed_play_history_read_keeps_the_stored_plays(live_mnf, league):
    redis = FlakyListRedis()
    poller = make_poller(league, redis)
    poller.tick(NOW)
    stored = list(redis.data[f"live:plays:{MNF}"])
    live_mnf.summaries[MNF] = live_mnf.full_summary
    redis.fail_lrange = True

    poller.tick(NOW + timedelta(minutes=1))

    assert redis.data[f"live:plays:{MNF}"] == stored
    assert redis.json("live:health")["last_success_at"] == "2026-11-23T18:01:00Z"


def test_a_play_first_seen_after_a_failed_history_read_has_a_null_delta(live_mnf, league):
    redis = FlakyListRedis()
    poller = make_poller(league, redis)
    poller.tick(NOW)
    live_mnf.summaries[MNF] = live_mnf.full_summary
    _set_live_points(league, CHASE_BROWN[1:], 5.5)
    redis.fail_lrange = True
    poller.tick(NOW + timedelta(minutes=1))
    redis.fail_lrange = False

    poller.tick(NOW + timedelta(minutes=2))

    newest = _stored_play(redis, live_mnf.newest["play_id"])
    assert newest["recorded_at"] == "2026-11-23T18:02:00Z"
    assert newest["fantasy_delta"] == {CHASE_BROWN: None}


def test_scoring_plays_are_flagged(league):
    plays = lg.shape_plays(fixture("summary_final.json"), roster(league))

    evans_td = next(p for p in plays if "to M.Evans for 4 yards, TOUCHDOWN" in p["text"])
    assert evans_td["scoring"] is True and evans_td["player_ids"] == ["p16737"]


def _player(first, last, team="1", pid="x", dst=False):
    return {"player_id": pid, "first_name": first, "last_name": last, "full_name": f"{first} {last}",
            "pro_team_id": team, "is_dst": dst}


def _play(text, offense="1", defense="2", **extra):
    return {"id": "1", "text": text, "teamParticipants": [{"id": offense, "type": "offense"},
                                                         {"id": defense, "type": "defense"}], **extra}


@pytest.mark.parametrize("first,last,text", [
    ("Ja'Marr", "Chase", "J.Burrow pass deep right to Ja.Chase for 40 yards"),
    ("Amon-Ra", "St. Brown", "J.Goff pass short to A.St. Brown for 9 yards"),
    ("Harold", "Fannin Jr.", "S.Sanders pass to H.Fannin for 6 yards"),
    ("George", "Kittle", "George Kittle 17 Yd pass from Brock Purdy (Eddy Pineiro Kick)"),
])
def test_name_forms_espn_uses(first, last, text):
    player = _player(first, last)
    patterns = {"x": lg._name_pattern(player)}

    assert lg._play_player_ids(_play(text), [player], patterns) == ["x"]


def test_names_match_whole_words_and_only_the_plays_teams():
    gibbs = _player("Jahmyr", "Gibbs")
    patterns = {"x": lg._name_pattern(gibbs)}

    assert lg._play_player_ids(_play("J.Gibbson up the middle"), [gibbs], patterns) == []
    assert lg._play_player_ids(_play("J.Gibbs up the middle", offense="5", defense="6"), [gibbs], patterns) == []


def test_dst_is_tagged_only_while_its_team_is_on_defense():
    dst = _player("Jaguars", "D/ST", team="2", pid="JAX", dst=True)

    assert lg._play_player_ids(_play("pass INTERCEPTED", isTurnover=True), [dst], {}) == ["JAX"]
    assert lg._play_player_ids(_play("run for 7 yards, TOUCHDOWN", scoringPlay=True), [dst], {}) == ["JAX"]
    assert lg._play_player_ids(_play("sacked for -6", type={"text": "Sack"}), [dst], {}) == ["JAX"]
    assert lg._play_player_ids(_play("run for 3 yards"), [dst], {}) == []
    # Its own offense: a score, a sack taken and an interception thrown don't count.
    own_offense = {"offense": "2", "defense": "1"}
    assert lg._play_player_ids(_play("pass for 18 yards, TOUCHDOWN", scoringPlay=True, **own_offense), [dst], {}) == []
    assert lg._play_player_ids(_play("pass INTERCEPTED", isTurnover=True, **own_offense), [dst], {}) == []


def test_dst_is_tagged_on_its_return_touchdowns():
    dst = _player("Jaguars", "D/ST", team="2", pid="JAX", dst=True)
    play = _play("punt returned for a TOUCHDOWN", offense="2", defense="1", scoringPlay=True,
                 type={"text": "Punt Return Touchdown"})

    assert lg._play_player_ids(play, [dst], {}) == ["JAX"]


def test_plays_are_capped():
    player = _player("Jahmyr", "Gibbs")
    summary = {"drives": {"previous": [{"plays": [
        {**_play(f"J.Gibbs run {i}"), "id": str(i)} for i in range(lg.MAX_PLAYS + 50)
    ]}]}}

    plays = lg.shape_plays(summary, [player])

    assert len(plays) == lg.MAX_PLAYS
    assert plays[0]["play_id"] == str(lg.MAX_PLAYS + 49)


def test_live_summary_tags_both_sides_and_lists_each_play_once(league):
    plays = lg.shape_plays(fixture("summary_in_progress.json"), roster(league))

    tagged = {pid for p in plays for pid in p["player_ids"]}
    assert tagged == {"p4362238", "p4360635"}  # Chase Brown (CIN, team 8), Chig Okonkwo (WSH, team 7)
    assert len({p["play_id"] for p in plays}) == len(plays)
    assert plays[0]["period"] == 2 and plays[-1]["period"] == 1


def test_current_drive_is_included_once():
    player = _player("Jahmyr", "Gibbs")
    last = {**_play("J.Gibbs run"), "id": "9"}
    summary = {"drives": {"previous": [{"plays": [last]}], "current": {"plays": [last, {**last, "id": "10"}]}}}

    assert [p["play_id"] for p in lg.shape_plays(summary, [player])] == ["10", "9"]


# ---------------------------------------------------------------- ticks


def make_poller(league, redis=None):
    return lg.LiveGamesPoller(redis or FakeRedis(), MagicMock(), 2026, 11, 11, TEAMS, espn_map(league))


@pytest.fixture
def espn(board, league):
    """Patches the three ESPN fetches; tests edit `.board`, `.league` and `.summaries`."""
    state = MagicMock()
    state.board, state.league = board, league
    state.summaries = {SNF: fixture("summary_final.json")}
    state.summary_calls = []

    def summary(event_id):
        state.summary_calls.append(event_id)
        return state.summaries.get(event_id)

    with patch.object(lg, "fetch_scoreboard", lambda season, week: state.board), \
            patch.object(lg, "fetch_matchup_payload", lambda *_: state.league), \
            patch.object(lg, "fetch_summary", summary):
        yield state


def test_tick_writes_every_key_with_its_ttl(espn, league):
    redis = FakeRedis()
    poller = make_poller(league, redis)

    games = poller.tick(NOW)

    assert len(games) == 11
    assert redis.json(f"live:game:{MNF}")["status"] == "not_started"
    assert redis.ttl[f"live:game:{MNF}"] == 6 * 3600
    assert redis.json("live:player:p4429795")["live_points"] == 27.8
    assert redis.ttl["live:player:p4429795"] == 6 * 3600
    plays = [json.loads(p) for p in redis.data[f"live:plays:{SNF}"]]
    assert plays and plays[-1]["text"].startswith("W.Reichard kicks")
    assert redis.ttl[f"live:plays:{SNF}"] == 6 * 3600
    assert redis.data["live:updated_at"] == "2026-11-23T18:00:00Z"
    health = redis.json("live:health")
    assert redis.ttl["live:health"] == 10 * 60
    assert health["last_success_at"] == "2026-11-23T18:00:00Z" and health["week"] == 11
    assert health["season"] == 2026
    assert MNF in health["tracked_events"]
    player_ids = json.loads(redis.data["live:player_ids"])
    assert "p4429795" in player_ids and player_ids == sorted(player_ids)
    assert redis.ttl["live:player_ids"] == 6 * 3600


def test_final_game_summary_is_fetched_once_and_pre_game_never(espn, league):
    poller = make_poller(league)

    poller.tick(NOW)
    poller.tick(NOW)

    assert espn.summary_calls.count(SNF) == 1
    assert MNF not in espn.summary_calls


def test_live_game_summary_is_fetched_every_tick(espn, board, league):
    espn.board = fixture("scoreboard_in_progress.json")
    espn.summaries[MNF] = fixture("summary_in_progress.json")
    redis = FakeRedis()
    poller = make_poller(league, redis)

    poller.tick(NOW)
    poller.tick(NOW)

    assert espn.summary_calls.count(MNF) == 2
    assert redis.data[f"live:plays:{MNF}"]


def test_unavailable_summary_keeps_previous_plays_and_scoreboard_state(espn, board, league):
    espn.board = fixture("scoreboard_red_zone.json")
    redis = FakeRedis()
    redis.data[f"live:plays:{MNF}"] = ["previous"]
    poller = make_poller(league, redis)

    games = poller.tick(NOW)

    assert redis.data[f"live:plays:{MNF}"] == ["previous"]
    game = next(g for g in games if g["event_id"] == MNF)
    assert (game["status"], game["red_zone"], game["yard_line"]) == ("in_progress", True, "WSH 20")


def test_failed_tick_still_writes_health(espn, league):
    espn.board = None
    redis = FakeRedis()
    poller = make_poller(league, redis)

    assert poller.tick(NOW) is None

    assert poller.failed_ticks == 1
    assert redis.json("live:health")["last_success_at"] is None
    assert not any(key.startswith("live:game:") for key in redis.data)


def test_tick_that_raises_is_logged_not_fatal(espn, league):
    redis = FakeRedis()
    poller = make_poller(league, redis)

    with patch.object(lg, "tracked_events", side_effect=KeyError("boom")):
        assert poller.tick(NOW) is None

    assert "live:health" in redis.data


def test_league_outage_reuses_the_last_rosters_teams(espn, league):
    redis = FakeRedis()
    poller = make_poller(league, redis)
    poller.tick(NOW)
    espn.league = None

    games = poller.tick(NOW)

    assert len(games) == 11


def test_league_outage_before_any_roster_fails_the_tick(espn, league):
    espn.league = None

    assert make_poller(league).tick(NOW) is None


# ---------------------------------------------------------------- cadence and exit


@pytest.mark.parametrize("value,expected", [
    (str((NOW - timedelta(minutes=2)).timestamp()), 30),
    (str((NOW - timedelta(minutes=6)).timestamp()), 180),
    ((NOW - timedelta(minutes=1)).isoformat(), 30),
    (None, 180),
    ("garbage", 180),
])
def test_cadence_follows_last_viewed(value, expected):
    redis = FakeRedis()
    if value is not None:
        redis.set("live:last_viewed", value)

    assert lg.tick_interval(redis, NOW) == expected


def test_cadence_is_slow_when_redis_cant_say():
    redis = MagicMock()
    redis.get.side_effect = ConnectionError("down")

    assert lg.tick_interval(redis, NOW) == 180


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def monotonic(self):
        return self.t

    def now(self):
        return NOW + timedelta(seconds=self.t)


class FakeStop(threading.Event):
    """A stop event whose wait advances the fake clock instead of sleeping."""

    def __init__(self, clock, on_wait=None):
        super().__init__()
        self.clock, self.on_wait, self.waits = clock, on_wait, []

    def wait(self, timeout=None):
        self.waits.append(timeout)
        self.clock.t += timeout
        if self.on_wait:
            self.on_wait(self.clock.t)
        return self.is_set()


def test_slow_wait_is_cut_short_by_a_new_viewer():
    clock, redis = FakeClock(), FakeRedis()

    def viewer_arrives(t):
        if t >= 40:
            redis.set("live:last_viewed", str(clock.now().timestamp()))

    stop = FakeStop(clock, viewer_arrives)
    lg.wait_for_next_tick(redis, stop, 0.0, monotonic=clock.monotonic, now=clock.now)

    # 10s slices until the viewer shows up at 40s, which is already past the 30s fast interval.
    assert clock.t == 40


def test_slow_wait_runs_the_full_interval():
    clock, redis = FakeClock(), FakeRedis()
    stop = FakeStop(clock)

    lg.wait_for_next_tick(redis, stop, 0.0, monotonic=clock.monotonic, now=clock.now)

    assert clock.t == 180


def test_stop_ends_the_wait():
    clock, redis = FakeClock(), FakeRedis()
    stop = FakeStop(clock, lambda t: stop.set())

    lg.wait_for_next_tick(redis, stop, 0.0, monotonic=clock.monotonic, now=clock.now)

    assert stop.waits == [10]


def _game(status, kickoff=NOW):
    return {"status": status, "kickoff": kickoff.isoformat()}


@pytest.mark.parametrize("games,expected", [
    ([_game("final"), _game("final")], True),
    ([_game("final"), _game("in_progress")], False),
    ([_game("final"), _game("not_started", NOW + timedelta(minutes=20))], False),
    ([_game("final"), _game("not_started", NOW + timedelta(minutes=45))], True),
    ([_game("not_started", NOW - timedelta(minutes=2))], False),
    ([], True),
])
def test_exit_condition(games, expected):
    assert lg.should_exit(games, NOW) is expected


def test_recorded_board_exits_until_mnf_is_near(board, league):
    games = [lg.shape_game(e, NOW) for e in lg.tracked_events(board, {"4", "28"}).values()]

    assert lg.should_exit(games, NOW) is True
    assert lg.should_exit(games, datetime(2026, 11, 24, 0, 50, tzinfo=timezone.utc)) is False


def test_loop_exits_once_the_window_is_over():
    poller = MagicMock()
    poller.tick.side_effect = [None, [_game("in_progress")], [_game("final")]]
    clock = FakeClock()

    reason = lg.run_loop(poller, FakeStop(clock), monotonic=clock.monotonic, now=clock.now)

    assert reason == "window_over"
    assert poller.tick.call_count == 3


def test_once_runs_a_single_tick():
    poller = MagicMock()
    poller.tick.return_value = [_game("in_progress")]
    clock = FakeClock()

    assert lg.run_loop(poller, FakeStop(clock), once=True, monotonic=clock.monotonic, now=clock.now) == "once"
    assert poller.tick.call_count == 1


# ---------------------------------------------------------------- fetch helpers


def test_fetch_retries_then_gives_up():
    with patch.object(lg, "_get_json", side_effect=lg.requests.ConnectionError("down")) as get:
        assert lg.fetch_summary(MNF) is None
    assert get.call_count == lg.FETCH_RETRY_ATTEMPTS


def test_unavailable_summary_body_is_none():
    response = MagicMock(status_code=503)
    response.raise_for_status.side_effect = lg.requests.HTTPError("503")
    with patch.object(lg.requests, "get", return_value=response):
        assert lg.fetch_summary(MNF) is None


def test_scoreboard_for_another_week_is_ignored(board):
    with patch.object(lg, "_get_json", return_value=board):
        assert lg.fetch_scoreboard(2026, 11) is board
        assert lg.fetch_scoreboard(2026, 12) is None


# ---------------------------------------------------------------- main


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "123")
    monkeypatch.setenv("ESPN_TEAM_ID", "8")


def test_main_exits_zero_when_the_season_is_over(env):
    with patch.object(lg, "resolve_upcoming_week", return_value=None):
        assert lg.main(["--season", "2026"]) == 0


def test_main_exits_one_when_the_week_cant_be_resolved(env):
    with patch.object(lg, "resolve_upcoming_week", side_effect=WeekResolutionError("down")):
        assert lg.main(["--season", "2026"]) == 1


def test_main_exits_zero_without_a_scheduled_matchup(env):
    cursor = MagicMock()
    cursor.fetchone.return_value = (11, None, "bye")
    with patch.object(lg, "get_db_cursor") as get_cursor, patch.object(lg, "get_redis_client") as redis:
        get_cursor.return_value.__enter__.return_value = cursor
        assert lg.main(["--season", "2026", "--week", "11"]) == 0
    redis.assert_not_called()


def test_main_polls_our_matchup(env):
    cursor = MagicMock()
    cursor.fetchone.return_value = (11, "7", "scheduled")
    cursor.fetchall.return_value = [("4429795", "9221")]
    with patch.object(lg, "get_db_cursor") as get_cursor, \
            patch.object(lg, "get_redis_client", return_value=FakeRedis()), \
            patch.object(lg, "build_espn_requests"), \
            patch.object(lg, "run_loop", return_value="once") as loop:
        get_cursor.return_value.__enter__.return_value = cursor
        assert lg.main(["--season", "2026", "--week", "11", "--once"]) == 0
    poller = loop.call_args.args[0]
    assert (poller.period, poller.team_ids, poller.espn_to_player) == (11, {"8", "7"}, {"4429795": "9221"})
    assert loop.call_args.kwargs == {"once": True}
