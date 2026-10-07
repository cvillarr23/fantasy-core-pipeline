from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.services import activity_runner, league_dashboard, matchup_service, read_cache


@pytest.fixture(autouse=True)
def league_env(monkeypatch):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "123")
    monkeypatch.setenv("ESPN_TEAM_ID", "8")


def _team(team_id, name):
    return SimpleNamespace(team_id=team_id, team_name=name, logo_url=f"https://logo/{team_id}")


def _box():
    return SimpleNamespace(
        home_team=_team(8, "Ours"), away_team=_team(9, "Theirs"),
        home_score=101.5, away_score=88.0, home_projected=110.0, away_projected=95.2,
    )


def _read_keys(redis):
    return [key for key in redis.store if key.startswith("ux:read:")]


def test_scoreboard_second_read_is_served_from_redis(read_cache_redis):
    with patch.object(matchup_service, "_fetch_box_scores", return_value=[_box()]) as fetch:
        first = matchup_service.get_league_scoreboard(2026, 5)
        second = matchup_service.get_league_scoreboard(2026, 5)

    fetch.assert_called_once_with(2026, 5)
    assert second == first
    assert second["matchups"][0]["home"]["score"] == 101.5
    [key] = _read_keys(read_cache_redis)
    assert read_cache_redis.ttls[key] == matchup_service.BOX_SCORE_CACHE_TTL_SECONDS


def test_scoreboard_weeks_are_cached_separately(read_cache_redis):
    with patch.object(matchup_service, "_fetch_box_scores", return_value=[_box()]) as fetch:
        matchup_service.get_league_scoreboard(2026, 5)
        matchup_service.get_league_scoreboard(2026, 6)

    assert fetch.call_count == 2
    assert len(_read_keys(read_cache_redis)) == 2


def test_invalidate_league_forces_reload_of_espn_reads(read_cache_redis):
    with patch.object(matchup_service, "_fetch_box_scores", return_value=[_box()]) as fetch, \
         patch.object(matchup_service, "_load_current_team_matchup", return_value={"week": 5}) as matchup, \
         patch.object(matchup_service, "_load_current_team_roster", return_value={"starters": [], "bench": []}) as roster:
        for _ in range(2):
            matchup_service.get_league_scoreboard(2026, 5)
            matchup_service.get_current_team_matchup(2026, 5)
            matchup_service.get_current_team_roster(2026, 5)
        read_cache.invalidate_league("123")
        matchup_service.get_league_scoreboard(2026, 5)
        matchup_service.get_current_team_matchup(2026, 5)
        matchup_service.get_current_team_roster(2026, 5)

    assert (fetch.call_count, matchup.call_count, roster.call_count) == (2, 2, 2)
    assert read_cache_redis.store["ux:version:123"] == "1"


def test_matchup_not_found_is_not_cached(read_cache_redis):
    bye = SimpleNamespace(home_team=_team(8, "Ours"), away_team=None)
    with patch.object(matchup_service, "_fetch_box_scores", return_value=[bye]) as fetch:
        for _ in range(2):
            with pytest.raises(matchup_service.NoMatchupFoundError):
                matchup_service.get_current_team_matchup(2026, 5)

    assert fetch.call_count == 2
    assert _read_keys(read_cache_redis) == []


def test_lineup_roster_default_week_shares_the_explicit_week_entry(read_cache_redis):
    with patch.object(matchup_service, "require_upcoming_week", return_value=5), \
         patch.object(matchup_service, "_load_current_team_roster", return_value={"starters": [], "bench": []}) as load:
        matchup_service.get_current_team_roster(2026)
        matchup_service.get_current_team_roster(2026, 5)
        matchup_service.get_current_team_roster(2026, 6)

    assert [c.args for c in load.call_args_list] == [(2026, 5, "8"), (2026, 6, "8")]
    assert all(
        read_cache_redis.ttls[key] == matchup_service.ROSTER_CACHE_TTL_SECONDS
        for key in _read_keys(read_cache_redis)
    )


def test_reads_fall_back_to_source_when_redis_is_unavailable():
    # The autouse conftest fixture makes Redis unreachable here.
    with patch.object(matchup_service, "_fetch_box_scores", return_value=[_box()]) as fetch:
        matchup_service.get_league_scoreboard(2026, 5)
        matchup_service.get_league_scoreboard(2026, 5)

    assert fetch.call_count == 2


def test_standings_use_deliberate_ttl(read_cache_redis):
    league = SimpleNamespace(teams=[SimpleNamespace(
        standing=1, team_id=8, team_name="Ours", wins=3, losses=1, ties=0,
        points_for=400.5, points_against=350.0,
    )])
    with patch.object(league_dashboard, "build_league", return_value=league) as build:
        league_dashboard.standings(2026)
        league_dashboard.standings(2026)

    build.assert_called_once()
    [key] = _read_keys(read_cache_redis)
    assert read_cache_redis.ttls[key] == 300


def test_verified_move_invalidates_before_publishing_outcome():
    action = {
        "league_id": "123", "execution": "submitted", "outcome": "unknown", "kind": "lineup_updates",
        "payload": {"season": 2026, "team_id": "8",
                    "optimized_swaps": [{"add_player_id": "1", "drop_player_id": "2", "slot": "RB"}]},
    }
    league = SimpleNamespace(teams=[SimpleNamespace(team_id=8, roster=[
        SimpleNamespace(playerId=1, lineupSlot="RB"), SimpleNamespace(playerId=2, lineupSlot="BE"),
    ])])
    order = MagicMock()
    with patch.object(activity_runner, "action_detail", return_value=action), \
         patch.object(activity_runner, "get_db_cursor"), \
         patch.object(activity_runner, "record_event", side_effect=lambda *a, **k: order("record", a[1])), \
         patch.object(read_cache, "invalidate_league", side_effect=lambda league_id: order("invalidate", league_id)):
        activity_runner.reconcile_action("id", {2026: league})

    assert [c.args for c in order.call_args_list][:2] == [("invalidate", "123"), ("record", "verified_complete")]
