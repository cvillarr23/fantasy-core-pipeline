from copy import deepcopy
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.services import trade_context_service as service


def game(period, home, away=None, home_points=0, away_points=0, winner="UNDECIDED"):
    row = {
        "matchupPeriodId": period,
        "home": {"teamId": home, "totalPoints": home_points},
        "winner": winner,
    }
    if away is not None:
        row["away"] = {"teamId": away, "totalPoints": away_points}
    return row


@pytest.fixture(autouse=True)
def no_committed_offers():
    with patch.object(service, "committed_outgoing_players", return_value={}) as committed:
        yield committed


@pytest.fixture
def raw():
    return {
        "id": 123,
        "seasonId": 2026,
        "scoringPeriodId": 3,
        "status": {"finalScoringPeriod": 18},
        "settings": {
            "scheduleSettings": {
                "matchupPeriodCount": 6,
                "matchupPeriods": {str(w): [w] for w in range(1, 8)},
            },
            "rosterSettings": {
                "lineupSlotCounts": {"0": 1, "2": 2, "4": 2, "6": 1, "23": 1, "20": 7}
            },
        },
        "teams": [
            {
                "id": 1,
                "name": "Us",
                "roster": {
                    "entries": [
                        {
                            "playerId": 100,
                            "lineupSlotId": 2,
                            "playerPoolEntry": {
                                "player": {
                                    "id": 100,
                                    "fullName": "Our Back",
                                    "defaultPositionId": 2,
                                    "proTeamId": 12,
                                    "eligibleSlots": [2, 23, 20],
                                }
                            },
                        }
                    ]
                },
            },
            {
                "id": 2,
                "name": "Them",
                "roster": {
                    "entries": [
                        {
                            "playerId": 500,
                            "lineupSlotId": 20,
                            "playerPoolEntry": {
                                "player": {
                                    "id": 500,
                                    "fullName": "Their Receiver",
                                    "defaultPositionId": 3,
                                    "injuryStatus": "OUT",
                                }
                            },
                        }
                    ]
                },
            },
            {
                "id": 3,
                "name": "Strong",
                "roster": {
                    "entries": [
                        {
                            "playerId": -1,
                            "lineupSlotId": 16,
                            "playerPoolEntry": {
                                "player": {
                                    "id": -1,
                                    "fullName": "Defense",
                                    "defaultPositionId": 16,
                                }
                            },
                        }
                    ]
                },
            },
            {"id": 4, "name": "Weak", "roster": {"entries": []}},
        ],
        "schedule": [
            game(1, 1, 2, 100, 80, "HOME"),
            game(1, 3, 4, 120, 60, "HOME"),
            game(2, 1, 3, 110, 110, "TIE"),
            game(2, 2, 4, 90, 70, "HOME"),
            game(3, 1, 2, 9999, 9999, "UNDECIDED"),
            game(3, 3, 4),
            game(4, 1),
            game(4, 2, 3),
            game(5, 1, 4),
            game(5, 2, 3),
            game(6, 1, 3),
            game(6, 2, 4),
            game(7, 1, 2, 5000, 5000, "HOME"),
        ],
    }


IDENTITIES = {"100": "500", "500": "100"}
METRICS = {
    ("500", 3): {"vorp_score": 4.0, "projected_points": 12.0},
    ("100", 3): {"vorp_score": -2.0, "projected_points": 7.0},
}
FETCHED_AT = "2026-10-05T12:00:00+00:00"


def shape(raw, week=3, horizon=4, identities=None, metrics=None, stored_events=None):
    return service.shape_trade_context(
        raw,
        IDENTITIES if identities is None else identities,
        METRICS if metrics is None else metrics,
        "123",
        2026,
        week,
        horizon,
        FETCHED_AT,
        stored_events=stored_events,
    )


def test_league_wide_current_rosters_preserve_both_id_spaces_and_unknowns(raw):
    result = shape(raw)
    assert set(result["teams"]) == {"1", "2", "3", "4"}
    player = result["teams"]["1"]["roster"][0]
    assert (
        player["espn_id"],
        player["player_id"],
        player["name"],
        player["position"],
        player["slot"],
    ) == ("100", "500", "Our Back", "RB", "RB")
    assert player["eligible_slots"] == ["RB", "RB/WR/TE", "BE"]
    assert player["metrics"]["3"] == METRICS[("500", 3)]
    assert player["metrics"]["4"] == {"vorp_score": None, "projected_points": None}
    theirs = result["teams"]["2"]["roster"][0]
    assert (
        theirs["player_id"],
        theirs["position"],
        theirs["slot"],
        theirs["injury_status"],
    ) == ("100", "WR", "BE", "OUT")
    unknown = result["teams"]["3"]["roster"][0]
    assert unknown["espn_id"] == "-1" and unknown["player_id"] is None
    assert result["coverage"]["unmapped_espn_ids"] == ["-1"]
    assert result["teams"]["4"]["roster"] == []
    assert result["roster_snapshot"] == {
        "kind": "current",
        "source": "ESPN mRoster",
        "fetched_at": FETCHED_AT,
        "observed_week": 3,
        "matches_as_of_week": True,
    }
    assert result["lineup_slot_counts"]["RB/WR/TE"] == 1


def test_strength_uses_only_completed_pre_as_of_fantasy_points(raw):
    result = shape(raw)
    strengths = {
        key: value["strength"]["points_per_week"]
        for key, value in result["teams"].items()
    }
    assert strengths == {"1": 105.0, "2": 85.0, "3": 115.0, "4": 65.0}
    assert result["strength_metric"]["league_mean"] == 92.5
    assert (
        result["strength_metric"]["source"]
        == "ESPN completed regular-season matchup totals"
    )
    assert (
        result["teams"]["3"]["strength"]["z_score"]
        > result["teams"]["2"]["strength"]["z_score"]
    )
    assert result["teams"]["1"]["strength"]["completed_scoring_weeks"] == 2
    raw["schedule"][-1]["home"]["totalPoints"] = 1000000
    assert shape(raw)["teams"]["1"]["strength"] == result["teams"]["1"]["strength"]


def test_horizon_regular_season_and_explicit_byes(raw):
    result = shape(raw, horizon=18)
    assert (
        result["start_week"],
        result["end_week"],
        result["regular_season_end_week"],
    ) == (3, 6, 6)
    us = result["teams"]["1"]
    assert [row["opponent_team_id"] for row in us["schedule"]] == ["2", None, "4", "3"]
    assert [row["status"] for row in us["schedule"]] == [
        "scheduled",
        "bye",
        "scheduled",
        "scheduled",
    ]
    assert us["schedule_coverage"] == {
        "scheduled_weeks": 3,
        "bye_weeks": 1,
        "unavailable_weeks": 0,
        "strength_known_weeks": 3,
    }
    expected = (
        sum(result["teams"][team]["strength"]["z_score"] for team in ("2", "4", "3"))
        / 3
    )
    assert us["schedule_strength_z"] == pytest.approx(expected)
    assert result["teams"]["4"]["schedule"][1]["status"] == "unavailable"
    assert result["teams"]["4"]["schedule_strength_z"] is None


def test_historical_request_marks_current_snapshot_and_avoids_score_leakage(raw):
    result = shape(raw, week=2, horizon=1)
    assert result["roster_snapshot"]["matches_as_of_week"] is False
    assert result["teams"]["1"]["strength"]["points_per_week"] == 100
    assert list(result["teams"]["1"]["roster"][0]["metrics"]) == ["2"]


def test_missing_schedule_is_unknown_not_a_bye(raw):
    raw.pop("schedule")
    us = shape(raw)["teams"]["1"]
    assert all(row["status"] == "unavailable" for row in us["schedule"])
    assert us["strength"]["points_per_week"] is None
    assert us["schedule_strength_z"] is None


def test_partial_strength_coverage_does_not_return_a_complete_schedule_score(raw):
    for row in raw["schedule"]:
        for side in ("home", "away"):
            if row.get(side, {}).get("teamId") == 2:
                row[side]["totalPoints"] = None
    result = shape(raw)
    assert result["teams"]["2"]["strength"]["points_per_week"] is None
    assert result["teams"]["1"]["schedule_coverage"]["strength_known_weeks"] == 2
    assert result["teams"]["1"]["schedule_strength_z"] is None


def test_week_one_and_zero_variance_have_distinct_strength_coverage(raw):
    week_one = shape(raw, week=1)
    assert week_one["strength_metric"]["teams_with_strength"] == 0
    assert all(
        team["strength"]["z_score"] is None for team in week_one["teams"].values()
    )
    for row in raw["schedule"]:
        for side in ("home", "away"):
            if side in row:
                row[side]["totalPoints"] = 100
    equal = shape(raw)
    assert all(team["strength"]["z_score"] == 0 for team in equal["teams"].values())
    assert equal["teams"]["1"]["schedule_strength_z"] == 0


def test_multiweek_matchups_use_actual_scoring_periods_and_points_per_week(raw):
    raw["settings"]["scheduleSettings"] = {
        "matchupPeriodCount": 3,
        "matchupPeriods": {"1": [1, 2], "2": [3, 4], "3": [5, 6], "4": [7, 8]},
    }
    raw["schedule"] = [
        game(1, 1, 2, 200, 160, "HOME"),
        game(1, 3, 4, 240, 120, "HOME"),
        game(2, 1, 3),
        game(2, 2, 4),
        game(3, 1, 4),
        game(3, 2, 3),
    ]
    result = shape(raw)
    assert result["teams"]["1"]["strength"]["points_per_week"] == 100
    assert result["teams"]["1"]["strength"]["completed_scoring_weeks"] == 2
    assert [row["opponent_team_id"] for row in result["teams"]["1"]["schedule"]] == [
        "3",
        "3",
        "4",
        "4",
    ]
    assert shape(raw, week=2)["teams"]["1"]["strength"]["points_per_week"] is None


def test_finished_regular_season_has_empty_horizon(raw):
    result = shape(raw, week=7)
    assert result["end_week"] == 6
    assert result["teams"]["1"]["schedule"] == []
    assert result["teams"]["1"]["schedule_strength_z"] is None
    assert result["teams"]["1"]["roster"][0]["metrics"] == {}


@pytest.mark.parametrize(
    "change",
    [
        lambda raw: raw.update(seasonId=2025),
        lambda raw: raw.update(id=456),
        lambda raw: raw["settings"].pop("scheduleSettings"),
        lambda raw: raw["settings"]["scheduleSettings"]["matchupPeriods"].pop("2"),
        lambda raw: raw["settings"]["scheduleSettings"]["matchupPeriods"].update(
            {"2": [1]}
        ),
        lambda raw: raw["teams"][0].pop("roster"),
        lambda raw: raw["teams"][1]["roster"]["entries"].append(
            deepcopy(raw["teams"][0]["roster"]["entries"][0])
        ),
    ],
)
def test_invalid_source_scope_or_structure_fails_instead_of_fabricating_context(
    raw, change
):
    change(raw)
    with pytest.raises(service.TradeContextUnavailable):
        shape(raw)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), True, "missing"])
def test_nonfinite_or_invalid_scores_are_missing(raw, value):
    for row in raw["schedule"]:
        if row["home"]["teamId"] == 1:
            row["home"]["totalPoints"] = value
    assert shape(raw)["teams"]["1"]["strength"]["points_per_week"] is None


def test_ambiguous_opponent_period_is_unavailable(raw):
    raw["schedule"].append(game(3, 1, 4))
    result = shape(raw)
    assert result["teams"]["1"]["schedule"][0]["status"] == "unavailable"
    assert result["teams"]["1"]["schedule_strength_z"] is None


def test_unknown_team_is_not_treated_as_a_bye(raw):
    raw["schedule"][4]["away"]["teamId"] = 99
    assert shape(raw)["teams"]["1"]["schedule"][0]["status"] == "unavailable"


@pytest.mark.parametrize(
    "args",
    [
        ("456", 2026, 3, 4),
        ("123", 2026, 0, 4),
        ("123", 2026, 3, 19),
        ("123", 1999, 3, 4),
    ],
)
def test_service_validates_scope_and_bounds_before_fetching(monkeypatch, args):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "123")
    with patch.object(service, "build_league") as build:
        with pytest.raises(ValueError):
            service.get_league_trade_context(*args)
    build.assert_not_called()


def test_service_fetches_bounded_live_snapshot_and_batched_identity_metrics(
    monkeypatch, raw
):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "123")
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [("500", "100"), ("100", "500")],
        [("500", 3, 4.0, 12.0), ("100", 3, -2.0, 7.0)],
    ]

    @contextmanager
    def db():
        yield cursor

    with patch.object(service, "build_league") as build, patch.object(
        service, "fetch_raw_league_data", return_value=raw
    ) as fetch, patch.object(service, "get_db_cursor", db), patch.object(
        service, "load_stored_events", return_value={}
    ) as history:
        result = service.get_league_trade_context("123", 2026, 3, 4)
    build.assert_called_once_with(2026, request_timeout_seconds=10)
    fetch.assert_called_once_with(build.return_value, include_schedule=True)
    assert cursor.execute.call_count == 2
    assert set(cursor.execute.call_args_list[0].args[1][0]) == {"100", "500", "-1"}
    assert cursor.execute.call_args_list[1].args[1][:3] == (2026, 3, 6)
    assert result["teams"]["1"]["roster"][0]["player_id"] == "500"
    history.assert_called_once_with("123", 2026)
    assert result["strength_metric"]["teams_with_strength"] == 4
    assert result["strength_metric"]["source"] == (
        "ESPN completed regular-season matchup totals"
    )
    assert result["teams"]["1"]["schedule_strength_z"] is not None
    assert (
        datetime.fromisoformat(result["roster_snapshot"]["fetched_at"]).tzinfo
        is not None
    )


def test_ambiguous_crosswalk_is_reported_as_unmapped(monkeypatch, raw):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "123")
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [("500", "100"), ("other", "100"), ("100", "500")],
        [],
    ]

    @contextmanager
    def db():
        yield cursor

    with patch.object(service, "build_league"), patch.object(
        service, "fetch_raw_league_data", return_value=raw
    ), patch.object(service, "get_db_cursor", db):
        result = service.get_league_trade_context("123", 2026, 3, 4)
    assert result["teams"]["1"]["roster"][0]["player_id"] is None
    assert "100" in result["coverage"]["unmapped_espn_ids"]


def test_service_does_not_use_cached_database_roster_ownership(monkeypatch, raw):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "123")
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[], []]

    @contextmanager
    def db():
        yield cursor

    with patch.object(service, "build_league"), patch.object(
        service, "fetch_raw_league_data", return_value=raw
    ), patch.object(service, "get_db_cursor", db):
        result = service.get_league_trade_context("123", 2026, 3, 4)
    assert result["teams"]["1"]["roster"][0]["espn_id"] == "100"
    assert all(
        "league_rosters" not in call.args[0] for call in cursor.execute.call_args_list
    )


def test_missing_historical_score_coverage_remains_explicit(raw):
    raw["schedule"][2]["home"]["totalPoints"] = None
    strength = shape(raw)["teams"]["1"]["strength"]
    assert strength["points_per_week"] == 100
    assert strength["completed_matchups"] == 1
    assert strength["unavailable_history_periods"] == 1


@pytest.mark.parametrize(
    "position_id,position",
    [(1, "QB"), (2, "RB"), (3, "WR"), (4, "TE"), (5, "K"), (16, "D/ST")],
)
def test_default_player_position_is_distinct_from_lineup_slot(
    raw, position_id, position
):
    raw["teams"][0]["roster"]["entries"][0]["playerPoolEntry"]["player"][
        "defaultPositionId"
    ] = position_id
    player = shape(raw)["teams"]["1"]["roster"][0]
    assert player["position"] == position
    assert player["slot"] == "RB"


def test_context_cache_is_horizon_scoped_and_invalidated_by_verified_moves(
    monkeypatch, raw, read_cache_redis
):
    from src.services.read_cache import invalidate_league

    monkeypatch.setenv("ESPN_LEAGUE_ID", "123")
    cursor = MagicMock()
    cursor.fetchall.return_value = []

    @contextmanager
    def db():
        yield cursor

    with patch.object(service, "build_league"), patch.object(
        service, "fetch_raw_league_data", return_value=raw
    ) as fetch, patch.object(service, "get_db_cursor", db):
        first = service.get_league_trade_context("123", 2026, 3, 4)
        assert service.get_league_trade_context("123", 2026, 3, 4) == first
        assert fetch.call_count == 1
        shorter = service.get_league_trade_context("123", 2026, 3, 1)
        assert shorter["end_week"] == 3
        assert fetch.call_count == 2
        invalidate_league("123")
        service.get_league_trade_context("123", 2026, 3, 4)
        assert fetch.call_count == 3


def test_populated_context_matches_the_public_response_schema(raw):
    from src.optimization.bye_outlook import attach_bye_outlooks
    from src.schemas.trade_context import LeagueTradeContext

    result = attach_bye_outlooks(shape(raw), {"KC": 9, "BUF": 7})
    result["committed_players"] = [{"espn_id": "100", "offer_keys": ["k"]}]
    assert LeagueTradeContext.model_validate(result).model_dump() == result


def test_completed_history_cannot_extend_past_observed_source_week(raw):
    raw["schedule"][4]["winner"] = "HOME"
    result = shape(raw, week=6, horizon=1)
    assert result["teams"]["1"]["strength"]["points_per_week"] == 105
    assert result["roster_snapshot"]["matches_as_of_week"] is False


def test_postseason_observed_period_still_allows_regular_season_context(raw):
    raw["scoringPeriodId"] = 19
    result = shape(raw, week=7)
    assert result["roster_snapshot"]["observed_week"] == 19
    assert result["teams"]["1"]["schedule"] == []


def stored_schedule(raw):
    return dict(
        service._schedule_events(
            raw,
            {str(team["id"]): team for team in raw["teams"]},
            service._regular_periods(raw),
        )
    )


@pytest.mark.parametrize("store_kind", ["empty", "future_only", "incomplete"])
def test_no_completed_stored_history_falls_back_to_live(raw, store_kind):
    events = stored_schedule(raw)
    if store_kind == "empty":
        events = {}
    elif store_kind == "future_only":
        events = {key: value for key, value in events.items() if key[1] >= 3}
    else:
        for matches in events.values():
            matches[0].update(completed=False, points=None)
    result = shape(raw, stored_events=events)
    assert result == shape(raw)
    assert result["strength_metric"]["teams_with_strength"] == 4
    assert result["strength_metric"]["source"] == (
        "ESPN completed regular-season matchup totals"
    )
    assert result["teams"]["1"]["strength"]["points_per_week"] == 105
    assert result["teams"]["1"]["schedule_strength_z"] is not None


@pytest.mark.parametrize("gap", ["lagging_week", "missing_team", "incomplete_score"])
def test_partial_store_falls_back_for_the_whole_league(raw, gap):
    events = stored_schedule(raw)
    if gap == "lagging_week":
        events = {key: value for key, value in events.items() if key[1] != 2}
    elif gap == "missing_team":
        del events[("2", 2)]
    else:
        events[("2", 2)][0].update(points=None, completed=False)
    # Differing stored scores ensure the fallback does not mix the two sources.
    events[("1", 1)][0]["points"] = 500
    original = deepcopy(events)
    assert shape(raw, stored_events=events) == shape(raw)
    assert events == original


def test_complete_store_keeps_stored_scores_and_pairings(raw):
    events = stored_schedule(raw)
    events[("1", 1)][0]["points"] = 200
    del events[("1", 3)]
    result = shape(raw, stored_events=events)
    assert result["strength_metric"]["source"] == (
        "Stored ESPN completed regular-season scoring weeks"
    )
    assert result["teams"]["1"]["strength"]["points_per_week"] == 155
    assert result["teams"]["1"]["schedule"][0]["status"] == "unavailable"


def test_completed_zero_scores_are_usable_stored_history(raw):
    events = stored_schedule(raw)
    for matches in events.values():
        matches[0]["points"] = 0
    result = shape(raw, stored_events=events)
    assert result["strength_metric"]["source"].startswith("Stored ESPN")
    assert result["strength_metric"]["teams_with_strength"] == 4
    assert all(
        team["strength"]["points_per_week"] == 0 for team in result["teams"].values()
    )


def test_gap_in_both_sources_stays_unknown_without_zero_scores(raw):
    raw["schedule"][2]["home"]["totalPoints"] = None
    events = stored_schedule(raw)
    del events[("1", 2)]
    result = shape(raw, stored_events=events)
    assert result["strength_metric"]["source"].startswith("Stored ESPN")
    strength = result["teams"]["1"]["strength"]
    assert strength["points_per_week"] == 100
    assert strength["completed_scoring_weeks"] == 1
    assert strength["unavailable_history_periods"] == 1


def test_empty_store_and_missing_live_scores_remain_unknown(raw):
    raw.pop("schedule")
    result = shape(raw, stored_events={})
    assert result["strength_metric"]["teams_with_strength"] == 0
    assert result["strength_metric"]["source"].startswith("ESPN completed")
    for team in result["teams"].values():
        assert team["strength"]["points_per_week"] is None
        assert team["strength"]["unavailable_history_periods"] == 2
        assert team["schedule_strength_z"] is None


@pytest.mark.parametrize("week,observed_week", [(2, 3), (6, 2)])
def test_later_live_completion_cannot_trigger_fallback(raw, week, observed_week):
    raw["scoringPeriodId"] = observed_week
    events = stored_schedule(raw)
    for key, matches in events.items():
        if key[1] >= 2:
            matches[0].update(completed=False, points=None)
    result = shape(raw, week=week, stored_events=events)
    assert result["strength_metric"]["source"].startswith("Stored ESPN")
    assert result["teams"]["1"]["strength"]["points_per_week"] == 100
    assert result["teams"]["1"]["strength"]["completed_scoring_weeks"] == 1


def test_partial_multiweek_store_uses_completed_live_totals(raw):
    from src.services.score_history import shape_stored_events

    raw["settings"]["scheduleSettings"] = {
        "matchupPeriodCount": 2,
        "matchupPeriods": {"1": [1, 2], "2": [3, 4]},
    }
    raw["schedule"] = [
        game(1, 1, 2, 200, 160, "HOME"),
        game(1, 3, 4, 240, 120, "HOME"),
        game(2, 1, 3),
        game(2, 2, 4),
    ]
    events = shape_stored_events(
        [
            ("1", 1, [1, 2], "2", "scheduled", 1, 100.0),
            ("1", 1, [1, 2], "2", "scheduled", 2, None),
            ("3", 1, [1, 2], "4", "scheduled", 1, 120.0),
            ("3", 1, [1, 2], "4", "scheduled", 2, 120.0),
        ]
    )
    result = shape(raw, stored_events=events)
    assert result == shape(raw)
    assert result["teams"]["1"]["strength"]["points_per_week"] == 100
    assert result["teams"]["1"]["strength"]["completed_scoring_weeks"] == 2


def test_context_lists_committed_players_outside_the_cache(monkeypatch, raw, no_committed_offers):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "123")
    no_committed_offers.return_value = {"500": ["k1"], "100": ["k1", "k2"]}
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[], []]

    @contextmanager
    def db():
        yield cursor

    with patch.object(service, "build_league"), patch.object(
        service, "fetch_raw_league_data", return_value=raw
    ), patch.object(service, "get_db_cursor", db):
        result = service.get_league_trade_context("123", 2026, 3, 4)
    no_committed_offers.assert_called_once_with("123", 2026)
    assert result["committed_players"] == [
        {"espn_id": "100", "offer_keys": ["k1", "k2"]},
        {"espn_id": "500", "offer_keys": ["k1"]},
    ]
