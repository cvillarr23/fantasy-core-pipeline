from unittest.mock import patch

from src.services import score_history, matchup_service, trade_context_service


def test_stored_matchup_avoids_espn_and_current_roster_badges(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "1")
    sides = [
        {
            "team_id": "1",
            "opponent_team_id": "2",
            "is_home": True,
            "team": {"name": "Us", "logo_url": "", "score": 10, "projected": 11},
            "starters": [],
            "bench": [],
        },
        {
            "team_id": "2",
            "opponent_team_id": "1",
            "is_home": False,
            "team": {"name": "Them", "logo_url": "", "score": 9, "projected": 10},
            "starters": [],
            "bench": [],
        },
    ]
    with patch.object(
        matchup_service, "load_completed_week", return_value=sides
    ), patch.object(matchup_service, "_fetch_box_scores") as live, patch.object(
        matchup_service, "suggest_lineup_swaps"
    ) as badges:
        result = matchup_service.get_current_team_matchup(2026, 4)
        board = matchup_service.get_league_scoreboard(2026, 4)
    assert result["our_team"]["team"]["score"] == 10
    assert board["matchups"][0]["away"]["score"] == 9
    live.assert_not_called()
    badges.assert_not_called()


def test_strength_cannot_fall_back_to_live_totals():
    from tests.test_trade_context_service import IDENTITIES, METRICS, FETCHED_AT
    from tests.test_trade_context_service import raw as raw_fixture

    raw = raw_fixture.__wrapped__()
    result = trade_context_service.shape_trade_context(
        raw,
        IDENTITIES,
        METRICS,
        "123",
        2026,
        3,
        4,
        FETCHED_AT,
        stored_events={},
    )
    assert result["teams"]["1"]["strength"]["points_per_week"] is None
    assert result["teams"]["1"]["schedule"][0]["status"] == "unavailable"


def test_history_requires_every_week_of_multiweek_period():
    rows = [
        ("1", 1, [1, 2], "2", "scheduled", 1, 100.0),
        ("1", 1, [1, 2], "2", "scheduled", 2, None),
    ]
    events = score_history.shape_stored_events(rows)
    assert events[("1", 1)][0]["completed"] is False
    rows[-1] = (*rows[-1][:-1], 80.0)
    events = score_history.shape_stored_events(rows)
    assert events[("1", 1)][0]["points"] == 180


def test_stored_context_preserves_strength_formula_and_no_future_leakage():
    from tests.test_trade_context_service import IDENTITIES, METRICS, FETCHED_AT
    from tests.test_trade_context_service import raw as raw_fixture

    raw = raw_fixture.__wrapped__()
    periods = trade_context_service._regular_periods(raw)
    events = trade_context_service._schedule_events(
        raw, {str(t["id"]): t for t in raw["teams"]}, periods
    )
    live = trade_context_service.shape_trade_context(
        raw, IDENTITIES, METRICS, "123", 2026, 3, 4, FETCHED_AT
    )
    stored = trade_context_service.shape_trade_context(
        raw, IDENTITIES, METRICS, "123", 2026, 3, 4, FETCHED_AT, stored_events=events
    )
    assert stored["teams"] == live["teams"]
    assert stored["teams"]["1"]["strength"]["points_per_week"] == 105
    assert stored["strength_metric"]["source"].startswith("Stored ESPN")
