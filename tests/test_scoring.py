from unittest.mock import patch

from src.backtesting import scoring


def _roster_pool():
    return [
        {"player_id": "qb1", "name": "QB1", "position": "QB", "vorp_score": 1.0},
        {"player_id": "rb1", "name": "RB1", "position": "RB", "vorp_score": 1.0},
        {"player_id": "rb2", "name": "RB2", "position": "RB", "vorp_score": 1.0},
        {"player_id": "wr1", "name": "WR1", "position": "WR", "vorp_score": 1.0},
        {"player_id": "wr2", "name": "WR2", "position": "WR", "vorp_score": 1.0},
        {"player_id": "te1", "name": "TE1", "position": "TE", "vorp_score": 1.0},
        {"player_id": "rb3", "name": "RB3 (bench)", "position": "RB", "vorp_score": 1.0},
    ]


def test_build_hindsight_pool_swaps_vorp_for_actual_points():
    pool = [{"player_id": "p1", "vorp_score": 99.0}]
    actuals = {"p1": 12.5}
    result = scoring.build_hindsight_pool(pool, actuals)
    assert result[0]["vorp_score"] == 12.5


def test_build_hindsight_pool_missing_player_defaults_zero():
    pool = [{"player_id": "p1", "vorp_score": 99.0}]
    result = scoring.build_hindsight_pool(pool, {})
    assert result[0]["vorp_score"] == 0.0


def test_compute_naive_starters_uses_projected_points():
    pool = [{"player_id": p["player_id"], "name": p["name"], "position": p["position"], "projected_points": p["vorp_score"]} for p in _roster_pool()]
    with patch.object(scoring, "build_and_solve_lineup") as mock_solve:
        mock_solve.return_value = ["fake"]
        result = scoring.compute_naive_starters(pool)
    called_pool = mock_solve.call_args.args[0]
    assert all(p["vorp_score"] == p_orig["vorp_score"] for p, p_orig in zip(called_pool, _roster_pool()))
    assert result == ["fake"]


def test_score_lineup_week_sums_actual_points_for_each_comparator():
    our_starters = [{"player_id": "qb1"}, {"player_id": "rb1"}]
    naive_starters = [{"player_id": "wr1"}]
    actuals = {"qb1": 20.0, "rb1": 10.0, "wr1": 5.0, "rb2": 30.0}
    with patch.object(scoring, "build_and_solve_lineup", return_value=[{"player_id": "rb2"}]):
        result = scoring.score_lineup_week(our_starters, _roster_pool(), naive_starters, actuals)
    assert result == {"our_points": 30.0, "optimal_points": 30.0, "naive_points": 5.0}


def test_score_waivers_sums_weekly_points():
    result = scoring.score_waivers([10.0, 12.0], [8.0, 9.0])
    assert result == {"our_waiver_points": 22.0, "no_moves_points": 17.0}


def test_score_draft_sums_season_totals_by_roster():
    totals = {"a": 100.0, "b": 50.0, "c": 25.0}
    result = scoring.score_draft(["a", "b"], ["b", "c"], totals)
    assert result == {"our_draft_points": 150.0, "pure_adp_draft_points": 75.0}


def test_score_draft_missing_player_defaults_zero():
    result = scoring.score_draft(["missing"], [], {})
    assert result["our_draft_points"] == 0.0


def test_summarize_season_aggregates_weekly_lineup_records():
    weekly = [
        {"our_points": 10.0, "optimal_points": 15.0, "naive_points": 8.0},
        {"our_points": 12.0, "optimal_points": 14.0, "naive_points": 9.0},
    ]
    waiver_summary = {"our_waiver_points": 22.0, "no_moves_points": 17.0}
    draft_summary = {"our_draft_points": 150.0, "pure_adp_draft_points": 75.0}
    result = scoring.summarize_season(weekly, waiver_summary, draft_summary)
    assert result["lineup"] == {
        "our_lineup_points": 22.0, "optimal_lineup_points": 29.0, "naive_baseline_points": 17.0,
    }
    assert result["waiver"] == waiver_summary
    assert result["draft"] == draft_summary
    assert result["weekly"] == weekly
