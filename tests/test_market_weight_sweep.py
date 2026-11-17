from src.backtesting import market_weight_sweep as sweep


def _summary(waiver, static, lineup, optimal):
    return {
        "waiver": {"our_waiver_points": waiver, "no_moves_points": static},
        "lineup": {"our_lineup_points": lineup, "optimal_lineup_points": optimal, "naive_baseline_points": 0.0},
    }


def test_run_sweep_replays_every_season_and_weight_with_ecr_and_rolling_base():
    calls = []

    def fake_replay(**kwargs):
        calls.append(kwargs)
        return _summary(100.0, 90.0, 50.0, 60.0)

    rows = sweep.run_sweep([2023, 2024], [0.0, 0.1], replay=fake_replay, out_root="/tmp/x")

    assert [(c["season"], c["market_weight"]) for c in calls] == [(2023, 0.0), (2023, 0.1), (2024, 0.0), (2024, 0.1)]
    assert {c["market_source"] for c in calls} == {"ecr"}
    assert {c["base_projection"] for c in calls} == {"rolling"}
    assert len({c["out_dir"] for c in calls}) == 4
    assert rows[0] == {"season": 2023, "weight": 0.0, "waiver_gain": 10.0, "lineup_points": 50.0, "optimal_points": 60.0}


def test_results_table_reports_change_vs_weight_zero_per_season_and_pooled():
    rows = [
        {"season": 2023, "weight": 0.0, "waiver_gain": 10.0, "lineup_points": 50.0, "optimal_points": 100.0},
        {"season": 2023, "weight": 0.1, "waiver_gain": 14.0, "lineup_points": 52.0, "optimal_points": 100.0},
        {"season": 2024, "weight": 0.0, "waiver_gain": 20.0, "lineup_points": 70.0, "optimal_points": 100.0},
        {"season": 2024, "weight": 0.1, "waiver_gain": 18.0, "lineup_points": 69.0, "optimal_points": 100.0},
    ]
    table = sweep.results_table(rows)

    assert "| 2023 | 0.1 | 14.0 | +4.0 | 52.0 | +2.0 | 52.0% |" in table
    assert "| 2024 | 0.1 | 18.0 | -2.0 | 69.0 | -1.0 | 69.0% |" in table
    assert "| all | 0.1 | 32.0 | +2.0 | 121.0 | +1.0 | 60.5% |" in table
