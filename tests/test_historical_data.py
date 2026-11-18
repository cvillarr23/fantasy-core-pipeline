from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.backtesting import historical_data


def _player_pool():
    return pl.DataFrame({
        "sleeper_id": ["1", "2", "3"],
        "gsis_id": ["00-01", "00-02", "00-03"],
        "fantasypros_id": ["100", "200", "300"],
        "name": ["Alpha", "Beta", "Gamma"],
        "position": ["RB", "WR", "QB"],
        "team": ["AAA", "BBB", "CCC"],
    })


def test_rank_to_synthetic_points_monotonic_and_floored():
    assert historical_data.rank_to_synthetic_points(1) > historical_data.rank_to_synthetic_points(50)
    assert historical_data.rank_to_synthetic_points(10_000) == historical_data._SYNTHETIC_POINTS_FLOOR


def test_load_actual_weekly_stats_filters_regular_season_and_renames():
    raw = pl.DataFrame({
        "player_id": ["00-01", "00-02"],
        "week": [1, 1],
        "season_type": ["REG", "POST"],
        "fantasy_points_ppr": [20.5, 30.0],
    })
    with patch("src.backtesting.historical_data.nfl.load_player_stats", return_value=raw):
        result = historical_data.load_actual_weekly_stats(2023)
    assert result.height == 1
    assert result["gsis_id"].to_list() == ["00-01"]
    assert result["actual_points"].to_list() == [20.5]


def test_load_actual_weekly_stats_handles_empty():
    with patch("src.backtesting.historical_data.nfl.load_player_stats", return_value=pl.DataFrame()):
        result = historical_data.load_actual_weekly_stats(2023)
    assert result.is_empty()
    assert set(result.columns) == {"gsis_id", "week", "actual_points"}


def test_build_player_pool_filters_skill_positions_and_sleeper_id():
    raw = pl.DataFrame({
        "sleeper_id": ["1", None, "3"],
        "gsis_id": ["00-01", "00-02", "00-03"],
        "fantasypros_id": ["100", "200", "300"],
        "name": ["Alpha", "Beta", "Gamma"],
        "position": ["RB", "WR", "K"],
        "team": ["AAA", "BBB", "CCC"],
    })
    with patch("src.backtesting.historical_data.nfl.load_ff_playerids", return_value=raw):
        result = historical_data.build_player_pool(2023)
    assert result["sleeper_id"].to_list() == ["1"]


def test_resolve_adp_uses_rankings_when_available():
    rankings = pl.DataFrame({
        "id": ["200", "100"],
        "player": ["Beta", "Alpha"],
        "pos": ["WR", "RB"],
        "ecr": [2.0, 5.0],
    })
    with patch("src.backtesting.historical_data.load_redraft_rankings", return_value=rankings):
        result = historical_data.resolve_adp(2023, _player_pool())
    assert result.sort("adp_rank")["player_id"].to_list() == ["2", "1"]
    assert result.sort("adp_rank")["adp_rank"].to_list() == [1, 2]


def test_resolve_adp_falls_back_to_prior_season_actuals_when_no_rankings():
    prior_actuals = pl.DataFrame({
        "gsis_id": ["00-01", "00-02", "00-01", "00-02"],
        "actual_points": [10.0, 5.0, 8.0, 25.0],
    })
    with patch("src.backtesting.historical_data.load_redraft_rankings", return_value=pl.DataFrame()):
        result = historical_data.resolve_adp(2023, _player_pool(), prior_season_actuals=prior_actuals)
    # gsis 00-02 totals 30.0, gsis 00-01 totals 18.0 -> sleeper_id 2 ranked ahead of 1
    ranked = result.sort("adp_rank")["player_id"].to_list()
    assert ranked[0] == "2"
    assert ranked[1] == "1"


def test_resolve_adp_no_rankings_no_fallback_returns_empty():
    with patch("src.backtesting.historical_data.load_redraft_rankings", return_value=pl.DataFrame()):
        result = historical_data.resolve_adp(2023, _player_pool())
    assert result.is_empty()


def test_seed_players_upserts_all_rows():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    count = historical_data.seed_players(mock_conn, _player_pool())
    assert count == 3
    mock_cursor.executemany.assert_called_once()


def test_seed_players_empty_pool_noop():
    mock_conn = MagicMock()
    count = historical_data.seed_players(mock_conn, pl.DataFrame(schema={
        "sleeper_id": pl.Utf8, "gsis_id": pl.Utf8, "fantasypros_id": pl.Utf8,
        "name": pl.Utf8, "position": pl.Utf8, "team": pl.Utf8,
    }))
    assert count == 0
    mock_conn.cursor.assert_not_called()


def test_seed_preseason_projections_writes_week_one_only():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    adp = pl.DataFrame({"player_id": ["1", "2"], "adp_rank": [1, 2]})
    count = historical_data.seed_preseason_projections(mock_conn, _player_pool(), adp, 2023)
    assert count == 2
    args, _ = mock_cursor.executemany.call_args
    query = args[0]
    assert "VALUES (%s, %s, 1, %s)" in query


def test_seed_league_settings_upserts_hardcoded_config():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    historical_data.seed_league_settings(mock_conn, 2023)
    args, _ = mock_cursor.execute.call_args
    params = args[1]
    assert params[0] == historical_data.SYNTHETIC_LEAGUE_ID
    assert params[3] == historical_data.HARDCODED_LEAGUE_CONFIG["team_count"]


def _schedule(rows):
    return pl.DataFrame({
        "week": [r[0] for r in rows],
        "game_type": [r[1] for r in rows],
        "gameday": [r[2] for r in rows],
        "home_team": [r[3] for r in rows],
        "away_team": [r[4] for r in rows],
    })


def test_load_week1_kickoff_date_returns_earliest_week1_reg_game():
    schedule = _schedule([
        (1, "REG", "2023-09-10", "BBB", "AAA"),
        (1, "REG", "2023-09-07", "CCC", "DDD"),
        (1, "POST", "2023-09-01", "EEE", "FFF"),
    ])
    with patch("src.backtesting.historical_data.nfl.load_schedules", return_value=schedule):
        result = historical_data.load_week1_kickoff_date(2023)
    assert result == "2023-09-07"


def test_load_week1_kickoff_date_empty_schedule_returns_none():
    with patch("src.backtesting.historical_data.nfl.load_schedules", return_value=pl.DataFrame()):
        assert historical_data.load_week1_kickoff_date(2023) is None


def test_load_redraft_rankings_excludes_scrapes_at_or_after_kickoff():
    rankings = pl.DataFrame({
        "page_type": ["redraft-overall", "redraft-overall", "redraft-overall"],
        "scrape_date": ["2023-08-31", "2023-09-07", "2023-09-14"],
        "id": ["1", "2", "3"],
        "player": ["A", "B", "C"],
        "pos": ["RB", "WR", "QB"],
        "ecr": [1.0, 2.0, 3.0],
    })
    with patch("src.backtesting.historical_data.nfl.load_ff_rankings", return_value=rankings), \
         patch("src.backtesting.historical_data.load_week1_kickoff_date", return_value="2023-09-07"):
        result = historical_data.load_redraft_rankings(2023)
    # only 2023-08-31 is strictly before kickoff; 09-07 and 09-14 are excluded
    assert result["id"].to_list() == ["1"]


def test_load_redraft_rankings_falls_back_to_conservative_cutoff_without_schedule():
    rankings = pl.DataFrame({
        "page_type": ["redraft-overall", "redraft-overall"],
        "scrape_date": ["2023-08-20", "2023-09-01"],
        "id": ["1", "2"],
        "player": ["A", "B"],
        "pos": ["RB", "WR"],
        "ecr": [1.0, 2.0],
    })
    with patch("src.backtesting.historical_data.nfl.load_ff_rankings", return_value=rankings), \
         patch("src.backtesting.historical_data.load_week1_kickoff_date", return_value=None):
        result = historical_data.load_redraft_rankings(2023)
    assert result["id"].to_list() == ["1"]


def test_load_regular_season_week_count_derives_from_schedule():
    schedule = _schedule([(w, "REG", "2023-09-07", "AAA", "BBB") for w in range(1, 19)])
    with patch("src.backtesting.historical_data.nfl.load_schedules", return_value=schedule):
        assert historical_data.load_regular_season_week_count(2023) == 18


def test_load_regular_season_week_count_falls_back_when_no_schedule():
    with patch("src.backtesting.historical_data.nfl.load_schedules", return_value=pl.DataFrame()):
        assert historical_data.load_regular_season_week_count(2023, default=17) == 17


def test_load_teams_playing_by_week_excludes_bye_teams():
    schedule = _schedule([
        (1, "REG", "2023-09-07", "AAA", "BBB"),
        (1, "REG", "2023-09-10", "CCC", "DDD"),
        (2, "REG", "2023-09-17", "AAA", "CCC"),
    ])
    with patch("src.backtesting.historical_data.nfl.load_schedules", return_value=schedule):
        result = historical_data.load_teams_playing_by_week(2023, [1, 2])
    assert result[1] == {"AAA", "BBB", "CCC", "DDD"}
    assert result[2] == {"AAA", "CCC"}
    assert "BBB" not in result[2]  # BBB is on bye in week 2


def test_load_teams_playing_by_week_no_schedule_returns_empty_sets():
    with patch("src.backtesting.historical_data.nfl.load_schedules", return_value=pl.DataFrame()):
        result = historical_data.load_teams_playing_by_week(2023, [1, 2])
    assert result == {1: set(), 2: set()}


def _ecr_schedule():
    return pl.DataFrame({
        "game_type": ["REG", "REG", "REG", "REG", "POST"],
        "week": [1, 1, 2, 2, 19],
        "gameday": ["2023-09-07", "2023-09-10", "2023-09-14", "2023-09-18", "2024-01-13"],
    })


def _ecr_archive():
    return pl.DataFrame({
        "page_type": ["weekly-rb", "weekly-wr", "weekly-k", "redraft-rb", "weekly-rb", "weekly-rb", "weekly-qb", "weekly-wr"],
        # 09-08 is after week 1's Thursday game but before its Sunday: still week 1.
        # 09-13 and 09-15 both precede week 2's last game; the later scrape wins.
        "scrape_date": ["2023-09-08", "2023-09-08", "2023-09-08", "2023-08-20", "2023-09-13", "2023-09-15", "2023-09-15", "2024-01-20"],
        "id": ["100", "200", "900", "100", "100", "100", "999", "200"],
        "pos": ["RB", "WR", "K", "RB", "RB", "RB", "QB", "WR"],
        "ecr": [3.0, 7.0, 1.0, 4.0, 9.0, 2.0, 5.0, 1.0],
    })


def _load_weekly_ecr():
    with patch("src.backtesting.historical_data.nfl.load_ff_rankings", return_value=_ecr_archive()), \
         patch("src.backtesting.historical_data.nfl.load_schedules", return_value=_ecr_schedule()):
        return historical_data.load_weekly_ecr(2023, _player_pool())


def test_load_weekly_ecr_keeps_weekly_skill_pages_mapped_to_their_week():
    out = _load_weekly_ecr().sort(["week", "player_id"])
    assert out.select(["season", "week", "player_id", "pos", "ecr"]).rows() == [
        (2023, 1, "1", "RB", 3.0),
        (2023, 1, "2", "WR", 7.0),
        (2023, 2, "1", "RB", 2.0),
    ]


def test_load_weekly_ecr_takes_the_latest_scrape_when_two_map_to_one_week():
    week2 = _load_weekly_ecr().filter(pl.col("week") == 2)
    assert week2["scrape_date"].unique().to_list() == ["2023-09-15"]


def test_load_weekly_ecr_logs_match_rate_per_position(caplog):
    with caplog.at_level("INFO", logger="BacktestHistoricalData"):
        _load_weekly_ecr()
    # The week-2 QB (fantasypros id 999) has no crosswalk row.
    assert any("QB" in r.message and "0/1" in r.message and r.levelname == "WARNING" for r in caplog.records)
    assert any("RB" in r.message and "2/2" in r.message for r in caplog.records)


def test_scrape_week_is_none_after_the_regular_season():
    reg = _ecr_schedule().filter(pl.col("game_type") == "REG")
    assert historical_data.scrape_week("2023-09-19", reg) is None
    assert historical_data.scrape_week("2023-09-01", reg) == 1


def test_seed_weekly_ecr_inserts_rows():
    mock_conn = MagicMock()
    mock_cursor = mock_conn.cursor.return_value.__enter__.return_value
    assert historical_data.seed_weekly_ecr(mock_conn, _load_weekly_ecr()) == 3
    query, rows = mock_cursor.executemany.call_args.args
    assert "fp_weekly_ecr" in query
    assert (2023, 1, "1", "RB", 3.0, "2023-09-08") in rows
