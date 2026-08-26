from unittest.mock import MagicMock, patch

import polars as pl

from src.ingestion import sync_defense_strength as sds


def _stats_row(position, opponent_team, week, fantasy_points_ppr, season_type="REG"):
    return {
        "position": position,
        "opponent_team": opponent_team,
        "week": week,
        "fantasy_points_ppr": fantasy_points_ppr,
        "season_type": season_type,
    }


def _raw_stats_df(rows):
    schema = {
        "position": pl.Utf8, "opponent_team": pl.Utf8, "week": pl.Int64,
        "fantasy_points_ppr": pl.Float64, "season_type": pl.Utf8,
    }
    return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)


def test_load_prior_season_positional_stats_filters_to_reg_and_skill_positions():
    raw = _raw_stats_df([
        _stats_row("WR", "BUF", 1, 12.0, season_type="REG"),
        _stats_row("WR", "BUF", 1, 3.0, season_type="POST"),
        _stats_row("K", "BUF", 1, 8.0, season_type="REG"),
    ])
    with patch("src.ingestion.sync_defense_strength.nfl.load_player_stats", return_value=raw):
        df = sds.load_prior_season_positional_stats(2025)

    assert df.height == 1
    assert df["position"].to_list() == ["WR"]


def test_load_prior_season_positional_stats_normalizes_opponent_team():
    # "LA" is a legacy nflverse code metrics_extractor.normalize_team maps to "LAR" - the
    # new defense_team column must agree with weekly_stats.opponent's own normalization,
    # since vorp_matrix.py joins on it directly.
    raw = _raw_stats_df([_stats_row("RB", "LA", 1, 10.0)])
    with patch("src.ingestion.sync_defense_strength.nfl.load_player_stats", return_value=raw):
        df = sds.load_prior_season_positional_stats(2025)

    assert df["defense_team"].to_list() == ["LAR"]


def test_load_prior_season_positional_stats_empty_source_returns_empty():
    with patch("src.ingestion.sync_defense_strength.nfl.load_player_stats", return_value=_raw_stats_df([])):
        df = sds.load_prior_season_positional_stats(2025)
    assert df.is_empty()


def test_aggregate_points_allowed_by_position_sums_across_players_same_week():
    df_stats = pl.DataFrame({
        "defense_team": ["BUF", "BUF"],
        "position": ["WR", "WR"],
        "week": [1, 1],
        "fantasy_points_ppr": [10.0, 5.0],
    })
    agg = sds.aggregate_points_allowed_by_position(df_stats)
    assert agg.filter(pl.col("defense_team") == "BUF")["points_allowed_per_game"].to_list() == [15.0]


def test_aggregate_points_allowed_by_position_averages_across_weeks():
    df_stats = pl.DataFrame({
        "defense_team": ["BUF", "BUF"],
        "position": ["WR", "WR"],
        "week": [1, 2],
        "fantasy_points_ppr": [10.0, 20.0],
    })
    agg = sds.aggregate_points_allowed_by_position(df_stats)
    assert agg["points_allowed_per_game"].to_list() == [15.0]


def test_aggregate_points_allowed_by_position_empty_input_returns_empty_with_schema():
    empty = pl.DataFrame(
        schema={"defense_team": pl.Utf8, "position": pl.Utf8, "week": pl.Int64, "fantasy_points_ppr": pl.Float64}
    )
    agg = sds.aggregate_points_allowed_by_position(empty)
    assert agg.is_empty()
    assert set(agg.columns) == {"defense_team", "position", "points_allowed_per_game"}


def test_compute_defense_strength_z_clips_extreme_outlier():
    # 31 teams clustered tightly, 1 extreme outlier - enough spread that the raw z-score for
    # the outlier exceeds DEFENSE_Z_CLAMP before clipping, proving the clip actually fires
    # rather than just landing on the bound by coincidence.
    agg = pl.DataFrame({
        "defense_team": [f"T{i}" for i in range(31)] + ["OUT"],
        "position": ["WR"] * 32,
        "points_allowed_per_game": [10.0] * 31 + [1000.0],
    })
    scored = sds.compute_defense_strength_z(agg)
    outlier_z = scored.filter(pl.col("defense_team") == "OUT")["strength_z"].item()
    assert outlier_z == sds.DEFENSE_Z_CLAMP


def test_compute_defense_strength_z_neutral_on_degenerate_group():
    agg = pl.DataFrame({
        "defense_team": ["BUF"],
        "position": ["WR"],
        "points_allowed_per_game": [15.0],
    })
    scored = sds.compute_defense_strength_z(agg)
    assert scored["strength_z"].to_list() == [0.0]


def test_compute_defense_strength_z_empty_input_returns_empty():
    empty = pl.DataFrame(schema={"defense_team": pl.Utf8, "position": pl.Utf8, "points_allowed_per_game": pl.Float64})
    scored = sds.compute_defense_strength_z(empty)
    assert scored.is_empty()
    assert "strength_z" in scored.columns


def test_persist_defense_strength_upserts_on_conflict():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    rows = [("BUF", "WR", 2026, 2025, 15.0, 0.4)]
    written = sds.persist_defense_strength(mock_conn, rows, season=2026, source_season=2025)

    assert written == 1
    query, executed_rows = mock_cursor.executemany.call_args.args
    assert "ON CONFLICT (defense_team, position, season)" in query
    assert executed_rows == rows


def test_persist_defense_strength_noop_on_empty_rows():
    mock_conn = MagicMock()
    assert sds.persist_defense_strength(mock_conn, [], season=2026, source_season=2025) == 0
    mock_conn.cursor.assert_not_called()


def test_sync_defense_strength_uses_season_minus_one_as_source():
    raw = _raw_stats_df([_stats_row("WR", "BUF", 1, 12.0)])
    with patch("src.ingestion.sync_defense_strength.nfl.load_player_stats", return_value=raw) as mock_load, \
         patch("src.ingestion.sync_defense_strength.get_db_connection") as mock_get_conn:
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        result = sds.sync_defense_strength(season=2026)

    mock_load.assert_called_once_with(seasons=[2025], summary_level="week")
    assert result["season"] == 2026
    assert result["source_season"] == 2025
    assert result["rows_written"] == 1


def test_sync_defense_strength_zero_current_season_data():
    """Cold-start acceptance criterion (#93): this job never queries weekly_stats/the
    current season at all, so it must run end-to-end off nflreadpy's prior-season data alone
    even when the target season has zero rows anywhere."""
    raw = _raw_stats_df([_stats_row("RB", "SF", 1, 9.0)])
    with patch("src.ingestion.sync_defense_strength.nfl.load_player_stats", return_value=raw), \
         patch("src.ingestion.sync_defense_strength.get_db_connection") as mock_get_conn:
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn

        result = sds.sync_defense_strength(season=2026)

    assert result["rows_written"] == 1
    mock_conn.cursor.return_value.__enter__.return_value.executemany.assert_called_once()
