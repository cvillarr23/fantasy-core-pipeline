from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from src.ingestion import metrics_extractor as me


def _pbp_frame(rows):
    schema = {
        "week": pl.Int64, "play_type": pl.Utf8, "posteam": pl.Utf8,
        "receiver_id": pl.Utf8, "rusher_id": pl.Utf8, "yardline_100": pl.Int64,
        "sack": pl.Float64, "qb_hit": pl.Float64, "pass_attempt": pl.Float64,
        "game_id": pl.Utf8, "play_id": pl.Float64,
    }
    defaults = {
        "week": 1, "play_type": "pass", "posteam": "BUF", "receiver_id": None, "rusher_id": None,
        "yardline_100": 50, "sack": 0.0, "qb_hit": 0.0, "pass_attempt": 1.0,
        "game_id": "2023_01_BUF_NYJ", "play_id": 1.0,
    }
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=schema)


def _snaps_frame(rows):
    schema = {"week": pl.Int64, "pfr_player_id": pl.Utf8, "team": pl.Utf8, "offense_pct": pl.Float64}
    defaults = {"week": 1, "pfr_player_id": "PfrX", "team": "BUF", "offense_pct": 0.5}
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=schema)


def _players_frame(rows):
    schema = {"gsis_id": pl.Utf8, "pfr_id": pl.Utf8}
    return pl.DataFrame([{**r} for r in rows], schema=schema)


def _participation_frame(rows):
    schema = {"nflverse_game_id": pl.Utf8, "play_id": pl.Float64, "offense_players": pl.Utf8, "offense_positions": pl.Utf8}
    defaults = {"nflverse_game_id": "2023_01_BUF_NYJ", "play_id": 1.0}
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=schema)


def _ngs_passing_frame(rows):
    schema = {"week": pl.Int64, "player_gsis_id": pl.Utf8, "avg_time_to_throw": pl.Float64,
              "aggressiveness": pl.Float64, "avg_intended_air_yards": pl.Float64}
    return pl.DataFrame([{**r} for r in rows], schema=schema)


def _ngs_receiving_frame(rows):
    schema = {"week": pl.Int64, "player_gsis_id": pl.Utf8, "avg_separation": pl.Float64,
              "avg_intended_air_yards": pl.Float64}
    return pl.DataFrame([{**r} for r in rows], schema=schema)


def _ftn_frame(rows):
    schema = {"week": pl.Int64, "nflverse_game_id": pl.Utf8, "nflverse_play_id": pl.Int64,
              "is_contested_ball": pl.Boolean, "is_qb_fault_sack": pl.Boolean}
    defaults = {"week": 1, "nflverse_game_id": "2023_01_BUF_NYJ", "is_contested_ball": False, "is_qb_fault_sack": False}
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=schema)


def _depth_old_frame(rows):
    schema = {"week": pl.Int64, "club_code": pl.Utf8, "gsis_id": pl.Utf8, "position": pl.Utf8, "depth_team": pl.Utf8}
    defaults = {"week": 1, "club_code": "BUF", "position": "WR"}
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=schema)


def _depth_new_frame(rows):
    schema = {"dt": pl.Utf8, "team": pl.Utf8, "gsis_id": pl.Utf8, "pos_abb": pl.Utf8, "pos_rank": pl.Int64}
    defaults = {"team": "BUF", "pos_abb": "WR"}
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=schema)


def _schedule_frame(rows):
    schema = {"week": pl.Int64, "home_team": pl.Utf8, "away_team": pl.Utf8, "spread_line": pl.Float64,
              "total_line": pl.Float64, "home_moneyline": pl.Int64, "away_moneyline": pl.Int64, "gameday": pl.Utf8}
    defaults = {"week": 1}
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=schema)


# ---------------------------------------------------------------------------
# normalize_team
# ---------------------------------------------------------------------------

def test_normalize_team_maps_legacy_abbreviations():
    assert me.normalize_team("OAK") == "LV"
    assert me.normalize_team("SD") == "LAC"
    assert me.normalize_team("BUF") == "BUF"
    assert me.normalize_team(None) is None


# ---------------------------------------------------------------------------
# filter_week_plays
# ---------------------------------------------------------------------------

def test_filter_week_plays_drops_other_weeks_and_non_offensive_plays():
    df = _pbp_frame([
        {"week": 1, "play_type": "pass"},
        {"week": 2, "play_type": "pass"},
        {"week": 1, "play_type": "punt"},
        {"week": 1, "play_type": "run"},
    ])
    result = me.filter_week_plays(df, 1)
    assert result.height == 2
    assert set(result["play_type"].to_list()) == {"pass", "run"}


def test_filter_week_plays_normalizes_team_abbreviation():
    df = _pbp_frame([{"posteam": "OAK"}])
    result = me.filter_week_plays(df, 1)
    assert result["team"].to_list() == ["LV"]


# ---------------------------------------------------------------------------
# compute_usage_shares
# ---------------------------------------------------------------------------

def test_compute_usage_shares_target_share_uses_team_targets_not_pass_plays():
    df_week = _pbp_frame([
        {"receiver_id": "WR1", "posteam": "BUF"},
        {"receiver_id": "WR2", "posteam": "BUF"},
        {"receiver_id": None, "rusher_id": None, "posteam": "BUF", "sack": 1.0, "pass_attempt": 0.0},
    ])
    result = me.compute_usage_shares(me.filter_week_plays(df_week, 1))
    wr1 = result.filter(pl.col("gsis_id") == "WR1").row(0, named=True)
    # 2 real targets thrown (the sack isn't a target), so target_share should be 1/2, not 1/3.
    assert wr1["target_share"] == pytest.approx(0.5)


def test_compute_usage_shares_includes_rush_only_players():
    df_week = _pbp_frame([
        {"receiver_id": "WR1", "posteam": "BUF"},
        {"rusher_id": "RB1", "posteam": "BUF", "play_type": "run", "receiver_id": None, "pass_attempt": 0.0},
    ])
    result = me.compute_usage_shares(me.filter_week_plays(df_week, 1))
    rb_row = result.filter(pl.col("gsis_id") == "RB1")
    assert rb_row.height == 1
    row = rb_row.row(0, named=True)
    assert row["carries"] == 1
    assert row["targets"] == 0
    # the team did throw passes this week, so a zero-target rusher's share is a real 0.0,
    # not a null (null is reserved for when the team had zero targets at all - see the
    # zero-denominator test below).
    assert row["target_share"] == pytest.approx(0.0)


def test_compute_usage_shares_high_value_touches_inside_the_10_only():
    # CORE-04 defines high_value_touches as touches "explicitly inside the opponent's
    # 10-yard line" - the 11-20 range (broader "red zone") must NOT count.
    df_week = _pbp_frame([
        {"receiver_id": "WR1", "posteam": "BUF", "yardline_100": 8},
        {"receiver_id": "WR1", "posteam": "BUF", "yardline_100": 15},
        {"receiver_id": "WR1", "posteam": "BUF", "yardline_100": 40},
    ])
    result = me.compute_usage_shares(me.filter_week_plays(df_week, 1))
    row = result.filter(pl.col("gsis_id") == "WR1").row(0, named=True)
    assert row["high_value_touches"] == 1


def test_compute_usage_shares_target_share_denominator_includes_throwaways():
    # A throwaway is a real pass_attempt with no receiver_id - it must count toward the
    # team's attempts denominator even though it never generates a "target" for anyone.
    df_week = _pbp_frame([
        {"receiver_id": "WR1", "posteam": "BUF", "pass_attempt": 1.0},
        {"receiver_id": None, "rusher_id": None, "posteam": "BUF", "pass_attempt": 1.0},
    ])
    result = me.compute_usage_shares(me.filter_week_plays(df_week, 1))
    row = result.filter(pl.col("gsis_id") == "WR1").row(0, named=True)
    assert row["target_share"] == pytest.approx(0.5)


def test_compute_usage_shares_null_share_when_team_has_zero_targets():
    df_week = _pbp_frame([{
        "rusher_id": "RB1", "posteam": "BUF", "play_type": "run", "receiver_id": None, "pass_attempt": 0.0,
    }])
    result = me.compute_usage_shares(me.filter_week_plays(df_week, 1))
    row = result.filter(pl.col("gsis_id") == "RB1").row(0, named=True)
    # the team threw zero passes this week, so target_share is null (not a computed 0.0/0).
    assert row["target_share"] is None


def test_compute_usage_shares_handles_empty_input():
    result = me.compute_usage_shares(me.filter_week_plays(_pbp_frame([]).clear(), 1))
    assert result.height == 0
    assert "target_share" in result.columns


# ---------------------------------------------------------------------------
# build_gsis_pfr_crosswalk / compute_snap_share
# ---------------------------------------------------------------------------

def test_build_gsis_pfr_crosswalk_drops_null_ids():
    df = _players_frame([
        {"gsis_id": "G1", "pfr_id": "P1"},
        {"gsis_id": None, "pfr_id": "P2"},
        {"gsis_id": "G3", "pfr_id": None},
    ])
    result = me.build_gsis_pfr_crosswalk(df)
    assert result.height == 1
    assert result.row(0, named=True) == {"gsis_id": "G1", "pfr_id": "P1"}


def test_compute_snap_share_joins_via_pfr_id():
    crosswalk = me.build_gsis_pfr_crosswalk(_players_frame([{"gsis_id": "G1", "pfr_id": "PfrX"}]))
    snaps = _snaps_frame([{"pfr_player_id": "PfrX", "team": "BUF", "offense_pct": 0.75}])
    result = me.compute_snap_share(snaps, 1, crosswalk)
    assert result.height == 1
    row = result.row(0, named=True)
    assert row["gsis_id"] == "G1"
    assert row["snap_share"] == pytest.approx(0.75)


def test_compute_snap_share_drops_player_missing_from_crosswalk():
    crosswalk = me.build_gsis_pfr_crosswalk(_players_frame([]))
    snaps = _snaps_frame([{"pfr_player_id": "PfrX"}])
    result = me.compute_snap_share(snaps, 1, crosswalk)
    assert result.height == 0


# ---------------------------------------------------------------------------
# compute_route_participation
# ---------------------------------------------------------------------------

def test_compute_route_participation_counts_skill_position_players_on_dropbacks():
    df_week = me.filter_week_plays(_pbp_frame([
        {"posteam": "BUF", "play_type": "pass", "game_id": "G1", "play_id": 1.0},
        {"posteam": "BUF", "play_type": "pass", "game_id": "G1", "play_id": 2.0},
        {"posteam": "BUF", "play_type": "run", "game_id": "G1", "play_id": 3.0},
    ]), 1)
    participation = _participation_frame([
        {"nflverse_game_id": "G1", "play_id": 1.0, "offense_players": "WR1;OL1;QB1", "offense_positions": "WR;T;QB"},
        {"nflverse_game_id": "G1", "play_id": 2.0, "offense_players": "WR1;OL1;QB1", "offense_positions": "WR;T;QB"},
    ])
    result = me.compute_route_participation(participation, df_week, 1)
    wr1 = result.filter(pl.col("gsis_id") == "WR1").row(0, named=True)
    assert wr1["route_rate"] == pytest.approx(1.0)  # ran a route on both of the 2 dropbacks
    assert result.filter(pl.col("gsis_id") == "QB1").height == 0  # QB isn't a route runner


def test_compute_route_participation_degrades_to_empty_when_feed_missing():
    df_week = me.filter_week_plays(_pbp_frame([{"play_type": "pass"}]), 1)
    result = me.compute_route_participation(pl.DataFrame(), df_week, 1)
    assert result.height == 0
    assert "route_rate" in result.columns


# ---------------------------------------------------------------------------
# normalize_nextgen_stats
# ---------------------------------------------------------------------------

def test_normalize_nextgen_stats_filters_week_zero_season_aggregates():
    passing = _ngs_passing_frame([
        {"week": 0, "player_gsis_id": "QB1", "avg_time_to_throw": 999.0, "aggressiveness": 999.0, "avg_intended_air_yards": 999.0},
        {"week": 1, "player_gsis_id": "QB1", "avg_time_to_throw": 2.5, "aggressiveness": 15.0, "avg_intended_air_yards": 8.0},
    ])
    result = me.normalize_nextgen_stats(passing, _ngs_receiving_frame([]), 1)
    row = result.filter(pl.col("gsis_id") == "QB1").row(0, named=True)
    assert row["avg_time_to_throw"] == pytest.approx(2.5)


def test_normalize_nextgen_stats_merges_passing_and_receiving_rows():
    passing = _ngs_passing_frame([{"week": 1, "player_gsis_id": "QB1", "avg_time_to_throw": 2.5, "aggressiveness": 15.0, "avg_intended_air_yards": 8.0}])
    receiving = _ngs_receiving_frame([{"week": 1, "player_gsis_id": "WR1", "avg_separation": 3.2, "avg_intended_air_yards": 11.0}])
    result = me.normalize_nextgen_stats(passing, receiving, 1)
    assert result.height == 2
    wr1 = result.filter(pl.col("gsis_id") == "WR1").row(0, named=True)
    assert wr1["avg_intended_air_yards"] == pytest.approx(11.0)
    assert wr1["avg_time_to_throw"] is None


# ---------------------------------------------------------------------------
# compute_contested_ball_rate / compute_oline_pressure_metrics
# ---------------------------------------------------------------------------

def test_compute_contested_ball_rate_joins_ftn_to_pbp_by_play_id():
    df_week = me.filter_week_plays(_pbp_frame([
        {"receiver_id": "WR1", "game_id": "G1", "play_id": 1.0},
        {"receiver_id": "WR1", "game_id": "G1", "play_id": 2.0},
    ]), 1)
    ftn = _ftn_frame([
        {"nflverse_game_id": "G1", "nflverse_play_id": 1, "is_contested_ball": True},
        {"nflverse_game_id": "G1", "nflverse_play_id": 2, "is_contested_ball": False},
    ])
    result = me.compute_contested_ball_rate(ftn, df_week, 1)
    row = result.filter(pl.col("gsis_id") == "WR1").row(0, named=True)
    assert row["contested_ball_rate"] == pytest.approx(0.5)


def test_compute_oline_pressure_metrics_excludes_qb_fault_sacks():
    df_week = me.filter_week_plays(_pbp_frame([
        {"posteam": "BUF", "game_id": "G1", "play_id": 1.0, "sack": 1.0, "pass_attempt": 0.0, "qb_hit": 0.0},
        {"posteam": "BUF", "game_id": "G1", "play_id": 2.0, "sack": 0.0, "pass_attempt": 1.0, "qb_hit": 0.0},
    ]), 1)
    ftn = _ftn_frame([{"nflverse_game_id": "G1", "nflverse_play_id": 1, "is_qb_fault_sack": True}])
    result = me.compute_oline_pressure_metrics(df_week, ftn, 1)
    row = result.filter(pl.col("team") == "BUF").row(0, named=True)
    # the only sack is QB-fault, so 0 pressures allowed out of 2 dropbacks
    assert row["pressure_rate_allowed"] == pytest.approx(0.0)
    assert row["pass_block_win_rate"] == pytest.approx(1.0)


def test_compute_oline_pressure_metrics_counts_sack_without_ftn():
    df_week = me.filter_week_plays(_pbp_frame([
        {"posteam": "BUF", "game_id": "G1", "play_id": 1.0, "sack": 1.0, "pass_attempt": 0.0, "qb_hit": 0.0},
    ]), 1)
    result = me.compute_oline_pressure_metrics(df_week, pl.DataFrame(), 1)
    row = result.filter(pl.col("team") == "BUF").row(0, named=True)
    assert row["pressure_rate_allowed"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# compute_team_vegas_context
# ---------------------------------------------------------------------------

def test_compute_team_vegas_context_home_implied_total():
    schedules = _schedule_frame([{
        "home_team": "KC", "away_team": "DET", "spread_line": 3.0, "total_line": 48.0,
        "home_moneyline": -198, "away_moneyline": 164,
    }])
    result = me.compute_team_vegas_context(schedules, 1)
    home = result.filter((pl.col("team") == "KC") & pl.col("is_home")).row(0, named=True)
    assert home["team_implied_total"] == pytest.approx(25.5)


def test_compute_team_vegas_context_away_implied_total():
    schedules = _schedule_frame([{
        "home_team": "KC", "away_team": "DET", "spread_line": 3.0, "total_line": 48.0,
        "home_moneyline": -198, "away_moneyline": 164,
    }])
    result = me.compute_team_vegas_context(schedules, 1)
    away = result.filter((pl.col("team") == "DET") & ~pl.col("is_home")).row(0, named=True)
    assert away["team_implied_total"] == pytest.approx(22.5)
    assert away["spread_line"] == pytest.approx(-3.0)


def test_compute_team_vegas_context_omits_bye_week_team():
    schedules = _schedule_frame([{"week": 2, "home_team": "KC", "away_team": "DET"}])
    result = me.compute_team_vegas_context(schedules, 1)
    assert result.height == 0


# ---------------------------------------------------------------------------
# depth chart ranks / delta
# ---------------------------------------------------------------------------

def test_compute_depth_chart_ranks_old_schema_deduplicates_to_best_rank():
    depth = _depth_old_frame([
        {"gsis_id": "WR1", "position": "WR", "depth_team": "2"},
        {"gsis_id": "WR1", "position": "WR", "depth_team": "1"},  # duplicate row, better rank
        {"gsis_id": "OL1", "position": "T", "depth_team": "1"},  # non-skill position, excluded
    ])
    result = me.compute_depth_chart_ranks(depth, week=1)
    assert result.height == 1
    row = result.row(0, named=True)
    assert row["gsis_id"] == "WR1"
    assert row["depth_chart_rank"] == 1


def test_compute_depth_chart_ranks_new_schema_picks_latest_snapshot_before_cutoff():
    depth = _depth_new_frame([
        {"dt": "2026-08-01T00:00:00Z", "gsis_id": "WR1", "pos_rank": 2},
        {"dt": "2026-08-10T00:00:00Z", "gsis_id": "WR1", "pos_rank": 1},
        {"dt": "2026-08-20T00:00:00Z", "gsis_id": "WR1", "pos_rank": 3},  # after cutoff, excluded
    ])
    result = me.compute_depth_chart_ranks(depth, as_of="2026-08-15T00:00:00Z")
    row = result.row(0, named=True)
    assert row["depth_chart_rank"] == 1


def test_compute_depth_chart_delta_positive_when_player_moves_up():
    current = pl.DataFrame({"gsis_id": ["WR1"], "team": ["BUF"], "depth_chart_rank": [1]})
    prior = pl.DataFrame({"gsis_id": ["WR1"], "team": ["BUF"], "depth_chart_rank": [2]})
    result = me.compute_depth_chart_delta(current, prior)
    assert result.row(0, named=True)["depth_chart_delta"] == 1


# ---------------------------------------------------------------------------
# assemble_weekly_rows
# ---------------------------------------------------------------------------

def test_assemble_weekly_rows_keeps_players_missing_from_supplementary_sources():
    usage = pl.DataFrame({
        "gsis_id": ["WR1"], "team": ["BUF"], "targets": [5], "carries": [0],
        "target_share": [0.3], "rush_share": [None], "high_value_touches": [1],
    }, schema={"gsis_id": pl.Utf8, "team": pl.Utf8, "targets": pl.Int64, "carries": pl.Int64,
               "target_share": pl.Float64, "rush_share": pl.Float64, "high_value_touches": pl.Int64})
    empty = pl.DataFrame()
    result = me.assemble_weekly_rows(usage, empty, empty, empty, empty, empty, empty, empty, empty)
    assert result.height == 1
    row = result.row(0, named=True)
    assert row["gsis_id"] == "WR1"
    assert row.get("snap_share") is None


def test_assemble_weekly_rows_attaches_team_level_data_by_team():
    usage = pl.DataFrame({
        "gsis_id": ["WR1"], "team": ["BUF"], "targets": [5], "carries": [0],
        "target_share": [0.3], "rush_share": [None], "high_value_touches": [1],
    }, schema={"gsis_id": pl.Utf8, "team": pl.Utf8, "targets": pl.Int64, "carries": pl.Int64,
               "target_share": pl.Float64, "rush_share": pl.Float64, "high_value_touches": pl.Int64})
    vegas = pl.DataFrame({
        "team": ["BUF"], "opponent": ["NYJ"], "is_home": [True], "spread_line": [2.5],
        "total_line": [44.5], "team_moneyline": [-135], "opponent_moneyline": [114], "team_implied_total": [23.5],
    })
    empty = pl.DataFrame()
    result = me.assemble_weekly_rows(usage, empty, empty, empty, empty, empty, empty, empty, vegas)
    row = result.row(0, named=True)
    assert row["team_implied_total"] == pytest.approx(23.5)


# ---------------------------------------------------------------------------
# build_upsert_batch
# ---------------------------------------------------------------------------

def _assembled_row(**overrides):
    base = {col: None for col in [
        "gsis_id", "team", "targets", "carries", "target_share", "rush_share", "high_value_touches",
        "snap_share", "route_rate", "avg_separation", "avg_intended_air_yards", "avg_time_to_throw",
        "aggressiveness", "contested_ball_rate", "pressure_rate_allowed", "pass_block_win_rate",
        "depth_chart_rank", "depth_chart_delta", "opponent", "is_home", "spread_line", "total_line",
        "team_moneyline", "opponent_moneyline", "team_implied_total",
    ]}
    base.update(overrides)
    return base


def test_build_upsert_batch_skips_players_with_no_gsis_match():
    assembled = pl.DataFrame([_assembled_row(gsis_id="WR1"), _assembled_row(gsis_id="UNKNOWN")])
    batch = me.build_upsert_batch(assembled, {"WR1": "player_1"}, 2023, 1)
    assert len(batch) == 1
    assert batch[0][0] == "player_1"


def test_build_upsert_batch_emits_none_for_missing_metrics():
    assembled = pl.DataFrame([_assembled_row(gsis_id="WR1")])
    batch = me.build_upsert_batch(assembled, {"WR1": "player_1"}, 2023, 1)
    row = batch[0]
    # snap_share is index 3 in UPSERT_COLUMNS - should be None, not 0.0
    assert row[me.UPSERT_COLUMNS.index("snap_share")] is None


def test_build_upsert_batch_column_order_matches_upsert_columns():
    assembled = pl.DataFrame([_assembled_row(
        gsis_id="WR1", team="BUF", target_share=0.5, snap_share=0.75, depth_chart_rank=1,
    )])
    batch = me.build_upsert_batch(assembled, {"WR1": "player_1"}, 2023, 1)
    row = batch[0]
    assert len(row) == len(me.UPSERT_COLUMNS)
    assert row[me.UPSERT_COLUMNS.index("team")] == "BUF"
    assert row[me.UPSERT_COLUMNS.index("target_share")] == pytest.approx(0.5)
    assert row[me.UPSERT_COLUMNS.index("depth_chart_rank")] == 1


# ---------------------------------------------------------------------------
# persist_weekly_metrics
# ---------------------------------------------------------------------------

def test_persist_weekly_metrics_upsert_query_never_touches_owned_columns():
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    me.persist_weekly_metrics(conn, [("p1", 2023, 1) + (None,) * 24])
    query = cursor.executemany.call_args.args[0]
    assert "ON CONFLICT (player_id, season, week) DO UPDATE" in query
    for owned_column in ["projected_points", "actual_points", "vorp_score"]:
        assert owned_column not in query


def test_persist_weekly_metrics_no_op_on_empty_batch():
    conn = MagicMock()
    written = me.persist_weekly_metrics(conn, [])
    assert written == 0
    conn.cursor.assert_not_called()


# ---------------------------------------------------------------------------
# extract_weekly_metrics (pipeline-level, mocked loaders + DB)
# ---------------------------------------------------------------------------

def _patch_loaders(**overrides):
    """Patches every nflreadpy loader metrics_extractor uses, defaulting to empty frames."""
    defaults = {
        "load_pbp": _pbp_frame([{"receiver_id": "WR1", "posteam": "BUF"}]),
        "load_players": _players_frame([{"gsis_id": "WR1", "pfr_id": "PfrWR1"}]),
        "load_snap_counts": pl.DataFrame(),
        "load_participation": pl.DataFrame(),
        "load_nextgen_stats": pl.DataFrame(),
        "load_ftn_charting": pl.DataFrame(),
        "load_depth_charts": pl.DataFrame(),
        "load_schedules": pl.DataFrame(),
    }
    defaults.update(overrides)
    patches = [patch(f"src.ingestion.metrics_extractor.nfl.{name}", return_value=frame) for name, frame in defaults.items()]
    return patches


def _mock_db(crosswalk_rows):
    """
    `with get_db_connection() as conn: with conn.cursor() as cursor: ...` for the
    crosswalk read, then a plain `with conn.cursor() as cursor: cursor.executemany(...)`
    for persist_weekly_metrics - both go through the same `conn.cursor()` mock, so
    `mock_cursor.executemany` (the `__enter__` result) is where the upsert call lands.
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = crosswalk_rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    ctx = MagicMock()
    ctx.__enter__.return_value = mock_conn
    return ctx, mock_conn, mock_cursor


def test_extract_weekly_metrics_end_to_end_upsert_payload():
    patches = _patch_loaders()
    ctx, mock_conn, mock_cursor = _mock_db([("WR1", "player_1")])
    for p in patches:
        p.start()
    try:
        with patch("src.ingestion.metrics_extractor.get_db_connection", return_value=ctx):
            written = me.extract_weekly_metrics(2023, 1)
    finally:
        for p in patches:
            p.stop()

    assert written == 1
    query, batch = mock_cursor.executemany.call_args.args
    assert "ON CONFLICT (player_id, season, week) DO UPDATE" in query
    assert batch[0][0] == "player_1"
    assert batch[0][1] == 2023
    assert batch[0][2] == 1


def test_extract_weekly_metrics_survives_missing_supplementary_dataset():
    ctx, mock_conn, mock_cursor = _mock_db([("WR1", "player_1")])

    with patch("src.ingestion.metrics_extractor.nfl.load_pbp", return_value=_pbp_frame([{"receiver_id": "WR1", "posteam": "BUF"}])), \
         patch("src.ingestion.metrics_extractor.nfl.load_players", return_value=_players_frame([{"gsis_id": "WR1", "pfr_id": "PfrWR1"}])), \
         patch("src.ingestion.metrics_extractor.nfl.load_snap_counts", return_value=pl.DataFrame()), \
         patch("src.ingestion.metrics_extractor.nfl.load_participation", return_value=pl.DataFrame()), \
         patch("src.ingestion.metrics_extractor.nfl.load_nextgen_stats", return_value=pl.DataFrame()), \
         patch("src.ingestion.metrics_extractor.nfl.load_ftn_charting", side_effect=Exception("FTN feed down")), \
         patch("src.ingestion.metrics_extractor.nfl.load_depth_charts", return_value=pl.DataFrame()), \
         patch("src.ingestion.metrics_extractor.nfl.load_schedules", return_value=pl.DataFrame()), \
         patch("src.ingestion.metrics_extractor.get_db_connection", return_value=ctx):
        written = me.extract_weekly_metrics(2023, 1)

    assert written == 1
    _, batch = mock_cursor.executemany.call_args.args
    assert batch[0][me.UPSERT_COLUMNS.index("contested_ball_rate")] is None


def test_extract_weekly_metrics_aborts_when_pbp_load_fails():
    ctx, mock_conn, mock_cursor = _mock_db([])
    with patch("src.ingestion.metrics_extractor.nfl.load_pbp", side_effect=Exception("nflverse mirror down")), \
         patch("src.ingestion.metrics_extractor.get_db_connection", return_value=ctx):
        written = me.extract_weekly_metrics(2023, 1)

    assert written == 0
    mock_cursor.executemany.assert_not_called()


def test_extract_weekly_metrics_no_plays_for_week_writes_nothing():
    ctx, mock_conn, mock_cursor = _mock_db([])
    with patch("src.ingestion.metrics_extractor.nfl.load_pbp", return_value=_pbp_frame([]).clear()), \
         patch("src.ingestion.metrics_extractor.get_db_connection", return_value=ctx):
        written = me.extract_weekly_metrics(2023, 1)

    assert written == 0
    mock_cursor.executemany.assert_not_called()


def _schedule(games):
    """games: (week, game_type, home_score, away_score) tuples."""
    return pl.DataFrame(
        [{"week": w, "game_type": gt, "home_team": "BUF", "away_team": "NYJ", "home_score": hs, "away_score": as_}
         for w, gt, hs, as_ in games],
        schema={"week": pl.Int64, "game_type": pl.Utf8, "home_team": pl.Utf8, "away_team": pl.Utf8,
                "home_score": pl.Int64, "away_score": pl.Int64},
    )


def test_last_completed_week_skips_a_partially_played_week():
    """Tuesday of Week 5 with Thursday's Week 5 game already final must still pick Week 4:
    ingesting a half-played week would write a partial weekly_stats row set."""
    df = _schedule([(3, "REG", 20, 17), (4, "REG", 24, 10), (4, "REG", 7, 3), (5, "REG", 21, 14), (5, "REG", None, None)])
    assert me.last_completed_week(df) == 4


def test_last_completed_week_is_none_before_any_game_is_final():
    df = _schedule([(1, "REG", None, None), (1, "REG", None, None)])
    assert me.last_completed_week(df) is None


def test_last_completed_week_ignores_postseason_games():
    df = _schedule([(18, "REG", 20, 17), (19, "WC", 30, 27)])
    assert me.last_completed_week(df) == 18


def test_resolve_default_year_week_uses_last_completed_week_from_schedule():
    """The ingestor runs Tuesdays, when nflreadpy's current week is already the upcoming,
    unplayed one (#178) - so the default must come from final scores, not current week."""
    df = _schedule([(3, "REG", 20, 17), (4, "REG", 24, 10), (5, "REG", None, None)])
    with patch("src.ingestion.metrics_extractor.nfl.get_current_season", return_value=2026), \
         patch("src.ingestion.metrics_extractor.nfl.get_current_week", return_value=5), \
         patch("src.ingestion.metrics_extractor.nfl.load_schedules", return_value=df):
        year, week = me.resolve_default_year_week()
    assert (year, week) == (2026, 4)


def test_resolve_default_year_week_falls_back_to_current_week_minus_one_without_schedule():
    with patch("src.ingestion.metrics_extractor.nfl.get_current_season", return_value=2026), \
         patch("src.ingestion.metrics_extractor.nfl.get_current_week", return_value=5), \
         patch("src.ingestion.metrics_extractor.nfl.load_schedules", side_effect=RuntimeError("boom")):
        year, week = me.resolve_default_year_week()
    assert (year, week) == (2026, 4)


def test_resolve_default_year_week_is_none_in_preseason():
    with patch("src.ingestion.metrics_extractor.nfl.get_current_season", return_value=2026), \
         patch("src.ingestion.metrics_extractor.nfl.get_current_week", return_value=1), \
         patch("src.ingestion.metrics_extractor.nfl.load_schedules", side_effect=RuntimeError("boom")):
        year, week = me.resolve_default_year_week()
    assert (year, week) == (2026, None)


def test_resolve_default_year_week_raises_when_week_cannot_be_resolved():
    with patch("src.ingestion.metrics_extractor.nfl.get_current_season", side_effect=RuntimeError("boom")), \
         patch("src.ingestion.metrics_extractor.nfl.get_current_week", side_effect=RuntimeError("boom")), \
         patch("src.ingestion.metrics_extractor.nfl.load_schedules", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError):
            me.resolve_default_year_week()


def test_main_exits_nonzero_when_a_completed_week_writes_nothing(caplog):
    """extract_weekly_metrics returning 0 for a week that should have been played is a
    failure, not a quiet success."""
    with patch("src.ingestion.metrics_extractor.extract_weekly_metrics", return_value=0):
        assert me.main(["--year", "2026", "--week", "4"]) == 1
    assert "0 weekly_stats rows" in caplog.text


def test_main_exits_zero_when_rows_written():
    with patch("src.ingestion.metrics_extractor.extract_weekly_metrics", return_value=812) as mock_extract:
        assert me.main(["--year", "2026", "--week", "4"]) == 0
    mock_extract.assert_called_once_with(year=2026, week=4)


def test_main_defaults_to_last_completed_week():
    with patch("src.ingestion.metrics_extractor.resolve_default_year_week", return_value=(2026, 4)), \
         patch("src.ingestion.metrics_extractor.extract_weekly_metrics", return_value=812) as mock_extract:
        assert me.main([]) == 0
    mock_extract.assert_called_once_with(year=2026, week=4)


def test_main_resolves_last_completed_week_for_an_explicit_year():
    with patch("src.ingestion.metrics_extractor.resolve_default_year_week", return_value=(2025, 18)) as mock_resolve, \
         patch("src.ingestion.metrics_extractor.extract_weekly_metrics", return_value=812) as mock_extract:
        assert me.main(["--year", "2025"]) == 0
    mock_resolve.assert_called_once_with(2025)
    mock_extract.assert_called_once_with(year=2025, week=18)


def test_main_rejects_week_without_year():
    with pytest.raises(SystemExit) as exc:
        me.main(["--week", "4"])
    assert exc.value.code != 0


def test_main_exits_zero_without_ingesting_in_preseason():
    with patch("src.ingestion.metrics_extractor.resolve_default_year_week", return_value=(2026, None)), \
         patch("src.ingestion.metrics_extractor.extract_weekly_metrics") as mock_extract:
        assert me.main([]) == 0
    mock_extract.assert_not_called()


def test_main_exits_nonzero_when_week_cannot_be_resolved():
    with patch("src.ingestion.metrics_extractor.resolve_default_year_week", side_effect=RuntimeError("boom")), \
         patch("src.ingestion.metrics_extractor.extract_weekly_metrics") as mock_extract:
        assert me.main([]) == 1
    mock_extract.assert_not_called()
