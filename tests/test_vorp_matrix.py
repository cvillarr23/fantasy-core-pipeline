from unittest.mock import MagicMock, patch

import polars as pl

from src.optimization import vorp_matrix as vm


COEFFICIENT_SCHEMA = {
    "player_id": pl.Utf8, "position": pl.Utf8, "projected_points": pl.Float64,
    "team_implied_total": pl.Float64, "avg_separation": pl.Float64,
    "pressure_rate_allowed": pl.Float64, "depth_chart_delta": pl.Int64,
    "team": pl.Utf8, "is_rostered": pl.Boolean, "season": pl.Int64, "week": pl.Int64,
}


def _coef_frame(rows):
    defaults = {
        "player_id": "p1", "position": "WR", "projected_points": 10.0,
        "team_implied_total": 22.0, "avg_separation": None, "pressure_rate_allowed": None,
        "depth_chart_delta": None, "team": "BUF", "is_rostered": False, "season": 2025, "week": 10,
    }
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=COEFFICIENT_SCHEMA)


# ---------------------------------------------------------------------------
# compute_matchup_coefficient
# ---------------------------------------------------------------------------

def _extreme_clamp_rows(player_id, background_count=7):
    """
    With only a handful of points, a z-score computed from a population that includes the
    outlier itself is mathematically bounded (max |z| = sqrt(n-1) at ddof=1), so a small
    fixture can never actually drive the coefficient into its clip. Using 8 rows with 7
    tightly clustered background values and one extreme outlier pushes both the market
    and position-signal z-scores well past Z_CLAMP=2.0, so the final clip(0.85, 1.15) is
    what's actually being exercised.
    """
    rows = [
        {"player_id": f"bg{i}", "position": "WR", "team": f"T{i}", "team_implied_total": 10.0, "avg_separation": 1.0}
        for i in range(background_count)
    ]
    return rows


def test_coefficient_clamps_at_upper_bound_under_extreme_market_and_signal():
    rows = _extreme_clamp_rows("d") + [
        {"player_id": "d", "position": "WR", "team": "DDD", "team_implied_total": 1000.0, "avg_separation": 100.0},
    ]
    df = _coef_frame(rows)
    out = vm.compute_matchup_coefficient(df)
    assert out.filter(pl.col("player_id") == "d")["matchup_coefficient"].item() == 1.15


def test_coefficient_clamps_at_lower_bound_under_extreme_market_and_signal():
    rows = _extreme_clamp_rows("d") + [
        {"player_id": "d", "position": "WR", "team": "DDD", "team_implied_total": -1000.0, "avg_separation": -100.0},
    ]
    df = _coef_frame(rows)
    out = vm.compute_matchup_coefficient(df)
    assert out.filter(pl.col("player_id") == "d")["matchup_coefficient"].item() == 0.85


def test_coefficient_is_neutral_when_signal_column_is_all_null():
    df = _coef_frame([
        {"player_id": "a", "position": "WR", "avg_separation": None, "team": "AAA", "team_implied_total": 22.0},
        {"player_id": "b", "position": "WR", "avg_separation": None, "team": "AAA", "team_implied_total": 22.0},
    ])
    out = vm.compute_matchup_coefficient(df)
    assert out["matchup_coefficient"].to_list() == [1.0, 1.0]
    assert not out["adjusted_projected_points"].is_nan().any()


def test_coefficient_zero_variance_group_yields_zero_z_not_nan():
    """All players on the same implied total (zero std) must not poison the column with
    a division-by-zero NaN."""
    df = _coef_frame([
        {"player_id": "a", "team": "AAA", "team_implied_total": 22.0},
        {"player_id": "b", "team": "BBB", "team_implied_total": 22.0},
    ])
    out = vm.compute_matchup_coefficient(df)
    assert out["matchup_coefficient"].to_list() == [1.0, 1.0]
    assert not out["matchup_coefficient"].is_nan().any()


def test_coefficient_single_row_group_yields_zero_z_not_nan():
    df = _coef_frame([{"player_id": "a", "team": "AAA", "team_implied_total": 22.0}])
    out = vm.compute_matchup_coefficient(df)
    assert out["matchup_coefficient"].item() == 1.0


def test_coefficient_qb_signal_uses_negative_pressure_rate():
    df = _coef_frame([
        {"player_id": "a", "position": "QB", "pressure_rate_allowed": 0.1, "team": "AAA", "team_implied_total": 22.0},
        {"player_id": "b", "position": "QB", "pressure_rate_allowed": 0.9, "team": "AAA", "team_implied_total": 22.0},
    ])
    out = vm.compute_matchup_coefficient(df)
    # Lower pressure allowed (better protection) -> higher coefficient.
    coef_a = out.filter(pl.col("player_id") == "a")["matchup_coefficient"].item()
    coef_b = out.filter(pl.col("player_id") == "b")["matchup_coefficient"].item()
    assert coef_a > coef_b


def test_coefficient_rb_signal_uses_positive_depth_chart_delta():
    """depth_chart_delta (from metrics_extractor.compute_depth_chart_delta) is
    prior_rank - current_rank: POSITIVE means the player climbed the depth chart, i.e.
    improved. The climber must score higher, not lower."""
    df = _coef_frame([
        {"player_id": "a", "position": "RB", "depth_chart_delta": 2, "team": "AAA", "team_implied_total": 22.0},
        {"player_id": "b", "position": "RB", "depth_chart_delta": -2, "team": "AAA", "team_implied_total": 22.0},
    ])
    out = vm.compute_matchup_coefficient(df)
    coef_a = out.filter(pl.col("player_id") == "a")["matchup_coefficient"].item()
    coef_b = out.filter(pl.col("player_id") == "b")["matchup_coefficient"].item()
    assert coef_a > coef_b


# ---------------------------------------------------------------------------
# resolve_flex_allocation / largest-remainder rounding
# ---------------------------------------------------------------------------

def test_flex_allocation_falls_back_to_default_weights_when_rosters_empty():
    empty_rosters = pl.DataFrame(schema={"slot_position": pl.Utf8, "position": pl.Utf8})
    alloc = vm.resolve_flex_allocation(10, empty_rosters)
    assert alloc == {"RB": 5, "WR": 4, "TE": 1}


def test_flex_allocation_uses_observed_rosters_when_at_least_ten_rows():
    rows = (
        [{"slot_position": "RB/WR/TE", "position": "RB"}] * 6
        + [{"slot_position": "RB/WR/TE", "position": "WR"}] * 3
        + [{"slot_position": "RB/WR/TE", "position": "TE"}] * 1
    )
    roster_df = pl.DataFrame(rows, schema={"slot_position": pl.Utf8, "position": pl.Utf8})
    alloc = vm.resolve_flex_allocation(10, roster_df)
    assert alloc == {"RB": 6, "WR": 3, "TE": 1}


def test_flex_allocation_falls_back_below_ten_flex_rows():
    rows = [{"slot_position": "RB/WR/TE", "position": "RB"}] * 5
    roster_df = pl.DataFrame(rows, schema={"slot_position": pl.Utf8, "position": pl.Utf8})
    alloc = vm.resolve_flex_allocation(10, roster_df)
    assert alloc == {"RB": 5, "WR": 4, "TE": 1}


def test_largest_remainder_allocation_sums_back_to_total():
    weights = {"RB": 0.53, "WR": 0.31, "TE": 0.16}
    alloc = vm._largest_remainder_allocate(weights, 7)
    assert sum(alloc.values()) == 7


def test_largest_remainder_allocation_zero_total():
    alloc = vm._largest_remainder_allocate({"RB": 0.5, "WR": 0.5}, 0)
    assert alloc == {"RB": 0, "WR": 0}


def test_resolve_all_flex_allocations_only_counts_configured_slot_types():
    """A league with only RB/WR/TE configured (this repo's real league) must not have its
    zero-count RB/WR and WR/TE entries contribute anything."""
    league_config = {"team_count": 10, "lineup_slot_counts": {"RB/WR/TE": 1, "RB/WR": 0, "WR/TE": 0}}
    empty_rosters = pl.DataFrame(schema={"slot_position": pl.Utf8, "position": pl.Utf8})
    alloc = vm.resolve_all_flex_allocations(league_config, empty_rosters)
    assert alloc == {"RB": 5, "WR": 4, "TE": 1}


def test_resolve_all_flex_allocations_sums_across_multiple_slot_types():
    """A league using RB/WR/TE AND WR/TE FLEX slots must have both contribute to the
    combined per-position allocation, not just the first/largest one."""
    league_config = {"team_count": 10, "lineup_slot_counts": {"RB/WR/TE": 1, "WR/TE": 1}}
    empty_rosters = pl.DataFrame(schema={"slot_position": pl.Utf8, "position": pl.Utf8})
    alloc = vm.resolve_all_flex_allocations(league_config, empty_rosters)
    # RB/WR/TE (10 slots, default weights 0.5/0.4/0.1) -> RB5/WR4/TE1
    # WR/TE (10 slots, renormalized WR/TE-only weights 0.8/0.2) -> WR8/TE2
    assert alloc["RB"] == 5
    assert alloc["WR"] == 4 + 8
    assert alloc["TE"] == 1 + 2


def test_resolve_flex_allocation_restricts_default_weights_to_eligible_positions():
    empty_rosters = pl.DataFrame(schema={"slot_position": pl.Utf8, "position": pl.Utf8})
    alloc = vm.resolve_flex_allocation(10, empty_rosters, eligible_positions=("WR", "TE"))
    assert "RB" not in alloc
    assert sum(alloc.values()) == 10


# ---------------------------------------------------------------------------
# calculate_replacement_baselines
# ---------------------------------------------------------------------------

def _deep_pool_frame(position: str, count: int, start: float = 100.0, step: float = 1.0):
    rows = [
        {"player_id": f"{position}{i}", "position": position, "adjusted_projected_points": start - i * step}
        for i in range(count)
    ]
    schema = {"player_id": pl.Utf8, "position": pl.Utf8, "adjusted_projected_points": pl.Float64}
    return rows, schema


def test_baseline_lands_on_rank_n_plus_1_for_real_ten_team_league_shape():
    """Real league shape: 10 teams, 1 QB / 2 RB / 2 WR / 1 TE + 1 FLEX, default FLEX
    weights. rank_at should compute to QB 10, RB 25, WR 24, TE 11, and each baseline
    should equal the (rank_at + 1)-th ranked player's adjusted_projected_points."""
    league_config = {
        "team_count": 10,
        "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1},
    }
    empty_rosters = pl.DataFrame(schema={"slot_position": pl.Utf8, "position": pl.Utf8})
    # 1 FLEX slot per team x 10 teams = 10 league-wide FLEX slots to distribute.
    flex_alloc = vm.resolve_flex_allocation(10, empty_rosters)
    rank_at = vm.calculate_rank_at(league_config, flex_alloc)

    # K/D-ST included too (VORP_POSITIONS) - both 0 since league_config has no slot for
    # either here, unlike the FLEX-bearing skill positions this test is actually about.
    assert rank_at == {"QB": 10, "RB": 25, "WR": 24, "TE": 11, "K": 0, "D/ST": 0}

    all_rows, schema = [], {"player_id": pl.Utf8, "position": pl.Utf8, "adjusted_projected_points": pl.Float64}
    for pos, count in [("QB", 30), ("RB", 40), ("WR", 40), ("TE", 30)]:
        rows, _ = _deep_pool_frame(pos, count)
        all_rows.extend(rows)
    df = pl.DataFrame(all_rows, schema=schema)

    baselines = vm.calculate_replacement_baselines(df, league_config, flex_alloc, rosters_populated=False)

    for pos in ["QB", "RB", "WR", "TE"]:
        expected = 100.0 - rank_at[pos] * 1.0
        assert baselines[pos] == expected


def test_baseline_computed_for_k_and_dst_not_left_at_zero():
    """Regression test for PR #86 review: K/D-ST are in VORP_POSITIONS now, so a
    populated pool gets a real rank_at-based baseline instead of falling through to
    DEFAULT_BASELINES/0.0 - the earlier version of this fix left their vorp_score equal to
    raw projected_points, not comparable to a skill position's delta-over-replacement
    score, which could rank a mediocre D/ST above a genuinely above-replacement WR."""
    league_config = {
        "team_count": 10,
        "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "K": 1, "D/ST": 1},
    }
    rows, schema = _deep_pool_frame("D/ST", 32)  # real-world-sized D/ST pool (32 NFL teams)
    df = pl.DataFrame(rows, schema=schema)

    baselines = vm.calculate_replacement_baselines(df, league_config, {}, rosters_populated=False)

    # rank_at["D/ST"] = 1 slot x 10 teams = 10 -> the 11th-ranked (index 10) D/ST's points.
    assert baselines["D/ST"] == df.sort(["adjusted_projected_points", "player_id"], descending=[True, False]).row(10, named=True)["adjusted_projected_points"]
    assert baselines["D/ST"] != 0.0


def test_baseline_falls_back_to_pool_minimum_when_pool_shorter_than_rank():
    league_config = {"team_count": 10, "lineup_slot_counts": {"QB": 1}}
    rows, schema = _deep_pool_frame("QB", 3)
    df = pl.DataFrame(rows, schema=schema)

    baselines = vm.calculate_replacement_baselines(df, league_config, {}, rosters_populated=False)

    assert baselines["QB"] == 98.0  # min of the 3-player pool (100, 99, 98)


def test_baseline_empty_pool_uses_default_baselines():
    schema = {"player_id": pl.Utf8, "position": pl.Utf8, "adjusted_projected_points": pl.Float64}
    df = pl.DataFrame(schema=schema)
    league_config = {"team_count": 10, "lineup_slot_counts": {}}

    baselines = vm.calculate_replacement_baselines(df, league_config, {}, rosters_populated=False)

    assert baselines == vm.DEFAULT_BASELINES


def test_baseline_uses_best_actual_non_starter_when_rosters_populated():
    """Post-draft, rank N+1 in raw sorted order is not reliable - a benched
    high-projection player can occupy that rank without being a starter. The baseline
    must be the best player NOT currently starting (a true replacement/waiver pickup),
    even if raw rank alone would have picked someone else."""
    league_config = {"team_count": 1, "lineup_slot_counts": {"RB": 1}}
    df = pl.DataFrame(
        [
            # Rank 1 by projection, but rostered as a bench player -> not the baseline.
            {"player_id": "bench_star", "position": "RB", "adjusted_projected_points": 20.0, "is_starter": False},
            {"player_id": "starter", "position": "RB", "adjusted_projected_points": 15.0, "is_starter": True},
            # Best actual non-starter/free agent -> this is the true replacement.
            {"player_id": "waiver_pickup", "position": "RB", "adjusted_projected_points": 10.0, "is_starter": False},
            {"player_id": "deep_bench", "position": "RB", "adjusted_projected_points": 5.0, "is_starter": False},
        ],
        schema={"player_id": pl.Utf8, "position": pl.Utf8, "adjusted_projected_points": pl.Float64, "is_starter": pl.Boolean},
    )

    baselines = vm.calculate_replacement_baselines(df, league_config, {}, rosters_populated=True)

    # Raw rank_at=1 (1 team x 1 RB slot) would have landed on "starter" (15.0) by rank
    # alone; the real answer is the best non-starter, "bench_star" (20.0).
    assert baselines["RB"] == 20.0


def test_baseline_falls_back_to_structural_rank_when_rosters_populated_but_no_non_starter_found():
    """Defensive fallback: if every player at a position is somehow marked a starter
    (shouldn't happen with real data, but must not crash), fall back to the structural
    rank_at approach rather than erroring."""
    league_config = {"team_count": 1, "lineup_slot_counts": {"RB": 1}}
    df = pl.DataFrame(
        [
            {"player_id": "a", "position": "RB", "adjusted_projected_points": 20.0, "is_starter": True},
            {"player_id": "b", "position": "RB", "adjusted_projected_points": 15.0, "is_starter": True},
        ],
        schema={"player_id": pl.Utf8, "position": pl.Utf8, "adjusted_projected_points": pl.Float64, "is_starter": pl.Boolean},
    )

    baselines = vm.calculate_replacement_baselines(df, league_config, {}, rosters_populated=True)

    assert baselines["RB"] == 15.0  # rank_at=1 -> second-ranked player, structural fallback


# ---------------------------------------------------------------------------
# compute_vorp — negative VORP regression guard
# ---------------------------------------------------------------------------

def test_below_replacement_player_yields_negative_vorp_score():
    df = pl.DataFrame(
        [{"player_id": "a", "position": "RB", "adjusted_projected_points": 3.0, "is_rostered": False}],
        schema={"player_id": pl.Utf8, "position": pl.Utf8, "adjusted_projected_points": pl.Float64, "is_rostered": pl.Boolean},
    )
    baselines = {"RB": 8.0, "WR": 8.0, "QB": 10.0, "TE": 5.0}
    out = vm.compute_vorp(df, baselines)
    out = vm.apply_scarcity(out, {"RB": 1.35, "WR": 1.0, "QB": 1.0, "TE": 1.0})

    assert out["vorp_base"].item() == -5.0
    assert out["vorp_score"].item() < 0
    # No clamp: scarcity must not have been applied to a negative vorp_base either.
    assert out["scarcity_multiplier"].item() == 1.0


# ---------------------------------------------------------------------------
# compute_scarcity_multipliers
# ---------------------------------------------------------------------------

def _vorp_frame(rows):
    defaults = {"is_rostered": False, "vorp_base": 1.0}
    schema = {"player_id": pl.Utf8, "position": pl.Utf8, "is_rostered": pl.Boolean, "vorp_base": pl.Float64}
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=schema)


def test_scarcity_multiplier_is_one_at_full_depth():
    # expected = round(0.5 * 10) = 5; surplus of 5 free agents above replacement -> ratio 1.0.
    df = _vorp_frame([{"player_id": f"p{i}", "position": "RB"} for i in range(5)])
    mult = vm.compute_scarcity_multipliers(df, {"RB": 10, "WR": 0, "QB": 0, "TE": 0})
    assert mult["RB"] == 1.0


def test_scarcity_multiplier_caps_at_max_when_zero_depth():
    df = _vorp_frame([])
    mult = vm.compute_scarcity_multipliers(df, {"RB": 10, "WR": 0, "QB": 0, "TE": 0})
    assert mult["RB"] == vm.SCARCITY_MAX_MULTIPLIER


def test_scarcity_multiplier_interpolates_between_bounds():
    # expected = 5; surplus = 2 -> ratio 0.4 -> multiplier between 1.0 and 1.35.
    df = _vorp_frame([{"player_id": f"p{i}", "position": "RB"} for i in range(2)])
    mult = vm.compute_scarcity_multipliers(df, {"RB": 10, "WR": 0, "QB": 0, "TE": 0})
    assert 1.0 < mult["RB"] < vm.SCARCITY_MAX_MULTIPLIER


def test_scarcity_not_applied_to_rostered_players():
    df = pl.DataFrame(
        [{"player_id": "a", "position": "RB", "is_rostered": True, "vorp_base": 20.0}],
        schema={"player_id": pl.Utf8, "position": pl.Utf8, "is_rostered": pl.Boolean, "vorp_base": pl.Float64},
    )
    out = vm.apply_scarcity(df, {"RB": 1.35, "WR": 1.0, "QB": 1.0, "TE": 1.0})
    assert out["scarcity_multiplier"].item() == 1.0
    assert out["vorp_score"].item() == 20.0


def test_scarcity_not_applied_to_negative_vorp_base():
    df = pl.DataFrame(
        [{"player_id": "a", "position": "RB", "is_rostered": False, "vorp_base": -5.0}],
        schema={"player_id": pl.Utf8, "position": pl.Utf8, "is_rostered": pl.Boolean, "vorp_base": pl.Float64},
    )
    out = vm.apply_scarcity(df, {"RB": 1.35, "WR": 1.0, "QB": 1.0, "TE": 1.0})
    assert out["scarcity_multiplier"].item() == 1.0
    assert out["vorp_score"].item() == -5.0


# ---------------------------------------------------------------------------
# Pre-draft: empty league_rosters end-to-end scarcity behavior
# ---------------------------------------------------------------------------

def test_empty_rosters_yields_all_scarcity_multipliers_of_one():
    """Pre-draft, every player is a free agent. With a pool deep enough to cover every
    starting slot (rank_at), the number of above-replacement free agents structurally
    equals rank_at, which is >= 2x the expected depth - so ratio always clamps to 1.0
    and every scarcity multiplier resolves to exactly 1.0. Not a bug - the correct
    pre-draft behavior per the CORE-05 spec. Includes K/D-ST (VORP_POSITIONS) with the
    same deep-pool shape, or those two would show as maximally scarce purely from having
    zero rows in this fixture rather than a real thin pool."""
    league_config = {
        "team_count": 10,
        "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "K": 1, "D/ST": 1},
    }
    empty_rosters = pl.DataFrame(schema={"slot_position": pl.Utf8, "position": pl.Utf8})
    flex_alloc = vm.resolve_flex_allocation(10, empty_rosters)
    rank_at = vm.calculate_rank_at(league_config, flex_alloc)

    all_rows = []
    for pos, count in [("QB", 30), ("RB", 40), ("WR", 40), ("TE", 30), ("K", 30), ("D/ST", 30)]:
        rows, _ = _deep_pool_frame(pos, count)
        all_rows.extend([{**r, "is_rostered": False} for r in rows])
    schema = {"player_id": pl.Utf8, "position": pl.Utf8, "adjusted_projected_points": pl.Float64, "is_rostered": pl.Boolean}
    df = pl.DataFrame(all_rows, schema=schema)

    baselines = vm.calculate_replacement_baselines(df, league_config, flex_alloc, rosters_populated=False)
    df = vm.compute_vorp(df, baselines)
    multipliers = vm.compute_scarcity_multipliers(df, rank_at)

    assert all(m == 1.0 for m in multipliers.values())


# ---------------------------------------------------------------------------
# compute_defense_matchup_multiplier / apply_defense_matchup_multiplier (#93)
# ---------------------------------------------------------------------------

def _defense_frame(rows):
    defaults = {"vorp_score": 10.0, "defense_strength_z": None}
    schema = {"player_id": pl.Utf8, "vorp_score": pl.Float64, "defense_strength_z": pl.Float64}
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=schema)


def test_defense_matchup_multiplier_clamps_at_upper_bound():
    # z=100 -> raw = 1 + 0.05*100 = 6.0, far past DEFENSE_MATCHUP_BOUNDS - proves the clip
    # itself fires rather than the arithmetic coincidentally landing on the bound.
    df = _defense_frame([{"player_id": "a", "defense_strength_z": 100.0}])
    out = vm.compute_defense_matchup_multiplier(df)
    assert out["defense_matchup_multiplier"].item() == vm.DEFENSE_MATCHUP_BOUNDS[1]


def test_defense_matchup_multiplier_clamps_at_lower_bound():
    df = _defense_frame([{"player_id": "a", "defense_strength_z": -100.0}])
    out = vm.compute_defense_matchup_multiplier(df)
    assert out["defense_matchup_multiplier"].item() == vm.DEFENSE_MATCHUP_BOUNDS[0]


def test_defense_matchup_multiplier_neutral_when_strength_z_null():
    """Cold-start / no ingested row for this (opponent, position, season) - must stay
    neutral (1.0), not error or default toward either bound."""
    df = _defense_frame([{"player_id": "a", "defense_strength_z": None}])
    out = vm.compute_defense_matchup_multiplier(df)
    assert out["defense_matchup_multiplier"].item() == 1.0


def test_defense_matchup_multiplier_neutral_when_column_absent():
    """Pre-ingestion (sync_defense_strength.py has never run): the load query still
    returns defense_strength_z as a real column (LEFT JOIN, always present), but this
    guards the case a caller passes a frame without it at all."""
    df = pl.DataFrame(
        [{"player_id": "a", "vorp_score": 10.0}],
        schema={"player_id": pl.Utf8, "vorp_score": pl.Float64},
    )
    out = vm.compute_defense_matchup_multiplier(df)
    assert out["defense_matchup_multiplier"].to_list() == [1.0]


def test_apply_defense_matchup_multiplier_scales_vorp_score():
    df = _defense_frame([{"player_id": "a", "vorp_score": 10.0}])
    df = df.with_columns(pl.lit(1.1).alias("defense_matchup_multiplier"))
    out = vm.apply_defense_matchup_multiplier(df)
    assert out["vorp_score"].item() == 11.0


# ---------------------------------------------------------------------------
# MATCHUP_WEIGHTING_ENABLED (#93) — off by default, byte-identical when disabled
# ---------------------------------------------------------------------------

def _defense_matchup_step(df):
    """Mirrors the MATCHUP_WEIGHTING_ENABLED-gated block in run_vorp_pipeline exactly."""
    if vm.MATCHUP_WEIGHTING_ENABLED:
        df = vm.compute_defense_matchup_multiplier(df)
        df = vm.apply_defense_matchup_multiplier(df)
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("defense_matchup_multiplier"))
    return df


def test_matchup_weighting_disabled_leaves_vorp_score_untouched_and_multiplier_null(monkeypatch):
    """Acceptance criterion: off by default; behavior byte-identical to pre-#93 vorp_score
    when disabled. MATCHUP_WEIGHTING_ENABLED is a module-level constant (same convention as
    CORE_API_BASE_URL/REDIS_HOST elsewhere in this repo, evaluated once from os.getenv at
    import), so it's toggled here via monkeypatch.setattr on the module, not
    monkeypatch.setenv (which would have no effect post-import)."""
    monkeypatch.setattr(vm, "MATCHUP_WEIGHTING_ENABLED", False)
    df = _defense_frame([{"player_id": "a", "vorp_score": 10.0, "defense_strength_z": 100.0}])

    out = _defense_matchup_step(df)

    assert out["vorp_score"].item() == 10.0
    assert out["defense_matchup_multiplier"].item() is None


def test_matchup_weighting_enabled_applies_multiplier(monkeypatch):
    monkeypatch.setattr(vm, "MATCHUP_WEIGHTING_ENABLED", True)
    df = _defense_frame([{"player_id": "a", "vorp_score": 10.0, "defense_strength_z": 100.0}])

    out = _defense_matchup_step(df)

    assert out["defense_matchup_multiplier"].item() == vm.DEFENSE_MATCHUP_BOUNDS[1]
    assert out["vorp_score"].item() == 10.0 * vm.DEFENSE_MATCHUP_BOUNDS[1]


def test_persist_vorp_matrix_writes_defense_matchup_multiplier():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    rows = [(1.0, 20.0, 5.0, 1.0, 5.0, 1.05, "p1", 2026, 1)]
    written = vm.persist_vorp_matrix(mock_conn, rows)

    assert written == 1
    query, executed_rows = mock_cursor.executemany.call_args.args
    assert "defense_matchup_multiplier = %s" in query
    assert executed_rows == rows


# ---------------------------------------------------------------------------
# run_vorp_pipeline — empty week, no crash
# ---------------------------------------------------------------------------

def test_run_vorp_pipeline_empty_week_does_not_crash():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.optimization.vorp_matrix.get_db_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        written = vm.run_vorp_pipeline(season=2025, week=99)

    assert written == 0


def test_load_league_config_falls_back_when_no_settings_row():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    config = vm.load_league_config(mock_cursor)
    assert config["team_count"] == 10
    assert config["lineup_slot_counts"]["RB/WR/TE"] == 1
