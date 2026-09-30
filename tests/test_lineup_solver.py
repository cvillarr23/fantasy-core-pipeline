from unittest.mock import MagicMock, patch

from src.optimization import lineup_solver as ls


def _player(pid, position, vorp_score, name=None, defense_matchup_multiplier=1.0):
    return {
        "player_id": pid, "name": name or pid, "position": position, "vorp_score": vorp_score,
        "defense_matchup_multiplier": defense_matchup_multiplier,
    }


def _current(position, slot_position=None, vorp_score=0.0):
    return {"position": position, "slot_position": slot_position or position, "vorp_score": vorp_score}


# ---------------------------------------------------------------------------
# build_and_solve_lineup
# ---------------------------------------------------------------------------

def test_build_and_solve_lineup_selects_optimal_by_vorp_score():
    # wr_low has more raw points than wr_high would if this used projected_points, but
    # vorp_score must be the objective weight - wr_high should win the flex/starting spot.
    pool = [
        _player("qb1", "QB", 10.0),
        _player("rb1", "RB", 8.0),
        _player("rb2", "RB", 7.0),
        _player("wr1", "WR", 6.0),
        _player("wr2", "WR", 5.0),
        _player("te1", "TE", 4.0),
        _player("dst1", "D/ST", 3.0),
        _player("k1", "K", 2.0),
        _player("wr_high", "WR", 9.0),
        _player("wr_low", "WR", 1.0),
    ]
    starters = {p["player_id"] for p in ls.build_and_solve_lineup(pool)}
    assert "wr_high" in starters
    assert "wr_low" not in starters


def test_build_and_solve_lineup_includes_vorp_score_in_each_starter():
    """CORE-17 (#35): compute_lineup_swaps needs each optimal starter's own vorp_score to
    populate decision_inputs - previously only used internally for the objective weight
    and dropped from the returned dict."""
    pool = [
        _player("qb1", "QB", 10.0),
        _player("rb1", "RB", 8.0),
        _player("rb2", "RB", 7.0),
        _player("wr1", "WR", 6.0),
        _player("wr2", "WR", 5.0),
        _player("te1", "TE", 4.0),
        _player("dst1", "D/ST", 3.0),
        _player("k1", "K", 2.0),
        _player("wr3", "WR", 1.0),
    ]
    starters = {p["player_id"]: p for p in ls.build_and_solve_lineup(pool)}
    assert starters["qb1"]["vorp_score"] == 10.0


def test_build_and_solve_lineup_respects_position_minimums():
    pool = [
        _player("qb1", "QB", 10.0),
        _player("rb1", "RB", 8.0),
        _player("rb2", "RB", 7.0),
        _player("wr1", "WR", 6.0),
        _player("wr2", "WR", 5.0),
        _player("te1", "TE", 4.0),
        _player("dst1", "D/ST", 3.0),
        _player("k1", "K", 2.0),
        _player("rb3", "RB", 3.0),
    ]
    starters = ls.build_and_solve_lineup(pool)
    assert len(starters) == 9
    counts = {}
    for p in starters:
        counts[p["position"]] = counts.get(p["position"], 0) + 1
    assert counts["QB"] == 1
    assert counts["RB"] >= 2
    assert counts["WR"] >= 2
    assert counts["TE"] >= 1
    assert counts["D/ST"] == 1
    assert counts["K"] == 1


def test_build_and_solve_lineup_allows_flex_at_any_eligible_position():
    # Only enough RB/TE to hit the minimums; a third strong WR must take the flex slot.
    pool = [
        _player("qb1", "QB", 10.0),
        _player("rb1", "RB", 8.0),
        _player("rb2", "RB", 7.0),
        _player("te1", "TE", 1.0),
        _player("dst1", "D/ST", 3.0),
        _player("k1", "K", 2.0),
        _player("wr1", "WR", 9.0),
        _player("wr2", "WR", 8.5),
        _player("wr3", "WR", 8.0),
    ]
    starters = ls.build_and_solve_lineup(pool)
    wr_count = sum(1 for p in starters if p["position"] == "WR")
    assert wr_count == 3


def test_build_and_solve_lineup_infeasible_roster_returns_empty():
    # Only 1 RB available; RB >= 2 constraint can never be satisfied.
    pool = [
        _player("qb1", "QB", 10.0),
        _player("rb1", "RB", 8.0),
        _player("wr1", "WR", 6.0),
        _player("wr2", "WR", 5.0),
        _player("te1", "TE", 4.0),
        _player("dst1", "D/ST", 3.0),
        _player("k1", "K", 2.0),
    ]
    assert ls.build_and_solve_lineup(pool) == []


def test_build_and_solve_lineup_empty_pool_returns_empty():
    assert ls.build_and_solve_lineup([]) == []


def test_build_and_solve_lineup_threads_defense_matchup_multiplier_through():
    pool = [
        _player("qb1", "QB", 10.0, defense_matchup_multiplier=1.07),
        _player("rb1", "RB", 8.0), _player("rb2", "RB", 7.0),
        _player("wr1", "WR", 6.0), _player("wr2", "WR", 5.0), _player("wr3", "WR", 4.5),
        _player("te1", "TE", 4.0), _player("dst1", "D/ST", 3.0), _player("k1", "K", 2.0),
    ]
    starters = {p["player_id"]: p for p in ls.build_and_solve_lineup(pool)}
    assert starters["qb1"]["defense_matchup_multiplier"] == 1.07


def test_build_and_solve_lineup_objective_actually_applies_the_multiplier():
    # wr_boosted has lower raw vorp_score than wr_plain, but its 1.1 multiplier pushes its
    # objective weight (6.0 * 1.1 = 6.6) above wr_plain's (6.5 * 1.0) - the flex slot must
    # go to wr_boosted, proving the multiplier is actually applied to the objective, not
    # just carried through on the output dict.
    pool = [
        _player("qb1", "QB", 10.0),
        _player("rb1", "RB", 8.0), _player("rb2", "RB", 7.0),
        _player("wr1", "WR", 9.0), _player("wr2", "WR", 8.0),
        _player("te1", "TE", 4.0), _player("dst1", "D/ST", 3.0), _player("k1", "K", 2.0),
        _player("wr_plain", "WR", 6.5, defense_matchup_multiplier=1.0),
        _player("wr_boosted", "WR", 6.0, defense_matchup_multiplier=1.1),
    ]
    starters = {p["player_id"] for p in ls.build_and_solve_lineup(pool)}
    assert "wr_boosted" in starters
    assert "wr_plain" not in starters


def test_build_and_solve_lineup_tolerates_pool_entries_missing_the_multiplier_key():
    """season_replay.scoring builds naive/hindsight pools (spread from
    load_roster_with_projected, which has no defense_matchup_multiplier key at all) and
    feeds them straight into this solver - it must not KeyError."""
    pool = [
        {"player_id": "qb1", "name": "QB One", "position": "QB", "vorp_score": 10.0},
        {"player_id": "rb1", "name": "RB One", "position": "RB", "vorp_score": 8.0},
        {"player_id": "rb2", "name": "RB Two", "position": "RB", "vorp_score": 7.0},
        {"player_id": "wr1", "name": "WR One", "position": "WR", "vorp_score": 6.0},
        {"player_id": "wr2", "name": "WR Two", "position": "WR", "vorp_score": 5.0},
        {"player_id": "wr3", "name": "WR Three", "position": "WR", "vorp_score": 4.5},
        {"player_id": "te1", "name": "TE One", "position": "TE", "vorp_score": 4.0},
        {"player_id": "dst1", "name": "DST One", "position": "D/ST", "vorp_score": 3.0},
        {"player_id": "k1", "name": "K One", "position": "K", "vorp_score": 2.0},
    ]
    starters = ls.build_and_solve_lineup(pool)  # should not raise
    assert len(starters) == 9
    assert all(p["defense_matchup_multiplier"] == 1.0 for p in starters)


# ---------------------------------------------------------------------------
# load_roster_pool
# ---------------------------------------------------------------------------

def test_load_roster_pool_excludes_out_players():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("rb1", "Runner Back", "RB", 5.0, 1.05)]

    rows = ls.load_roster_pool(mock_cursor, team_id="1", season=2025, week=3)

    query = mock_cursor.execute.call_args[0][0]
    assert "injury_status" in query
    assert "'Out'" in query
    assert rows == [{
        "player_id": "rb1", "name": "Runner Back", "position": "RB", "vorp_score": 5.0,
        "defense_matchup_multiplier": 1.05,
    }]


def test_load_roster_pool_filters_by_season_and_week():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []

    ls.load_roster_pool(mock_cursor, team_id="1", season=2025, week=3)

    query, params = mock_cursor.execute.call_args[0]
    assert "s.season = %s" in query
    assert "s.week = %s" in query
    assert params == ("1", 2025, 3)


def test_load_roster_pool_coalesces_null_vorp_score():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("wr1", "Wide Out", "WR", 0.0, 1.0)]

    ls.load_roster_pool(mock_cursor, team_id="1", season=2025, week=3)

    query = mock_cursor.execute.call_args[0][0]
    assert "COALESCE(s.vorp_score, 0.0)" in query


def test_load_roster_pool_coalesces_null_defense_matchup_multiplier():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("wr1", "Wide Out", "WR", 0.0, 1.0)]

    rows = ls.load_roster_pool(mock_cursor, team_id="1", season=2025, week=3)

    query = mock_cursor.execute.call_args[0][0]
    assert "COALESCE(s.defense_matchup_multiplier, 1.0)" in query
    assert rows[0]["defense_matchup_multiplier"] == 1.0


def test_load_lineup_slot_counts_reads_league_settings():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ({"QB": 1, "RB": 2, "WR": 2, "TE": 1, "D/ST": 1, "K": 1, "RB/WR/TE": 1},)

    counts = ls.load_lineup_slot_counts(mock_cursor, season=2026)

    assert counts["D/ST"] == 1
    query, params = mock_cursor.execute.call_args[0]
    assert "FROM league_settings" in query
    assert params == (2026,)


def test_load_lineup_slot_counts_falls_back_when_missing():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    counts = ls.load_lineup_slot_counts(mock_cursor, season=2026)

    assert counts == ls.DEFAULT_LINEUP_SLOT_COUNTS


# ---------------------------------------------------------------------------
# load_current_starters
# ---------------------------------------------------------------------------

def test_load_current_starters_includes_vorp_score():
    """CORE-17 (#35): compute_lineup_swaps needs the dropped starter's own vorp_score to
    populate decision_inputs, which load_current_starters didn't carry before."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("rb_old", "RB", "RB1", 4.0)]

    starters = ls.load_current_starters(mock_cursor, team_id="1", season=2025, week=3)

    query, params = mock_cursor.execute.call_args[0]
    assert "weekly_stats" in query
    assert "LEFT JOIN" in query
    assert params == (2025, 3, "1")
    assert starters == {"rb_old": {"position": "RB", "slot_position": "RB1", "vorp_score": 4.0}}


def test_load_current_starters_coalesces_null_vorp_score_for_a_bye_week_starter():
    """A starter's weekly_stats row is only missing (COALESCE fires) on a bye - the join
    is LEFT specifically so a starter still on a bye still shows up as droppable."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("wr_bye", "WR", "FLEX", 0.0)]

    ls.load_current_starters(mock_cursor, team_id="1", season=2025, week=3)

    query = mock_cursor.execute.call_args[0][0]
    assert "COALESCE(s.vorp_score, 0.0)" in query


# ---------------------------------------------------------------------------
# compute_lineup_swaps
# ---------------------------------------------------------------------------

def test_compute_lineup_swaps_pairs_add_and_drop_by_position():
    optimal = [
        {"player_id": "rb_new", "name": "New RB", "position": "RB", "vorp_score": 9.0},
        {"player_id": "rb_old_kept", "name": "Kept RB", "position": "RB"},
    ]
    current = {"rb_old_kept": _current("RB", "RB1"), "rb_bench_me": _current("RB", "RB2", vorp_score=2.0)}

    swaps = ls.compute_lineup_swaps(optimal, current)

    assert swaps == [{
        "drop_player_id": "rb_bench_me", "add_player_id": "rb_new", "slot": "RB2",
        "matchup_multiplier": 1.0,
        "decision_inputs": {
            "add_player_vorp_score": 9.0, "drop_player_vorp_score": 2.0, "matchup_multiplier": 1.0,
        },
    }]


def test_compute_lineup_swaps_returns_empty_when_already_optimal():
    optimal = [{"player_id": "rb1", "name": "RB One", "position": "RB"}]
    current = {"rb1": _current("RB", "RB1")}
    assert ls.compute_lineup_swaps(optimal, current) == []


def test_compute_lineup_swaps_reports_dropped_players_actual_flex_slot():
    # The dropped starter is a WR sitting in the FLEX slot, not a dedicated WR slot.
    # The emitted "slot" must be the vacated FLEX frame, not the player's own position.
    optimal = [{"player_id": "rb_new", "name": "New RB", "position": "RB", "vorp_score": 6.0}]
    current = {"wr_in_flex": _current("WR", "FLEX", vorp_score=1.5)}

    swaps = ls.compute_lineup_swaps(optimal, current)

    assert swaps == [{
        "drop_player_id": "wr_in_flex", "add_player_id": "rb_new", "slot": "FLEX",
        "matchup_multiplier": 1.0,
        "decision_inputs": {
            "add_player_vorp_score": 6.0, "drop_player_vorp_score": 1.5, "matchup_multiplier": 1.0,
        },
    }]


def test_compute_lineup_swaps_includes_decision_inputs_with_both_vorp_scores():
    """CORE-17 (#35): decision_inputs must carry both sides of the swap's VORP, not just
    the added player's - the audit trail needs to show what was actually gained."""
    optimal = [{"player_id": "rb_new", "name": "New RB", "position": "RB", "vorp_score": 9.0, "defense_matchup_multiplier": 1.05}]
    current = {"rb_old": _current("RB", "RB1", vorp_score=4.0)}

    swaps = ls.compute_lineup_swaps(optimal, current)

    assert swaps[0]["decision_inputs"] == {
        "add_player_vorp_score": 9.0, "drop_player_vorp_score": 4.0, "matchup_multiplier": 1.05,
    }


def test_compute_lineup_swaps_includes_matchup_multiplier():
    optimal = [{"player_id": "rb_new", "name": "New RB", "position": "RB", "defense_matchup_multiplier": 1.08}]
    current = {"rb_old": _current("RB", "RB1")}

    swaps = ls.compute_lineup_swaps(optimal, current)

    assert swaps[0]["matchup_multiplier"] == 1.08


def test_compute_lineup_swaps_defaults_matchup_multiplier_when_absent():
    # optimal_starters entries built without defense_matchup_multiplier (e.g. an older/other
    # code path) must not KeyError - default to neutral 1.0.
    optimal = [{"player_id": "rb_new", "name": "New RB", "position": "RB"}]
    current = {"rb_old": _current("RB", "RB1")}

    swaps = ls.compute_lineup_swaps(optimal, current)

    assert swaps[0]["matchup_multiplier"] == 1.0


# ---------------------------------------------------------------------------
# publish_lineup_swaps
# ---------------------------------------------------------------------------

def test_publish_lineup_swaps_posts_expected_payload():
    swaps = [{"drop_player_id": "d1", "add_player_id": "a1", "slot": "RB", "matchup_multiplier": 1.03}]

    with patch("src.optimization.lineup_solver.post_trigger") as mock_post:
        ls.publish_lineup_swaps("team-1", swaps)

    args, kwargs = mock_post.call_args
    assert args[0] == "lineup"
    assert args[1] == {"team_id": "team-1", "optimized_swaps": swaps}
    assert kwargs["timeout"] == 10


def test_publish_lineup_swaps_skips_request_when_no_swaps():
    with patch("src.optimization.lineup_solver.post_trigger") as mock_post:
        ls.publish_lineup_swaps("team-1", [])
    mock_post.assert_not_called()


# ---------------------------------------------------------------------------
# optimize_weekly_lineup (orchestrator)
# ---------------------------------------------------------------------------

# The orchestrator reads three result sets in order: the team's roster, the roster's
# weekly_stats metrics for [target_week - 3, target_week] in the target season, and the
# current starters. Rows mirror each query's SELECT column order.

TARGET_SEASON = 2026
TARGET_WEEK = 5
ALL_TEAMS_PLAYING = {"KC", "BUF", "PHI", "DAL", "SF", "LAR"}

ROSTER = [
    ("qb1", "QB One", "QB", "KC", None),
    ("rb1", "RB One", "RB", "BUF", None),
    ("rb2", "RB Two", "RB", "PHI", None),
    ("wr1", "WR One", "WR", "DAL", None),
    ("wr2", "WR Two", "WR", "SF", None),
    ("te1", "TE One", "TE", "KC", None),
    ("wr_old", "Old WR", "WR", "BUF", None),
    ("wr_bench", "Bench WR", "WR", "LAR", None),
    ("dst1", "DST One", "D/ST", "KC", None),
    ("k1", "K One", "K", "PHI", None),
]

CURRENT_STARTERS = [
    ("qb1", "QB", "QB", 0.0), ("rb1", "RB", "RB", 0.0), ("rb2", "RB", "RB", 0.0),
    ("wr1", "WR", "WR", 0.0), ("wr2", "WR", "WR", 0.0), ("te1", "TE", "TE", 0.0),
    ("wr_old", "WR", "RB/WR/TE", 0.0), ("dst1", "D/ST", "D/ST", 0.0), ("k1", "K", "K", 0.0),
]


def _metric_rows(week, vorp_by_pid, multiplier=None):
    return [(pid, week, vorp, multiplier) for pid, vorp in vorp_by_pid.items()]


# Skill-position VORP where the current lineup (wr_old in FLEX) is already optimal.
BASE_VORP = {"qb1": 10.0, "rb1": 8.0, "rb2": 7.0, "wr1": 6.0, "wr2": 5.0, "te1": 4.0, "wr_old": 3.0, "wr_bench": 1.0}


def _mock_conn_with_cursor(mock_cursor):
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_conn


def _run_optimize(roster, metrics, starters=CURRENT_STARTERS, week=TARGET_WEEK,
                  teams_playing=ALL_TEAMS_PLAYING, dry_run=False):
    """Runs optimize_weekly_lineup against a mocked DB. Returns (result_or_exception,
    post_trigger mock, cursor mock)."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [roster, metrics, starters]
    schedule_patch = (
        patch("src.optimization.lineup_solver.load_teams_playing", side_effect=teams_playing)
        if isinstance(teams_playing, Exception)
        else patch("src.optimization.lineup_solver.load_teams_playing", return_value=teams_playing)
    )
    with patch("src.optimization.lineup_solver.get_db_connection") as mock_get_conn, \
         patch("src.optimization.lineup_solver.load_lineup_slot_counts", return_value=ls.DEFAULT_LINEUP_SLOT_COUNTS), \
         schedule_patch, \
         patch("src.optimization.lineup_solver.post_trigger") as mock_post:
        mock_get_conn.return_value.__enter__.return_value = _mock_conn_with_cursor(mock_cursor)
        try:
            result = ls.optimize_weekly_lineup(
                team_id="8", season=TARGET_SEASON, week=week, dry_run=dry_run,
            )
        except ls.InsufficientLineupInputError as exc:
            result = exc
    return result, mock_post, mock_cursor


# (a) no target-week weekly_stats, but usable prior same-season data --------------

def test_optimize_uses_rolling_prior_weeks_when_target_week_has_no_rows():
    week3 = dict(BASE_VORP, wr_bench=10.0)
    week4 = dict(BASE_VORP, wr_bench=30.0)
    metrics = _metric_rows(3, week3) + _metric_rows(4, week4)

    result, mock_post, _ = _run_optimize(ROSTER, metrics)

    assert result == 1
    mock_post.assert_called_once()
    path, payload = mock_post.call_args[0]
    assert path == "lineup"
    [swap] = payload["optimized_swaps"]
    assert (swap["drop_player_id"], swap["add_player_id"], swap["slot"]) == ("wr_old", "wr_bench", "RB/WR/TE")
    inputs = swap["decision_inputs"]
    # Rolling mean of weeks 3 and 4, and the dropped starter's VORP from the same source.
    assert inputs["add_player_vorp_score"] == 20.0
    assert inputs["drop_player_vorp_score"] == 3.0
    assert inputs["metric_source"] == "weekly_stats.vorp_score"
    assert inputs["source_mode"] == "rolling_prior_weeks"
    assert inputs["source_season"] == TARGET_SEASON
    assert inputs["source_weeks"] == [3, 4]
    assert inputs["season"] == TARGET_SEASON
    assert inputs["week"] == TARGET_WEEK
    assert inputs["held_slots"] == ["D/ST", "K"]


def test_optimize_rolling_window_only_queries_the_target_season_and_prior_three_weeks():
    metrics = _metric_rows(4, BASE_VORP)

    _, _, mock_cursor = _run_optimize(ROSTER, metrics)

    metrics_query, params = mock_cursor.execute.call_args_list[1][0]
    assert "s.season = %s" in metrics_query
    assert "vorp_score IS NOT NULL" in metrics_query
    assert params == ("8", TARGET_SEASON, TARGET_WEEK - 3, TARGET_WEEK)


def test_optimize_prefers_target_week_projections_when_present():
    week5 = dict(BASE_VORP, wr_bench=50.0)
    week4 = dict(BASE_VORP, wr_bench=0.0, wr_old=40.0)
    metrics = _metric_rows(4, week4) + _metric_rows(5, week5, multiplier=1.0)

    result, mock_post, _ = _run_optimize(ROSTER, metrics)

    assert result == 1
    [swap] = mock_post.call_args[0][1]["optimized_swaps"]
    assert swap["add_player_id"] == "wr_bench"
    assert swap["decision_inputs"]["source_mode"] == "target_week"
    assert swap["decision_inputs"]["source_weeks"] == [TARGET_WEEK]
    assert swap["decision_inputs"]["add_player_vorp_score"] == 50.0


def test_optimize_target_week_mode_does_not_need_the_schedule():
    """Target-week rows already encode byes (no projection row), so the schedule lookup
    is only a fallback-mode requirement."""
    metrics = _metric_rows(5, BASE_VORP)

    result, mock_post, _ = _run_optimize(ROSTER, metrics, teams_playing=RuntimeError("nflverse down"))

    assert result == 0
    mock_post.assert_not_called()


def test_optimize_fallback_excludes_players_whose_team_is_on_bye_in_target_week():
    metrics = _metric_rows(4, dict(BASE_VORP, wr_bench=99.0))

    result, mock_post, _ = _run_optimize(ROSTER, metrics, teams_playing=ALL_TEAMS_PLAYING - {"LAR"})

    assert result == 0
    mock_post.assert_not_called()


def test_optimize_fallback_normalizes_schedule_team_aliases():
    """nflverse schedules call the Rams "LA"; Sleeper's players.team says "LAR"."""
    metrics = _metric_rows(4, dict(BASE_VORP, wr_bench=99.0))

    result, _, _ = _run_optimize(ROSTER, metrics, teams_playing=(ALL_TEAMS_PLAYING - {"LAR"}) | {"LA"})

    assert result == 1


def test_optimize_excludes_out_players_from_pool():
    roster = [row if row[0] != "wr_bench" else ("wr_bench", "Bench WR", "WR", "LAR", "Out") for row in ROSTER]
    metrics = _metric_rows(4, dict(BASE_VORP, wr_bench=99.0))

    result, mock_post, _ = _run_optimize(roster, metrics)

    assert result == 0
    mock_post.assert_not_called()


def test_optimize_holds_kicker_and_dst_starters_when_they_have_no_metrics():
    metrics = _metric_rows(4, dict(BASE_VORP, wr_bench=99.0))

    _, mock_post, _ = _run_optimize(ROSTER, metrics)

    swaps = mock_post.call_args[0][1]["optimized_swaps"]
    touched = {s["drop_player_id"] for s in swaps} | {s["add_player_id"] for s in swaps}
    assert not touched & {"k1", "dst1"}


def test_optimize_dry_run_never_publishes():
    metrics = _metric_rows(4, dict(BASE_VORP, wr_bench=99.0))

    result, mock_post, _ = _run_optimize(ROSTER, metrics, dry_run=True)

    assert result == 1
    mock_post.assert_not_called()


# (b) missing / insufficient input must not report success ------------------------

def test_optimize_raises_when_no_usable_metrics_exist():
    result, mock_post, _ = _run_optimize(ROSTER, [])

    assert isinstance(result, ls.InsufficientLineupInputError)
    assert "no usable weekly_stats.vorp_score" in str(result)
    mock_post.assert_not_called()


def test_optimize_raises_when_team_has_no_roster():
    result, mock_post, _ = _run_optimize([], _metric_rows(4, BASE_VORP))

    assert isinstance(result, ls.InsufficientLineupInputError)
    assert "no league_rosters rows" in str(result)
    mock_post.assert_not_called()


def test_optimize_raises_when_prior_week_data_is_stale():
    """Newest usable week is target-2: last week's refresh/ingest never landed."""
    result, mock_post, _ = _run_optimize(ROSTER, _metric_rows(3, BASE_VORP))

    assert isinstance(result, ls.InsufficientLineupInputError)
    assert "stale" in str(result)
    mock_post.assert_not_called()


def test_optimize_week_one_without_projections_fails_and_never_reads_last_season():
    result, mock_post, mock_cursor = _run_optimize(ROSTER, [], week=1)

    assert isinstance(result, ls.InsufficientLineupInputError)
    assert "Week 1" in str(result)
    _, params = mock_cursor.execute.call_args_list[1][0]
    assert params == ("8", TARGET_SEASON, 1, 1)
    mock_post.assert_not_called()


def test_optimize_raises_when_a_skill_slot_cannot_be_filled():
    no_te = {pid: v for pid, v in BASE_VORP.items() if pid != "te1"}

    result, mock_post, _ = _run_optimize(ROSTER, _metric_rows(4, no_te))

    assert isinstance(result, ls.InsufficientLineupInputError)
    assert "TE" in str(result)
    mock_post.assert_not_called()


def test_optimize_raises_when_schedule_unavailable_in_fallback_mode():
    result, mock_post, _ = _run_optimize(
        ROSTER, _metric_rows(4, BASE_VORP), teams_playing=RuntimeError("nflverse down"),
    )

    assert isinstance(result, ls.InsufficientLineupInputError)
    assert "schedule" in str(result)
    mock_post.assert_not_called()


# (c) a valid zero-swap result still succeeds ---------------------------------------

def test_optimize_returns_zero_when_lineup_already_optimal():
    result, mock_post, _ = _run_optimize(ROSTER, _metric_rows(4, BASE_VORP))

    assert result == 0
    mock_post.assert_not_called()


# ---------------------------------------------------------------------------
# main (CronJob entrypoint exit codes)
# ---------------------------------------------------------------------------

ARGV = ["--team-id", "8", "--season", "2026", "--week", "5"]


def test_main_exits_zero_when_lineup_already_optimal():
    with patch("src.optimization.lineup_solver.optimize_weekly_lineup", return_value=0) as mock_opt:
        assert ls.main(ARGV) == 0
    mock_opt.assert_called_once_with(team_id="8", season=2026, week=5, dry_run=False)


def test_main_exits_zero_when_swaps_published():
    with patch("src.optimization.lineup_solver.optimize_weekly_lineup", return_value=2):
        assert ls.main(ARGV) == 0


def test_main_exits_nonzero_with_reason_on_insufficient_input(caplog):
    error = ls.InsufficientLineupInputError("no usable weekly_stats.vorp_score for Season 2026")
    with patch("src.optimization.lineup_solver.optimize_weekly_lineup", side_effect=error):
        assert ls.main(ARGV) != 0
    assert "cannot evaluate Team 8 Season 2026 Week 5" in caplog.text
    assert "no usable weekly_stats.vorp_score" in caplog.text


def test_main_end_to_end_no_usable_data_exits_nonzero():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [ROSTER, [], CURRENT_STARTERS]
    with patch("src.optimization.lineup_solver.get_db_connection") as mock_get_conn, \
         patch("src.optimization.lineup_solver.load_lineup_slot_counts", return_value=ls.DEFAULT_LINEUP_SLOT_COUNTS), \
         patch("src.optimization.lineup_solver.load_teams_playing", return_value=ALL_TEAMS_PLAYING), \
         patch("src.optimization.lineup_solver.post_trigger") as mock_post:
        mock_get_conn.return_value.__enter__.return_value = _mock_conn_with_cursor(mock_cursor)
        assert ls.main(ARGV) != 0
    mock_post.assert_not_called()


def test_main_passes_dry_run_flag():
    with patch("src.optimization.lineup_solver.optimize_weekly_lineup", return_value=0) as mock_opt:
        assert ls.main(ARGV + ["--dry-run"]) == 0
    assert mock_opt.call_args.kwargs["dry_run"] is True


def test_main_requires_team_id(monkeypatch):
    monkeypatch.delenv("ESPN_TEAM_ID", raising=False)
    import pytest
    with pytest.raises(SystemExit) as exc:
        ls.main(["--season", "2026", "--week", "5"])
    assert exc.value.code != 0


# ---------------------------------------------------------------------------
# evaluate_lineup (shared by the optimizer, the matchup badge, and rumor actions)
# ---------------------------------------------------------------------------

def test_evaluate_lineup_returns_swaps_with_source_and_never_publishes():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [ROSTER, _metric_rows(4, dict(BASE_VORP, wr_bench=99.0)), CURRENT_STARTERS]
    with patch("src.optimization.lineup_solver.load_lineup_slot_counts", return_value=ls.DEFAULT_LINEUP_SLOT_COUNTS), \
         patch("src.optimization.lineup_solver.load_teams_playing", return_value=ALL_TEAMS_PLAYING), \
         patch("src.optimization.lineup_solver.post_trigger") as mock_post:
        result = ls.evaluate_lineup(mock_cursor, "8", TARGET_SEASON, TARGET_WEEK)

    mock_post.assert_not_called()
    [swap] = result["swaps"]
    assert (swap["drop_player_id"], swap["add_player_id"]) == ("wr_old", "wr_bench")
    assert swap["decision_inputs"]["source_mode"] == "rolling_prior_weeks"
    assert result["source"]["held_slots"] == ["D/ST", "K"]
    assert {p["player_id"] for p in result["roster_pool"]} >= {"wr_bench", "wr_old"}


def test_evaluate_lineup_raises_on_insufficient_input():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [ROSTER, [], CURRENT_STARTERS]
    import pytest
    with patch("src.optimization.lineup_solver.load_lineup_slot_counts", return_value=ls.DEFAULT_LINEUP_SLOT_COUNTS), \
         pytest.raises(ls.InsufficientLineupInputError):
        ls.evaluate_lineup(mock_cursor, "8", TARGET_SEASON, TARGET_WEEK)
