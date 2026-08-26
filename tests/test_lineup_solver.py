from unittest.mock import MagicMock, patch

from src.optimization import lineup_solver as ls


def _player(pid, position, vorp_score, name=None, defense_matchup_multiplier=1.0):
    return {
        "player_id": pid, "name": name or pid, "position": position, "vorp_score": vorp_score,
        "defense_matchup_multiplier": defense_matchup_multiplier,
    }


def _current(position, slot_position=None):
    return {"position": position, "slot_position": slot_position or position}


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
# compute_lineup_swaps
# ---------------------------------------------------------------------------

def test_compute_lineup_swaps_pairs_add_and_drop_by_position():
    optimal = [
        {"player_id": "rb_new", "name": "New RB", "position": "RB"},
        {"player_id": "rb_old_kept", "name": "Kept RB", "position": "RB"},
    ]
    current = {"rb_old_kept": _current("RB", "RB1"), "rb_bench_me": _current("RB", "RB2")}

    swaps = ls.compute_lineup_swaps(optimal, current)

    assert swaps == [{
        "drop_player_id": "rb_bench_me", "add_player_id": "rb_new", "slot": "RB2",
        "matchup_multiplier": 1.0,
    }]


def test_compute_lineup_swaps_returns_empty_when_already_optimal():
    optimal = [{"player_id": "rb1", "name": "RB One", "position": "RB"}]
    current = {"rb1": _current("RB", "RB1")}
    assert ls.compute_lineup_swaps(optimal, current) == []


def test_compute_lineup_swaps_reports_dropped_players_actual_flex_slot():
    # The dropped starter is a WR sitting in the FLEX slot, not a dedicated WR slot.
    # The emitted "slot" must be the vacated FLEX frame, not the player's own position.
    optimal = [{"player_id": "rb_new", "name": "New RB", "position": "RB"}]
    current = {"wr_in_flex": _current("WR", "FLEX")}

    swaps = ls.compute_lineup_swaps(optimal, current)

    assert swaps == [{
        "drop_player_id": "wr_in_flex", "add_player_id": "rb_new", "slot": "FLEX",
        "matchup_multiplier": 1.0,
    }]


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

    with patch("src.optimization.lineup_solver.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=202)
        ls.publish_lineup_swaps("team-1", swaps)

    url, kwargs = mock_post.call_args[0], mock_post.call_args[1]
    assert url[0] == f"{ls.CORE_API_BASE_URL}/v1/trigger/lineup"
    assert kwargs["json"] == {"team_id": "team-1", "optimized_swaps": swaps}


def test_publish_lineup_swaps_skips_request_when_no_swaps():
    with patch("src.optimization.lineup_solver.requests.post") as mock_post:
        ls.publish_lineup_swaps("team-1", [])
    mock_post.assert_not_called()


# ---------------------------------------------------------------------------
# optimize_weekly_lineup (orchestrator)
# ---------------------------------------------------------------------------

def _mock_conn_with_cursor(mock_cursor):
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_conn


def test_optimize_weekly_lineup_skips_publish_when_no_delta():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("qb1", "QB One", "QB", 10.0, 1.0), ("rb1", "RB One", "RB", 8.0, 1.0), ("rb2", "RB Two", "RB", 7.0, 1.0),
         ("wr1", "WR One", "WR", 6.0, 1.0), ("wr2", "WR Two", "WR", 5.0, 1.0), ("te1", "TE One", "TE", 4.0, 1.0),
         ("dst1", "DST One", "D/ST", 3.0, 1.0), ("k1", "K One", "K", 2.0, 1.0), ("wr3", "WR Three", "WR", 1.0, 1.0)],
        [("qb1", "QB", "QB"), ("rb1", "RB", "RB1"), ("rb2", "RB", "RB2"), ("wr1", "WR", "WR1"),
         ("wr2", "WR", "WR2"), ("te1", "TE", "TE"), ("dst1", "D/ST", "D/ST"), ("k1", "K", "K"),
         ("wr3", "WR", "FLEX")],
    ]

    with patch("src.optimization.lineup_solver.get_db_connection") as mock_get_conn, \
         patch("src.optimization.lineup_solver.load_lineup_slot_counts", return_value=ls.DEFAULT_LINEUP_SLOT_COUNTS), \
         patch("src.optimization.lineup_solver.requests.post") as mock_post:
        mock_get_conn.return_value.__enter__.return_value = _mock_conn_with_cursor(mock_cursor)
        swap_count = ls.optimize_weekly_lineup(team_id="1", season=2025, week=1)

    assert swap_count == 0
    mock_post.assert_not_called()


def test_optimize_weekly_lineup_end_to_end_with_swaps():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("qb1", "QB One", "QB", 10.0, 1.0), ("rb1", "RB One", "RB", 8.0, 1.0), ("rb2", "RB Two", "RB", 7.0, 1.0),
         ("wr1", "WR One", "WR", 6.0, 1.0), ("wr2", "WR Two", "WR", 5.0, 1.0), ("te1", "TE One", "TE", 4.0, 1.0),
         ("dst1", "DST One", "D/ST", 3.0, 1.0), ("k1", "K One", "K", 2.0, 1.0),
         ("wr_old", "Old WR", "WR", 1.0, 1.0), ("wr_bench", "Bench WR", "WR", 20.0, 1.09)],
        [("qb1", "QB", "QB"), ("rb1", "RB", "RB1"), ("rb2", "RB", "RB2"), ("wr1", "WR", "WR1"),
         ("wr2", "WR", "WR2"), ("te1", "TE", "TE"), ("dst1", "D/ST", "D/ST"), ("k1", "K", "K"),
         ("wr_old", "WR", "FLEX")],
    ]

    with patch("src.optimization.lineup_solver.get_db_connection") as mock_get_conn, \
         patch("src.optimization.lineup_solver.load_lineup_slot_counts", return_value=ls.DEFAULT_LINEUP_SLOT_COUNTS), \
         patch("src.optimization.lineup_solver.requests.post") as mock_post:
        mock_get_conn.return_value.__enter__.return_value = _mock_conn_with_cursor(mock_cursor)
        mock_post.return_value = MagicMock(status_code=202)
        swap_count = ls.optimize_weekly_lineup(team_id="1", season=2025, week=1)

    assert swap_count == 1
    mock_post.assert_called_once()
    url = mock_post.call_args[0][0]
    payload = mock_post.call_args[1]["json"]
    assert url == f"{ls.CORE_API_BASE_URL}/v1/trigger/lineup"
    assert payload["optimized_swaps"] == [
        {
            "drop_player_id": "wr_old", "add_player_id": "wr_bench", "slot": "FLEX",
            "matchup_multiplier": 1.09,
        }
    ]


def test_optimize_weekly_lineup_no_roster_returns_zero():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []

    with patch("src.optimization.lineup_solver.get_db_connection") as mock_get_conn, \
         patch("src.optimization.lineup_solver.load_lineup_slot_counts", return_value=ls.DEFAULT_LINEUP_SLOT_COUNTS), \
         patch("src.optimization.lineup_solver.requests.post") as mock_post:
        mock_get_conn.return_value.__enter__.return_value = _mock_conn_with_cursor(mock_cursor)
        swap_count = ls.optimize_weekly_lineup(team_id="1", season=2025, week=1)

    assert swap_count == 0
    mock_post.assert_not_called()
