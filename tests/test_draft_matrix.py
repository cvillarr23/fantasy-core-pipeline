import json
import threading
import time
import pytest
from unittest.mock import MagicMock, patch

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.util._once import Once

from src.optimization import draft_matrix as dm

LEAGUE_CONFIG = {
    "team_count": 10,
    "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 7},
}


def _player(pid, position, vorp_score, name=None, team="KC", espn_id="e-1"):
    return {"player_id": pid, "espn_id": espn_id, "name": name or pid, "position": position,
            "vorp_score": vorp_score, "team": team}


def _board_client(**initial):
    """A Redis mock whose eval() applies the real semantics of draft_matrix's two Lua scripts
    to a plain dict, exposed as client.board so tests can assert on the resulting board."""
    board = dict(initial)

    def fake_eval(script, _numkeys, _key, *argv):
        if script == dm._SURFACE_PUBLISH_ERROR_LUA:
            if board.get("listener_state") == "Failed":
                return 0
            if not board.get("listener_state"):
                board["listener_state"] = "Active"
            board["listener_error"], board["listener_error_owner"] = argv
        elif script == dm._CLEAR_PUBLISH_ERROR_LUA:
            if board.get("listener_state") == "Failed":
                return 0
            if board.get("listener_error_owner") == argv[0]:
                board["listener_error"] = ""
                board["listener_error_owner"] = ""
        else:
            raise AssertionError(f"unexpected script: {script}")
        return 1

    client = MagicMock()
    client.get.return_value = None
    client.eval.side_effect = fake_eval
    client.board = board
    return client


# ---------------------------------------------------------------------------
# resolve_own_team_flex_allocation / compute_position_caps
# ---------------------------------------------------------------------------

def test_resolve_own_team_flex_allocation_uses_default_weights_pre_draft():
    alloc = dm.resolve_own_team_flex_allocation(LEAGUE_CONFIG)
    assert sum(alloc.values()) == 1
    assert set(alloc.keys()) <= {"RB", "WR", "TE"}


def test_compute_position_caps_no_picks_yet():
    position_caps, starting_needs = dm.compute_position_caps(LEAGUE_CONFIG, drafted_by_us={})

    # Starting needs: QB=1, RB=2, WR=2, TE=1, plus 1 FLEX slot split across RB/WR/TE.
    assert starting_needs["QB"] == 1
    assert starting_needs["RB"] >= 2
    assert starting_needs["WR"] >= 2
    assert starting_needs["TE"] >= 1

    # Caps must be >= starting needs (bench adds on top), and QB caps stay tight since
    # QB has no FLEX/bench share in FLEX_DEFAULT_WEIGHTS.
    for pos in dm.VORP_POSITIONS:
        assert position_caps[pos] >= starting_needs[pos]
    assert position_caps["QB"] == starting_needs["QB"]


def test_compute_position_caps_shrinks_as_we_draft():
    # RB's starting target includes its FLEX_DEFAULT_WEIGHTS share of the 1 FLEX slot
    # (0.5 weight -> the FLEX slot itself under largest-remainder rounding), so 2 drafted
    # RBs still leaves 1 unit of starting need (the FLEX slot), unlike QB which has none.
    position_caps, starting_needs = dm.compute_position_caps(LEAGUE_CONFIG, drafted_by_us={"QB": 1, "RB": 3})
    assert starting_needs["QB"] == 0
    assert starting_needs["RB"] == 0
    assert position_caps["QB"] == 0  # QB cap has no bench share, so fully consumed


# ---------------------------------------------------------------------------
# compute_draftable_roster_size
# ---------------------------------------------------------------------------

def test_compute_draftable_roster_size_uses_be_not_full_sum():
    # IR must not inflate bench capacity - only BE is real draftable bench depth.
    config = {
        "team_count": 10,
        "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 6, "IR": 2},
    }
    starting_total, bench_total = dm.compute_draftable_roster_size(config)
    assert starting_total == 7
    assert bench_total == 6


def test_compute_draftable_roster_size_falls_back_without_be_key():
    config = {"team_count": 10, "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1}}
    starting_total, bench_total = dm.compute_draftable_roster_size(config)
    assert starting_total == 7
    assert bench_total == dm.DEFAULT_ROSTER_SIZE - 7


def test_compute_total_picks_is_team_count_times_roster_size():
    """#82: the threshold a draft's pick_number must reach before anything treats it as
    over - team_count * (starting + bench), same roster shape as compute_draftable_roster_size."""
    config = {"team_count": 10, "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 6}}
    # starting_total=7, bench_total=6 -> 13 per team, 10 teams -> 130.
    assert dm.compute_total_picks(10, config) == 130


def test_compute_total_picks_scales_with_team_count():
    config = {"team_count": 12, "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1}}
    starting_total, bench_total = dm.compute_draftable_roster_size(config)
    assert dm.compute_total_picks(12, config) == 12 * (starting_total + bench_total)


# ---------------------------------------------------------------------------
# build_and_solve_draft_pick
# ---------------------------------------------------------------------------

def test_build_and_solve_draft_pick_respects_position_caps():
    pool = [
        _player("rb1", "RB", 10.0),
        _player("rb2", "RB", 9.0),
        _player("rb3", "RB", 8.0),
        _player("wr1", "WR", 7.0),
    ]
    selected = dm.build_and_solve_draft_pick(pool, position_caps={"RB": 2, "WR": 1, "QB": 0, "TE": 0}, remaining_picks=5)
    positions = [p["position"] for p in selected]
    assert positions.count("RB") == 2
    assert positions.count("WR") == 1
    # Must take the two highest-VORP RBs, not rb3.
    ids = {p["player_id"] for p in selected}
    assert "rb1" in ids and "rb2" in ids and "rb3" not in ids


def test_build_and_solve_draft_pick_respects_remaining_picks_budget():
    pool = [_player("rb1", "RB", 10.0), _player("wr1", "WR", 9.0), _player("wr2", "WR", 8.0)]
    selected = dm.build_and_solve_draft_pick(pool, position_caps={"RB": 5, "WR": 5, "QB": 5, "TE": 5}, remaining_picks=1)
    assert len(selected) == 1
    assert selected[0]["player_id"] == "rb1"


def test_build_and_solve_draft_pick_empty_pool_returns_empty():
    assert dm.build_and_solve_draft_pick([], {}, remaining_picks=5) == []


def test_build_and_solve_draft_pick_zero_remaining_picks_returns_empty():
    pool = [_player("rb1", "RB", 10.0)]
    assert dm.build_and_solve_draft_pick(pool, {"RB": 5}, remaining_picks=0) == []


def test_build_and_solve_draft_pick_fills_slots_even_with_negative_vorp():
    # vorp_score is intentionally signed - a "<=" budget constraint would let the solver
    # maximize by taking nobody once every candidate is below replacement, even though a
    # real draft still forces a pick every round. Must still fill target_picks.
    pool = [_player("rb1", "RB", -5.0), _player("rb2", "RB", -3.0), _player("wr1", "WR", -10.0)]
    selected = dm.build_and_solve_draft_pick(pool, position_caps={"RB": 2, "WR": 1, "QB": 0, "TE": 0}, remaining_picks=2)
    assert len(selected) == 2
    assert {p["player_id"] for p in selected} == {"rb1", "rb2"}


def test_build_and_solve_draft_pick_zero_position_caps_returns_empty():
    pool = [_player("rb1", "RB", 10.0)]
    selected = dm.build_and_solve_draft_pick(pool, position_caps={"RB": 0, "WR": 0, "QB": 0, "TE": 0}, remaining_picks=5)
    assert selected == []


def test_build_and_solve_draft_pick_recommends_when_only_k_dst_remain():
    # Regression test for #83: once every skill-position slot is full, the pool and the
    # league's remaining need are both K/D-ST-only - the solver must still recommend one
    # of each rather than returning [] (the "No feasible draft recommendation" symptom).
    pool = [
        _player("k1", "K", 8.0),
        _player("k2", "K", 6.0),
        _player("dst1", "D/ST", 9.0),
        _player("dst2", "D/ST", 5.0),
    ]
    position_caps = {"QB": 0, "RB": 0, "WR": 0, "TE": 0, "K": 1, "D/ST": 1}
    selected = dm.build_and_solve_draft_pick(pool, position_caps, remaining_picks=2)

    assert len(selected) == 2
    ids = {p["player_id"] for p in selected}
    assert ids == {"k1", "dst1"}  # best-VORP K and D/ST, not the pair by chance


def test_build_and_solve_draft_pick_feasible_when_a_capped_position_has_no_candidates():
    """Regression test for PR #86 review: summing position_caps (K=1 + D/ST=1 = 2) can
    overstate what's actually fillable when the pool doesn't have a candidate at every
    capped position - here there are 2 kickers but zero D/ST left in the pool. The old
    target_picks (min(remaining, len(pool), sum(caps))) would demand 2 picks, the
    Remaining_Picks equality constraint could never be satisfied (at most 1 kicker, 0
    D/ST), and CBC would report the whole ILP infeasible - the exact "No feasible draft
    recommendation" symptom #83 was about, just triggered by pool depletion instead of
    missing ingestion. Must still return the 1 pick that's genuinely available."""
    pool = [_player("k1", "K", 8.0), _player("k2", "K", 6.0)]
    position_caps = {"QB": 0, "RB": 0, "WR": 0, "TE": 0, "K": 1, "D/ST": 1}
    selected = dm.build_and_solve_draft_pick(pool, position_caps, remaining_picks=2)

    assert len(selected) == 1
    assert selected[0]["player_id"] == "k1"


def test_build_and_solve_draft_pick_caps_dst_at_one_even_with_surplus_budget():
    # Without a Cap_D/ST constraint the solver would happily take every D/ST in the pool
    # once remaining_picks/other caps allow it - a real roster only ever starts one.
    pool = [_player("dst1", "D/ST", 9.0), _player("dst2", "D/ST", 7.0), _player("dst3", "D/ST", 5.0)]
    position_caps = {"QB": 0, "RB": 0, "WR": 0, "TE": 0, "K": 0, "D/ST": 1}
    selected = dm.build_and_solve_draft_pick(pool, position_caps, remaining_picks=3)

    assert len(selected) == 1
    assert selected[0]["player_id"] == "dst1"


def test_compute_position_caps_includes_k_and_dst_from_lineup_slot_counts():
    config = {
        "team_count": 10,
        "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "K": 1, "D/ST": 1, "BE": 7},
    }
    position_caps, starting_needs = dm.compute_position_caps(config, drafted_by_us={})
    assert starting_needs["K"] == 1
    assert starting_needs["D/ST"] == 1
    assert position_caps["K"] == 1  # no bench/FLEX share for K, same as QB
    assert position_caps["D/ST"] == 1


def test_compute_position_caps_k_and_dst_drop_to_zero_once_drafted():
    config = {
        "team_count": 10,
        "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "K": 1, "D/ST": 1, "BE": 7},
    }
    position_caps, starting_needs = dm.compute_position_caps(config, drafted_by_us={"K": 1, "D/ST": 1})
    assert starting_needs["K"] == 0
    assert starting_needs["D/ST"] == 0
    assert position_caps["K"] == 0
    assert position_caps["D/ST"] == 0


def test_compute_draftable_roster_size_includes_k_and_dst():
    config = {
        "team_count": 10,
        "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "K": 1, "D/ST": 1, "BE": 6},
    }
    starting_total, bench_total = dm.compute_draftable_roster_size(config)
    assert starting_total == 9  # 7 skill-position slots (incl. FLEX) + K + D/ST
    assert bench_total == 6


# ---------------------------------------------------------------------------
# select_recommended_target
# ---------------------------------------------------------------------------

def test_select_recommended_target_prioritizes_open_starting_need():
    selected = [_player("rb1", "RB", 20.0), _player("te1", "TE", 5.0)]
    # RB starting need already filled (0); TE still needed - TE should win even though
    # its raw vorp_score is lower.
    target = dm.select_recommended_target(selected, starting_needs={"RB": 0, "TE": 1})
    assert target["player_id"] == "te1"


def test_select_recommended_target_falls_back_to_best_available():
    selected = [_player("rb1", "RB", 20.0), _player("te1", "TE", 5.0)]
    target = dm.select_recommended_target(selected, starting_needs={"RB": 0, "TE": 0})
    assert target["player_id"] == "rb1"


def test_select_recommended_target_empty_selection_returns_none():
    assert dm.select_recommended_target([], starting_needs={}) is None


# ---------------------------------------------------------------------------
# rank_recommended_targets
# ---------------------------------------------------------------------------

def test_rank_recommended_targets_orders_by_vorp_desc_within_starter_needed():
    selected = [_player("rb1", "RB", 5.0), _player("te1", "TE", 20.0), _player("rb2", "RB", 12.0)]
    ranked = dm.rank_recommended_targets(selected, starting_needs={"RB": 1, "TE": 1}, limit=5)
    assert [p["player_id"] for p in ranked] == ["te1", "rb2", "rb1"]


def test_rank_recommended_targets_respects_limit():
    selected = [_player(f"p{i}", "RB", float(i)) for i in range(10)]
    ranked = dm.rank_recommended_targets(selected, starting_needs={"RB": 1}, limit=3)
    assert len(ranked) == 3
    assert [p["player_id"] for p in ranked] == ["p9", "p8", "p7"]


def test_rank_recommended_targets_empty_selection_returns_empty_list():
    assert dm.rank_recommended_targets([], starting_needs={}) == []


def test_rank_recommended_targets_fills_remaining_slots_with_best_available():
    """A single starter-needed candidate must not truncate the ranked list to just that one
    player when limit=5 and other selected candidates exist - the comparison panel needs
    real alternatives, not an artificially short list."""
    selected = [
        _player("rb1", "RB", 20.0), _player("wr1", "WR", 15.0), _player("wr2", "WR", 14.0),
        _player("te1", "TE", 12.0), _player("qb1", "QB", 10.0),
    ]
    # Only RB is still an open starting need; everything else is already filled.
    ranked = dm.rank_recommended_targets(selected, starting_needs={"RB": 1, "WR": 0, "TE": 0, "QB": 0}, limit=5)
    assert [p["player_id"] for p in ranked] == ["rb1", "wr1", "wr2", "te1", "qb1"]


# ---------------------------------------------------------------------------
# load_undrafted_pool / load_team_position_counts / persist_draft_pick (query shape)
# ---------------------------------------------------------------------------

def test_load_undrafted_pool_excludes_drafted_players():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []

    dm.load_undrafted_pool(mock_cursor, league_id="L1", season=2026, week=1)

    query, params = mock_cursor.execute.call_args[0]
    assert "NOT EXISTS" in query
    assert "draft_picks" in query
    assert "COALESCE(s.team, p.team)" in query
    assert params == (2026, 1, "L1", 2026)


def test_load_team_position_counts_query_shape():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("RB", 2), ("WR", 1)]

    counts = dm.load_team_position_counts(mock_cursor, league_id="L1", season=2026, team_id="7")

    assert counts == {"RB": 2, "WR": 1}
    query, params = mock_cursor.execute.call_args[0]
    assert "draft_picks" in query
    assert params == ("L1", 2026, "7")


def test_persist_draft_pick_returns_true_when_inserted():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (5,)
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    inserted = dm.persist_draft_pick(mock_conn, league_id="L1", season=2026, pick_number=5, round_=1, team_id="7", player_id="p1")

    assert inserted is True
    query, params = mock_cursor.execute.call_args[0]
    assert "ON CONFLICT" in query
    assert "DO NOTHING" in query
    assert "RETURNING" in query
    assert params == ("L1", 2026, 5, 1, "7", "p1", None)


def test_persist_draft_pick_returns_false_when_already_recorded():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    inserted = dm.persist_draft_pick(mock_conn, league_id="L1", season=2026, pick_number=5, round_=1, team_id="7", player_id="p1")

    assert inserted is False


# ---------------------------------------------------------------------------
# wait_for_pause_or_timeout
# ---------------------------------------------------------------------------

def test_wait_for_pause_or_timeout_proceeds_when_never_paused():
    mock_client = MagicMock()
    mock_client.get.return_value = None
    sleeps = []

    result = dm.wait_for_pause_or_timeout(mock_client, "L1", pick_number=3, delay_seconds=3, sleep_fn=sleeps.append)

    assert result is True
    assert len(sleeps) == 3


def test_wait_for_pause_or_timeout_aborts_when_paused():
    mock_client = MagicMock()
    mock_client.get.return_value = "1"

    result = dm.wait_for_pause_or_timeout(mock_client, "L1", pick_number=3, delay_seconds=5, sleep_fn=lambda s: None)

    assert result is False


def test_wait_for_pause_or_timeout_checks_again_after_final_sleep():
    # A pause set during the very last polling window must still be caught - checking
    # only before each sleep would miss it and publish anyway.
    mock_client = MagicMock()
    calls = {"n": 0}

    def _get(key):
        calls["n"] += 1
        return "1" if calls["n"] > 2 else None

    mock_client.get.side_effect = _get

    result = dm.wait_for_pause_or_timeout(mock_client, "L1", pick_number=3, delay_seconds=2, sleep_fn=lambda s: None)

    assert result is False
    assert calls["n"] == 3  # 2 pre-sleep checks + 1 final check


def test_wait_for_pause_or_timeout_aborts_when_cancelled():
    mock_client = MagicMock()
    mock_client.get.return_value = None
    cancel_event = threading.Event()
    cancel_event.set()

    result = dm.wait_for_pause_or_timeout(
        mock_client, "L1", pick_number=3, delay_seconds=5, cancel_event=cancel_event, sleep_fn=lambda s: None
    )

    assert result is False


# ---------------------------------------------------------------------------
# publish_draft_target
# ---------------------------------------------------------------------------

def test_publish_draft_target_posts_expected_payload():
    target = _player("p1", "RB", 12.0, name="Some Back")

    with patch("src.optimization.draft_matrix.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=202)
        dm.publish_draft_target("7", target)

    url, kwargs = mock_post.call_args[0], mock_post.call_args[1]
    assert url[0] == f"{dm.CORE_API_BASE_URL}/v1/trigger/draft-pick"
    assert kwargs["json"] == {
        "team_id": "7",
        # ESPN's id, not the internal one: the worker clicks data-player-id in the draft room.
        "player_id": "e-1",
        "name": "Some Back",
        "position": "RB",
        "pro_team": "KC",
        # Absent unless the caller is retrying; the gateway treats None as "no dedup".
        "idempotency_key": None,
        # Absent for a direct call; hold_and_publish always sets these.
        "expires_at": None,
        "league_id": None,
        "pick_number": None,
        # DRAFT_MODE env var, defaulting "live" - read once at import time.
        "mode": "live",
    }


def test_publish_draft_target_mode_override_takes_precedence_over_env_default():
    """DRAFT_MODE is a per-process env var, correctly set on the per-league draft-sync Job
    this function normally runs inside - but never set on the FastAPI gateway Deployment,
    which also calls this (e.g. /resume) and would otherwise always send "live" even for a
    practice league's resume. An explicit mode= argument must win over that env default."""
    target = _player("p1", "RB", 12.0, name="Some Back")

    with patch("src.optimization.draft_matrix.requests.post") as mock_post, \
         patch("src.optimization.draft_matrix.DRAFT_MODE", "live"):
        mock_post.return_value = MagicMock(status_code=202)
        dm.publish_draft_target("7", target, mode="practice")

    assert mock_post.call_args[1]["json"]["mode"] == "practice"


def test_publish_draft_target_falls_back_to_env_default_when_mode_not_given():
    target = _player("p1", "RB", 12.0, name="Some Back")

    with patch("src.optimization.draft_matrix.requests.post") as mock_post, \
         patch("src.optimization.draft_matrix.DRAFT_MODE", "practice"):
        mock_post.return_value = MagicMock(status_code=202)
        dm.publish_draft_target("7", target)

    assert mock_post.call_args[1]["json"]["mode"] == "practice"


def test_publish_draft_target_refuses_a_player_with_no_crosswalk_entry():
    """182 of 1027 players have no espn_id. Posting a null id would either time out in the
    draft room or be coerced into clicking a different player - never silently proceed."""
    target = _player("p1", "RB", 12.0, name="Some Back", espn_id=None)

    with patch("src.optimization.draft_matrix.requests.post") as mock_post:
        with pytest.raises(ValueError, match="no espn_id"):
            dm.publish_draft_target("7", target)

    mock_post.assert_not_called()


def test_publish_draft_target_logs_error_response_body(caplog):
    target = _player("p1", "RB", 12.0, name="Some Back")
    response = dm.requests.Response()
    response.status_code = 422
    response._content = b'{"detail":[{"loc":["body","pro_team"],"msg":"Input should be a valid string"}]}'
    response.url = f"{dm.CORE_API_BASE_URL}/v1/trigger/draft-pick"

    with patch("src.optimization.draft_matrix.requests.post", return_value=response):
        with caplog.at_level("ERROR", logger="DraftMatrix"):
            with pytest.raises(dm.requests.HTTPError):
                dm.publish_draft_target("7", target)

    assert '"pro_team"' in caplog.text
    assert "Input should be a valid string" in caplog.text


def test_publish_draft_target_records_last_attempted_target(monkeypatch):
    """Every publish_draft_target call (successful HTTP response, regardless of what
    ESPN ultimately does with it) must record what we attempted for this pick_number,
    so a later opponent-side pick event for our own team can be compared against it
    in handle_pick_event (issue #108, Task F)."""
    target = _player("p1", "RB", 12.0, name="Some Back", espn_id="e-1")
    mock_client = MagicMock()

    with patch("src.optimization.draft_matrix.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=202)
        dm.publish_draft_target(
            "7", target, league_id="L1", pick_number=5, r_client=mock_client,
        )

    mock_client.hset.assert_called_once_with(
        "draft:L1:last_attempted_target", "5", json.dumps({"espn_id": "e-1", "name": "Some Back"}),
    )


def test_publish_draft_target_skips_recording_without_r_client_league_id_or_pick_number():
    """A direct call missing any of r_client/league_id/pick_number (e.g. /v1/draft/resume,
    which does not thread a Redis client through) must not attempt the hset - there is
    nothing meaningful to key it on."""
    target = _player("p1", "RB", 12.0, name="Some Back", espn_id="e-1")

    with patch("src.optimization.draft_matrix.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=202)
        dm.publish_draft_target("7", target)  # no r_client, league_id, or pick_number


def test_record_publish_error_writes_status_visible_execution_log_row():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm._record_publish_error("L1", _player("p1", "RB", 10.0, name="Some Back"), "HTTP 422", season=2026)

    query, params = mock_cursor.execute.call_args.args
    assert "draft_target_publish" in query
    assert "execution_log" in query
    # season is recorded because a reactivated league keeps its league_id year over year;
    # without it /status opens next year's draft showing this year's failures.
    assert params == ("HTTP 422", "L1", 2026, "e-1", "Some Back")


# ---------------------------------------------------------------------------
# hold_and_publish
# ---------------------------------------------------------------------------

def test_hold_and_publish_publishes_when_not_cancelled():
    mock_client = MagicMock()
    mock_client.get.return_value = None
    target = _player("p1", "RB", 10.0, name="Some Back")

    with patch("src.optimization.draft_matrix.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=202)
        dm.hold_and_publish(mock_client, "L1", "7", 5, [target], delay_seconds=1, sleep_fn=lambda s: None)

    mock_post.assert_called_once()


def test_hold_and_publish_skips_when_cancelled():
    mock_client = MagicMock()
    mock_client.get.return_value = None
    cancel_event = threading.Event()
    cancel_event.set()
    target = _player("p1", "RB", 10.0)

    with patch("src.optimization.draft_matrix.requests.post") as mock_post:
        dm.hold_and_publish(mock_client, "L1", "7", 5, [target], delay_seconds=1, cancel_event=cancel_event, sleep_fn=lambda s: None)

    mock_post.assert_not_called()


def test_hold_and_publish_abandons_when_auto_publish_disabled():
    """A false auto_publish_enabled must stop hold_and_publish before it ever calls
    publish_draft_target, exactly like an operator pause (#108/#109)."""
    mock_client = MagicMock()
    mock_client.get.return_value = None
    target = _player("p1", "RB", 10.0)

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (False,)
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.requests.post") as mock_post:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.hold_and_publish(mock_client, "L1", "7", 5, [target], delay_seconds=1, sleep_fn=lambda s: None)

    mock_post.assert_not_called()
    query, params = mock_cursor.execute.call_args.args
    assert "auto_publish_enabled" in query
    assert params == ("L1",)


def test_auto_publish_disabled_reads_fresh_each_call():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (True,)
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        assert dm._auto_publish_disabled("L1") is False


def test_auto_publish_disabled_fails_open_on_db_error():
    """A DB hiccup reading auto_publish_enabled must not silently convert every league
    into permanent manual-only mode - fail open (not disabled)."""
    with patch("src.optimization.draft_matrix.get_db_connection", side_effect=RuntimeError("db down")):
        assert dm._auto_publish_disabled("L1") is False


def test_set_auto_publish_enabled_disables_and_records_reason():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm._set_auto_publish_enabled("L1", False, disabled_by="manual")

    query, params = mock_cursor.execute.call_args.args
    assert "auto_publish_enabled" in query
    assert "auto_publish_disabled_by" in query
    assert params == (False, "manual", "L1")


def test_set_auto_publish_enabled_clears_disabled_by_when_reenabled():
    """Re-enabling always clears disabled_by to NULL, regardless of what the caller passes -
    a re-enabled league has no active disabled reason."""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm._set_auto_publish_enabled("L1", True, disabled_by="manual")

    query, params = mock_cursor.execute.call_args.args
    assert params == (True, None, "L1")


def test_hold_and_publish_retries_transient_publish_failure():
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)
    sleeps = []

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=[dm.requests.ConnectionError("unavailable"), None]) as mock_publish, \
         patch.object(dm, "_record_publish_error") as mock_record:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=8,
            sleep_fn=sleeps.append,
        )

    assert mock_publish.call_count == 2
    # After the retry succeeds, hold_and_publish now polls for a confirmed rejection for up
    # to PICK_REJECTION_GRACE_SECONDS (see draft_matrix.py) before declaring success - this
    # mock_client never produces a matching rejection, so every poll falls through to a
    # sleep_fn(PICK_REJECTION_POLL_INTERVAL_SECONDS) call.
    max_grace_polls = int(dm.PICK_REJECTION_GRACE_SECONDS // dm.PICK_REJECTION_POLL_INTERVAL_SECONDS) + 1
    assert sleeps == [dm.PUBLISH_RETRY_INTERVAL_SECONDS] + [dm.PICK_REJECTION_POLL_INTERVAL_SECONDS] * max_grace_polls
    mock_record.assert_called_once()
    # The retracting write is a compare-and-set on this thread's ownership token.
    assert mock_client.eval.call_args.args[0] == dm._CLEAR_PUBLISH_ERROR_LUA
    assert mock_client.eval.call_args.args[-1].startswith("publish:L1:5:")
    assert mock_client.board["listener_error"] == ""


def test_hold_and_publish_does_not_clobber_a_newer_listener_error_on_retry_success():
    """The Stream consumer can record a real listener failure while our HTTP attempt is in
    flight. Clearing unconditionally would blank it and report a healthy draft."""
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)
    attempts = []

    def publish(*_args, **_kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise dm.requests.ConnectionError("unavailable")
        # The consumer thread records a genuine listener failure while this attempt runs,
        # replacing the transient publish error we surfaced a moment ago.
        mock_client.board["listener_state"] = "Failed"
        mock_client.board["listener_error"] = "Listener died: connection refused"
        mock_client.board["listener_error_owner"] = "listener"

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=publish), \
         patch.object(dm, "_record_publish_error"):
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=8, sleep_fn=lambda _s: None,
        )

    assert mock_client.board["listener_error"] == "Listener died: connection refused"
    assert mock_client.board["listener_state"] == "Failed"


def test_hold_and_publish_identifies_the_slot_so_the_gateway_can_revalidate():
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0, sleep_fn=lambda _s: None,
        )

    assert mock_publish.call_args.kwargs["league_id"] == "L1"
    assert mock_publish.call_args.kwargs["pick_number"] == 5


def test_hold_and_publish_sends_an_absolute_deadline_to_the_gateway():
    """The gateway handler is synchronous and can resume from a blocked broker call after our
    request timed out, so it needs the deadline itself rather than our timeout."""
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 90), \
         patch.object(dm, "DRAFT_PUBLISH_SAFETY_SECONDS", 0), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0, sleep_fn=lambda _s: None,
            monotonic_fn=lambda: 30.0, clock_origin=0.0, time_fn=lambda: 1_000.0,
        )

    # 60s of clock left at wall time 1000.
    assert mock_publish.call_args.kwargs["expires_at"] == 1_060.0


def test_hold_and_publish_will_not_retract_an_error_a_failed_listener_now_owns():
    """_write_listener_state resets listener_error_owner, so a publisher holding a stale
    token cannot clear the fatal reason the listener wrote after it."""
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)
    attempts = []

    def publish(*_args, **_kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise dm.requests.ConnectionError("unavailable")
        dm._write_listener_state(mock_client, "L1", "Failed", error="Listener crashed")
        # _write_listener_state goes through hset, which the board mock does not model.
        mock_client.board.update({
            "listener_state": "Failed",
            "listener_error": "Listener crashed",
            "listener_error_owner": "",
        })

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=publish), \
         patch.object(dm, "_record_publish_error"):
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0, sleep_fn=lambda _s: None,
        )

    assert mock_client.board["listener_error"] == "Listener crashed"


def test_hold_and_publish_leaves_a_failed_listeners_error_intact():
    """When the Stream consumer has already recorded a fatal listener reason, a later publish
    error must not replace it on the board - that reason is the one an operator acts on. The
    publish failure still reaches execution_log, which is the durable record."""
    mock_client = _board_client(listener_state="Failed", listener_error="Listener died: connection refused")
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=RuntimeError("no espn_id")), \
         patch.object(dm, "_record_publish_error") as mock_record:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0, sleep_fn=lambda _s: None,
        )

    assert mock_client.board["listener_state"] == "Failed"
    assert mock_client.board["listener_error"] == "Listener died: connection refused"
    mock_record.assert_called_once()


def test_hold_and_publish_does_not_clear_an_identical_error_owned_by_another_publisher():
    """Two publisher threads can record the same message text. Matching on text alone would
    let the older thread's success retract the newer thread's still-current failure."""
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)
    attempts = []

    def publish(*_args, **_kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise dm.requests.ConnectionError("connection refused")
        # A concurrent publisher for the next pick records the identical message.
        mock_client.board["listener_error"] = "connection refused"
        mock_client.board["listener_error_owner"] = "publish:L1:6:other"

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=publish), \
         patch.object(dm, "_record_publish_error"):
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0, sleep_fn=lambda _s: None,
        )

    assert mock_client.board["listener_error"] == "connection refused"
    assert mock_client.board["listener_error_owner"] == "publish:L1:6:other"


def test_hold_and_publish_surfaces_failure_after_clock_expires():
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)
    now = [0]

    def _sleep(seconds):
        now[0] += seconds

    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 5), \
         patch.object(dm, "DRAFT_PUBLISH_SAFETY_SECONDS", 0), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=dm.requests.ConnectionError("unavailable")) as mock_publish, \
         patch.object(dm, "_record_publish_error") as mock_record:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0,
            sleep_fn=_sleep, monotonic_fn=lambda: now[0],
        )

    assert mock_publish.call_count > 1
    # Twice: the first failure, then again for the one that actually ended the attempt.
    # Reporting only the first hides the terminal cause, which is the one a human acts on.
    assert mock_record.call_count == 2
    assert mock_record.call_args.args[:2] == ("L1", target)
    assert "unavailable" in mock_record.call_args.args[2]
    assert mock_client.board["listener_state"] == "Active"
    assert "unavailable" in mock_client.board["listener_error"]


def test_hold_and_publish_realistic_clock_is_not_superseded_without_a_new_pick():
    mock_client = MagicMock()
    mock_client.get.return_value = None
    target = _player("p1", "RB", 10.0)
    sleeps = []

    with patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=8,
            sleep_fn=sleeps.append,
        )

    # 8s of hold delay, plus hold_and_publish's post-publish grace-window polling (see
    # PICK_REJECTION_GRACE_SECONDS) - this mock_client never produces a matching rejection,
    # so every poll falls through to a sleep_fn(PICK_REJECTION_POLL_INTERVAL_SECONDS) call.
    max_grace_polls = int(dm.PICK_REJECTION_GRACE_SECONDS // dm.PICK_REJECTION_POLL_INTERVAL_SECONDS) + 1
    assert sum(sleeps) == 8 + max_grace_polls * dm.PICK_REJECTION_POLL_INTERVAL_SECONDS
    assert mock_publish.call_count == 1
    assert mock_publish.call_args.args == ("7", target)
    assert mock_publish.call_args.kwargs["idempotency_key"] == "L1:5:e-1"
    assert mock_publish.call_args.kwargs["timeout"] == dm.PUBLISH_TIMEOUT_SECONDS


# ---------------------------------------------------------------------------
# hold_and_publish - PR #85 review fixes
# ---------------------------------------------------------------------------

def test_hold_and_publish_does_not_retry_a_deterministic_client_error():
    """A 422 answers identically to a byte-identical retry. Retrying it spends the rest of
    the pick clock re-asking a settled question and buries the one useful log line."""
    mock_client = MagicMock()
    mock_client.get.return_value = None
    target = _player("p1", "RB", 10.0)
    response = dm.requests.Response()
    response.status_code = 422
    response._content = b'{"detail":[{"loc":["body","pro_team"]}]}'
    error = dm.requests.HTTPError("422 Client Error", response=response)
    sleeps = []

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=error) as mock_publish, \
         patch.object(dm, "_record_publish_error"):
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=8, sleep_fn=sleeps.append,
        )

    assert mock_publish.call_count == 1
    assert sleeps == []


def test_hold_and_publish_does_not_retry_a_missing_crosswalk_entry():
    """publish_draft_target's own ValueError is deterministic too - the target will still
    have no espn_id two seconds later."""
    mock_client = MagicMock()
    mock_client.get.return_value = None
    target = _player("p1", "RB", 10.0)
    sleeps = []

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=ValueError("no espn_id")) as mock_publish, \
         patch.object(dm, "_record_publish_error"):
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=8, sleep_fn=sleeps.append,
        )

    assert mock_publish.call_count == 1
    assert sleeps == []


@pytest.mark.parametrize("status", sorted(dm.RETRYABLE_PUBLISH_STATUSES))
def test_retryable_publish_statuses_are_retried(status):
    response = dm.requests.Response()
    response.status_code = status
    assert dm._is_retryable_publish_error(dm.requests.HTTPError("boom", response=response)) is True


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_deterministic_client_errors_are_not_retried(status):
    response = dm.requests.Response()
    response.status_code = status
    assert dm._is_retryable_publish_error(dm.requests.HTTPError("boom", response=response)) is False


def test_transport_failures_with_no_response_are_retried():
    assert dm._is_retryable_publish_error(dm.requests.ConnectionError("reset")) is True
    assert dm._is_retryable_publish_error(dm.requests.Timeout("slow")) is True
    assert dm._is_retryable_publish_error(RuntimeError("programming error")) is False


def test_hold_and_publish_rechecks_cancellation_between_retries():
    """A newer pick can supersede this target *during* the retry sleep. Checking only after
    a failure - and not again before the next attempt - published the stale target anyway."""
    mock_client = MagicMock()
    mock_client.get.return_value = None
    target = _player("p1", "RB", 10.0)
    cancel_event = threading.Event()

    def _sleep(_seconds):
        cancel_event.set()

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=dm.requests.ConnectionError("unavailable")) as mock_publish, \
         patch.object(dm, "_record_publish_error"):
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=8,
            cancel_event=cancel_event, sleep_fn=_sleep,
        )

    assert mock_publish.call_count == 1


def test_hold_and_publish_rechecks_operator_pause_between_retries():
    mock_client = MagicMock()
    pause_key = "draft:L1:pause:5"
    paused = {"value": False}
    mock_client.get.side_effect = lambda key: "1" if (key == pause_key and paused["value"]) else None
    target = _player("p1", "RB", 10.0)

    def _sleep(_seconds):
        paused["value"] = True

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=dm.requests.ConnectionError("unavailable")) as mock_publish, \
         patch.object(dm, "_record_publish_error"):
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=8, sleep_fn=_sleep,
        )

    assert mock_publish.call_count == 1


def test_hold_and_publish_abandons_when_espn_reports_a_different_team_on_the_clock():
    """#107: our own snake-draft-derived schedule assumed it would be team 7's turn by now,
    but ESPN's real-time SELECTING broadcast says team 3 is actually up - a practice room can
    let the same team pick twice in a row, which our math doesn't model. Must not publish."""
    mock_client = MagicMock()
    selecting_key = "draft:L1:selecting"
    mock_client.get.side_effect = lambda key: (
        json.dumps({"team_id": 3}) if key == selecting_key else None
    )
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(mock_client, "L1", "7", 5, [target], delay_seconds=1, sleep_fn=lambda _s: None)

    mock_publish.assert_not_called()


def test_hold_and_publish_publishes_when_espn_confirms_our_team_on_the_clock():
    mock_client = MagicMock()
    selecting_key = "draft:L1:selecting"
    mock_client.get.side_effect = lambda key: (
        json.dumps({"team_id": 7}) if key == selecting_key else None
    )
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(mock_client, "L1", "7", 5, [target], delay_seconds=1, sleep_fn=lambda _s: None)

    mock_publish.assert_called_once()


def test_hold_and_publish_publishes_when_espn_selecting_signal_is_unavailable():
    """None (older worker version, key not written yet) must not block a publish that would
    otherwise be correct - only a *positive* contradiction should abandon it."""
    mock_client = MagicMock()
    mock_client.get.return_value = None
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(mock_client, "L1", "7", 5, [target], delay_seconds=1, sleep_fn=lambda _s: None)

    mock_publish.assert_called_once()


def test_hold_and_publish_skips_hold_entirely_when_espn_autodraft_is_on_for_our_team():
    """Regression test for the bug confirmed live 2026-08-28 (league 298899823): ESPN's own
    per-team AUTODRAFT toggle was ON for our seat - there is no clock at all in that state,
    ESPN fills the pick instantly, and no hold delay, however short, can ever win that race.
    The hold must be skipped entirely (delay_seconds=0), not merely shortened."""
    mock_client = MagicMock()
    mock_client.get.side_effect = lambda key: (
        json.dumps({"team_id": "7", "active": True}) if key == "draft:L1:autodraft" else None
    )
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True) as mock_wait, \
         patch.object(dm, "publish_draft_target"):
        dm.hold_and_publish(mock_client, "L1", "7", 5, [target], delay_seconds=15, sleep_fn=lambda _s: None)

    assert mock_wait.call_args.args[3] == 0


def test_hold_and_publish_ignores_autodraft_active_for_a_different_team():
    """ESPN's autodraft toggle being on for someone else's seat says nothing about ours -
    must not affect our own hold delay."""
    mock_client = MagicMock()
    mock_client.get.side_effect = lambda key: (
        json.dumps({"team_id": "3", "active": True}) if key == "draft:L1:autodraft" else None
    )
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True) as mock_wait, \
         patch.object(dm, "publish_draft_target"):
        dm.hold_and_publish(mock_client, "L1", "7", 5, [target], delay_seconds=15, sleep_fn=lambda _s: None)

    assert mock_wait.call_args.args[3] == 15


def test_hold_and_publish_prefers_autodraft_active_signal_over_clock_ms():
    """When ESPN reports both signals at once (autodraft toggled on for our team, plus a
    stale/irrelevant clock_ms elsewhere), the autodraft signal - the more decisive one, since
    it means no clock exists at all - must win: skip the hold entirely rather than merely
    shrink it to whatever clock_ms happened to say."""
    mock_client = MagicMock()

    def _get(key):
        if key == "draft:L1:autodraft":
            return json.dumps({"team_id": "7", "active": True})
        if key == "draft:L1:selecting":
            return json.dumps({"team_id": "7", "clock_ms": 10000})
        return None

    mock_client.get.side_effect = _get
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True) as mock_wait, \
         patch.object(dm, "publish_draft_target"):
        dm.hold_and_publish(mock_client, "L1", "7", 5, [target], delay_seconds=15, sleep_fn=lambda _s: None)

    assert mock_wait.call_args.args[3] == 0


def test_hold_and_publish_shortens_initial_hold_when_espn_reports_less_time():
    """Regression test for the bug confirmed live 2026-08-28 (league 1986717528):
    DRAFT_PICK_CLOCK_SECONDS/AUTO_DRAFT_DELAY_SECONDS are hardcoded per-mode guesses -
    ESPN autodrafted our own pick out from under us on every single attempt because the
    system held its full 15s delay while ESPN's real clock for our team was only 10s. When
    ESPN confirms it's genuinely our turn with clock_ms=10000 (5s of headroom reserved via
    DRAFT_PUBLISH_SAFETY_SECONDS), the hold must shrink to 5s, not stay at 15."""
    mock_client = MagicMock()
    mock_client.get.side_effect = lambda key: (
        json.dumps({"team_id": "7", "clock_ms": 10000}) if key == "draft:L1:selecting" else None
    )
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True) as mock_wait, \
         patch.object(dm, "publish_draft_target"):
        dm.hold_and_publish(mock_client, "L1", "7", 5, [target], delay_seconds=15, sleep_fn=lambda _s: None)

    assert mock_wait.call_args.args[3] == 5  # 10s real clock - 5s DRAFT_PUBLISH_SAFETY_SECONDS


def test_hold_and_publish_never_extends_initial_hold_past_the_configured_delay():
    """Shrink-only: ESPN reporting *more* real time than our own delay must never make the
    hold wait longer than the operator/mode configured - only ever tighten toward reality."""
    mock_client = MagicMock()
    mock_client.get.side_effect = lambda key: (
        json.dumps({"team_id": "7", "clock_ms": 60000}) if key == "draft:L1:selecting" else None
    )
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True) as mock_wait, \
         patch.object(dm, "publish_draft_target"):
        dm.hold_and_publish(mock_client, "L1", "7", 5, [target], delay_seconds=15, sleep_fn=lambda _s: None)

    assert mock_wait.call_args.args[3] == 15


def test_hold_and_publish_ignores_clock_ms_for_a_different_teams_selecting_signal():
    """clock_ms only means something when ESPN confirms it's genuinely OUR turn - a different
    team's clock reading must not shrink (or otherwise affect) our own hold."""
    mock_client = MagicMock()
    mock_client.get.side_effect = lambda key: (
        json.dumps({"team_id": "3", "clock_ms": 1000}) if key == "draft:L1:selecting" else None
    )
    target = _player("p1", "RB", 10.0)

    # team_id 3 != our team (7), so hold_and_publish abandons before ever reaching
    # wait_for_pause_or_timeout - this asserts that abandonment, not a shortened hold.
    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True) as mock_wait, \
         patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(mock_client, "L1", "7", 5, [target], delay_seconds=15, sleep_fn=lambda _s: None)

    mock_wait.assert_called_once()
    assert mock_wait.call_args.args[3] == 15
    mock_publish.assert_not_called()


def test_hold_and_publish_tightens_retry_deadline_when_espn_reports_less_time():
    """Same clock-trusting fix, applied on every loop iteration (not just the initial hold):
    if a retry is still in flight and ESPN's real clock for us is shorter than our own
    DRAFT_PICK_CLOCK_SECONDS-derived deadline, the deadline must shrink to match - so a
    too-tight-for-us real clock is detected and abandoned instead of over-retrying past it."""
    mock_client = MagicMock()
    mock_client.get.side_effect = lambda key: (
        json.dumps({"team_id": "7", "clock_ms": 500}) if key == "draft:L1:selecting" else None
    )
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target") as mock_publish, \
         patch.object(dm, "_surface_clock_expired") as mock_surface, \
         patch.object(dm, "_record_publish_outcome") as mock_record:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0,
            sleep_fn=lambda _s: None, monotonic_fn=lambda: 0.0, clock_origin=0.0,
        )

    # clock_ms=500 (0.5s) minus DRAFT_PUBLISH_SAFETY_SECONDS (5s) is negative - the tightened
    # deadline is already in the past, so the target must never be published.
    mock_publish.assert_not_called()
    mock_surface.assert_called_once()
    mock_record.assert_called_once_with(mock_client, "L1", succeeded=False)


def test_hold_and_publish_long_delay_still_honors_pause():
    mock_client = MagicMock()
    pause_key = "draft:L1:pause:5"
    paused = {"value": False}
    mock_client.get.side_effect = lambda key: "1" if (key == pause_key and paused["value"]) else None
    target = _player("p1", "RB", 10.0)

    def _sleep(_seconds):
        paused["value"] = True

    with patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=20,
            sleep_fn=_sleep, monotonic_fn=lambda: 0.0, clock_origin=0.0,
        )

    mock_publish.assert_not_called()


def test_hold_and_publish_never_publishes_after_the_clock_has_already_expired():
    """The deadline is anchored to when the upstream pick was observed, so a slow ILP solve
    can consume the entire clock before this thread even starts. Publishing then would send
    a target for a pick ESPN has already autodrafted."""
    mock_client = MagicMock()
    mock_client.get.return_value = None
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 30), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target") as mock_publish, \
         patch.object(dm, "_record_publish_error"):
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0,
            sleep_fn=lambda _s: None, monotonic_fn=lambda: 100.0,
            clock_origin=0.0,  # the pick landed 100s ago; a 30s clock is long gone
        )

    mock_publish.assert_not_called()


def test_hold_and_publish_uses_the_supplied_clock_origin_not_thread_start():
    """Regression: anchoring the deadline at thread start excluded everything
    handle_pick_event does synchronously first, silently extending the real clock."""
    mock_client = MagicMock()
    mock_client.get.return_value = None
    target = _player("p1", "RB", 10.0)

    # 20s of the 30s clock was already spent persisting the pick and solving the ILP, so
    # only ~5s of publish window remains after the safety margin - one attempt, no retry.
    now = [20.0]

    def _sleep(seconds):
        now[0] += seconds

    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 30), \
         patch.object(dm, "DRAFT_PUBLISH_SAFETY_SECONDS", 5), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=dm.requests.ConnectionError("unavailable")) as mock_publish, \
         patch.object(dm, "_record_publish_error"):
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0,
            sleep_fn=_sleep, monotonic_fn=lambda: now[0], clock_origin=0.0,
        )

    # Deadline is 0 + 30 - 5 = 25, so attempts land at t=20, 22 and 24 and the loop then
    # stops rather than running to t=45 as a thread-start anchor would have.
    assert mock_publish.call_count == 3
    assert now[0] < 25.0


def test_hold_and_publish_clamps_the_request_timeout_to_the_remaining_window():
    """Starting an attempt inside the deadline is not enough: with a fixed 10s timeout, a
    request begun one second before it could enqueue nine seconds after the pick clock."""
    mock_client = MagicMock()
    mock_client.get.return_value = None
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 30), \
         patch.object(dm, "DRAFT_PUBLISH_SAFETY_SECONDS", 0), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0,
            sleep_fn=lambda _s: None, monotonic_fn=lambda: 27.0, clock_origin=0.0,
        )

    # 3s of window left, so the attempt gets 3s - not the 10s default.
    assert mock_publish.call_args.kwargs["timeout"] == 3.0


def test_hold_and_publish_uses_the_full_timeout_when_the_window_is_wide():
    mock_client = MagicMock()
    mock_client.get.return_value = None
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 90), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0,
            sleep_fn=lambda _s: None, monotonic_fn=lambda: 0.0, clock_origin=0.0,
        )

    assert mock_publish.call_args.kwargs["timeout"] == dm.PUBLISH_TIMEOUT_SECONDS


def test_hold_and_publish_does_not_start_an_attempt_that_cannot_finish():
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 30), \
         patch.object(dm, "DRAFT_PUBLISH_SAFETY_SECONDS", 0), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "_record_publish_error") as mock_record, \
         patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0,
            sleep_fn=lambda _s: None, monotonic_fn=lambda: 29.5, clock_origin=0.0,
        )

    mock_publish.assert_not_called()
    # An abandoned pick hands the slot to autodraft, so it has to be visible in
    # /v1/draft/status and execution_log, not only in the pod log.
    mock_record.assert_called_once()
    assert "Pick clock expired" in mock_client.board["listener_error"]
    assert "minimum request window" in mock_client.board["listener_error"]


def test_hold_and_publish_does_not_report_a_cancellation_as_a_missed_pick():
    """A cancellation landing during the final retry sleep must not be recorded as a clock
    expiry: the retry was stopped on purpose, and a false missed-pick error in /status and
    execution_log sends the operator looking for a problem that does not exist."""
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)
    cancel_event = threading.Event()
    cancel_event.set()

    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 30), \
         patch.object(dm, "DRAFT_PUBLISH_SAFETY_SECONDS", 0), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "_record_publish_error") as mock_record, \
         patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0, cancel_event=cancel_event,
            sleep_fn=lambda _s: None, monotonic_fn=lambda: 45.0, clock_origin=0.0,
        )

    mock_publish.assert_not_called()
    mock_record.assert_not_called()
    assert not mock_client.board.get("listener_error")


def test_hold_and_publish_records_the_season_with_a_publish_failure():
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=RuntimeError("no espn_id")), \
         patch.object(dm, "_record_publish_error") as mock_record:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0, sleep_fn=lambda _s: None,
            season=2026,
        )

    assert mock_record.call_args.kwargs["season"] == 2026


def test_hold_and_publish_reports_a_pick_abandoned_at_the_deadline():
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 30), \
         patch.object(dm, "DRAFT_PUBLISH_SAFETY_SECONDS", 0), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "_record_publish_error") as mock_record, \
         patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, [target], delay_seconds=0,
            sleep_fn=lambda _s: None, monotonic_fn=lambda: 45.0, clock_origin=0.0,
        )

    mock_publish.assert_not_called()
    mock_record.assert_called_once()
    assert "Pick clock expired" in mock_client.board["listener_error"]


# ---------------------------------------------------------------------------
# hold_and_publish - automatic circuit breaker (issue #108, Task D)
# ---------------------------------------------------------------------------

def _failure_counter_client(**initial_board):
    """A Redis mock whose hincrby/hset on `draft:{league}:board` fields actually persist
    across separate hold_and_publish calls, so a test can drive several invocations in a
    row and observe the real, accumulating consecutive_publish_failures counter - the same
    "fake the two hash operations against a plain dict" approach _board_client already uses
    for the Lua-script fields."""
    board = dict(initial_board)

    def fake_hincrby(_key, field, amount=1):
        board[field] = board.get(field, 0) + amount
        return board[field]

    def fake_hset(_key, *args, **kwargs):
        if "mapping" in kwargs:
            board.update(kwargs["mapping"])
        elif len(args) == 2:
            board[args[0]] = args[1]

    client = MagicMock()
    client.get.return_value = None
    client.hincrby.side_effect = fake_hincrby
    client.hset.side_effect = fake_hset
    client.board = board
    return client


def test_hold_and_publish_trips_circuit_breaker_after_max_consecutive_failures(monkeypatch):
    """Three consecutive terminal publish failures for the same league (not necessarily the
    same pick - the counter is per-league, not per-pick) must flip auto_publish_enabled to
    False with auto_publish_disabled_by='circuit_breaker' (NOT 'manual' - a future
    auto-recovery task depends on this distinction) and stop attempting further
    auto-publishes."""
    mock_client = _failure_counter_client()
    target = _player("p1", "RB", 10.0)
    error = dm.requests.HTTPError(
        "422 Client Error",
        response=type("Resp", (), {"status_code": 422, "text": "bad request"})(),
    )

    with patch.object(dm, "MAX_CONSECUTIVE_PUBLISH_FAILURES", 3), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=error), \
         patch.object(dm, "_set_auto_publish_enabled") as mock_set_enabled:
        for pick_number in (1, 2, 3):
            dm.hold_and_publish(
                mock_client, "L1", "7", pick_number, [target], delay_seconds=0,
                sleep_fn=lambda _s: None,
            )

    assert mock_client.board["consecutive_publish_failures"] == 3
    mock_set_enabled.assert_called_once_with("L1", False, disabled_by="circuit_breaker")
    assert "3 consecutive publish" in mock_client.board["listener_error"]


def test_hold_and_publish_does_not_trip_circuit_breaker_below_the_threshold():
    mock_client = _failure_counter_client()
    target = _player("p1", "RB", 10.0)
    error = dm.requests.HTTPError(
        "422 Client Error",
        response=type("Resp", (), {"status_code": 422, "text": "bad request"})(),
    )

    with patch.object(dm, "MAX_CONSECUTIVE_PUBLISH_FAILURES", 3), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=error), \
         patch.object(dm, "_set_auto_publish_enabled") as mock_set_enabled:
        for pick_number in (1, 2):
            dm.hold_and_publish(
                mock_client, "L1", "7", pick_number, [target], delay_seconds=0,
                sleep_fn=lambda _s: None,
            )

    assert mock_client.board["consecutive_publish_failures"] == 2
    mock_set_enabled.assert_not_called()


def test_hold_and_publish_successful_publish_resets_the_failure_counter():
    """A successful publish in between two failures must reset the streak to 0, so the
    circuit breaker only trips on a genuinely *consecutive* run of failures, not a lifetime
    total."""
    mock_client = _failure_counter_client()
    target = _player("p1", "RB", 10.0)
    error = dm.requests.HTTPError(
        "422 Client Error",
        response=type("Resp", (), {"status_code": 422, "text": "bad request"})(),
    )

    with patch.object(dm, "MAX_CONSECUTIVE_PUBLISH_FAILURES", 3), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "_set_auto_publish_enabled") as mock_set_enabled:
        with patch.object(dm, "publish_draft_target", side_effect=error):
            dm.hold_and_publish(
                mock_client, "L1", "7", 1, [target], delay_seconds=0, sleep_fn=lambda _s: None,
            )
            dm.hold_and_publish(
                mock_client, "L1", "7", 2, [target], delay_seconds=0, sleep_fn=lambda _s: None,
            )
        assert mock_client.board["consecutive_publish_failures"] == 2

        with patch.object(dm, "publish_draft_target", return_value=None):
            dm.hold_and_publish(
                mock_client, "L1", "7", 3, [target], delay_seconds=0, sleep_fn=lambda _s: None,
            )
        assert mock_client.board["consecutive_publish_failures"] == 0

        with patch.object(dm, "publish_draft_target", side_effect=error):
            dm.hold_and_publish(
                mock_client, "L1", "7", 4, [target], delay_seconds=0, sleep_fn=lambda _s: None,
            )
        assert mock_client.board["consecutive_publish_failures"] == 1
        mock_set_enabled.assert_not_called()


def test_hold_and_publish_circuit_breaker_counts_a_clock_expiry_as_a_failure():
    """A deadline-reached abandonment is a terminal failure too, not just an HTTP error - it
    means ESPN autodrafted the slot instead of us, exactly like a publish that errored out."""
    mock_client = _failure_counter_client()
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "MAX_CONSECUTIVE_PUBLISH_FAILURES", 3), \
         patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 5), \
         patch.object(dm, "DRAFT_PUBLISH_SAFETY_SECONDS", 0), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target") as mock_publish, \
         patch.object(dm, "_set_auto_publish_enabled") as mock_set_enabled:
        dm.hold_and_publish(
            mock_client, "L1", "7", 1, [target], delay_seconds=0,
            sleep_fn=lambda _s: None, monotonic_fn=lambda: 45.0, clock_origin=0.0,
        )

    mock_publish.assert_not_called()
    mock_set_enabled.assert_not_called()
    assert mock_client.board["consecutive_publish_failures"] == 1


def test_hold_and_publish_does_not_count_a_pause_or_kill_switch_abandonment_as_a_failure():
    """Abandoning a pick because an operator paused it, or because auto-publish was already
    disabled (manually or by a prior circuit-breaker trip), is a deliberate non-attempt, not
    a failure - it must not feed the counter that would otherwise re-trip or extend an
    already-tripped breaker."""
    mock_client = _failure_counter_client()
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=False), \
         patch.object(dm, "publish_draft_target") as mock_publish, \
         patch.object(dm, "_set_auto_publish_enabled") as mock_set_enabled:
        dm.hold_and_publish(
            mock_client, "L1", "7", 1, [target], delay_seconds=0, sleep_fn=lambda _s: None,
        )

    mock_publish.assert_not_called()
    mock_set_enabled.assert_not_called()
    assert "consecutive_publish_failures" not in mock_client.board


def test_max_consecutive_publish_failures_defaults_to_three():
    assert dm.MAX_CONSECUTIVE_PUBLISH_FAILURES == 3


# ---------------------------------------------------------------------------
# hold_and_publish - retry with next-best candidate on confirmed rejection
# (issue #108, Task B: fantasy-browser-worker publishes a confirmed ESPN pick
# rejection on draft_pick_rejected:{league_id})
# ---------------------------------------------------------------------------

class _FakePubSub:
    """Minimal stand-in for redis.Redis().pubsub(): records the channel(s) subscribed to
    and hands back a queue of canned messages one at a time via get_message, then None
    once the queue is drained - mirroring the real client's non-blocking
    get_message(timeout=0) behavior once nothing new has arrived.

    arrive_after_polls models a message that isn't visible on the very first poll - matching
    hold_and_publish's real-world grace-window polling (see PICK_REJECTION_GRACE_SECONDS): a
    value of 0 (the default) means "available on the first get_message() call", same as the
    original immediate-delivery behavior; a value of N means the message is only handed back
    once get_message() has been called at least N times, modeling an ESPN response that takes
    a few polls' worth of wall-clock time to arrive."""

    def __init__(self, messages=None, arrive_after_polls=None):
        self.subscribed_channels = []
        messages = list(messages or [])
        delays = list(arrive_after_polls or [0] * len(messages))
        self._pending = list(zip(messages, delays))
        self._poll_count = 0
        self.closed = False

    def subscribe(self, channel):
        self.subscribed_channels.append(channel)

    def get_message(self, ignore_subscribe_messages=True, timeout=0):
        self._poll_count += 1
        if self._pending and self._poll_count >= self._pending[0][1]:
            message, _ = self._pending.pop(0)
            return message
        return None

    def close(self):
        self.closed = True


def _rejection_client(pubsub):
    client = MagicMock()
    client.get.return_value = None
    client.pubsub.return_value = pubsub
    return client


def _rejection_message(pick_number, idempotency_key, error_message="Invalid Selection."):
    return {
        "type": "message",
        "data": json.dumps({
            "league_id": "L1",
            "pick_number": pick_number,
            "idempotency_key": idempotency_key,
            "team_id": "8",
            "player_id": "111",
            "error_code": "1",
            "error_message": error_message,
            "rejected_at": 1735300000.123,
        }),
    }


def test_hold_and_publish_retries_next_candidate_on_confirmed_rejection():
    """A rejection message matching the pick currently being held should trigger an
    immediate republish of candidates[1], not wait out the normal deadline."""
    candidates = [
        _player("p1", "RB", 10.0, name="Player One", espn_id="111"),
        _player("p2", "RB", 9.0, name="Player Two", espn_id="222"),
    ]
    publish_calls = []

    def fake_publish_draft_target(_team_id, target, **_kwargs):
        publish_calls.append(target["espn_id"])

    pubsub = _FakePubSub(messages=[_rejection_message(5, "L1:5:111")])
    mock_client = _rejection_client(pubsub)

    with patch.object(dm, "publish_draft_target", side_effect=fake_publish_draft_target):
        dm.hold_and_publish(
            mock_client, "L1", "8", 5, candidates, delay_seconds=0,
            sleep_fn=lambda s: None,
        )

    assert publish_calls == ["111", "222"]
    assert pubsub.subscribed_channels == ["draft_pick_rejected:L1"]
    assert pubsub.closed is True


def test_hold_and_publish_gives_up_after_all_candidates_rejected():
    """If every candidate in the list is rejected, hold_and_publish must surface a
    terminal failure (not retry forever) once the list is exhausted."""
    candidates = [
        _player("p1", "RB", 10.0, name="Player One", espn_id="111"),
        _player("p2", "RB", 9.0, name="Player Two", espn_id="222"),
    ]
    publish_calls = []

    def fake_publish_draft_target(_team_id, target, **_kwargs):
        publish_calls.append(target["espn_id"])

    pubsub = _FakePubSub(messages=[
        _rejection_message(5, "L1:5:111"),
        _rejection_message(5, "L1:5:222"),
    ])
    mock_client = _rejection_client(pubsub)

    with patch.object(dm, "publish_draft_target", side_effect=fake_publish_draft_target), \
         patch.object(dm, "_surface_clock_expired") as mock_surface, \
         patch.object(dm, "_record_publish_outcome") as mock_record:
        dm.hold_and_publish(
            mock_client, "L1", "8", 5, candidates, delay_seconds=0,
            sleep_fn=lambda s: None,
        )

    assert publish_calls == ["111", "222"]
    mock_surface.assert_called_once()
    assert mock_surface.call_args.args[0] is mock_client
    assert mock_surface.call_args.args[1] == "L1"
    assert mock_surface.call_args.args[3] == 5
    mock_record.assert_called_once_with(mock_client, "L1", succeeded=False)


def test_hold_and_publish_ignores_a_rejection_for_a_different_pick():
    """A rejection for a pick/idempotency_key this thread isn't holding must not advance
    the candidate index - it belongs to a different (already-superseded or unrelated)
    publish attempt."""
    candidates = [
        _player("p1", "RB", 10.0, name="Player One", espn_id="111"),
        _player("p2", "RB", 9.0, name="Player Two", espn_id="222"),
    ]
    publish_calls = []

    def fake_publish_draft_target(_team_id, target, **_kwargs):
        publish_calls.append(target["espn_id"])

    # Mismatched pick_number - must be ignored, so only candidates[0] is ever published.
    pubsub = _FakePubSub(messages=[_rejection_message(999, "L1:999:111")])
    mock_client = _rejection_client(pubsub)

    with patch.object(dm, "publish_draft_target", side_effect=fake_publish_draft_target):
        dm.hold_and_publish(
            mock_client, "L1", "8", 5, candidates, delay_seconds=0,
            sleep_fn=lambda s: None,
        )

    assert publish_calls == ["111"]


def test_hold_and_publish_catches_rejection_that_arrives_after_publish_returns():
    """Regression test for the bug confirmed live 2026-08-28 (league 696093652, team 8):
    ESPN's rejection does not arrive in the same instant publish_draft_target returns - it
    took ~7.8s in that incident, by which point a single non-blocking pubsub check (the
    original implementation) had already declared success and closed the subscription.
    hold_and_publish must keep polling for PICK_REJECTION_GRACE_SECONDS, not just once,
    so a rejection that arrives a few polls later still triggers the next-candidate retry."""
    candidates = [
        _player("p1", "RB", 10.0, name="Player One", espn_id="111"),
        _player("p2", "RB", 9.0, name="Player Two", espn_id="222"),
    ]
    publish_calls = []

    def fake_publish_draft_target(_team_id, target, **_kwargs):
        publish_calls.append(target["espn_id"])

    # Not visible on the first poll - only after a few empty ones, modeling the real-world
    # delay between "gateway accepted the publish" and "ESPN told us it rejected the pick".
    pubsub = _FakePubSub(messages=[_rejection_message(5, "L1:5:111")], arrive_after_polls=[3])
    mock_client = _rejection_client(pubsub)

    with patch.object(dm, "publish_draft_target", side_effect=fake_publish_draft_target):
        dm.hold_and_publish(
            mock_client, "L1", "8", 5, candidates, delay_seconds=0,
            sleep_fn=lambda s: None,
        )

    assert publish_calls == ["111", "222"]


def test_hold_and_publish_logs_a_non_matching_rejection_instead_of_discarding_it_silently(caplog):
    """A rejection that doesn't match the pick currently held must not be indistinguishable
    from 'no message at all' in the logs - see the #108 follow-up fixing the retry-timing
    bug, where a silently-discarded mismatch made a broken match condition impossible to
    tell apart from a healthy 'nothing rejected yet'."""
    candidates = [_player("p1", "RB", 10.0, name="Player One", espn_id="111")]

    def fake_publish_draft_target(_team_id, target, **_kwargs):
        return None

    pubsub = _FakePubSub(messages=[_rejection_message(999, "L1:999:111")])
    mock_client = _rejection_client(pubsub)

    with patch.object(dm, "publish_draft_target", side_effect=fake_publish_draft_target), \
         caplog.at_level("INFO", logger="DraftMatrix"):
        dm.hold_and_publish(
            mock_client, "L1", "8", 5, candidates, delay_seconds=0,
            sleep_fn=lambda s: None,
        )

    assert "did not match currently-held pick" in caplog.text
    assert "idempotency_key=L1:999:111" in caplog.text


def test_hold_and_publish_returns_immediately_when_candidates_is_empty():
    mock_client = MagicMock()

    with patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(mock_client, "L1", "8", 5, [], delay_seconds=0, sleep_fn=lambda s: None)

    mock_publish.assert_not_called()


def test_process_entries_anchors_the_deadline_to_the_stream_entry_not_the_read_time():
    # Redis Stream id is "<unixMillis>-<seq>". This entry was written 60s ago, so its clock
    # origin must be 60s in the past - not "now", which is what a fresh monotonic() reading
    # at read time produced and which gave every drained backlog entry a brand new clock.
    entry_ms = int((time.time() - 60) * 1000)
    entries = [(f"draft_picks:L1", [(f"{entry_ms}-0", {"payload": json.dumps({"pick_number": 4})})])]

    with patch.object(dm, "handle_pick_event", return_value=(_player("p1", "RB", 1.0), 5)), \
         patch.object(dm, "_schedule_publish") as mock_schedule:
        before = time.monotonic()
        dm._process_entries(MagicMock(), "draft_picks:L1", "L1", 2026, 1, "7", 8, {}, entries)
        after = time.monotonic()

    origin = mock_schedule.call_args.kwargs["clock_origin"]
    assert before - 61 <= origin <= after - 59


def test_stream_entry_clock_origin_falls_back_to_now_for_an_unparseable_id():
    # The DOM/REST tiers may write ids we did not mint. Falling back to "now" is only wrong
    # in the generous direction and must never block a live pick.
    assert dm._stream_entry_clock_origin("not-an-id", monotonic_fn=lambda: 500.0) == 500.0
    assert dm._stream_entry_clock_origin(None, monotonic_fn=lambda: 500.0) == 500.0


def test_stream_entry_clock_origin_ignores_a_future_dated_entry():
    # Clock skew between the worker writing the entry and this process reading it must not
    # push the deadline into the future and extend the clock.
    future_ms = int((time.time() + 30) * 1000)
    assert dm._stream_entry_clock_origin(f"{future_ms}-0", monotonic_fn=lambda: 500.0) == 500.0


# ---------------------------------------------------------------------------
# _schedule_publish
# ---------------------------------------------------------------------------

def test_schedule_publish_cancels_previous_pending_job():
    mock_client = MagicMock()
    pending_state = {}
    old_event = threading.Event()
    pending_state["cancel_event"] = old_event

    with patch("src.optimization.draft_matrix.threading.Thread") as mock_thread_cls:
        mock_thread = MagicMock()
        mock_thread_cls.return_value = mock_thread
        dm._schedule_publish(mock_client, "L1", "7", 5, [_player("p1", "RB", 1.0)], delay_seconds=8, pending_state=pending_state)

    assert old_event.is_set()
    assert pending_state["cancel_event"] is not old_event
    mock_thread.start.assert_called_once()


def test_schedule_publish_propagates_trace_context_across_thread_boundary():
    """hold_and_publish runs on a background OS thread (see _schedule_publish), which -
    unlike an asyncio task - does not automatically inherit the calling thread's OTel
    context. This exercises the real (unmocked) threading.Thread path end-to-end and
    verifies the span created inside the background thread shares its parent's trace_id,
    guarding against a future argument-reordering regression silently breaking the
    context handoff (context.get_current() -> Thread args -> context.attach())."""
    original_provider = trace._TRACER_PROVIDER
    original_once = trace._TRACER_PROVIDER_SET_ONCE
    trace._TRACER_PROVIDER = None
    trace._TRACER_PROVIDER_SET_ONCE = Once()
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    try:
        mock_client = MagicMock()
        pending_state = {}
        with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
             patch.object(dm, "publish_draft_target"):
            with dm.tracer.start_as_current_span("draft_pick.handle_event") as parent_span:
                parent_trace_id = parent_span.get_span_context().trace_id
                dm._schedule_publish(
                    mock_client, "L1", "7", 5, [_player("p1", "RB", 1.0)],
                    delay_seconds=0, pending_state=pending_state,
                )

            for _ in range(100):
                if exporter.get_finished_spans():
                    break
                time.sleep(0.02)

        spans = exporter.get_finished_spans()
        publish_spans = [s for s in spans if s.name == "draft_pick.hold_and_publish"]
        assert len(publish_spans) == 1
        assert publish_spans[0].context.trace_id == parent_trace_id
    finally:
        trace._TRACER_PROVIDER = original_provider
        trace._TRACER_PROVIDER_SET_ONCE = original_once


def test_schedule_publish_clamps_delay_and_writes_absolute_deadlines():
    mock_client = MagicMock()
    pending_state = {}

    monotonic_values = iter([100.0, 100.0])

    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 30), \
         patch.object(dm, "DRAFT_PUBLISH_SAFETY_SECONDS", 5), \
         patch.object(dm, "MIN_AUTO_PUBLISH_WINDOW_SECONDS", 0), \
         patch("src.optimization.draft_matrix.threading.Thread") as mock_thread_cls, \
         patch.object(dm.logger, "warning") as mock_warning:
        mock_thread = MagicMock()
        mock_thread_cls.return_value = mock_thread
        dm._schedule_publish(
            mock_client, "L1", "7", 5, [_player("p1", "RB", 1.0)], delay_seconds=99,
            pending_state=pending_state, clock_origin=95.0,
            monotonic_fn=lambda: next(monotonic_values), time_fn=lambda: 1000.0,
        )

    args = mock_thread_cls.call_args.kwargs["args"]
    assert args[5] == 25
    mock_warning.assert_called_once()
    mock_client.hset.assert_called_once_with(
        "draft:L1:board",
        mapping={"auto_publish_at": 1020, "pick_clock_expires_at": 1025},
    )


def test_clamp_auto_draft_delay_reserves_min_publish_window_even_when_under_the_full_clock():
    """Confirmed live 2026-08-27 (league 1683291251): a configured delay smaller than the
    whole clock (20s of a 30s practice clock) still isn't automatically safe - it can still
    leave too little of the clock for the actual publish attempt (and any recovery from a
    rejected SELECT) once DRAFT_PUBLISH_SAFETY_SECONDS is subtracted. The floor must clamp
    even a delay that the old (pre-floor) check would have accepted outright."""
    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 30), \
         patch.object(dm, "DRAFT_PUBLISH_SAFETY_SECONDS", 5), \
         patch.object(dm, "MIN_AUTO_PUBLISH_WINDOW_SECONDS", 10), \
         patch.object(dm.logger, "warning") as mock_warning:
        assert dm._clamp_auto_draft_delay_seconds(20) == 15

    mock_warning.assert_called_once()


def test_clamp_auto_draft_delay_leaves_a_delay_already_inside_the_window_untouched():
    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 90), \
         patch.object(dm, "DRAFT_PUBLISH_SAFETY_SECONDS", 5), \
         patch.object(dm, "MIN_AUTO_PUBLISH_WINDOW_SECONDS", 10), \
         patch.object(dm.logger, "warning") as mock_warning:
        assert dm._clamp_auto_draft_delay_seconds(45) == 45

    mock_warning.assert_not_called()


# ---------------------------------------------------------------------------
# handle_pick_event (orchestrator)
# ---------------------------------------------------------------------------

def _mock_conn_returning(inserted: bool, team_count: int = 10):
    """Feeds handle_pick_event's own cursor lookups by matching on the executed SQL rather
    than a fixed position: which of these actually run as real (non-mocked) queries varies
    per test depending on which helper functions that test patches out, so a positional
    list breaks the moment a new always-runs lookup (#82's load_league_config completion
    check) is added."""
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query
    mock_cursor.execute.side_effect = _execute

    def _fetchone():
        query = mock_cursor.last_query
        if "lineup_slot_counts" in query:
            # #82's completion check only needs the roster shape here - the team_count it
            # uses is whatever handle_pick_event already resolved above, from whichever
            # source, not this row's own team_count column.
            return (10, {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 6})
        if "FROM league_settings" in query:
            return (team_count,) if team_count is not None else None
        if "FROM draft_config" in query:
            # Covers both league_team_count's fallback lookup and derive_team_count's mode
            # read - every test needing a non-None answer from either mocks the function
            # that would have issued it (derive_team_count itself, or team_count=10 short-
            # circuits league_team_count before this query ever runs).
            return None
        if "FROM players" in query:
            return ("internal-1",)
        if "INSERT INTO draft_picks" in query:
            return (1,) if inserted else None
        return None
    mock_cursor.fetchone.side_effect = _fetchone
    # backfill_missing_rounds' UPDATE ... RETURNING, and derive_team_count's pick scan;
    # nothing to repair and nothing to derive by default.
    mock_cursor.fetchall.return_value = []
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_conn, mock_cursor


def test_handle_pick_event_skips_when_already_recorded():
    mock_conn, _ = _mock_conn_returning(inserted=False)
    mock_client = MagicMock()
    event = {"pick_number": "1", "team_id": "3", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        result = dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert result is None
    mock_client.hset.assert_not_called()


def test_handle_pick_event_derives_round_from_pick_number_not_event():
    """The WS ingest tier puts ESPN's lineup slot id in "round", so a slot-1 (QB) pick
    arriving at overall pick 34 must NOT be treated as a round-1 pick and claim a draft
    slot - that is what corrupted the slot map and the on-the-clock team."""
    mock_conn, _ = _mock_conn_returning(inserted=True, team_count=10)
    mock_client = MagicMock()
    event = {"pick_number": "34", "round": "1", "team_id": "3", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.persist_draft_slot") as mock_slot, \
         patch("src.optimization.draft_matrix.persist_draft_pick", return_value=True) as mock_pick, \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", return_value=[]):
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    mock_slot.assert_not_called()
    assert mock_pick.call_args.kwargs["round_"] == 4


def test_handle_pick_event_records_draft_slot_for_real_round_one_pick():
    mock_conn, _ = _mock_conn_returning(inserted=True, team_count=10)
    mock_client = MagicMock()
    # Lineup slot 6 (a TE) on overall pick 7 - genuinely round 1 despite "round" saying 6.
    event = {"pick_number": "7", "round": "6", "team_id": "3", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.persist_draft_slot") as mock_slot, \
         patch("src.optimization.draft_matrix.persist_draft_pick", return_value=True) as mock_pick, \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", return_value=[]):
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert mock_pick.call_args.kwargs["round_"] == 1
    assert mock_slot.call_args[0][3] == "3"
    assert mock_slot.call_args[0][4] == 7


def test_handle_pick_event_leaves_round_unknown_without_league_settings():
    mock_conn, _ = _mock_conn_returning(inserted=True, team_count=None)
    mock_client = MagicMock()
    event = {"pick_number": "3", "round": "2", "team_id": "3", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.persist_draft_slot") as mock_slot, \
         patch("src.optimization.draft_matrix.persist_draft_pick", return_value=True) as mock_pick, \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", return_value=[]):
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert mock_pick.call_args.kwargs["round_"] is None
    mock_slot.assert_not_called()


def test_backfill_missing_rounds_repairs_null_rounds_and_claims_round_one_slots():
    """Picks recorded before league_settings existed keep round NULL forever - ON CONFLICT
    DO NOTHING stops any Stream replay from filling them - so without this repair a league
    configured one pick late would never get a complete draft_slots map."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(3, "t3", 1), (14, "t7", 2)]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.optimization.draft_matrix.persist_draft_slot") as mock_slot:
        repaired = dm.backfill_missing_rounds(mock_conn, "L1", 2026, team_count=10)

    assert repaired == 2
    # Only the round-1 pick claims a slot; pick 14 is round 2 and must not.
    mock_slot.assert_called_once_with(mock_conn, "L1", 2026, "t3", 3)


def test_handle_pick_event_backfills_once_league_settings_appear():
    """The first pick that can read a team_count must repair everything recorded before it,
    otherwise turn-order tracking stays permanently broken for that draft."""
    mock_conn, _ = _mock_conn_returning(inserted=True, team_count=10)
    mock_client = MagicMock()
    event = {"pick_number": "11", "team_id": "3", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.backfill_missing_rounds", return_value=0) as mock_backfill, \
         patch("src.optimization.draft_matrix.persist_draft_pick", return_value=True), \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", return_value=[]):
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    mock_backfill.assert_called_once_with(mock_conn, "L1", 2026, 10)


def test_handle_pick_event_skips_backfill_when_team_count_unknown():
    mock_conn, _ = _mock_conn_returning(inserted=True, team_count=None)
    mock_client = MagicMock()
    event = {"pick_number": "3", "team_id": "3", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.backfill_missing_rounds") as mock_backfill, \
         patch("src.optimization.draft_matrix.persist_draft_pick", return_value=True), \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", return_value=[]):
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    mock_backfill.assert_not_called()


def _mock_conn_with_picks(picks, mode="practice"):
    mock_cursor = MagicMock()
    # derive_team_count reads draft_config.mode before it will look at picks at all.
    mock_cursor.fetchone.return_value = (mode,) if mode else None
    mock_cursor.fetchall.return_value = picks
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_conn


def test_derive_team_count_reads_the_snake_turnaround():
    """A snake reverses at the round-1/2 boundary, so the last team of round 1 picks again
    immediately. That back-to-back pair sits at picks team_count and team_count + 1."""
    picks = [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "d")]
    assert dm.derive_team_count(_mock_conn_with_picks(picks), "L1", 2026) == 4


def test_derive_team_count_unknown_until_a_team_repeats():
    picks = [(1, "a"), (2, "b"), (3, "c")]
    assert dm.derive_team_count(_mock_conn_with_picks(picks), "L1", 2026) is None


def test_derive_team_count_refuses_to_guess_when_a_pick_is_missing():
    """A dropped pick would put the boundary one position early and yield a count one too
    small - silently wrong rounds and draft slots for the entire draft. Unknown is better."""
    picks = [(1, "a"), (2, "b"), (4, "c"), (5, "c")]
    assert dm.derive_team_count(_mock_conn_with_picks(picks), "L1", 2026) is None


def test_derive_team_count_ignores_picks_that_do_not_start_at_one():
    picks = [(2, "a"), (3, "b"), (4, "b")]
    assert dm.derive_team_count(_mock_conn_with_picks(picks), "L1", 2026) is None


def test_derive_team_count_refuses_a_repeat_that_is_not_the_snake_turnaround():
    """A team reappearing without being the immediately preceding picker cannot be the
    boundary - round 1 is every team exactly once. It means traded picks or malformed
    ownership data, and reading it as the boundary would derive a count far below the real
    one and mis-size every round and slot from then on."""
    picks = [(1, "a"), (2, "b"), (3, "a")]
    assert dm.derive_team_count(_mock_conn_with_picks(picks), "L1", 2026) is None


def test_derive_team_count_refuses_to_infer_for_a_live_league():
    """Live leagues can trade picks, which makes an adjacent repeat indistinguishable from a
    turnaround - and they have an authoritative count from sync_league.py anyway. Inferring
    here would persist a count far below the real one off a traded first-round pick."""
    picks = [(1, "a"), (2, "b"), (3, "c"), (4, "c")]
    assert dm.derive_team_count(_mock_conn_with_picks(picks, mode="live"), "L1", 2026) is None


def test_derive_team_count_refuses_when_no_draft_config_row_exists():
    picks = [(1, "a"), (2, "b"), (3, "c"), (4, "c")]
    assert dm.derive_team_count(_mock_conn_with_picks(picks, mode=None), "L1", 2026) is None


def test_derive_team_count_refuses_back_to_back_picks_at_the_very_start():
    """Consecutive picks at 1 and 2 would imply a one-team league; in practice it is a traded
    pick, not a turnaround."""
    picks = [(1, "a"), (2, "a"), (3, "b")]
    assert dm.derive_team_count(_mock_conn_with_picks(picks), "L1", 2026) is None


def test_derive_team_count_prefers_connector_health_over_ambiguous_adjacent_repeat():
    """Regression for league 1447332236: picks 1-6 were 8,2,10,10,2,8 (adjacent repeat at
    3-4), but connector health already knew expected_total_picks=160 => 10 teams."""
    mode_cursor = MagicMock()
    mode_cursor.fetchone.return_value = ("practice",)
    config_cursor = MagicMock()
    config_cursor.fetchone.return_value = (
        10,
        {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 9},
    )
    mode_ctx = MagicMock()
    mode_ctx.__enter__.return_value = mode_cursor
    mode_ctx.__exit__.return_value = False
    config_ctx = MagicMock()
    config_ctx.__enter__.return_value = config_cursor
    config_ctx.__exit__.return_value = False
    mock_conn = MagicMock()
    # If derive_team_count tries to continue to the pick-heuristic path here, this test will
    # fail with StopIteration; with valid connector health it should return from the first
    # two cursors and never accept the (3,4) adjacent repeat as team_count=3.
    mock_conn.cursor.side_effect = [mode_ctx, config_ctx]
    mock_client = MagicMock()
    mock_client.get.return_value = json.dumps({
        "expected_total_picks": 160,
        "picks_known": 6,
        "sample_sequence": [8, 2, 10, 10, 2, 8],
    })

    assert dm.derive_team_count(mock_conn, "1447332236", 2026, r_client=mock_client) == 10


def test_handle_pick_event_derives_and_persists_team_count_for_unsynced_league():
    """Practice/mock rooms are never touched by sync_league, so league_settings has nothing
    for them; without this the round stays NULL and on_the_clock_team is never known."""
    mock_conn, _ = _mock_conn_returning(inserted=True, team_count=None)
    mock_client = MagicMock()
    event = {"pick_number": "5", "team_id": "a", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.derive_team_count", return_value=4) as mock_derive, \
         patch("src.optimization.draft_matrix.persist_derived_team_count") as mock_persist_count, \
         patch("src.optimization.draft_matrix.backfill_missing_rounds", return_value=0) as mock_backfill, \
         patch("src.optimization.draft_matrix.persist_draft_slot") as mock_slot, \
         patch("src.optimization.draft_matrix.persist_draft_pick", return_value=True), \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", return_value=[]):
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    mock_derive.assert_called_once_with(mock_conn, "L1", 2026, r_client=mock_client)
    mock_persist_count.assert_called_once_with(mock_conn, "L1", 2026, 4)
    # Repairs every round left NULL while the count was unknown, this pick included.
    mock_backfill.assert_called_once_with(mock_conn, "L1", 2026, 4)
    # Pick 5 in a 4-team league is round 2, so it must not claim a draft slot.
    mock_slot.assert_not_called()


def test_handle_pick_event_derivation_makes_a_round_one_pick_claim_its_slot():
    mock_conn, _ = _mock_conn_returning(inserted=True, team_count=None)
    mock_client = MagicMock()
    event = {"pick_number": "3", "team_id": "c", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.derive_team_count", return_value=4), \
         patch("src.optimization.draft_matrix.persist_derived_team_count"), \
         patch("src.optimization.draft_matrix.backfill_missing_rounds", return_value=0), \
         patch("src.optimization.draft_matrix.persist_draft_slot") as mock_slot, \
         patch("src.optimization.draft_matrix.persist_draft_pick", return_value=True), \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", return_value=[]):
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    mock_slot.assert_called_once()
    assert mock_slot.call_args[0][3] == "c"
    assert mock_slot.call_args[0][4] == 3


def test_league_team_count_falls_back_to_draft_config():
    mock_cursor = MagicMock()
    # league_settings has no row for this league (never synced); draft_config carries the
    # count derived from pick arrivals.
    mock_cursor.fetchone.side_effect = [None, (12,)]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    assert dm.league_team_count(mock_conn, "L1", 2026) == 12
    # The draft_config fallback must be season-scoped; the row is keyed by league_id only.
    assert mock_cursor.execute.call_args_list[1][0][1] == ("L1", 2026)


def test_league_team_count_prefers_league_settings():
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [(10,), (99,)]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    assert dm.league_team_count(mock_conn, "L1", 2026) == 10


def test_league_team_count_ignores_a_draft_config_count_from_another_season():
    """draft_config is keyed by league_id alone, so a room reused in a later season keeps its
    old row. Inheriting that count would block re-derivation and size every round wrongly."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.side_effect = [None, None]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    assert dm.league_team_count(mock_conn, "L1", 2027) is None


def test_persist_derived_team_count_is_scoped_to_league_and_season():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    dm.persist_derived_team_count(mock_conn, "L1", 2026, 8)

    assert mock_cursor.execute.call_args[0][1] == (8, "L1", 2026)


def test_decode_pick_event_unwraps_worker_payload_envelope():
    fields = {"payload": json.dumps({"pick_number": 12, "team_id": 3, "player_id": 99})}
    assert dm.decode_pick_event(fields) == {"pick_number": 12, "team_id": 3, "player_id": 99}


def test_decode_pick_event_passes_through_flat_fields():
    fields = {"pick_number": "12", "team_id": "3", "player_id": "99"}
    assert dm.decode_pick_event(fields) == fields


def test_process_entries_supersedes_a_pending_publish_when_our_own_pick_lands():
    """handle_pick_event returns None for our own pick, so _schedule_publish never runs and
    used to leave the old publisher alive. It could then recover from a transient failure and
    enqueue a target for a slot ESPN had already filled - a click on the next team's pick."""
    mock_client = MagicMock()
    cancel_event = threading.Event()
    pending_state = {"cancel_event": cancel_event, "pick_number": 6}
    event = {"pick_number": 6, "team_id": "7", "player_id": "p1"}
    entries = [("draft_picks:L1", [("1-0", {"payload": json.dumps(event)})])]

    with patch("src.optimization.draft_matrix.handle_pick_event", return_value=None):
        dm._process_entries(
            mock_client, stream_key="draft_picks:L1", league_id="L1", season=2026, week=1,
            team_id="7", delay_seconds=8, pending_state=pending_state, entries=entries,
        )

    assert cancel_event.is_set()
    assert "cancel_event" not in pending_state


def test_process_entries_leaves_a_pending_publish_alone_for_a_replayed_older_pick():
    """A redelivered entry from before the pending target must not cancel a live publish."""
    mock_client = MagicMock()
    cancel_event = threading.Event()
    pending_state = {"cancel_event": cancel_event, "pick_number": 6}
    event = {"pick_number": 4, "team_id": "3", "player_id": "p1"}
    entries = [("draft_picks:L1", [("1-0", {"payload": json.dumps(event)})])]

    with patch("src.optimization.draft_matrix.handle_pick_event", return_value=None):
        dm._process_entries(
            mock_client, stream_key="draft_picks:L1", league_id="L1", season=2026, week=1,
            team_id="7", delay_seconds=8, pending_state=pending_state, entries=entries,
        )

    assert not cancel_event.is_set()
    assert pending_state["cancel_event"] is cancel_event


def test_process_entries_decodes_payload_before_handling():
    """Regression: the listener used to hand raw Stream fields to handle_pick_event, so every
    pick raised KeyError('pick_number'), got acked, and the listener reported itself Failed."""
    mock_client = MagicMock()
    event = {"pick_number": 5, "team_id": "3", "player_id": "p1"}
    entries = [("draft_picks:L1", [("1-0", {"payload": json.dumps(event)})])]

    with patch("src.optimization.draft_matrix.handle_pick_event", return_value=None) as mock_handle:
        dm._process_entries(
            mock_client,
            stream_key="draft_picks:L1",
            league_id="L1",
            season=2026,
            week=1,
            team_id="7",
            delay_seconds=8,
            pending_state={},
            entries=entries,
        )

    assert mock_handle.call_args[0][5] == event
    failed = [
        c for c in mock_client.hset.call_args_list
        if c.kwargs.get("mapping", {}).get("listener_state") == "Failed"
    ]
    assert not failed


def test_handle_pick_event_skips_recommendation_when_we_just_picked():
    mock_conn, _ = _mock_conn_returning(inserted=True)
    mock_client = MagicMock()
    mock_client.hget.return_value = None
    event = {"pick_number": "1", "team_id": "7", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.persist_pick_autodraft_flag") as mock_persist_flag:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        result = dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert result is None
    mapping = mock_client.hset.call_args_list[0].kwargs["mapping"]
    assert mapping["listener_state"] == "Active"
    assert mapping["last_processed_pick_number"] == 1
    # No attempt was ever recorded for this pick, so it must count as autodrafted.
    mock_persist_flag.assert_called_once_with(mock_conn, "L1", 2026, 1, True)


def test_handle_pick_event_flags_was_autodrafted_on_mismatch(monkeypatch):
    """Our own team's pick landing with a different espn_player_id than the last target
    we attempted for that pick_number must be recorded as was_autodrafted=True."""
    mock_conn, _ = _mock_conn_returning(inserted=True)
    mock_client = MagicMock()
    mock_client.hget.return_value = json.dumps({"espn_id": "other-player", "name": "Someone Else"})
    event = {"pick_number": "1", "team_id": "7", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.persist_pick_autodraft_flag") as mock_persist_flag:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        result = dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert result is None
    mock_client.hget.assert_called_once_with("draft:L1:last_attempted_target", "1")
    mock_persist_flag.assert_called_once_with(mock_conn, "L1", 2026, 1, True)


def test_handle_pick_event_flags_not_autodrafted_on_match(monkeypatch):
    """The landed player matching the last target we attempted for that pick_number means
    our own recommendation actually went through - was_autodrafted must be False."""
    mock_conn, _ = _mock_conn_returning(inserted=True)
    mock_client = MagicMock()
    mock_client.hget.return_value = json.dumps({"espn_id": "p1", "name": "Some Back"})
    event = {"pick_number": "1", "team_id": "7", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.persist_pick_autodraft_flag") as mock_persist_flag:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        result = dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert result is None
    mock_persist_flag.assert_called_once_with(mock_conn, "L1", 2026, 1, False)


def test_handle_pick_event_flags_was_autodrafted_true_when_no_attempt_was_recorded(monkeypatch):
    """No recorded attempt at all (e.g. auto-publish was off, or the recompute never
    produced a feasible candidate) also counts as autodrafted - we did not choose this
    player, regardless of the reason."""
    mock_conn, _ = _mock_conn_returning(inserted=True)
    mock_client = MagicMock()
    mock_client.hget.return_value = None
    event = {"pick_number": "1", "team_id": "7", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.persist_pick_autodraft_flag") as mock_persist_flag:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        result = dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert result is None
    mock_persist_flag.assert_called_once_with(mock_conn, "L1", 2026, 1, True)


def test_handle_pick_event_matches_team_id_across_the_json_int_str_boundary(monkeypatch):
    """Regression test for the bug confirmed live 2026-08-28 (leagues 369361154, 696093652):
    decode_pick_event's json.loads decodes fantasy-browser-worker's team_id (written via
    JS's Number(teamId)) as a Python int, while our_team_id is always str (argparse's
    --team-id). An unguarded == between event["team_id"] and our_team_id is therefore False
    even when they represent the same team, permanently unreachable in production - was_
    autodrafted stayed NULL forever, even for genuinely successful publishes, because this
    whole branch (and its logging) never ran. Every other test in this file constructs
    event["team_id"] as a pre-typed string, which is exactly why this shipped broken."""
    mock_conn, _ = _mock_conn_returning(inserted=True)
    mock_client = MagicMock()
    mock_client.hget.return_value = json.dumps({"espn_id": "p1", "name": "Some Back"})
    event = {"pick_number": "1", "team_id": 7, "player_id": "p1"}  # int, matching json.loads

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.persist_pick_autodraft_flag") as mock_persist_flag:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        result = dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert result is None
    mock_persist_flag.assert_called_once_with(mock_conn, "L1", 2026, 1, False)


def _mock_conn_with_disabled_by(disabled_by, inserted: bool = True, team_count: int = 10):
    """Extends _mock_conn_returning's query-routed fetchone with an answer for
    _maybe_recover_circuit_breaker's own SELECT auto_publish_disabled_by lookup, without
    disturbing any of the other "FROM draft_config" routes that helper already covers
    (league_team_count's fallback, derive_team_count's mode read)."""
    mock_conn, mock_cursor = _mock_conn_returning(inserted=inserted, team_count=team_count)
    base_fetchone = mock_cursor.fetchone.side_effect

    def _fetchone():
        query = mock_cursor.last_query
        if "auto_publish_disabled_by" in query:
            return (disabled_by,)
        return base_fetchone()
    mock_cursor.fetchone.side_effect = _fetchone
    return mock_conn, mock_cursor


def test_handle_pick_event_does_not_reenable_manual_disable_on_opponent_pick():
    """A manually-disabled league (auto_publish_disabled_by='manual') must stay disabled
    across opponent pick events - only a circuit-breaker trip self-heals."""
    mock_conn, _ = _mock_conn_with_disabled_by("manual", inserted=True)
    mock_client = MagicMock()
    mock_client.hget.return_value = None
    event = {"pick_number": "1", "team_id": "7", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.persist_pick_autodraft_flag"), \
         patch.object(dm, "_set_auto_publish_enabled") as mock_set_enabled:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    mock_set_enabled.assert_not_called()


def test_handle_pick_event_reenables_circuit_breaker_disable_on_next_opponent_pick():
    """A circuit-breaker-disabled league (auto_publish_disabled_by='circuit_breaker')
    must automatically re-enable on the very next new pick event observed for that
    league, and consecutive_publish_failures must reset to 0."""
    mock_conn, _ = _mock_conn_with_disabled_by("circuit_breaker", inserted=True)
    mock_client = MagicMock()
    mock_client.hget.return_value = None
    event = {"pick_number": "1", "team_id": "7", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.persist_pick_autodraft_flag"), \
         patch.object(dm, "_set_auto_publish_enabled") as mock_set_enabled:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    mock_set_enabled.assert_called_once_with("L1", True)
    mock_client.hset.assert_any_call("draft:L1:board", "consecutive_publish_failures", 0)


def test_handle_pick_event_marks_draft_complete_at_final_pick():
    """#82: pick_number reaching total_picks ends the draft, regardless of whose pick it
    was - clears the recommendation (nothing left to recommend once picks run out) and
    writes draft_state so run_draft_listener's main loop knows to exit."""
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query
    mock_cursor.execute.side_effect = _execute

    def _fetchone():
        query = mock_cursor.last_query
        if "lineup_slot_counts" in query:
            # 1-slot, no-bench league: starting_total=1, bench_total=0 -> 1 pick/team.
            return (2, {"QB": 1, "BE": 0})
        if "FROM league_settings" in query:
            return (2,)
        return None
    mock_cursor.fetchone.side_effect = _fetchone
    mock_cursor.fetchall.return_value = []
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_client = MagicMock()
    event = {"pick_number": "2", "team_id": "3", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.resolve_player_id", return_value="internal-1"), \
         patch("src.optimization.draft_matrix.persist_draft_pick", return_value=True), \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates") as mock_candidates:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        result = dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert result is None
    mock_candidates.assert_not_called()
    complete_calls = [
        c for c in mock_client.hset.call_args_list
        if c.kwargs.get("mapping", {}).get("draft_state") == "complete"
    ]
    assert complete_calls
    assert complete_calls[-1].kwargs["mapping"]["recommended_player_id"] == ""
    assert complete_calls[-1].kwargs["mapping"]["top_candidates"] == "[]"
    # Regression for a review comment on #82's first pass: leaving the earlier "Active"
    # this same call already wrote on the board would have _listener_status keep reporting
    # listener_state == "Active" forever, even once the Job exits and becomes Succeeded.
    assert complete_calls[-1].kwargs["mapping"]["listener_state"] == "Complete"


def test_handle_pick_event_returns_recommendation_after_opponent_pick():
    mock_conn, _ = _mock_conn_returning(inserted=True)
    mock_client = MagicMock()
    event = {"pick_number": "1", "team_id": "3", "player_id": "p1"}
    target = _player("p2", "RB", 10.0, name="Best Back")
    runner_up = _player("p3", "WR", 8.0, name="Second Choice")

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", return_value=[target, runner_up]):
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        result = dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert result == ([target, runner_up], 2)
    assert mock_client.hset.call_count == 2
    hset_kwargs = mock_client.hset.call_args_list[-1].kwargs
    assert hset_kwargs["mapping"]["recommended_player_id"] == "p2"
    assert hset_kwargs["mapping"]["listener_state"] == "Active"
    assert hset_kwargs["mapping"]["last_processed_pick_number"] == 1
    top_candidates = json.loads(hset_kwargs["mapping"]["top_candidates"])
    assert [c["player_id"] for c in top_candidates] == ["p2", "p3"]


# Confirmed live 2026-08-27 via the recommendation_compute_duration_ms timing added to
# handle_pick_event: compute_draft_recommendation_candidates alone routinely takes
# 550-950ms in a 10-team practice room, while ESPN bot picks land 0.5-1.5s apart.
# Recomputing after every opponent pick (the previous, KNOWN-LIMITATION behavior) raced
# the solve against 9 other teams' picks per round and lost often enough that a full
# 160-pick practice draft produced zero successful publishes for our team - every single
# one of our picks fell back to ESPN autodraft. These two tests cover the fix: gate the
# recompute on the persisted draft_slots map so it only runs for the one opponent pick
# right before our actual turn.
def test_handle_pick_event_skips_recompute_when_slot_map_shows_not_our_turn():
    mock_conn, mock_cursor = _mock_conn_returning(inserted=True, team_count=10)
    mock_cursor.fetchall.side_effect = (
        lambda: [("3", 6)] if "draft_slots" in mock_cursor.last_query else []
    )
    mock_client = MagicMock()
    # Next pick (6) belongs to team "3" per the slot map below, not our team "7".
    event = {"pick_number": "5", "team_id": "4", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates") as mock_candidates:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        result = dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert result is None
    mock_candidates.assert_not_called()


def test_handle_pick_event_still_recomputes_when_slot_map_confirms_our_turn():
    mock_conn, mock_cursor = _mock_conn_returning(inserted=True, team_count=10)
    mock_cursor.fetchall.side_effect = (
        lambda: [("7", 6)] if "draft_slots" in mock_cursor.last_query else []
    )
    mock_client = MagicMock()
    # Next pick (6) belongs to our own team "7" per the slot map below.
    event = {"pick_number": "5", "team_id": "4", "player_id": "p1"}
    target = _player("p2", "RB", 10.0, name="Best Back")

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", return_value=[target]) as mock_candidates:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        result = dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert result == ([target], 6)
    mock_candidates.assert_called_once()


def test_handle_pick_event_publishes_vorp_by_espn_id_for_the_full_undrafted_pool():
    """CORE-19/#80: the ESPN-suggested-pick column needs a VORP score for whichever player
    ESPN suggests, which is frequently NOT one of the roster-constrained top_candidates -
    so handle_pick_event also publishes a lookup covering every scored undrafted candidate
    (via compute_draft_recommendation_candidates' pool_out param), keyed by espn_id since
    that's the only id the ESPN suggestion itself carries."""
    mock_conn, _ = _mock_conn_returning(inserted=True)
    mock_client = MagicMock()
    event = {"pick_number": "1", "team_id": "3", "player_id": "p1"}
    target = _player("p2", "RB", 10.0, name="Best Back", espn_id="e-2")
    runner_up = _player("p3", "WR", 8.0, name="Second Choice", espn_id="e-3")
    # Didn't make the ILP's top picks, but is still in the full scored pool - exactly the
    # case a lookup built only from top_candidates would miss.
    long_shot = _player("p4", "TE", 1.0, name="Deep Sleeper", espn_id="e-4")

    def fake_candidates(cursor, league_id, season, week, team_id, limit=5, pool_out=None):
        if pool_out is not None:
            pool_out.extend([target, runner_up, long_shot])
        return [target, runner_up]

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", side_effect=fake_candidates):
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    hset_kwargs = mock_client.hset.call_args_list[-1].kwargs
    vorp_by_espn_id = json.loads(hset_kwargs["mapping"]["vorp_by_espn_id"])
    assert vorp_by_espn_id == {"e-2": 10.0, "e-3": 8.0, "e-4": 1.0}


def test_process_entries_marks_listener_failed_on_handler_exception():
    mock_client = MagicMock()
    entries = [("draft_picks:L1", [("1-0", {"pick_number": "1"})])]

    with patch("src.optimization.draft_matrix.handle_pick_event", side_effect=Exception("boom")):
        dm._process_entries(
            mock_client,
            stream_key="draft_picks:L1",
            league_id="L1",
            season=2026,
            week=1,
            team_id="7",
            delay_seconds=8,
            pending_state={},
            entries=entries,
        )

    hset_calls = [c for c in mock_client.hset.call_args_list if c.args and c.args[0] == "draft:L1:board"]
    assert hset_calls
    assert hset_calls[-1].kwargs["mapping"]["listener_state"] == "Failed"
    assert hset_calls[-1].kwargs["mapping"]["listener_error"] == "boom"


def test_handle_pick_event_records_draft_slot_on_round_one_pick():
    """CORE-10 turn-order tracking: a round-1 pick's arrival order is the team's
    permanent snake-draft slot, and it must be recorded even when we're the team that
    just picked (no recommendation is computed in that branch, but the slot still is)."""
    mock_conn, _ = _mock_conn_returning(inserted=True)
    mock_client = MagicMock()
    event = {"pick_number": "3", "round": "1", "team_id": "7", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.persist_draft_slot") as mock_persist_slot:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    mock_persist_slot.assert_called_once_with(mock_conn, "L1", 2026, "7", 3)


def test_handle_pick_event_skips_slot_recording_outside_round_one():
    mock_conn, _ = _mock_conn_returning(inserted=True)
    mock_client = MagicMock()
    event = {"pick_number": "12", "round": "2", "team_id": "3", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", return_value=[]), \
         patch("src.optimization.draft_matrix.persist_draft_slot") as mock_persist_slot:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    mock_persist_slot.assert_not_called()


# ---------------------------------------------------------------------------
# persist_draft_slot / compute_on_the_clock_team (CORE-10 turn order)
# ---------------------------------------------------------------------------
def test_persist_draft_slot_query_shape():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    dm.persist_draft_slot(mock_conn, league_id="L1", season=2026, team_id="7", slot_number=3)

    query, params = mock_cursor.execute.call_args[0]
    assert "ON CONFLICT" in query
    assert "DO NOTHING" in query
    assert params == ("L1", 2026, "7", 3)


def test_compute_on_the_clock_team_round_one_is_direct_lookup():
    slot_map = {"a": 1, "b": 2, "c": 3}
    assert dm.compute_on_the_clock_team(2, team_count=3, slot_map=slot_map) == "b"


def test_compute_on_the_clock_team_round_two_reverses_order():
    # 3-team snake: round 1 = a,b,c (picks 1-3); round 2 reverses = c,b,a (picks 4-6).
    slot_map = {"a": 1, "b": 2, "c": 3}
    assert dm.compute_on_the_clock_team(4, team_count=3, slot_map=slot_map) == "c"
    assert dm.compute_on_the_clock_team(5, team_count=3, slot_map=slot_map) == "b"
    assert dm.compute_on_the_clock_team(6, team_count=3, slot_map=slot_map) == "a"


def test_compute_on_the_clock_team_returns_none_when_slot_unknown():
    assert dm.compute_on_the_clock_team(2, team_count=3, slot_map={"a": 1}) is None


def test_compute_on_the_clock_team_returns_none_for_zero_team_count():
    assert dm.compute_on_the_clock_team(1, team_count=0, slot_map={}) is None


def test_handle_pick_event_maps_espn_player_id_to_internal_id():
    """The stream carries ESPN's player id; draft_picks and every join key off the internal
    players.player_id. Recording the raw ESPN id attributed a pick to a different player
    entirely, because the two id spaces overlap numerically."""
    mock_conn, _ = _mock_conn_returning(inserted=True, team_count=10)
    mock_client = MagicMock()
    event = {"pick_number": "5", "team_id": "3", "player_id": "4685526"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.resolve_player_id", return_value="13302") as mock_resolve, \
         patch("src.optimization.draft_matrix.persist_draft_pick", return_value=True) as mock_pick, \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", return_value=[]):
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    mock_resolve.assert_called_once()
    assert mock_resolve.call_args.args[1] == "4685526"
    assert mock_pick.call_args.kwargs["player_id"] == "13302"
    assert mock_pick.call_args.kwargs["espn_player_id"] == "4685526"


def test_handle_pick_event_still_records_unresolvable_players():
    """D/ST and K have no players row at all. Dropping their picks would leave a gap in
    pick_number, which breaks the gapless run team-count derivation depends on and mis-sizes
    every round after it - an unnamed pick is far cheaper than a missing one."""
    mock_conn, _ = _mock_conn_returning(inserted=True, team_count=10)
    mock_client = MagicMock()
    event = {"pick_number": "5", "team_id": "3", "player_id": "-16033"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.resolve_player_id", return_value=None), \
         patch("src.optimization.draft_matrix.persist_draft_pick", return_value=True) as mock_pick, \
         patch("src.optimization.draft_matrix.compute_draft_recommendation_candidates", return_value=[]):
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert mock_pick.call_args.kwargs["player_id"] is None
    assert mock_pick.call_args.kwargs["espn_player_id"] == "-16033"


def test_resolve_player_id_looks_up_espn_id_only():
    """No fallback to treating the value as an internal id: the id spaces overlap, so a
    fallback silently attributes picks to the wrong player, which is undetectable
    downstream."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    assert dm.resolve_player_id(mock_conn, "12483") is None
    assert mock_cursor.execute.call_count == 1
    assert "espn_id" in mock_cursor.execute.call_args.args[0]


# ---------------------------------------------------------------------------
# run_draft_listener's Redis client construction (#87)
# ---------------------------------------------------------------------------
def test_run_draft_listener_sets_socket_timeout_above_the_block_duration():
    """Regression test for #87: an unset socket_timeout leaves this repo's pinned
    redis==5.0.4 unbounded, so a genuinely dropped connection hangs the listener forever
    with no exception - the consumer-group backlog-drain-on-restart design never gets a
    chance to run if the process never actually dies. socket_timeout must stay set and
    comfortably larger than the block duration, so a normal quiet gap (already handled
    server-side by BLOCK) still resolves as a clean nil, not an exception."""
    with patch("src.optimization.draft_matrix.redis.Redis") as mock_redis_cls:
        mock_client = MagicMock()
        mock_client.ping.side_effect = RuntimeError("boom")
        mock_redis_cls.return_value = mock_client

        with pytest.raises(RuntimeError):
            dm.run_draft_listener(league_id="L1", team_id="7", season=2026, week=1, delay_seconds=1)

    _, kwargs = mock_redis_cls.call_args
    assert kwargs["socket_timeout"] * 1000 > dm.XREADGROUP_BLOCK_MS


def test_run_draft_listener_retries_transient_redis_errors_instead_of_dying():
    """Regression test for #87 review: the generated Job sets backoff_limit=0 and
    restart_policy="Never" (src/api/draft.py), so nothing outside this process will ever
    restart it. Bounding socket_timeout means a merely transient stall now raises
    TimeoutError/ConnectionError where it previously would have just kept blocking - an
    unhandled exception here would turn a brief, self-healing network blip into permanent
    loss of recommendations for the rest of the draft. The main loop must retry these two
    exception types indefinitely rather than let them propagate; any other exception
    (a real bug, not a transient Redis condition) must still propagate normally."""
    mock_client = MagicMock()
    mock_client.xreadgroup.side_effect = [
        [],  # startup backlog drain (id="0"): nothing pending, breaks immediately
        dm.redis.exceptions.TimeoutError("stall"),
        dm.redis.exceptions.ConnectionError("dropped"),
        RuntimeError("a real bug, must not be swallowed"),
    ]

    with patch("src.optimization.draft_matrix.redis.Redis", return_value=mock_client), \
         patch("src.optimization.draft_matrix.time.sleep") as mock_sleep:
        with pytest.raises(RuntimeError, match="a real bug"):
            dm.run_draft_listener(league_id="L1", team_id="7", season=2026, week=1, delay_seconds=1)

    # Retried (slept) exactly twice - once per caught transient error - not on the final,
    # unrelated RuntimeError that's expected to propagate.
    assert mock_sleep.call_count == 2
    assert mock_client.xreadgroup.call_count == 4


def test_run_draft_listener_exits_cleanly_once_draft_is_complete():
    """#82: a plain return once the board reports draft_state == 'complete' - not a crash
    - is the only way the generated Job (backoff_limit=0/restart_policy='Never', see the
    test above) ever reaches a real terminal state (Active -> Succeeded) instead of
    running forever against an ESPN socket that has nothing left to send."""
    mock_client = MagicMock()
    mock_client.xreadgroup.side_effect = [
        [],  # startup backlog drain: nothing pending
        [("draft_picks:L1", [("1-0", {"pick_number": "160"})])],
    ]
    # Not complete yet when the main loop's first iteration checks (top of loop, before
    # that iteration's read) - only becomes complete once _process_entries has handled
    # the batch above, which the next iteration's check must catch.
    mock_client.hget.side_effect = [None, "complete"]

    with patch("src.optimization.draft_matrix.redis.Redis", return_value=mock_client), \
         patch("src.optimization.draft_matrix._process_entries") as mock_process:
        dm.run_draft_listener(league_id="L1", team_id="7", season=2026, week=1, delay_seconds=1)

    mock_process.assert_called_once()
    # Exited at the top of the second iteration - never issued a second real read.
    assert mock_client.xreadgroup.call_count == 2


def test_run_draft_listener_exits_immediately_if_already_complete_on_startup():
    """Regression for a review comment on #82's first pass: the completion check only ran
    after processing a non-empty batch, so a draft already marked complete before this
    process started (e.g. a restart right after the final pick, or completion detected
    during the startup backlog drain above) was never caught - an empty main-loop poll's
    `continue` skipped the check forever, leaving the Job Active against a socket that will
    never send anything else. The check must run at the top of every iteration, including
    the very first."""
    mock_client = MagicMock()
    mock_client.xreadgroup.side_effect = [
        [],  # startup backlog drain: nothing pending
    ]
    mock_client.hget.return_value = "complete"

    with patch("src.optimization.draft_matrix.redis.Redis", return_value=mock_client), \
         patch("src.optimization.draft_matrix._process_entries") as mock_process:
        dm.run_draft_listener(league_id="L1", team_id="7", season=2026, week=1, delay_seconds=1)

    mock_process.assert_not_called()
    # Never even attempted the main loop's real read.
    assert mock_client.xreadgroup.call_count == 1


def test_run_draft_listener_seeds_vorp_lookup_before_draining_backlog():
    """CORE-19/#97 review: the ESPN-suggested-pick lookup must exist before this listener
    starts consuming picks, not just after the first opponent pick - see
    seed_initial_vorp_lookup's header. Order matters: seeding after the drain would leave
    the same window open for any pick processed during the drain itself."""
    mock_client = MagicMock()
    mock_client.hget.return_value = "complete"  # exit immediately after startup

    call_order = []
    mock_client.xreadgroup.side_effect = lambda *a, **k: call_order.append("drain") or []

    with patch("src.optimization.draft_matrix.redis.Redis", return_value=mock_client), \
         patch("src.optimization.draft_matrix.seed_initial_vorp_lookup", side_effect=lambda *a, **k: call_order.append("seed")):
        dm.run_draft_listener(league_id="L1", team_id="7", season=2026, week=1, delay_seconds=1)

    assert call_order == ["seed", "drain"]


def test_seed_initial_vorp_lookup_writes_full_pool_scores():
    """Unit-level: given a scored undrafted pool, writes vorp_by_espn_id keyed by espn_id
    (dropping the espn_id-less row, same as handle_pick_event's own lookup) directly to the
    board hash - no top_candidates/recommendation computed, since there's no pick to react
    to yet."""
    mock_client = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    fake_pool_df = dm.pl.DataFrame({"espn_id": ["e-1", "e-2", None], "vorp_score": [5.0, 3.0, 1.0]})

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn, \
         patch("src.optimization.draft_matrix.load_league_config", return_value=LEAGUE_CONFIG), \
         patch("src.optimization.draft_matrix.load_undrafted_pool") as mock_load_pool, \
         patch("src.optimization.draft_matrix.recompute_scarcity_vorp", return_value=fake_pool_df):
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm.seed_initial_vorp_lookup(mock_client, "L1", 2026, 1)

    mock_load_pool.assert_called_once_with(mock_cursor, "L1", 2026, 1)
    hset_kwargs = mock_client.hset.call_args.kwargs
    assert json.loads(hset_kwargs["mapping"]["vorp_by_espn_id"]) == {"e-1": 5.0, "e-2": 3.0}


def test_seed_initial_vorp_lookup_is_best_effort_on_db_failure():
    """Must not block the listener from starting - the per-pick recompute in
    handle_pick_event still catches up once the first opponent pick lands."""
    mock_client = MagicMock()

    with patch("src.optimization.draft_matrix.get_db_connection", side_effect=RuntimeError("db down")):
        dm.seed_initial_vorp_lookup(mock_client, "L1", 2026, 1)  # must not raise

    mock_client.hset.assert_not_called()
