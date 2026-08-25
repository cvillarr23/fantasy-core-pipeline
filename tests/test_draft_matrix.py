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
    for pos in dm.SKILL_POSITIONS:
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
        # Absent for a direct call; hold_and_publish always sets it from the pick clock.
        "expires_at": None,
    }


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


def test_record_publish_error_writes_status_visible_execution_log_row():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        dm._record_publish_error("L1", _player("p1", "RB", 10.0, name="Some Back"), "HTTP 422")

    query, params = mock_cursor.execute.call_args.args
    assert "draft_target_publish" in query
    assert "execution_log" in query
    assert params == ("HTTP 422", "L1", "e-1", "Some Back")


# ---------------------------------------------------------------------------
# hold_and_publish
# ---------------------------------------------------------------------------

def test_hold_and_publish_publishes_when_not_cancelled():
    mock_client = MagicMock()
    mock_client.get.return_value = None
    target = _player("p1", "RB", 10.0, name="Some Back")

    with patch("src.optimization.draft_matrix.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=202)
        dm.hold_and_publish(mock_client, "L1", "7", 5, target, delay_seconds=1, sleep_fn=lambda s: None)

    mock_post.assert_called_once()


def test_hold_and_publish_skips_when_cancelled():
    mock_client = MagicMock()
    mock_client.get.return_value = None
    cancel_event = threading.Event()
    cancel_event.set()
    target = _player("p1", "RB", 10.0)

    with patch("src.optimization.draft_matrix.requests.post") as mock_post:
        dm.hold_and_publish(mock_client, "L1", "7", 5, target, delay_seconds=1, cancel_event=cancel_event, sleep_fn=lambda s: None)

    mock_post.assert_not_called()


def test_hold_and_publish_retries_transient_publish_failure():
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)
    sleeps = []

    with patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "publish_draft_target", side_effect=[dm.requests.ConnectionError("unavailable"), None]) as mock_publish, \
         patch.object(dm, "_record_publish_error") as mock_record:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, target, delay_seconds=8,
            sleep_fn=sleeps.append,
        )

    assert mock_publish.call_count == 2
    assert sleeps == [dm.PUBLISH_RETRY_INTERVAL_SECONDS]
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
            mock_client, "L1", "7", 5, target, delay_seconds=8, sleep_fn=lambda _s: None,
        )

    assert mock_client.board["listener_error"] == "Listener died: connection refused"
    assert mock_client.board["listener_state"] == "Failed"


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
            mock_client, "L1", "7", 5, target, delay_seconds=0, sleep_fn=lambda _s: None,
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
            mock_client, "L1", "7", 5, target, delay_seconds=0, sleep_fn=lambda _s: None,
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
            mock_client, "L1", "7", 5, target, delay_seconds=0, sleep_fn=lambda _s: None,
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
            mock_client, "L1", "7", 5, target, delay_seconds=0, sleep_fn=lambda _s: None,
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
            mock_client, "L1", "7", 5, target, delay_seconds=0,
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
            mock_client, "L1", "7", 5, target, delay_seconds=8,
            sleep_fn=sleeps.append,
        )

    assert sum(sleeps) == 8
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
            mock_client, "L1", "7", 5, target, delay_seconds=8, sleep_fn=sleeps.append,
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
            mock_client, "L1", "7", 5, target, delay_seconds=8, sleep_fn=sleeps.append,
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
            mock_client, "L1", "7", 5, target, delay_seconds=8,
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
            mock_client, "L1", "7", 5, target, delay_seconds=8, sleep_fn=_sleep,
        )

    assert mock_publish.call_count == 1


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
            mock_client, "L1", "7", 5, target, delay_seconds=0,
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
            mock_client, "L1", "7", 5, target, delay_seconds=0,
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
            mock_client, "L1", "7", 5, target, delay_seconds=0,
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
            mock_client, "L1", "7", 5, target, delay_seconds=0,
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
            mock_client, "L1", "7", 5, target, delay_seconds=0,
            sleep_fn=lambda _s: None, monotonic_fn=lambda: 29.5, clock_origin=0.0,
        )

    mock_publish.assert_not_called()
    # An abandoned pick hands the slot to autodraft, so it has to be visible in
    # /v1/draft/status and execution_log, not only in the pod log.
    mock_record.assert_called_once()
    assert "Pick clock expired" in mock_client.board["listener_error"]
    assert "minimum request window" in mock_client.board["listener_error"]


def test_hold_and_publish_reports_a_pick_abandoned_at_the_deadline():
    mock_client = _board_client()
    target = _player("p1", "RB", 10.0)

    with patch.object(dm, "DRAFT_PICK_CLOCK_SECONDS", 30), \
         patch.object(dm, "DRAFT_PUBLISH_SAFETY_SECONDS", 0), \
         patch.object(dm, "wait_for_pause_or_timeout", return_value=True), \
         patch.object(dm, "_record_publish_error") as mock_record, \
         patch.object(dm, "publish_draft_target") as mock_publish:
        dm.hold_and_publish(
            mock_client, "L1", "7", 5, target, delay_seconds=0,
            sleep_fn=lambda _s: None, monotonic_fn=lambda: 45.0, clock_origin=0.0,
        )

    mock_publish.assert_not_called()
    mock_record.assert_called_once()
    assert "Pick clock expired" in mock_client.board["listener_error"]


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
        dm._schedule_publish(mock_client, "L1", "7", 5, _player("p1", "RB", 1.0), delay_seconds=8, pending_state=pending_state)

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
                    mock_client, "L1", "7", 5, _player("p1", "RB", 1.0),
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


# ---------------------------------------------------------------------------
# handle_pick_event (orchestrator)
# ---------------------------------------------------------------------------

def _mock_conn_returning(inserted: bool, team_count: int = 10):
    mock_cursor = MagicMock()
    insert_row = (1,) if inserted else None
    if team_count is None:
        # league_team_count falls through league_settings to draft_config before giving up.
        lookups = [None, None]
    else:
        lookups = [(team_count,)]
    # resolve_player_id's espn_id -> internal players.player_id lookup sits between the
    # team-count lookup and the insert.
    mock_cursor.fetchone.side_effect = lookups + [("internal-1",)] + [insert_row] + [None] * 8
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

    mock_derive.assert_called_once_with(mock_conn, "L1", 2026)
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
    event = {"pick_number": "1", "team_id": "7", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        result = dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert result is None
    mock_client.hset.assert_called_once()
    mapping = mock_client.hset.call_args.kwargs["mapping"]
    assert mapping["listener_state"] == "Active"
    assert mapping["last_processed_pick_number"] == 1


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

    assert result == (target, 2)
    assert mock_client.hset.call_count == 2
    hset_kwargs = mock_client.hset.call_args_list[-1].kwargs
    assert hset_kwargs["mapping"]["recommended_player_id"] == "p2"
    assert hset_kwargs["mapping"]["listener_state"] == "Active"
    assert hset_kwargs["mapping"]["last_processed_pick_number"] == 1
    top_candidates = json.loads(hset_kwargs["mapping"]["top_candidates"])
    assert [c["player_id"] for c in top_candidates] == ["p2", "p3"]


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
