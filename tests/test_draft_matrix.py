import json
import threading
import time
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


def _player(pid, position, vorp_score, name=None, team="KC"):
    return {"player_id": pid, "name": name or pid, "position": position, "vorp_score": vorp_score, "team": team}


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
    assert params == ("L1", 2026, 5, 1, "7", "p1")


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
        "player_id": "p1",
        "name": "Some Back",
        "position": "RB",
        "pro_team": "KC",
    }


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

def _mock_conn_returning(inserted: bool):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,) if inserted else None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_conn, mock_cursor


def test_handle_pick_event_skips_when_already_recorded():
    mock_conn, _ = _mock_conn_returning(inserted=False)
    mock_client = MagicMock()
    event = {"pick_number": "1", "round": "1", "team_id": "3", "player_id": "p1"}

    with patch("src.optimization.draft_matrix.get_db_connection") as mock_get_conn:
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        result = dm.handle_pick_event(mock_client, "L1", 2026, 1, our_team_id="7", event=event)

    assert result is None
    mock_client.hset.assert_not_called()


def test_handle_pick_event_skips_recommendation_when_we_just_picked():
    mock_conn, _ = _mock_conn_returning(inserted=True)
    mock_client = MagicMock()
    event = {"pick_number": "1", "round": "1", "team_id": "7", "player_id": "p1"}

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
    event = {"pick_number": "1", "round": "1", "team_id": "3", "player_id": "p1"}
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
