import pytest
from unittest.mock import MagicMock, patch

from src.optimization import lineup_solver
from src.services import rumor_action_service as ras

pytestmark = pytest.mark.usefixtures("rumor_checkpoint")


def _fake_redis(initial_watermark=None):
    store = {} if initial_watermark is None else {ras.WATERMARK_REDIS_KEY: str(initial_watermark)}

    client = MagicMock()
    client.get.side_effect = lambda key: store.get(key)
    client.set.side_effect = lambda key, value, **kwargs: store.__setitem__(key, value)
    client.store = store
    return client


def _db_context(cursor):
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn


def test_no_new_signals_leaves_watermark_unchanged():
    redis_client = _fake_redis(initial_watermark=5)
    cursor = MagicMock()
    cursor.fetchall.return_value = []

    with patch("src.services.rumor_action_service._redis_client", return_value=redis_client), \
         patch("src.services.rumor_action_service.get_db_connection", return_value=_db_context(cursor)):
        result = ras.evaluate_and_publish_rumor_actions("7", 2026, 3)

    assert result == {
        "considered": 0,
        "waiver_claims_published": [],
        "lineup_swaps_published": [],
        "watermark": 5,
    }


def test_qualifying_boost_waiver_signal_publishes_matching_move():
    redis_client = _fake_redis()
    signals_cursor = MagicMock()
    signals_cursor.fetchall.return_value = [(10, "4046", "positive", 0.9, ras.BOOST_WAIVER_ACTION)]

    resolved_target = {"player_id": "e-4046", "drop_player_id": "e-9999", "name": "Some Back", "team": "KC"}
    move = {"position": "RB", "drop_player_id": "bench1", "add_player_id": "4046", "add_vorp_score": 5.0, "drop_vorp_score": 1.0}

    with patch("src.services.rumor_action_service._redis_client", return_value=redis_client), \
         patch("src.services.rumor_action_service.get_db_connection", return_value=_db_context(signals_cursor)), \
         patch("src.services.rumor_action_service.waiver_strategy.load_bench_by_position") as mock_bench, \
         patch("src.services.rumor_action_service.waiver_strategy.load_best_free_agent_by_position") as mock_fa, \
         patch("src.services.rumor_action_service.waiver_strategy.propose_waiver_moves", return_value=[move]), \
         patch("src.services.rumor_action_service.resolve_live_waiver_moves", return_value=[resolved_target]) as mock_resolve, \
         patch("src.services.rumor_action_service.publish_waiver_claim") as mock_publish:
        mock_bench.return_value = {"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}
        mock_fa.return_value = {"RB": {"player_id": "4046", "vorp_score": 5.0}}

        result = ras.evaluate_and_publish_rumor_actions("7", 2026, 3)

    mock_resolve.assert_called_once_with(signals_cursor, [move], 2026, 3)
    mock_publish.assert_called_once()
    published = result["waiver_claims_published"]
    assert published == [resolved_target]
    assert published[0]["decision_inputs"] == {
        "rumor_signal_id": 10,
        "rumor_sentiment": "positive",
        "rumor_confidence": 0.9,
    }
    assert result["considered"] == 1
    assert result["watermark"] == 10
    assert redis_client.store[ras.WATERMARK_REDIS_KEY] == "10"


def test_boost_waiver_never_drops_an_ir_eligible_player_while_ir_is_open():
    redis_client = _fake_redis()
    signals_cursor = MagicMock()
    signals_cursor.fetchall.return_value = [(10, "4046", "positive", 0.9, ras.BOOST_WAIVER_ACTION)]
    move = {"position": "RB", "drop_player_id": "hurt", "add_player_id": "4046", "add_vorp_score": 5.0, "drop_vorp_score": -1.0}

    with patch("src.services.rumor_action_service._redis_client", return_value=redis_client), \
         patch("src.services.rumor_action_service.get_db_connection", return_value=_db_context(signals_cursor)), \
         patch("src.services.rumor_action_service.load_live_waiver_inputs", return_value=(
             {"RB": [{"player_id": "hurt", "vorp_score": -1.0, "espn_injury_status": "INJURY_RESERVE"}]},
             {"RB": {"player_id": "4046", "vorp_score": 5.0}},
         )), \
         patch("src.services.rumor_action_service.waiver_strategy.propose_waiver_moves", return_value=[move]), \
         patch("src.optimization.waiver_evaluator.load_open_ir_slots", return_value=1), \
         patch("src.services.rumor_action_service.resolve_live_waiver_moves") as mock_resolve, \
         patch("src.services.rumor_action_service.publish_waiver_claim") as mock_publish:
        result = ras.evaluate_and_publish_rumor_actions("7", 2026, 3)

    mock_resolve.assert_not_called()
    mock_publish.assert_not_called()
    assert result["waiver_claims_published"] == []


def test_waiver_claims_publish_outside_the_db_block_and_skip_already_queued():
    """publish_waiver_claim retries with backoff (up to ~2 min), so it must not run while a
    pooled connection is checked out; and a claim the gateway reports as already queued
    (False) isn't a new publish."""
    waiver_conn_open = {"open": False}
    waiver_conn = MagicMock()
    waiver_cursor = MagicMock()
    waiver_conn.__enter__.side_effect = lambda: waiver_conn_open.update(open=True) or waiver_conn
    waiver_conn.__exit__.side_effect = lambda *a: waiver_conn_open.update(open=False)
    waiver_conn.cursor.return_value.__enter__.return_value = waiver_cursor

    targets = [
        {"player_id": "e-1", "drop_player_id": "e-9", "name": "Queued Already", "team": "KC"},
        {"player_id": "e-2", "drop_player_id": "e-8", "name": "Brand New", "team": "SF"},
    ]
    moves = [{"add_player_id": "1"}, {"add_player_id": "2"}]
    waiver_cursor.fetchall.return_value = [
        (1, "1", "positive", 0.9, ras.BOOST_WAIVER_ACTION),
        (2, "2", "positive", 0.9, ras.BOOST_WAIVER_ACTION),
    ]

    def _publish(team_id, target):
        assert not waiver_conn_open["open"], "published while a DB connection was checked out"
        return target["name"] == "Brand New"

    with patch("src.services.rumor_action_service.get_db_connection", return_value=waiver_conn), \
         patch("src.services.rumor_action_service.load_live_waiver_inputs", return_value=({}, {})), \
         patch("src.services.rumor_action_service.waiver_strategy.propose_waiver_moves", return_value=moves), \
         patch("src.services.rumor_action_service.resolve_live_waiver_moves", side_effect=[[targets[0]], [targets[1]]]), \
         patch("src.services.rumor_action_service._redis_client", return_value=_fake_redis()), \
         patch("src.services.rumor_action_service.publish_waiver_claim", side_effect=_publish) as mock_publish:
        published = ras.evaluate_and_publish_rumor_actions("7", 2026, 4)["waiver_claims_published"]

    assert mock_publish.call_count == 2
    assert [t["name"] for t in published] == ["Brand New"]


def test_qualifying_bench_watch_signal_publishes_matching_swap():
    redis_client = _fake_redis()
    signals_cursor = MagicMock()
    signals_cursor.fetchall.return_value = [(20, "9001", "negative", 0.8, ras.BENCH_WATCH_ACTION)]

    swap = {
        "drop_player_id": "9001", "add_player_id": "bench2", "slot": "FLEX",
        "matchup_multiplier": 1.0, "decision_inputs": {"add_player_vorp_score": 4.0},
    }

    with patch("src.services.rumor_action_service._redis_client", return_value=redis_client), \
         patch("src.services.rumor_action_service.get_db_connection", return_value=_db_context(signals_cursor)), \
         patch("src.services.rumor_action_service.lineup_solver.evaluate_lineup",
               return_value={"swaps": [swap], "roster_pool": [], "optimal_starters": [], "source": {}}), \
         patch("src.optimization.lineup_solver.resolve_espn_identity",
               side_effect=lambda _cursor, pid: {"espn_id": f"espn-{pid}", "name": f"Name {pid}", "team": "KC"}), \
         patch("src.services.rumor_action_service.lineup_solver.publish_lineup_swaps") as mock_publish:
        result = ras.evaluate_and_publish_rumor_actions("7", 2026, 3)

    # Matched on the internal drop id (the signal's player_id), published with ESPN ids (#187).
    published = result["lineup_swaps_published"]
    assert mock_publish.call_args.args == ("7", published)
    assert mock_publish.call_args.kwargs["idempotency_key"].startswith("lineup-7-")
    assert len(published) == 1
    assert published[0]["drop_player_id"] == "espn-9001"
    assert published[0]["add_player_id"] == "espn-bench2"
    assert published[0]["drop_player_name"] == "Name 9001"
    assert published[0]["decision_inputs"] == {
        "add_player_vorp_score": 4.0,
        "rumor_signal_id": 20,
        "rumor_sentiment": "negative",
        "rumor_confidence": 0.8,
        "add_player_internal_id": "bench2",
        "drop_player_internal_id": "9001",
    }
    assert result["watermark"] == 20


def test_bench_watch_fails_loudly_on_insufficient_lineup_input():
    """Same contract as the weekly optimizer (#178): missing lineup inputs raise (the
    endpoint 500s, the CronJob fails) instead of reporting zero swaps."""
    redis_client = _fake_redis()
    signals_cursor = MagicMock()
    signals_cursor.fetchall.return_value = [(21, "9001", "negative", 0.8, ras.BENCH_WATCH_ACTION)]
    error = lineup_solver.InsufficientLineupInputError("stale weekly_stats.vorp_score")

    with patch("src.services.rumor_action_service._redis_client", return_value=redis_client), \
         patch("src.services.rumor_action_service.get_db_connection", return_value=_db_context(signals_cursor)), \
         patch("src.services.rumor_action_service.lineup_solver.evaluate_lineup", side_effect=error), \
         patch("src.services.rumor_action_service.lineup_solver.publish_lineup_swaps") as mock_publish, \
         pytest.raises(RuntimeError, match="stale weekly_stats"):
        ras.evaluate_and_publish_rumor_actions("7", 2026, 3)

    mock_publish.assert_not_called()
    assert redis_client.store[ras.WATERMARK_REDIS_KEY] == "0"


def test_signal_below_threshold_is_never_published():
    redis_client = _fake_redis()
    signals_cursor = MagicMock()
    signals_cursor.fetchall.return_value = [(30, "4046", "positive", 0.5, ras.BOOST_WAIVER_ACTION)]

    with patch("src.services.rumor_action_service._redis_client", return_value=redis_client), \
         patch("src.services.rumor_action_service.get_db_connection", return_value=_db_context(signals_cursor)), \
         patch("src.services.rumor_action_service.waiver_strategy.load_bench_by_position"), \
         patch("src.services.rumor_action_service.waiver_strategy.load_best_free_agent_by_position"), \
         patch("src.services.rumor_action_service.waiver_strategy.propose_waiver_moves") as mock_propose, \
         patch("src.services.rumor_action_service.publish_waiver_claim") as mock_publish:
        result = ras.evaluate_and_publish_rumor_actions("7", 2026, 3)

    mock_propose.assert_not_called()
    mock_publish.assert_not_called()
    assert result["waiver_claims_published"] == []
    assert result["watermark"] == 30


def test_qualifying_signal_with_no_matching_proposed_move_still_advances_watermark():
    redis_client = _fake_redis()
    signals_cursor = MagicMock()
    signals_cursor.fetchall.return_value = [(40, "4046", "positive", 0.95, ras.BOOST_WAIVER_ACTION)]

    with patch("src.services.rumor_action_service._redis_client", return_value=redis_client), \
         patch("src.services.rumor_action_service.get_db_connection", return_value=_db_context(signals_cursor)), \
         patch("src.services.rumor_action_service.waiver_strategy.load_bench_by_position"), \
         patch("src.services.rumor_action_service.waiver_strategy.load_best_free_agent_by_position"), \
         patch("src.services.rumor_action_service.waiver_strategy.propose_waiver_moves", return_value=[]), \
         patch("src.services.rumor_action_service.publish_waiver_claim") as mock_publish:
        result = ras.evaluate_and_publish_rumor_actions("7", 2026, 3)

    mock_publish.assert_not_called()
    assert result["waiver_claims_published"] == []
    assert result["watermark"] == 40
