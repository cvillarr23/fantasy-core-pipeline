"""A fresh service invocation shares only durable checkpoints and gateway dedupe."""

from copy import deepcopy
from unittest.mock import MagicMock, patch

import pytest

from src.services import rumor_action_service as ras


@pytest.fixture
def retry_env(monkeypatch):
    monkeypatch.setenv("JOB_NAME", "rumor-job-1")
    redis_store = {}
    redis = MagicMock()
    redis.get.side_effect = redis_store.get
    redis.set.side_effect = lambda k, v: redis_store.__setitem__(k, v)
    database = {}

    def load(team, initial=None):
        return deepcopy(
            database.get(
                team,
                (
                    0,
                    {
                        "watermark": (
                            initial
                            if initial is not None
                            else ras._get_watermark(redis)
                        ),
                        "signals": {},
                        "actions": {},
                        "failed_jobs": {},
                    },
                ),
            )
        )

    def save(team, version, state):
        assert database.get(team, (0,))[0] == version
        database[team] = (version + 1, deepcopy(state))
        return version + 1

    rows = [
        {
            "id": i,
            "player_id": f"p{i}",
            "sentiment": "positive",
            "confidence": 0.9,
            "suggested_action": ras.BOOST_WAIVER_ACTION,
        }
        for i in (1, 2, 3)
    ]
    with patch.object(ras, "_redis_client", return_value=redis), patch.object(
        ras, "get_db_connection"
    ), patch.object(
        ras,
        "fetch_new_signals",
        side_effect=lambda c, w: [r for r in rows if r["id"] > w],
    ), patch.object(
        ras, "_load_checkpoint", side_effect=load, create=True
    ), patch.object(
        ras, "_save_checkpoint", side_effect=save, create=True
    ), patch.object(
        ras, "load_live_waiver_inputs", return_value=({}, {})
    ), patch.object(
        ras.waiver_strategy,
        "propose_waiver_moves",
        return_value=[{"add_player_id": f"p{i}"} for i in (1, 2, 3)],
    ), patch.object(
        ras,
        "resolve_live_waiver_moves",
        side_effect=lambda c, m, s, w: [
            {
                "player_id": m[0]["add_player_id"],
                "decision_inputs": {"season": s, "week": w},
            }
        ],
    ), patch.object(
        ras, "_record_execution"
    ) as record:
        yield rows, redis_store, database, record


def test_partial_failure_retry_publishes_only_remainder(retry_env):
    rows, redis, _, record = retry_env
    calls = []

    def publish(team, target):
        pid = target["player_id"]
        calls.append(pid)
        if pid == "p2" and calls.count(pid) == 1:
            raise RuntimeError("gateway unavailable")
        return True

    with patch.object(ras, "publish_waiver_claim", side_effect=publish):
        with pytest.raises(RuntimeError, match="gateway unavailable"):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        assert redis.get(ras.WATERMARK_REDIS_KEY) == "1"
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    assert calls.count("p1") == calls.count("p3") == 1
    assert calls.count("p2") == 2
    assert redis[ras.WATERMARK_REDIS_KEY] == "3"
    assert [c.kwargs["status"] for c in record.call_args_list] == [
        "partial_failure",
        "success",
    ]


def test_every_attempt_fails_job_and_records_failure(retry_env):
    _, _, _, record = retry_env
    with patch.object(
        ras, "publish_waiver_claim", side_effect=RuntimeError("gateway unavailable")
    ):
        for _ in range(3):
            with pytest.raises(RuntimeError):
                ras.main(["--team-id", "7", "--season", "2026", "--week", "4"])
    assert [c.kwargs["status"] for c in record.call_args_list] == ["failure"] * 3
    # Even an extra pod in the exhausted Job must not report empty success.
    with pytest.raises(RuntimeError):
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4)


def test_permanent_failure_dropped_with_reason_next_job_progresses(
    retry_env, monkeypatch
):
    rows, redis, _, record = retry_env
    rows[:] = rows[:1]
    with patch.object(
        ras, "publish_waiver_claim", side_effect=RuntimeError("bad claim")
    ) as publish:
        for _ in range(3):
            with pytest.raises(RuntimeError):
                ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        assert publish.call_count == 3
        details = record.call_args.kwargs["outcome_details"]
        assert details["dropped_signals"] == [
            {
                "signal_id": 1,
                "reason": "retry_limit_exceeded",
                "attempts": 3,
                "error": "bad claim",
            }
        ]
        monkeypatch.setenv("JOB_NAME", "rumor-job-2")
        result = ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        assert result["considered"] == 0
        assert publish.call_count == 3
    assert redis[ras.WATERMARK_REDIS_KEY] == "1"


@pytest.mark.parametrize("failure", ["response_lost", "checkpoint_failed"])
def test_lineup_replays_saved_payload_and_job_key(retry_env, monkeypatch, failure):
    rows, redis, _, record = retry_env
    rows[:] = [{**rows[0], "suggested_action": ras.BENCH_WATCH_ACTION}]
    swaps = [
        {
            "drop_player_id": "p1",
            "add_player_id": "bench",
            "slot": "RB",
            "decision_inputs": {"season": 2026, "week": 4},
        }
    ]
    deliveries, calls = set(), []

    def gateway(path, payload, key, **kwargs):
        calls.append((deepcopy(payload), key))
        response = MagicMock()
        duplicate = key in deliveries
        deliveries.add(key)
        if len(calls) == 1 and failure == "response_lost":
            raise RuntimeError("response lost after enqueue")
        response.json.return_value = {
            "status": "duplicate" if duplicate else "accepted"
        }
        return response

    save = ras._save_checkpoint.side_effect
    failed = False

    def unreliable_save(team, version, state):
        nonlocal failed
        if (
            failure == "checkpoint_failed"
            and not failed
            and any(a["status"] == "accepted" for a in state["actions"].values())
        ):
            failed = True
            raise RuntimeError("checkpoint commit failed")
        return save(team, version, state)

    with patch.object(
        ras.lineup_solver, "evaluate_lineup", return_value={"swaps": swaps}
    ) as evaluate, patch.object(
        ras.lineup_solver, "resolve_lineup_swaps", side_effect=lambda c, s: s
    ), patch.object(
        ras.lineup_solver, "post_trigger_idempotent", side_effect=gateway
    ), patch.object(
        ras, "_save_checkpoint", side_effect=unreliable_save
    ):
        with pytest.raises(RuntimeError):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        monkeypatch.setenv("JOB_NAME", "different-job")
        result = ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    assert evaluate.call_count == 1
    assert calls[0] == calls[1]
    assert calls[0][1] == ras.lineup_solver.lineup_idempotency_key(
        "7", swaps, job_name="rumor-job-1"
    )
    assert len(deliveries) == 1
    assert result["lineup_swaps_published"] == []
    assert record.call_args.kwargs["outcome_details"]["actions_already_queued"] == 1
    assert redis[ras.WATERMARK_REDIS_KEY] == "1"


def test_lineup_signals_wait_while_another_lineup_action_is_pending(retry_env):
    rows, _, database, record = retry_env
    rows[:] = [{**rows[0], "suggested_action": ras.BENCH_WATCH_ACTION}]
    swaps = [
        {"drop_player_id": "p1", "add_player_id": "bench", "slot": "RB"},
        {
            "drop_player_id": "different-player",
            "add_player_id": "another-bench",
            "slot": "WR",
        },
    ]
    evaluations = []
    publishes = []

    def evaluate(*args, **kwargs):
        evaluations.append(args)
        return {"swaps": [swaps[min(len(evaluations) - 1, 1)]]}

    def publish(*args, **kwargs):
        publishes.append(args)
        if len(publishes) == 1:
            raise RuntimeError("temporary gateway failure")
        return True

    with patch.object(
        ras.lineup_solver, "evaluate_lineup", side_effect=evaluate
    ), patch.object(
        ras.lineup_solver,
        "resolve_lineup_swaps",
        side_effect=lambda conn, proposed: proposed,
    ), patch.object(
        ras.lineup_solver, "publish_lineup_swaps", side_effect=publish
    ):
        with pytest.raises(RuntimeError, match="temporary gateway failure"):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        rows.append(
            {**rows[0], "id": 2, "player_id": "different-player", "sentiment": "new"}
        )
        result = ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        assert len(evaluations) == 1
        assert database["7"][1]["signals"]["2"]["prepared"] is False
        assert database["7"][1]["watermark"] == 1
        assert not any(
            action["status"] == "pending"
            for action in database["7"][1]["actions"].values()
        )
        assert result["watermark"] == 1
        assert record.call_args.kwargs["status"] == "success"
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    assert len(evaluations) == 2
    assert len(publishes) == 3


def test_checkpoint_prunes_completed_drop_and_job_failure_history(retry_env):
    _, _, database, _ = retry_env
    database["7"] = (
        1,
        {
            "watermark": 0,
            "signals": {},
            "actions": {},
            "failed_jobs": {
                "old-job": {
                    "reason": "old failure",
                    "updated_at": ras.time.time()
                    - ras.FAILED_JOB_MARKER_TTL_SECONDS
                    - 1,
                }
            },
            "drops": {str(i): {"reason": "old"} for i in range(100)},
        },
    )
    with patch.object(ras, "fetch_new_signals", return_value=[]):
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    _, state = database["7"]
    assert state["failed_jobs"] == {}
    assert state["drops"] == {}


def test_failed_job_marker_survives_an_unrelated_http_invocation(
    retry_env, monkeypatch
):
    _, _, database, _ = retry_env
    database["7"] = (
        1,
        {
            "watermark": 0,
            "signals": {},
            "actions": {},
            "failed_jobs": {
                "failed-job": {
                    "reason": "retry limit exceeded",
                    "updated_at": ras.time.time(),
                }
            },
        },
    )
    monkeypatch.delenv("JOB_NAME")
    with patch.object(ras, "fetch_new_signals", return_value=[]):
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4, source="http")
    assert "failed-job" in database["7"][1]["failed_jobs"]
    monkeypatch.setenv("JOB_NAME", "failed-job")
    with patch.object(ras, "fetch_new_signals", return_value=[]), pytest.raises(
        RuntimeError, match="retry limit exceeded"
    ):
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4)


def test_expired_lineup_is_dropped_before_gateway_dedupe_expires(retry_env):
    rows, _, database, record = retry_env
    rows[:] = [{**rows[0], "suggested_action": ras.BENCH_WATCH_ACTION}]
    swaps = [{"drop_player_id": "p1", "add_player_id": "bench", "slot": "RB"}]
    with patch.object(
        ras.lineup_solver, "evaluate_lineup", return_value={"swaps": swaps}
    ), patch.object(
        ras.lineup_solver, "resolve_lineup_swaps", side_effect=lambda c, s: s
    ), patch.object(
        ras.lineup_solver, "publish_lineup_swaps", side_effect=RuntimeError("ambiguous")
    ) as publish:
        with pytest.raises(RuntimeError):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        action = next(iter(database["7"][1]["actions"].values()))
        action["prepared_at"] -= ras.LINEUP_RETRY_WINDOW_SECONDS
        with pytest.raises(RuntimeError, match="lineup_retry_window_expired"):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        assert publish.call_count == 1
    assert (
        record.call_args.kwargs["outcome_details"]["dropped_signals"][0]["reason"]
        == "lineup_retry_window_expired"
    )


def test_shared_evaluation_failure_does_not_consume_signal_attempts(retry_env):
    rows, redis, database, record = retry_env
    rows[:] = rows[:1]
    with patch.object(
        ras, "load_live_waiver_inputs", side_effect=RuntimeError("projections missing")
    ), patch.object(ras, "publish_waiver_claim") as publish:
        for _ in range(3):
            with pytest.raises(RuntimeError, match="projections missing"):
                ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        with pytest.raises(RuntimeError, match="retry limit exceeded"):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        publish.assert_not_called()
    assert database["7"][1]["actions"] == {}
    assert record.call_args_list[2].kwargs["outcome_details"]["dropped_signals"][0][
        "reason"
    ] == ("evaluation_retry_limit_exceeded")
    assert database["7"][1]["watermark"] == 1
    assert database["7"][1]["signals"] == {}
    assert database["7"][1]["drops"] == {}
    assert redis[ras.WATERMARK_REDIS_KEY] == "1"
    assert all(c.kwargs["status"] == "failure" for c in record.call_args_list)


def test_checkpoint_failure_prevents_publish(retry_env):
    with patch.object(
        ras, "_save_checkpoint", side_effect=RuntimeError("db down")
    ), patch.object(ras, "publish_waiver_claim") as publish:
        with pytest.raises(RuntimeError, match="db down"):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        publish.assert_not_called()


def test_redis_reset_does_not_replay_completed_signals(retry_env):
    _, redis, _, _ = retry_env
    with patch.object(ras, "publish_waiver_claim", return_value=True) as publish:
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        redis.clear()
        result = ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        assert result["considered"] == 0
        assert publish.call_count == 3


def test_prepared_actions_are_not_replayed_in_a_different_week(retry_env):
    with patch.object(
        ras, "publish_waiver_claim", side_effect=RuntimeError("gateway")
    ) as publish:
        with pytest.raises(RuntimeError):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        with pytest.raises(RuntimeError, match="decision_week_expired"):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 5)
        assert publish.call_count == 3


def test_new_signal_for_pending_player_is_covered_by_saved_action(retry_env):
    rows, _, database, _ = retry_env
    rows[:] = rows[:1]
    calls = []

    def publish(team, target):
        calls.append(deepcopy(target))
        if len(calls) == 1:
            raise RuntimeError("ambiguous timeout")
        return True

    with patch.object(ras, "publish_waiver_claim", side_effect=publish), patch.object(
        ras.waiver_strategy,
        "propose_waiver_moves",
        return_value=[{"add_player_id": "p1"}],
    ) as propose:
        with pytest.raises(RuntimeError):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        rows.append({**rows[0], "id": 2, "sentiment": "very positive"})
        result = ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert propose.call_count == 1
    assert result["considered"] == 2
    assert result["watermark"] == 2
    assert database["7"][1]["watermark"] == 2


def test_stable_local_hostname_is_not_a_job_failure_key(retry_env, monkeypatch):
    _, _, database, record = retry_env
    monkeypatch.delenv("JOB_NAME")
    monkeypatch.setenv("HOSTNAME", "workstation")
    with patch.object(ras, "fetch_new_signals", return_value=[]):
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4, source="cli")
    assert database["7"][1]["failed_jobs"] == {}
    assert record.call_args.kwargs["outcome_details"]["job_name"] is None


def test_expired_action_does_not_cover_a_new_signal_for_the_same_player(
    retry_env, monkeypatch
):
    rows, _, database, record = retry_env
    rows[:] = rows[:1]
    calls = []

    def publish(team, target):
        calls.append(deepcopy(target))
        if len(calls) == 1:
            raise RuntimeError("ambiguous timeout")
        return True

    with patch.object(ras, "publish_waiver_claim", side_effect=publish), patch.object(
        ras.waiver_strategy,
        "propose_waiver_moves",
        return_value=[{"add_player_id": "p1"}],
    ) as propose:
        with pytest.raises(RuntimeError):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        action = next(iter(database["7"][1]["actions"].values()))
        action["prepared_at"] -= ras.WAIVER_RETRY_WINDOW_SECONDS
        rows.append({**rows[0], "id": 2, "sentiment": "new signal"})
        with pytest.raises(RuntimeError, match="waiver_retry_window_expired"):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        assert database["7"][1]["watermark"] == 2
        dropped = record.call_args_list[-1].kwargs["outcome_details"]["dropped_signals"]
        monkeypatch.setenv("JOB_NAME", "rumor-job-2")
        with patch.object(ras, "fetch_new_signals", return_value=[]):
            result = ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    assert result["watermark"] == 2
    assert len(calls) == 2
    assert calls[1]["decision_inputs"]["rumor_signal_id"] == 2
    assert propose.call_count == 2
    assert [d["signal_id"] for d in dropped] == [1]


def test_covered_signal_stays_dropped_when_parent_action_expires(retry_env):
    rows, _, database, record = retry_env
    rows[:] = rows[:1]
    calls = []

    def publish(team, target):
        calls.append(deepcopy(target))
        if len(calls) < 3:
            raise RuntimeError("ambiguous timeout")
        return True

    with patch.object(ras, "publish_waiver_claim", side_effect=publish), patch.object(
        ras.waiver_strategy,
        "propose_waiver_moves",
        return_value=[{"add_player_id": "p1"}],
    ) as propose:
        with pytest.raises(RuntimeError):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        rows.append({**rows[0], "id": 2, "sentiment": "covered signal"})
        with pytest.raises(RuntimeError):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
        action = next(iter(database["7"][1]["actions"].values()))
        assert database["7"][1]["signals"]["2"]["covered_action_id"]
        action["prepared_at"] -= ras.WAIVER_RETRY_WINDOW_SECONDS
        rows.append({**rows[0], "id": 3, "sentiment": "fresh signal"})
        with pytest.raises(RuntimeError, match="waiver_retry_window_expired"):
            ras.evaluate_and_publish_rumor_actions("7", 2026, 4)

    assert len(calls) == 3
    assert calls[2]["decision_inputs"]["rumor_signal_id"] == 3
    assert propose.call_count == 2
    dropped = record.call_args.kwargs["outcome_details"]["dropped_signals"]
    assert sorted(detail["signal_id"] for detail in dropped) == [1, 2]


def test_redis_mirror_failure_does_not_fail_checkpointed_run(retry_env):
    _, _, _, record = retry_env
    with patch.object(ras, "fetch_new_signals", return_value=[]), patch.object(
        ras, "_redis_client", side_effect=RuntimeError("redis down")
    ):
        result = ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    assert result["considered"] == 0
    assert record.call_args.kwargs["status"] == "success"
