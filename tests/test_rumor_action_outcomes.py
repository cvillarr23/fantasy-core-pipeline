"""Application outcomes, independent of the Kubernetes Job's exit status."""

from unittest.mock import MagicMock, patch

import pytest

from src.services import rumor_action_service as ras


@pytest.fixture
def run_env():
    cursor = MagicMock()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cursor
    redis = MagicMock()
    redis.get.return_value = "0"
    with patch.object(ras, "get_db_connection", return_value=conn), patch.object(
        ras, "_redis_client", return_value=redis
    ), patch.object(ras, "_record_execution") as record:
        yield cursor, redis, record


@pytest.mark.parametrize(
    "rows,reason",
    [
        ([], "no_new_signals"),
        ([(1, "p", "positive", 0.1, ras.BOOST_WAIVER_ACTION)], "non_qualifying"),
        ([(1, "p", "neutral", 0.99, "no_op")], "non_qualifying"),
    ],
)
def test_legitimate_noop_records_one_success(run_env, rows, reason):
    cursor, _, record = run_env
    cursor.fetchall.return_value = rows
    ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    record.assert_called_once()
    assert record.call_args.kwargs["status"] == "success"
    details = record.call_args.kwargs["outcome_details"]
    assert details["reason"] == reason
    assert details["signals_fetched"] == len(rows)
    assert details["actions_accepted"] == 0


@pytest.mark.parametrize(
    "resolution,publish_result,status,reason",
    [
        (True, True, "success", "completed"),
        (True, False, "success", "already_queued"),
        (False, True, "failure", "missing_identity"),
    ],
)
def test_waiver_action_outcomes(run_env, resolution, publish_result, status, reason):
    cursor, _, record = run_env
    cursor.fetchall.return_value = [(1, "p", "positive", 0.9, ras.BOOST_WAIVER_ACTION)]
    target = {"player_id": "espn-p"}
    with patch.object(
        ras, "load_live_waiver_inputs", return_value=({}, {})
    ), patch.object(
        ras.waiver_strategy,
        "propose_waiver_moves",
        return_value=[{"add_player_id": "p"}],
    ), patch.object(
        ras, "resolve_live_waiver_moves", return_value=[target] if resolution else []
    ), patch.object(
        ras, "publish_waiver_claim", return_value=publish_result
    ):
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    details = record.call_args.kwargs["outcome_details"]
    assert record.call_args.kwargs["status"] == status
    assert details["reason"] == reason
    assert details["actions_proposed"] == 1
    assert details["actions_accepted"] == int(resolution and publish_result)
    assert details["actions_already_queued"] == int(resolution and not publish_result)


def test_partial_publish_preserves_progress_on_exception(run_env):
    cursor, _, record = run_env
    cursor.fetchall.return_value = [
        (1, "p1", "positive", 0.9, ras.BOOST_WAIVER_ACTION),
        (2, "p2", "positive", 0.9, ras.BOOST_WAIVER_ACTION),
    ]
    with patch.object(
        ras, "load_live_waiver_inputs", return_value=({}, {})
    ), patch.object(
        ras.waiver_strategy,
        "propose_waiver_moves",
        return_value=[{"add_player_id": "p1"}, {"add_player_id": "p2"}],
    ), patch.object(
        ras,
        "resolve_live_waiver_moves",
        side_effect=[[{"player_id": "e1"}], [{"player_id": "e2"}]],
    ), patch.object(
        ras,
        "publish_waiver_claim",
        side_effect=[True, RuntimeError("gateway unavailable")],
    ), pytest.raises(
        RuntimeError, match="gateway unavailable"
    ):
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    record.assert_called_once()
    assert record.call_args.kwargs["status"] == "partial_failure"
    details = record.call_args.kwargs["outcome_details"]
    assert details["actions_accepted"] == 1
    assert details["actions_failed"] == 1


def test_fetch_failure_is_not_no_new_signals(run_env):
    cursor, _, record = run_env
    cursor.fetchall.side_effect = RuntimeError("db unavailable")
    with pytest.raises(RuntimeError):
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    assert record.call_args.kwargs["status"] == "failure"
    assert record.call_args.kwargs["outcome_details"]["reason"] == "exception"


def test_http_service_records_exactly_once(client, run_env):
    cursor, _, record = run_env
    cursor.fetchall.return_value = []
    response = client.post(
        "/v1/trigger/rumor-action", json={"team_id": "7", "season": 2026, "week": 4}
    )
    assert response.status_code == 200
    record.assert_called_once()
    assert record.call_args.kwargs["outcome_details"]["source"] == "http"


def test_cli_records_exactly_once(run_env, monkeypatch):
    cursor, _, record = run_env
    cursor.fetchall.return_value = []
    monkeypatch.setenv("JOB_NAME", "fantasy-rumor-action-trigger-123")
    ras.main(["--team-id", "7", "--season", "2026", "--week", "4"])
    record.assert_called_once()
    details = record.call_args.kwargs["outcome_details"]
    assert details["source"] == "cli"
    assert details["job_name"] == "fantasy-rumor-action-trigger-123"


def test_no_beneficial_move_and_superseded_signal_are_healthy(run_env):
    cursor, _, record = run_env
    cursor.fetchall.return_value = [
        (1, "p", "positive", 0.9, ras.BOOST_WAIVER_ACTION),
        (2, "p", "positive", 0.9, ras.BOOST_WAIVER_ACTION),
    ]
    with patch.object(
        ras, "load_live_waiver_inputs", return_value=({}, {})
    ), patch.object(ras.waiver_strategy, "propose_waiver_moves", return_value=[]):
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    assert record.call_args.kwargs["status"] == "success"
    details = record.call_args.kwargs["outcome_details"]
    assert details["reason"] == "no_beneficial_move"
    assert details["signal_reasons"] == {"superseded": 1, "no_beneficial_move": 1}


@pytest.mark.parametrize(
    "missing,accepted,status",
    [(False, False, "success"), (True, True, "partial_failure")],
)
def test_lineup_duplicate_or_partial_identity_resolution(
    run_env, missing, accepted, status
):
    cursor, _, record = run_env
    cursor.fetchall.return_value = [(1, "p", "negative", 0.9, ras.BENCH_WATCH_ACTION)]
    # One signal can yield several actions: accounting must not compare action and signal totals.
    swaps = [{"drop_player_id": "p", "add_player_id": f"b{i}"} for i in range(2)]
    resolved = swaps[:1] if missing else swaps
    with patch.object(
        ras.lineup_solver, "evaluate_lineup", return_value={"swaps": swaps}
    ), patch.object(
        ras.lineup_solver, "resolve_lineup_swaps", return_value=resolved
    ), patch.object(
        ras.lineup_solver, "publish_lineup_swaps", return_value=accepted
    ):
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    assert record.call_args.kwargs["status"] == status
    details = record.call_args.kwargs["outcome_details"]
    assert details["actions_missing_identity"] == int(missing)
    assert details["actions_already_queued"] == (0 if accepted else 2)
    assert details["signals_unaccounted"] == 0
    assert details["actions_unaccounted"] == 0


def test_unaccounted_signals_cannot_succeed(run_env):
    cursor, _, record = run_env
    cursor.fetchall.return_value = [(1, "p", "positive", 0.9, ras.BOOST_WAIVER_ACTION)]
    with patch.object(ras, "_evaluate_and_publish_waiver_claims", return_value=[]):
        ras.evaluate_and_publish_rumor_actions("7", 2026, 4)
    assert record.call_args.kwargs["status"] == "failure"
    assert record.call_args.kwargs["outcome_details"]["reason"] == "unaccounted_work"


def test_cli_failure_propagates_and_records_once(run_env):
    cursor, _, record = run_env
    cursor.fetchall.side_effect = RuntimeError("query failed")
    with pytest.raises(RuntimeError, match="query failed"):
        ras.main(["--team-id", "7", "--season", "2026", "--week", "4"])
    record.assert_called_once()
    assert record.call_args.kwargs["status"] == "failure"


def test_http_failure_records_once(client, run_env):
    cursor, _, record = run_env
    cursor.fetchall.side_effect = RuntimeError("query failed")
    response = client.post(
        "/v1/trigger/rumor-action", json={"team_id": "7", "season": 2026, "week": 4}
    )
    assert response.status_code == 500
    record.assert_called_once()
    assert record.call_args.kwargs["status"] == "failure"


def test_record_serializes_details_and_survives_db_failure(caplog):
    import logging

    cursor = MagicMock()
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cursor
    with patch.object(ras, "get_db_connection", return_value=conn):
        ras._record_execution(
            "partial_failure", 42, season=2026, outcome_details={"actions_accepted": 1}
        )
    sql, params = cursor.execute.call_args.args
    assert "outcome_details" in sql
    assert params[-1].adapted == {"actions_accepted": 1}
    with patch.object(
        ras, "get_db_connection", side_effect=RuntimeError("db down")
    ), caplog.at_level(logging.INFO):
        ras._record_execution("failure", 42, outcome_details={"reason": "exception"})
    assert "rumor_action_outcome" in caplog.text
    assert "Failed to record" in caplog.text


@pytest.mark.parametrize(
    "response_status,expected", [("duplicate", False), ("queued", True)]
)
def test_lineup_publisher_reports_acceptance(response_status, expected):
    response = MagicMock()
    response.json.return_value = {"status": response_status}
    swaps = [{"drop_player_id": "p", "add_player_id": "b", "slot": "RB"}]
    with patch.object(
        ras.lineup_solver, "post_trigger_idempotent", return_value=response
    ):
        assert ras.lineup_solver.publish_lineup_swaps("7", swaps) is expected
