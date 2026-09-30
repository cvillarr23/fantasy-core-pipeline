from unittest.mock import MagicMock, patch

import pytest

from src.optimization import waiver_evaluator as we


def test_resolve_live_waiver_moves_translates_to_espn_identity():
    cursor = MagicMock()
    # add player_id lookup, then drop player_id lookup.
    cursor.fetchone.side_effect = [("e-4046", "Some Back", "KC"), ("e-9999", "Bench Guy", "SEA")]
    moves = [{
        "position": "RB", "drop_player_id": "bench1", "add_player_id": "4046",
        "add_vorp_score": 5.0, "drop_vorp_score": 1.0,
    }]

    resolved = we.resolve_live_waiver_moves(cursor, moves, season=2023, week=5)

    assert resolved == [{
        "player_id": "e-4046",
        "drop_player_id": "e-9999",
        "name": "Some Back",
        "team": "KC",
        "decision_inputs": {
            "add_player_vorp_score": 5.0,
            "drop_player_vorp_score": 1.0,
            "season": 2023,
            "week": 5,
        },
    }]


def test_resolve_live_waiver_moves_includes_vorp_scores_and_season_week_in_decision_inputs():
    """CORE-17 (#35): /v1/trigger/waiver's decision_inputs field needs the VORP delta that
    justified the claim, so a caller with no season/week (e.g. an older test fixture)
    still gets a decision_inputs dict with None for the missing pieces rather than
    crashing on a missing key."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("e-4046", "Some Back", "KC")]
    moves = [{
        "position": "RB", "drop_player_id": None, "add_player_id": "4046",
        "add_vorp_score": 7.5, "drop_vorp_score": -2.0,
    }]

    resolved = we.resolve_live_waiver_moves(cursor, moves)

    assert resolved[0]["decision_inputs"] == {
        "add_player_vorp_score": 7.5,
        "drop_player_vorp_score": -2.0,
        "season": None,
        "week": None,
    }


def test_resolve_live_waiver_moves_skips_when_add_player_has_no_espn_id():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [(None, "Some Back", "KC")]
    moves = [{"position": "RB", "drop_player_id": "bench1", "add_player_id": "4046"}]

    resolved = we.resolve_live_waiver_moves(cursor, moves)

    assert resolved == []


def test_resolve_live_waiver_moves_skips_when_drop_player_has_no_espn_id():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("e-4046", "Some Back", "KC"), (None, "Bench Guy", "SEA")]
    moves = [{"position": "RB", "drop_player_id": "bench1", "add_player_id": "4046"}]

    resolved = we.resolve_live_waiver_moves(cursor, moves)

    assert resolved == []


def test_resolve_live_waiver_moves_handles_a_move_with_no_drop_player():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [("e-4046", "Some Back", "KC")]
    moves = [{
        "position": "RB", "drop_player_id": None, "add_player_id": "4046",
        "add_vorp_score": 5.0, "drop_vorp_score": None,
    }]

    resolved = we.resolve_live_waiver_moves(cursor, moves, season=2023, week=5)

    assert resolved == [{
        "player_id": "e-4046",
        "drop_player_id": None,
        "name": "Some Back",
        "team": "KC",
        "decision_inputs": {
            "add_player_vorp_score": 5.0,
            "drop_player_vorp_score": None,
            "season": 2023,
            "week": 5,
        },
    }]


def test_publish_waiver_claim_posts_expected_payload():
    target = {"player_id": "e-4046", "drop_player_id": "e-9999", "name": "Some Back", "team": "KC"}

    with patch("src.optimization.waiver_evaluator.post_trigger") as mock_post:
        we.publish_waiver_claim("7", target)

    args, kwargs = mock_post.call_args
    assert args[0] == "waiver"
    assert args[1] == {"team_id": "7", **target}
    assert kwargs["core_api_base_url"] == we.CORE_API_BASE_URL


def test_run_weekly_waivers_live_wires_decision_logic_to_publish():
    resolved_target = {"player_id": "e-4046", "drop_player_id": "e-9999", "name": "Some Back", "team": "KC"}

    with patch("src.optimization.waiver_evaluator.get_db_connection") as mock_get_conn, \
         patch("src.optimization.waiver_evaluator.waiver_strategy.load_bench_by_position") as mock_bench, \
         patch("src.optimization.waiver_evaluator.waiver_strategy.load_best_free_agent_by_position") as mock_fa, \
         patch("src.optimization.waiver_evaluator.waiver_strategy.propose_waiver_moves") as mock_propose, \
         patch("src.optimization.waiver_evaluator.resolve_live_waiver_moves", return_value=[resolved_target]) as mock_resolve, \
         patch("src.optimization.waiver_evaluator.publish_waiver_claim") as mock_publish:
        mock_conn = MagicMock()
        mock_get_conn.return_value.__enter__.return_value = mock_conn
        mock_bench.return_value = {"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}
        mock_fa.return_value = {"RB": {"player_id": "4046", "vorp_score": 5.0}}
        mock_propose.return_value = [{"position": "RB", "drop_player_id": "bench1", "add_player_id": "4046"}]

        result = we.run_weekly_waivers_live("7", 2023, 5)

    mock_propose.assert_called_once_with(mock_bench.return_value, mock_fa.return_value)
    # CORE-17 (#35): resolve_live_waiver_moves needs season/week to fold into
    # decision_inputs - the reasoning bot_decisions records for the eventual claim.
    expected_cursor = mock_conn.cursor.return_value.__enter__.return_value
    mock_resolve.assert_called_once_with(expected_cursor, mock_propose.return_value, 2023, 5)
    mock_publish.assert_called_once_with("7", resolved_target, we.CORE_API_BASE_URL)
    assert result == [resolved_target]


def _run_with_loaded_inputs(bench, free_agents, proposed):
    with patch("src.optimization.waiver_evaluator.get_db_connection"), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.load_bench_by_position", return_value=bench), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.load_best_free_agent_by_position", return_value=free_agents), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.propose_waiver_moves", return_value=proposed) as mock_propose, \
         patch("src.optimization.waiver_evaluator.resolve_live_waiver_moves", return_value=[]), \
         patch("src.optimization.waiver_evaluator.publish_waiver_claim") as mock_publish:
        result = we.run_weekly_waivers_live("8", 2026, 4)
    return result, mock_propose, mock_publish


def test_run_weekly_waivers_live_fails_when_no_bench_inputs_loaded():
    """#159: the 2026-09-29 CronJob found nothing to evaluate for an unplayed week and still
    exited 0, so bot_decisions never saw a waiver row and nobody noticed. Missing inputs must
    fail the Job - the same distinction #178 draws for the lineup optimizer."""
    with pytest.raises(we.WaiverInputsMissing, match="bench"):
        _run_with_loaded_inputs({}, {"RB": {"player_id": "4046", "vorp_score": 5.0}}, [])


def test_run_weekly_waivers_live_fails_when_no_free_agent_inputs_loaded():
    with pytest.raises(we.WaiverInputsMissing, match="free-agent"):
        _run_with_loaded_inputs({"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}, {}, [])


def test_run_weekly_waivers_live_zero_moves_with_real_inputs_succeeds_and_says_so(caplog):
    bench = {"RB": [{"player_id": "bench1", "vorp_score": 9.0}]}
    free_agents = {"RB": {"player_id": "4046", "vorp_score": 5.0}}

    with caplog.at_level("INFO", logger="WaiverEvaluator"):
        result, mock_propose, mock_publish = _run_with_loaded_inputs(bench, free_agents, [])

    assert result == []
    mock_propose.assert_called_once_with(bench, free_agents)
    mock_publish.assert_not_called()
    assert any("No waiver moves proposed" in r.getMessage() for r in caplog.records)
