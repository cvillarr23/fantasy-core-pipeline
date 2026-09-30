from unittest.mock import MagicMock, patch

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


# ---------------------------------------------------------------------------
# Missing target-week inputs (#178 follow-up): an empty bench/free-agent pool means the
# target week's weekly_stats haven't landed, not "no waiver moves worth making".
# ---------------------------------------------------------------------------

import pytest


def test_load_live_waiver_inputs_raises_when_no_free_agents_have_target_week_rows():
    with patch("src.optimization.waiver_evaluator.waiver_strategy.load_bench_by_position",
               return_value={"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.load_best_free_agent_by_position", return_value={}), \
         pytest.raises(we.InsufficientWaiverInputError) as exc:
        we.load_live_waiver_inputs(MagicMock(), "7", 2026, 5)
    assert "free-agent" in str(exc.value)
    assert "Season 2026 Week 5" in str(exc.value)


def test_load_live_waiver_inputs_raises_when_bench_has_no_target_week_rows():
    with patch("src.optimization.waiver_evaluator.waiver_strategy.load_bench_by_position", return_value={}), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.load_best_free_agent_by_position",
               return_value={"RB": {"player_id": "4046", "vorp_score": 5.0}}), \
         pytest.raises(we.InsufficientWaiverInputError) as exc:
        we.load_live_waiver_inputs(MagicMock(), "7", 2026, 5)
    assert "bench" in str(exc.value)


def test_load_live_waiver_inputs_returns_both_pools():
    bench = {"RB": [{"player_id": "bench1", "vorp_score": 1.0}]}
    fa = {"RB": {"player_id": "4046", "vorp_score": 5.0}}
    with patch("src.optimization.waiver_evaluator.waiver_strategy.load_bench_by_position", return_value=bench), \
         patch("src.optimization.waiver_evaluator.waiver_strategy.load_best_free_agent_by_position", return_value=fa):
        assert we.load_live_waiver_inputs(MagicMock(), "7", 2026, 5) == (bench, fa)


def test_run_weekly_waivers_live_publishes_nothing_on_insufficient_input():
    with patch("src.optimization.waiver_evaluator.get_db_connection"), \
         patch("src.optimization.waiver_evaluator.load_live_waiver_inputs",
               side_effect=we.InsufficientWaiverInputError("no free-agent rows")), \
         patch("src.optimization.waiver_evaluator.publish_waiver_claim") as mock_publish, \
         pytest.raises(we.InsufficientWaiverInputError):
        we.run_weekly_waivers_live("7", 2026, 5)
    mock_publish.assert_not_called()


WAIVER_ARGV = ["--team-id", "7", "--season", "2026", "--week", "5"]


def test_main_exits_nonzero_with_reason_on_insufficient_input(caplog):
    with patch("src.optimization.waiver_evaluator.run_weekly_waivers_live",
               side_effect=we.InsufficientWaiverInputError("no free-agent rows")):
        assert we.main(WAIVER_ARGV) == 1
    assert "cannot evaluate Team 7 Season 2026 Week 5" in caplog.text
    assert "no free-agent rows" in caplog.text


def test_main_exits_zero_when_no_moves_are_worth_making():
    with patch("src.optimization.waiver_evaluator.run_weekly_waivers_live", return_value=[]) as mock_run:
        assert we.main(WAIVER_ARGV) == 0
    mock_run.assert_called_once_with("7", 2026, 5)
