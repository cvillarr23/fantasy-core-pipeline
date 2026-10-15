from unittest.mock import patch

import pytest

from src.llm_client import LlmResult
from src.schemas.chat import TradeProposal
from src.services import chat_service

EMPTY_ROSTER = {
    "team_id": "10", "season": 2026, "week": 3, "roster_snapshot": "current", "starters": [], "bench": [],
}


def test_answer_chat_query_skips_trade_evaluation_without_proposal():
    roster_deficits = {"season": 2026, "week": 3, "teams": {}}
    llm_result = LlmResult(text="answer", provider="groq", model="llama-3.3-70b-versatile", attempt=1, latency_ms=10)

    with patch("src.services.chat_service.compute_roster_deficits", return_value=roster_deficits) as mock_deficits, \
         patch("src.services.chat_service.get_team_roster_players", return_value=EMPTY_ROSTER), \
         patch("src.services.chat_service.evaluate_trade") as mock_evaluate, \
         patch("src.services.chat_service.complete_with_fallback", return_value=llm_result):
        result = chat_service.answer_chat_query("How thin am I at RB?", 2026, 3, team_id="10")

    mock_deficits.assert_called_once_with(2026, 3)
    mock_evaluate.assert_not_called()
    assert result["grounding"]["trade_evaluation"] is None
    assert result["answer"] == "answer"


def test_answer_chat_query_includes_trade_evaluation_when_proposed():
    roster_deficits = {"season": 2026, "week": 3, "teams": {}}
    trade_evaluation = {"incoming_total": 10.0, "outgoing_total": 4.0, "delta_vorp": 6.0, "missing_player_ids": []}
    llm_result = LlmResult(text="answer", provider="groq", model="llama-3.3-70b-versatile", attempt=1, latency_ms=10)
    proposal = TradeProposal(incoming_player_ids=["p1"], outgoing_player_ids=["p2"])

    with patch("src.services.chat_service.compute_roster_deficits", return_value=roster_deficits), \
         patch("src.services.chat_service.get_team_roster_players", return_value=EMPTY_ROSTER), \
         patch("src.services.chat_service.evaluate_trade", return_value=trade_evaluation) as mock_evaluate, \
         patch("src.services.chat_service.complete_with_fallback", return_value=llm_result):
        result = chat_service.answer_chat_query(
            "Should I make this trade?", 2026, 3, team_id="10", trade_proposal=proposal,
        )

    mock_evaluate.assert_called_once_with(["p1"], ["p2"], 2026, 3)
    assert result["grounding"]["trade_evaluation"] == trade_evaluation


def test_answer_chat_query_propagates_value_error_from_evaluate_trade():
    proposal = TradeProposal(incoming_player_ids=["p1"], outgoing_player_ids=["p1"])

    with patch("src.services.chat_service.compute_roster_deficits", return_value={}), \
         patch("src.services.chat_service.get_team_roster_players", return_value=EMPTY_ROSTER), \
         patch("src.services.chat_service.evaluate_trade", side_effect=ValueError("overlap")):
        with pytest.raises(ValueError):
            chat_service.answer_chat_query("Bad trade?", 2026, 3, team_id="10", trade_proposal=proposal)


def test_answer_chat_query_puts_team_id_in_the_prompt():
    llm_result = LlmResult(text="answer", provider="groq", model="llama-3.3-70b-versatile", attempt=1, latency_ms=10)

    with patch("src.services.chat_service.compute_roster_deficits", return_value={}), \
         patch("src.services.chat_service.get_team_roster_players", return_value=EMPTY_ROSTER), \
         patch("src.services.chat_service.complete_with_fallback", return_value=llm_result) as mock_complete:
        chat_service.answer_chat_query("How thin am I at RB?", 2026, 3, team_id="10")

    prompt = mock_complete.call_args.args[0]
    assert 'team_id "10"' in prompt


def test_answer_chat_query_excludes_ollama_from_provider_chain(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER_PRIORITY", "groq:llama-3.3-70b-versatile,ollama:llama3.1:8b")
    llm_result = LlmResult(text="answer", provider="groq", model="llama-3.3-70b-versatile", attempt=1, latency_ms=10)

    with patch("src.services.chat_service.compute_roster_deficits", return_value={}), \
         patch("src.services.chat_service.get_team_roster_players", return_value=EMPTY_ROSTER), \
         patch("src.services.chat_service.complete_with_fallback", return_value=llm_result) as mock_complete:
        chat_service.answer_chat_query("How thin am I at RB?", 2026, 3, team_id="10")

    providers = mock_complete.call_args.kwargs["providers"]
    assert all(provider != "ollama" for provider, _ in providers)
    assert ("groq", "llama-3.3-70b-versatile") in providers


def test_answer_chat_query_grounds_prompt_in_own_team_roster():
    team_roster = {
        **EMPTY_ROSTER,
        "starters": [{"player_id": "p1", "name": "Weak Starter", "position": "WR", "nfl_team": "KC", "slot": "WR",
                      "injury_status": None, "vorp_score": -1.2, "projected_points": 6.5}],
    }
    llm_result = LlmResult(text="answer", provider="groq", model="llama-3.3-70b-versatile", attempt=1, latency_ms=10)

    with patch("src.services.chat_service.compute_roster_deficits", return_value={}), \
         patch("src.services.chat_service.get_team_roster_players", return_value=team_roster) as mock_roster, \
         patch("src.services.chat_service.complete_with_fallback", return_value=llm_result) as mock_complete:
        result = chat_service.answer_chat_query("Who are my weakest starters?", 2026, 3, team_id="10")

    mock_roster.assert_called_once_with("10", 2026, 3)
    assert "Weak Starter" in mock_complete.call_args.args[0]
    assert "own roster" in mock_complete.call_args.kwargs["system_prompt"]
    assert result["grounding"]["team_roster"] == team_roster


FREE_AGENT_POOL = {
    "season": 2026, "week": 3, "total": 7, "limit": None, "offset": 0,
    "source_mode": "target_week", "source_weeks": [3],
    "free_agents": [
        {"player_id": f"rb{i}", "espn_id": str(i), "name": f"Back {i}", "position": "RB", "pro_team": "SEA",
         "injury_status": None, "projected_points": 10.0 - i, "vorp_score": 5.0 - i / 3,
         "bench_player": None, "vorp_delta": None}
        for i in range(6)
    ] + [
        {"player_id": "wr1", "espn_id": "99", "name": "Open Wideout", "position": "WR", "pro_team": "NYJ",
         "injury_status": "QUESTIONABLE", "projected_points": 8.0, "vorp_score": 2.0,
         "bench_player": None, "vorp_delta": None},
    ],
}
WAIVER_RECOMMENDATIONS = {
    "season": 2026, "week": 3, "open_roster_slots": 1, "open_ir_slots": 0, "ir_moves": [],
    "source_mode": "target_week", "source_weeks": [3],
    "recommendations": [{
        "kind": "add", "position": "WR", "vorp_delta": 2.0, "bench_player": None, "ir_player": None,
        "free_agent": {"espn_id": "99", "name": "Open Wideout", "pro_team": "NYJ",
                       "injury_status": "QUESTIONABLE", "net_vorp": 2.0},
        "rumor_sentiment": None, "rumor_confidence": None, "rumor_suggested_action": None,
    }],
}


def _ask(question, pool, recommendations):
    llm_result = LlmResult(text="answer", provider="groq", model="llama-3.3-70b-versatile", attempt=1, latency_ms=10)
    with patch("src.services.chat_service.compute_roster_deficits", return_value={}), \
         patch("src.services.chat_service.get_team_roster_players", return_value=EMPTY_ROSTER), \
         patch("src.services.chat_service.browse_free_agents", **pool) as mock_browse, \
         patch("src.services.chat_service.compute_waiver_recommendations", **recommendations) as mock_recs, \
         patch("src.services.chat_service.complete_with_fallback", return_value=llm_result) as mock_complete:
        result = chat_service.answer_chat_query(question, 2026, 3, team_id="10")
    return result, mock_browse, mock_recs, mock_complete


def test_waiver_question_gets_free_agent_and_recommendation_context(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "10")
    result, mock_browse, mock_recs, mock_complete = _ask(
        "Who should I pick up this week?",
        pool={"return_value": FREE_AGENT_POOL}, recommendations={"return_value": WAIVER_RECOMMENDATIONS},
    )

    mock_browse.assert_called_once_with(2026, 3, sort="vorp", limit=None)
    mock_recs.assert_called_once_with(2026, 3)
    free_agents = result["grounding"]["free_agents"]
    assert [p["name"] for p in free_agents["top_by_position"]["RB"]] == [f"Back {i}" for i in range(5)]
    assert free_agents["top_by_position"]["WR"] == [{
        "name": "Open Wideout", "position": "WR", "pro_team": "NYJ", "bye_week": None,
        "injury_status": "QUESTIONABLE", "projected_points": 8.0, "vorp_score": 2.0,
    }]
    assert free_agents["total_free_agents"] == 7
    waivers = result["grounding"]["waiver_recommendations"]
    assert waivers["open_roster_slots"] == 1
    assert waivers["recommendations"][0]["kind"] == "add"
    assert waivers["recommendations"][0]["drop"] is None
    prompt = mock_complete.call_args.args[0]
    assert "Open Wideout" in prompt
    assert "Back 4" in prompt and "Back 5" not in prompt
    assert "open_roster_slots" in prompt
    system_prompt = mock_complete.call_args.kwargs["system_prompt"]
    assert "never claim to have made or submitted" in system_prompt


def test_non_waiver_question_skips_free_agent_context():
    result, mock_browse, mock_recs, mock_complete = _ask(
        "Who are my weakest starters?",
        pool={"return_value": FREE_AGENT_POOL}, recommendations={"return_value": WAIVER_RECOMMENDATIONS},
    )

    mock_browse.assert_not_called()
    mock_recs.assert_not_called()
    assert result["grounding"]["free_agents"] is None
    assert result["grounding"]["waiver_recommendations"] is None
    assert "Free agents" not in mock_complete.call_args.args[0]


def test_waiver_recommendations_are_withheld_for_another_team(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "4")
    result, mock_browse, mock_recs, _ = _ask(
        "Is anyone good available at WR?",
        pool={"return_value": FREE_AGENT_POOL}, recommendations={"return_value": WAIVER_RECOMMENDATIONS},
    )

    mock_browse.assert_called_once()
    mock_recs.assert_not_called()
    assert "unavailable" in result["grounding"]["waiver_recommendations"]


def test_unavailable_waiver_inputs_are_reported_not_raised(monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "10")
    error = chat_service.WaiverInputsUnavailableError("stale vorp_score")
    result, _, _, mock_complete = _ask(
        "Any free agents worth a claim?",
        pool={"side_effect": error}, recommendations={"side_effect": error},
    )

    assert result["grounding"]["free_agents"] == {"unavailable": "stale vorp_score"}
    assert result["grounding"]["waiver_recommendations"] == {"unavailable": "stale vorp_score"}
    assert "stale vorp_score" in mock_complete.call_args.args[0]


def test_follow_up_to_a_waiver_question_keeps_free_agent_context():
    assert chat_service._is_waiver_question(
        "What about him?", [{"role": "user", "content": "Who's the best free agent RB?"},
                            {"role": "answer", "content": "Back 0."}],
    )
    assert not chat_service._is_waiver_question("Should I start Kelce?", [])


BYE_OUTLOOK = {
    "team_id": "10", "team_name": "Us", "season": 2026, "start_week": 3, "end_week": 6,
    "schedule_known": True, "median_lineup_value": 30.0, "crunch_weeks": [5],
    "weeks": [{"week": 5, "value": 12.0, "shortfall": 18.0, "unfilled_slots": ["RB"], "held_slots": [],
               "on_bye": ["101"], "unknown": [], "replacement_level_starters": 0, "crunch": True}],
    "players": [{"espn_id": "101", "name": "Bye Back", "position": "RB", "nfl_team": "BUF",
                 "nfl_bye_week": 5}],
}


def test_chat_is_grounded_in_the_bye_outlook_and_bye_rules():
    with patch("src.services.chat_service.get_bye_outlook", return_value=BYE_OUTLOOK) as mock_outlook:
        result, _, _, mock_complete = _ask(
            "Who should I start?",
            pool={"return_value": FREE_AGENT_POOL}, recommendations={"return_value": WAIVER_RECOMMENDATIONS},
        )

    mock_outlook.assert_called_once()
    outlook = result["grounding"]["bye_outlook"]
    assert outlook["crunch_weeks"] == [5]
    assert outlook["by_week"][0]["on_bye"] == ["Bye Back"]
    assert "Bye Back" in mock_complete.call_args.args[0]
    system_prompt = mock_complete.call_args.kwargs["system_prompt"]
    assert "just because he is on bye" in system_prompt


def test_chat_survives_an_unavailable_bye_outlook():
    with patch("src.services.chat_service.get_bye_outlook", side_effect=RuntimeError("ESPN down")):
        result, _, _, _ = _ask(
            "Who should I start?",
            pool={"return_value": FREE_AGENT_POOL}, recommendations={"return_value": WAIVER_RECOMMENDATIONS},
        )

    assert result["grounding"]["bye_outlook"] == {"unavailable": "ESPN down"}
