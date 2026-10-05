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
