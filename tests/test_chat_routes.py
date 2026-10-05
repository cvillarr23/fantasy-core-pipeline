from unittest.mock import patch

from src.llm_client import LlmProviderChainExhaustedError, LlmResult


def _llm_result(**overrides):
    defaults = dict(text="You're thin at RB.", provider="groq", model="llama-3.3-70b-versatile",
                     attempt=1, latency_ms=42)
    defaults.update(overrides)
    return LlmResult(**defaults)


EMPTY_ROSTER = {
    "team_id": "10", "season": 2026, "week": 3, "roster_snapshot": "current", "starters": [], "bench": [],
}


def test_chat_query_success_without_trade_proposal(client):
    roster_deficits = {"season": 2026, "week": 3, "roster_snapshot": "current", "teams": {}}

    with patch("src.services.chat_service.compute_roster_deficits", return_value=roster_deficits), \
         patch("src.services.chat_service.get_team_roster_players", return_value=EMPTY_ROSTER), \
         patch("src.services.chat_service.complete_with_fallback", return_value=_llm_result()):
        response = client.post(
            "/v1/chat/query",
            json={"question": "How thin am I at RB?", "season": 2026, "week": 3, "team_id": "10"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["answer"] == "You're thin at RB."
    assert body["provider"] == "groq"
    assert body["grounding"]["trade_evaluation"] is None


def test_chat_query_success_with_trade_proposal(client):
    roster_deficits = {"season": 2026, "week": 3, "roster_snapshot": "current", "teams": {}}
    trade_evaluation = {"incoming_total": 10.0, "outgoing_total": 4.0, "delta_vorp": 6.0, "missing_player_ids": []}

    with patch("src.services.chat_service.compute_roster_deficits", return_value=roster_deficits), \
         patch("src.services.chat_service.get_team_roster_players", return_value=EMPTY_ROSTER), \
         patch("src.services.chat_service.evaluate_trade", return_value=trade_evaluation), \
         patch("src.services.chat_service.complete_with_fallback", return_value=_llm_result()):
        response = client.post(
            "/v1/chat/query",
            json={
                "question": "Should I make this trade?", "season": 2026, "week": 3, "team_id": "10",
                "trade_proposal": {"incoming_player_ids": ["p1"], "outgoing_player_ids": ["p2"]},
            },
        )

    assert response.status_code == 200
    assert response.json()["grounding"]["trade_evaluation"] == trade_evaluation


def test_chat_query_rejects_overlapping_trade_ids_with_422(client):
    with patch("src.services.chat_service.compute_roster_deficits", return_value={}), \
         patch("src.services.chat_service.get_team_roster_players", return_value=EMPTY_ROSTER):
        response = client.post(
            "/v1/chat/query",
            json={
                "question": "Should I make this trade?", "season": 2026, "week": 3, "team_id": "10",
                "trade_proposal": {"incoming_player_ids": ["p1"], "outgoing_player_ids": ["p1"]},
            },
        )

    assert response.status_code == 422


def test_chat_query_returns_503_when_provider_chain_exhausted(client):
    with patch("src.services.chat_service.compute_roster_deficits", return_value={}), \
         patch("src.services.chat_service.get_team_roster_players", return_value=EMPTY_ROSTER), \
         patch("src.services.chat_service.complete_with_fallback",
               side_effect=LlmProviderChainExhaustedError("all providers failed")):
        response = client.post(
            "/v1/chat/query",
            json={"question": "How thin am I at RB?", "season": 2026, "week": 3, "team_id": "10"},
        )

    assert response.status_code == 503
