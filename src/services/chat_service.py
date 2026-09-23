"""
Chat query orchestration (CORE-153), used by the REST endpoint (src/api/chat.py).

Grounds an LLM answer in real roster/VORP data by calling the same service functions the MCP
tools in src/api/mcp_server.py wrap (compute_roster_deficits, evaluate_trade) directly, in
process -- not a round trip through this app's own MCP/OAuth transport, which exists for
remote callers like fantasy-browser-worker's trade_bot.js, not for a same-process caller.

Read-only by construction: this module only ever imports read/compute functions, never
anything that publishes a waiver claim or lineup change -- that stays behind the existing
bot pipeline in fantasy-browser-worker.

Excludes the "ollama" tier from this call site's provider chain. llm_client.py's fallback
chain can block ~90s scaling Ollama 0->1 on a cold pod, which is fine for the queue/CronJob
callers that motivated it but not for this synchronous HTTP endpoint a human is waiting on.
"""
import json
import os
from typing import Optional

from src.llm_client import complete_with_fallback, parse_priority_chain
from src.schemas.chat import TradeProposal
from src.services.roster_service import compute_roster_deficits
from src.services.trade_service import evaluate_trade

SYSTEM_PROMPT = (
    "You are a fantasy football assistant answering questions about one manager's team. "
    "Answer only using the JSON context provided below the question -- never invent roster, "
    "VORP, or trade numbers. If the context doesn't cover what's asked, say so explicitly "
    "instead of guessing. You are read-only: never claim to have made a trade, waiver claim, "
    "or lineup change -- you can only describe and recommend, not act."
)


def _chat_provider_chain() -> list[tuple[str, str]]:
    raw = os.getenv("CHAT_LLM_PROVIDER_PRIORITY") or os.getenv("LLM_PROVIDER_PRIORITY")
    return [(provider, model) for provider, model in parse_priority_chain(raw) if provider != "ollama"]


def _build_prompt(question: str, team_id: str, roster_deficits: dict, trade_evaluation: Optional[dict]) -> str:
    sections = [
        f"You are advising the manager of team_id \"{team_id}\".",
        f"Question: {question}",
        f"League roster/VORP context (all teams):\n{json.dumps(roster_deficits)}",
    ]
    if trade_evaluation is not None:
        sections.append(f"Proposed trade evaluation:\n{json.dumps(trade_evaluation)}")
    return "\n\n".join(sections)


def answer_chat_query(
    question: str,
    season: int,
    week: int,
    team_id: str,
    trade_proposal: Optional[TradeProposal] = None,
) -> dict:
    roster_deficits = compute_roster_deficits(season, week)

    trade_evaluation = None
    if trade_proposal is not None:
        # A malformed proposal (duplicate/overlapping ids) raises ValueError here -- propagates
        # to the router, which maps it the same way src/api/trade.py already does.
        trade_evaluation = evaluate_trade(
            trade_proposal.incoming_player_ids, trade_proposal.outgoing_player_ids, season, week,
        )

    prompt = _build_prompt(question, team_id, roster_deficits, trade_evaluation)
    result = complete_with_fallback(
        prompt, system_prompt=SYSTEM_PROMPT, call_site="chat_query", providers=_chat_provider_chain(),
    )

    return {
        "answer": result.text,
        "provider": result.provider,
        "model": result.model,
        "latency_ms": result.latency_ms,
        "grounding": {"roster_deficits": roster_deficits, "trade_evaluation": trade_evaluation},
    }
