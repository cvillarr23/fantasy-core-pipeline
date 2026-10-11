"""
Chat query orchestration (CORE-153), used by the REST endpoint (src/api/chat.py).

Grounds an LLM answer in real roster/VORP data by calling the same service functions the MCP
tools in src/api/mcp_server.py wrap (compute_roster_deficits, get_team_roster_players,
evaluate_trade, browse_free_agents) directly, in process -- not a round trip through this
app's own MCP/OAuth transport, which exists for remote callers like fantasy-browser-worker's
trade_bot.js, not for a same-process caller.

Read-only by construction: this module only ever imports read/compute functions, never
anything that publishes a waiver claim or lineup change -- that stays behind the existing
bot pipeline in fantasy-browser-worker.

Excludes the "ollama" tier from this call site's provider chain. llm_client.py's fallback
chain can block ~90s scaling Ollama 0->1 on a cold pod, which is fine for the queue/CronJob
callers that motivated it but not for this synchronous HTTP endpoint a human is waiting on.
"""

import json
import os
import re
from typing import Optional

from src.llm_client import complete_with_fallback, parse_priority_chain
from src.schemas.chat import TradeProposal
from src.services.roster_service import compute_roster_deficits, get_team_roster_players
from src.services.free_agent_service import browse_free_agents
from src.services.trade_service import evaluate_trade
from src.services.waiver_service import (
    WaiverInputsUnavailableError,
    compute_waiver_recommendations,
)

SYSTEM_PROMPT = (
    "You are a fantasy football assistant answering questions about one manager's team. "
    "Answer only using the JSON context provided below the question -- never invent roster, "
    "VORP, or trade numbers. If the context doesn't cover what's asked, say so explicitly "
    "instead of guessing. Answer player-level questions (weakest starters, who to bench, who to "
    "start) from the manager's own roster section, and pickup questions from the free-agent and "
    "waiver-recommendation sections when present; a null vorp_score or projected_points means no "
    "data for that week (bye or not yet synced), not a zero. You are read-only: never claim to "
    "have made or submitted a trade, waiver claim, or lineup change -- you can only describe and "
    "recommend, not act."
)

# Waiver context costs a roster refresh and two pool queries and adds a few thousand prompt
# characters, so it's only fetched when the question (or the previous one, for follow-ups
# like "what about him?") is about pickups.
WAIVER_QUESTION_PATTERN = re.compile(
    r"\b(waivers?|free[- ]?agents?|pick(?:ing)?[- ]?ups?|pick(?:ed|ing)? up|add(?:s|ing)?|"
    r"drop(?:s|ping)?|claims?|stream(?:s|ing)?|available|unrostered|replacements?)\b",
    re.IGNORECASE,
)
FREE_AGENTS_PER_POSITION = 5
FREE_AGENT_FIELDS = ("name", "position", "pro_team", "injury_status", "projected_points", "vorp_score")


def _chat_provider_chain() -> list[tuple[str, str]]:
    raw = os.getenv("CHAT_LLM_PROVIDER_PRIORITY") or os.getenv("LLM_PROVIDER_PRIORITY")
    return [
        (provider, model)
        for provider, model in parse_priority_chain(raw)
        if provider != "ollama"
    ]


def _is_waiver_question(question: str, history: list[dict]) -> bool:
    last_user = next((m["content"] for m in reversed(history) if m["role"] == "user"), "")
    return any(
        isinstance(text, str) and WAIVER_QUESTION_PATTERN.search(text)
        for text in (question, last_user)
    )


def _round(value):
    return round(value, 2) if isinstance(value, float) else value


def _compact_free_agents(season: int, week: int) -> dict:
    """Top FREE_AGENTS_PER_POSITION unrostered players per position, best VORP first."""
    try:
        # limit=None keeps the whole sorted pool; it's grouped and trimmed below.
        pool = browse_free_agents(season, week, sort="vorp", limit=None)
    except WaiverInputsUnavailableError as exc:
        return {"unavailable": str(exc)}
    by_position: dict = {}
    for player in pool["free_agents"]:
        top = by_position.setdefault(player["position"], [])
        if len(top) < FREE_AGENTS_PER_POSITION:
            top.append({field: _round(player[field]) for field in FREE_AGENT_FIELDS})
    return {
        "season": season,
        "week": week,
        "source_mode": pool["source_mode"],
        "source_weeks": pool["source_weeks"],
        "total_free_agents": pool["total"],
        "top_by_position": by_position,
    }


def _summarize_waiver_player(player: Optional[dict]) -> Optional[dict]:
    if player is None:
        return None
    return {
        "name": player["name"],
        "pro_team": player["pro_team"],
        "injury_status": player["injury_status"],
        "net_vorp": _round(player["net_vorp"]),
    }


def _compact_waiver_recommendations(team_id: str, season: int, week: int) -> dict:
    # compute_waiver_recommendations plans for ESPN_TEAM_ID's roster only; showing its moves
    # to another team's manager would be advice about someone else's bench.
    our_team_id = os.getenv("ESPN_TEAM_ID")
    if our_team_id is None or str(team_id) != str(our_team_id):
        return {"unavailable": "waiver recommendations are only computed for the bot's own team"}
    try:
        result = compute_waiver_recommendations(season, week)
    except WaiverInputsUnavailableError as exc:
        return {"unavailable": str(exc)}
    return {
        "season": season,
        "week": week,
        "source_mode": result.get("source_mode"),
        "source_weeks": result.get("source_weeks"),
        "open_roster_slots": result["open_roster_slots"],
        "open_ir_slots": result["open_ir_slots"],
        "recommendations": [
            {
                "kind": rec["kind"],
                "position": rec["position"],
                "vorp_delta": _round(rec["vorp_delta"]),
                "add": _summarize_waiver_player(rec["free_agent"]),
                "drop": _summarize_waiver_player(rec["bench_player"]),
                "move_to_ir": _summarize_waiver_player(rec["ir_player"]),
            }
            for rec in result["recommendations"]
        ],
        "ir_moves": [
            {"action": m["action"], "position": m["position"], "player": _summarize_waiver_player(m["player"])}
            for m in result["ir_moves"]
        ],
    }


def _build_prompt(
    question: str,
    team_id: str,
    team_roster: dict,
    roster_deficits: dict,
    trade_evaluation: Optional[dict],
    free_agents: Optional[dict] = None,
    waiver_recommendations: Optional[dict] = None,
) -> str:
    sections = [
        f'You are advising the manager of team_id "{team_id}".',
        f"Question: {question}",
        "The manager's own roster, player by player (starters + bench, with vorp_score/projected_points "
        f"for this week):\n{json.dumps(team_roster)}",
        f"League roster/VORP context (all teams):\n{json.dumps(roster_deficits)}",
    ]
    if trade_evaluation is not None:
        sections.append(f"Proposed trade evaluation:\n{json.dumps(trade_evaluation)}")
    if free_agents is not None:
        sections.append(
            "Free agents (unrostered players, top by vorp_score per position; a null "
            f"vorp_score/projected_points means a bye or no data):\n{json.dumps(free_agents)}"
        )
    if waiver_recommendations is not None:
        sections.append(
            "Current waiver recommendations for this team (kind \"add\" fills an open roster slot "
            "with no drop; \"swap\" drops the listed bench player; \"ir_and_add\" moves a player "
            f"to IR, then adds):\n{json.dumps(waiver_recommendations)}"
        )
    return "\n\n".join(sections)


def answer_chat_query(
    question: str,
    season: int,
    week: int,
    team_id: str,
    trade_proposal: Optional[TradeProposal] = None,
    session_id: Optional[str] = None,
) -> dict:
    from src.services.activity_service import session_start, session_event, sessions
    from src.services.llm_history import current_session, observe

    previous = sessions(session_id) if session_id else []
    if session_id and not previous:
        raise ValueError("Session not found in this league.")
    history = (
        [m for m in previous[0]["content"] if m["role"] in {"user", "answer"}]
        if previous
        else []
    )
    identity = session_start(question, identity=session_id)
    session_event(identity, "user", question, "running")
    token = current_session.set(identity)
    try:
        observe("stage", "Fetching current roster context")
        # Only the calling team's roster at player level -- every team's full roster would bloat
        # the prompt for little benefit; league-wide comparisons are covered by the deficits.
        team_roster = get_team_roster_players(team_id, season, week)
        roster_deficits = compute_roster_deficits(season, week)

        trade_evaluation = None
        if trade_proposal is not None:
            # A malformed proposal (duplicate/overlapping ids) raises ValueError here -- propagates
            # to the router, which maps it the same way src/api/trade.py already does.
            trade_evaluation = evaluate_trade(
                trade_proposal.incoming_player_ids,
                trade_proposal.outgoing_player_ids,
                season,
                week,
            )

        free_agents = None
        waiver_recommendations = None
        if _is_waiver_question(question, history):
            observe("stage", "Fetching free agents and waiver recommendations")
            free_agents = _compact_free_agents(season, week)
            waiver_recommendations = _compact_waiver_recommendations(team_id, season, week)

        grounding = {
            "team_roster": team_roster,
            "roster_deficits": roster_deficits,
            "trade_evaluation": trade_evaluation,
            "free_agents": free_agents,
            "waiver_recommendations": waiver_recommendations,
        }
        observe("tool_result", grounding)
        prompt = _build_prompt(
            question, team_id, team_roster, roster_deficits, trade_evaluation,
            free_agents, waiver_recommendations,
        )
        if history:
            prompt += (
                "\nPrevious conversation (context, not current roster facts):\n"
                + json.dumps(history[-12:])
            )
        result = complete_with_fallback(
            prompt,
            system_prompt=SYSTEM_PROMPT,
            call_site="chat_query",
            providers=_chat_provider_chain(),
        )

        session_event(identity, "answer", result.text, "completed")
        return {
            "session_id": identity,
            "answer": result.text,
            "provider": result.provider,
            "model": result.model,
            "latency_ms": result.latency_ms,
            "grounding": grounding,
        }

    except Exception as exc:
        observe("error", str(exc), "failed")
        raise
    finally:
        current_session.reset(token)
