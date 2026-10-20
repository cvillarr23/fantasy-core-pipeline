"""Regenerate docs/openapi.json and the endpoint tables in docs/api-reference.md.

Run from the repo root:

    DB_PASSWORD=test python scripts/export_openapi.py          # rewrite both files
    DB_PASSWORD=test python scripts/export_openapi.py --check  # exit 1 if either is stale

Importing the app opens a database pool, so this stubs the pool the same way
tests/conftest.py does; no Postgres or broker is needed. Every route needs an entry in
DESCRIPTIONS, so adding an endpoint without documenting it fails ``--check`` (and the test
that runs it).
"""

import argparse
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import psycopg2.pool

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
psycopg2.pool.ThreadedConnectionPool = MagicMock(return_value=MagicMock())
os.environ.setdefault("DB_PASSWORD", "test")
os.environ.pop("OTEL_EXPORTER_OTLP_ENDPOINT", None)

OPENAPI_PATH = ROOT / "docs" / "openapi.json"
REFERENCE_PATH = ROOT / "docs" / "api-reference.md"
START = "<!-- BEGIN GENERATED: endpoints -->"
END = "<!-- END GENERATED: endpoints -->"

# Area -> (heading, path prefixes). First match wins, so list specific prefixes first.
AREAS = [
    ("Triggers", "Trigger", ("/v1/trigger",)),
    ("Trade", "Trade", ("/v1/trade",)),
    ("Draft", "Draft day", ("/v1/draft",)),
    ("Activity", "Activity", ("/v1/activity",)),
    ("Roster and waivers", "Roster and waivers", ("/v1/roster", "/v1/waiver")),
    (
        "League and matchup",
        "League and matchup",
        ("/v1/league", "/v1/matchup", "/v1/scoreboard"),
    ),
    ("Players and stats", "Players and stats", ("/v1/players", "/v1/stats")),
    ("Backtests", "Backtests", ("/v1/backtest",)),
    ("Other", "Chat and health", ("/v1/chat", "/v1/health")),
]

# Queue each endpoint publishes to, checked against src/api/routes.py,
# src/services/trade_offer_service.py and src/services/trade_inbox_service.py.
QUEUES = {
    "POST /v1/trigger/lineup": "lineup_updates",
    "POST /v1/trigger/waiver": "waiver_actions",
    "POST /v1/trigger/draft-pick": "draft_targets",
    "POST /v1/trade/offers/{offer_key}/approve": "trade_submissions",
    "POST /v1/trade/incoming/{espn_transaction_id}/decision": "trade_responses",
    "POST /v1/trade/incoming/{espn_transaction_id}/decision/retry": "trade_responses",
    "POST /v1/trade/incoming/{espn_transaction_id}/counter": "trade_negotiations",
    "POST /v1/trigger/rumor-action": "lineup_updates, waiver_actions",
}

DESCRIPTIONS = {
    "POST /v1/trigger/lineup": "Queue lineup swaps and IR moves.",
    "POST /v1/trigger/waiver": "Queue a waiver claim, optionally held behind an IR move.",
    "POST /v1/trigger/draft-pick": "Queue the recommended draft pick.",
    "POST /v1/trigger/rumor-action": "Evaluate rumor signals now and queue the resulting moves.",
    "GET /v1/health": "Check Postgres and RabbitMQ reachability.",
    "GET /v1/trade/incoming": "List incoming trade offers with scores and negotiation state.",
    "GET /v1/trade/incoming/{espn_transaction_id}/thread": "Show one offer's negotiation thread.",
    "POST /v1/trade/incoming/{espn_transaction_id}/decision": "Queue an accept or decline for an incoming offer.",
    "POST /v1/trade/incoming/{espn_transaction_id}/decision/retry": "Queue the same response again after a rejected or unknown attempt.",
    "POST /v1/trade/incoming/{espn_transaction_id}/counter": "Queue a counter offer for an incoming offer.",
    "POST /v1/trade/incoming/{espn_transaction_id}/triage": "Dismiss or snooze an incoming offer.",
    "GET /v1/trade/offers": "List generated trade proposals with their stored score evidence.",
    "POST /v1/trade/offers/{offer_key}/approve": "Approve a staged proposal and queue it.",
    "POST /v1/trade/evaluate": "Compute raw VORP delta for a trade.",
    "POST /v1/trade/score": "Compute the schedule-aware trade score.",
    "GET /v1/trade/context": "Show current rosters and head-to-head schedule evidence for trades.",
    "GET /v1/trade/candidates": "List the latest precomputed league-wide trade candidates.",
    "GET /v1/draft/config": "Read the stored live draft config.",
    "POST /v1/draft/config": "Store the league, team, season and mode for the draft listener.",
    "POST /v1/draft/start": "Create the draft-sync Kubernetes Job.",
    "POST /v1/draft/stop": "Delete a draft-sync Job and its Redis state.",
    "GET /v1/draft/status": "Show draft board, Job and listener state.",
    "GET /v1/draft/stream": "Stream draft status as server-sent events.",
    "POST /v1/draft/pause": "Pause the auto-publish for a pick.",
    "POST /v1/draft/resume": "Recompute and republish the recommendation.",
    "POST /v1/draft/auto-publish": "Turn draft auto-publish on or off.",
    "GET /v1/draft/logs": "Tail the draft-sync pod log.",
    "POST /v1/draft/practice/sync-job": "Create a draft-sync Job for a practice room.",
    "GET /v1/draft/practice/sync-job/status": "Show the practice Job's state.",
    "DELETE /v1/draft/practice/sync-job": "Delete the practice Job and its Redis state.",
    "GET /v1/activity/actions": "List queued actions with their delivery and verification state.",
    "GET /v1/activity/actions/{identity}": "Show one action with its event timeline.",
    "POST /v1/activity/actions/{identity}/check": "Re-verify an action against ESPN now.",
    "GET /v1/activity/sessions": "List bot sessions.",
    "GET /v1/activity/sessions/{identity}": "Show one bot session.",
    "POST /v1/activity/read/{identity}": "Mark an activity item read.",
    "GET /v1/activity/bots": "Summarize each bot's recent activity.",
    "GET /v1/activity/runs": "List job and bot runs (audit log), newest first.",
    "GET /v1/activity/runs/{identity}": "Show one run with its trigger, inputs, outputs and child runs.",
    "GET /v1/activity/jobs": "Latest run and recent failure count per job, plus dead-letter queue depths.",
    "GET /v1/activity/trace": "Trace an article, rumor evaluation or move through the rumor pipeline.",
    "GET /v1/roster/deficits": "Show per-position roster deficits.",
    "GET /v1/roster/bye-outlook": "Show which upcoming weeks NFL byes leave the lineup short.",
    "POST /v1/roster/move-preview": "Preview the effect of a roster move.",
    "GET /v1/waiver/recommendations": "Recommend waiver adds and drops.",
    "GET /v1/waiver/free-agents": "Browse all claimable free agents with metrics and same-position swap values.",
    "GET /v1/league/teams": "List league teams.",
    "GET /v1/league/current-week": "Show the current NFL week.",
    "GET /v1/league/standings": "Show league standings.",
    "GET /v1/league/player-leaders": "Show top scorers.",
    "GET /v1/matchup": "Show our matchup for a season and week.",
    "GET /v1/matchup/roster": "Show a matchup roster with swap and IR suggestions.",
    "GET /v1/matchup/scoreboard": "Show every matchup's score for a season and week.",
    "GET /v1/scoreboard": "Same as `/v1/matchup/scoreboard`.",
    "GET /v1/players/search": "Search players by name.",
    "GET /v1/players/{player_id}/summary": "Show one player's season summary.",
    "GET /v1/stats/vorp": "List VORP scores.",
    "GET /v1/backtest/results": "List backtest runs.",
    "GET /v1/backtest/results/{season}": "Show the backtest for a season.",
    "POST /v1/chat/query": "Ask the LLM a roster or waiver question.",
}


def load_openapi() -> dict:
    from src.api.main import app

    return app.openapi()


def operations(schema: dict):
    for path, item in schema["paths"].items():
        for method, op in item.items():
            yield method.upper(), path, op


def area_of(path: str) -> int:
    for index, (_, _, prefixes) in enumerate(AREAS):
        if path.startswith(prefixes):
            return index
    raise SystemExit(f"{path} matches no area in scripts/export_openapi.py")


def render_endpoints(schema: dict) -> str:
    rows = {i: [] for i in range(len(AREAS))}
    missing = []
    for method, path, op in operations(schema):
        label = f"{method} {path}"
        if label not in DESCRIPTIONS:
            missing.append(label)
            continue
        needs_key = any(
            p["name"].lower() == "x-api-key" for p in op.get("parameters", [])
        )
        rows[area_of(path)].append(
            (
                path,
                method,
                DESCRIPTIONS[label],
                "Yes" if needs_key else "No",
                QUEUES.get(label, ""),
            )
        )
    if missing:
        raise SystemExit(
            "Add DESCRIPTIONS entries in scripts/export_openapi.py for: "
            + ", ".join(missing)
        )
    stale = set(DESCRIPTIONS) - {f"{m} {p}" for m, p, _ in operations(schema)}
    if stale:
        raise SystemExit("Remove stale DESCRIPTIONS entries: " + ", ".join(sorted(stale)))

    out = []
    for index, (_, heading, _) in enumerate(AREAS):
        if not rows[index]:
            continue
        out.append(f"### {heading}\n")
        out.append("| Method | Path | Purpose | `X-API-Key` | Publishes to |")
        out.append("|---|---|---|---|---|")
        for path, method, purpose, key, queue in sorted(rows[index]):
            queue_cell = ", ".join(f"`{q.strip()}`" for q in queue.split(",")) if queue else ""
            out.append(f"| `{method}` | `{path}` | {purpose} | {key} | {queue_cell} |")
        out.append("")
    return "\n".join(out)


def build() -> tuple[str, str]:
    schema = load_openapi()
    openapi_text = json.dumps(schema, indent=2, sort_keys=True) + "\n"
    reference = REFERENCE_PATH.read_text()
    head, _, rest = reference.partition(START)
    _, _, tail = rest.partition(END)
    if START not in reference or END not in rest:
        raise SystemExit(f"{REFERENCE_PATH} is missing the generated-section markers")
    new_reference = f"{head}{START}\n\n{render_endpoints(schema)}\n{END}{tail}"
    return openapi_text, new_reference


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if generated files are stale")
    args = parser.parse_args()
    openapi_text, reference = build()
    current = (
        OPENAPI_PATH.read_text() if OPENAPI_PATH.exists() else None,
        REFERENCE_PATH.read_text(),
    )
    if args.check:
        if current != (openapi_text, reference):
            print("Generated API docs are stale. Run: python scripts/export_openapi.py", file=sys.stderr)
            return 1
        return 0
    OPENAPI_PATH.write_text(openapi_text)
    REFERENCE_PATH.write_text(reference)
    return 0


if __name__ == "__main__":
    sys.exit(main())
