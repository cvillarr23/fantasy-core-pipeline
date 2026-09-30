# fantasy-core-pipeline

## Testing

- Tests live in `tests/`, using `pytest` + FastAPI's `TestClient`.
- `src/database.py` opens a real `ThreadedConnectionPool` at **import time** — importing
  `src.database` (or anything that imports it, e.g. `src.api.routes`) requires a reachable
  Postgres unless the pool is stubbed first. `tests/conftest.py` patches
  `psycopg2.pool.ThreadedConnectionPool` before test collection so the whole suite runs
  without a live database or broker — keep that patch in place (or update it) if
  `src/database.py`'s pool construction changes.
- `src/api/main.py`'s startup event calls `run_migrations()`, which also needs a real DB.
  Tests that build a `TestClient` must patch `src.api.main.run_migrations` first (see the
  `client` fixture in `tests/test_routes.py`) — patch it at the `src.api.main` import site,
  not `src.migrations`, since `main.py` imports the function by reference.
- Mock `pika.BlockingConnection` (patch `src.api.routes.pika.BlockingConnection`) for any
  route that touches RabbitMQ — no local broker is assumed to be running.
- Run the suite with:
  ```bash
  DB_PASSWORD=test pytest tests/
  ```
  (`DB_PASSWORD` just silences a startup warning in `src/database.py`; no real credential
  is used since the pool is mocked.)
- This repo's pinned `requirements.txt` versions target the `python:3.11-slim` image used
  in `Dockerfile`. On newer local Python versions (e.g. 3.13+), some pins (`pydantic-core`,
  `psycopg2-binary`) may fail to build from source — install unpinned/latest versions in a
  local venv for running tests rather than editing the pins.
- `tests/conftest.py` sets `TRIGGER_API_KEY` at module scope (not inside the `client`
  fixture) since `tests/test_routes.py` imports `src.api.routes` — which reads
  `TRIGGER_API_KEY` at import time — before any fixture runs.

## API layer conventions

- `src/api/*.py` routers must be thin HTTP controllers: parse request params, call into
  `src/services/<domain>_service.py`, map service-raised exceptions to HTTP status codes,
  and shape the response via `src/schemas/<domain>.py` Pydantic models. They must not run
  SQL or other business logic directly. Pattern: `src/api/trade.py` +
  `src/services/trade_service.py` (simplest example); also
  `src/services/roster_service.py`, `src/services/matchup_service.py`,
  `src/services/league_service.py`, `src/services/backtest_service.py`,
  `src/services/players_service.py`, `src/services/stats_service.py`.
- A service exists so the same logic is reusable by both the REST router and an MCP tool
  in `src/api/mcp_server.py` where applicable — see `roster_service`/`trade_service`'s
  module docstrings. Services raise domain-specific exceptions for "expected" not-found
  conditions (e.g. `NoMatchupFoundError`, `BacktestResultNotFoundError`,
  `PlayerNotFoundError`) instead of returning `None` for the router to check.
- Pydantic request/response models live in `src/schemas/<domain>.py`, not inline in the
  router. **Do not confuse this with `src/models.py`**, which holds only raw DB DDL string
  constants consumed by `run_migrations()` and has nothing to do with the API layer — a
  `src/models/` package can't coexist with the `src/models.py` module, which is why
  Pydantic models live under `src/schemas/` instead.
- When a route handler's natural Python function name would collide with its service
  function's name, suffix the route handler `_endpoint` rather than renaming the service
  function — e.g. `trade_service.evaluate_trade` (service) vs. `evaluate_trade_endpoint`
  (route, in `src/api/trade.py`).
- Tests mock `get_db_cursor` at the **service's** import site
  (`patch("src.services.<domain>_service.get_db_cursor", ...)`), not the router's — true
  for both the dedicated `tests/test_<domain>_service.py` unit tests and the
  `tests/test_<domain>_routes.py` end-to-end route tests, which keep exercising the real
  service and just mock the DB underneath it.
- Known follow-up, not yet migrated to this pattern: `src/api/routes.py` (AMQP publish +
  Redis idempotency logic inline in the `/trigger/*` handlers) and `src/api/draft.py`
  (Kubernetes Job orchestration, Redis draft state, and SQL all inline) — both mix in
  non-SQL orchestration concerns, not just SQL-behind-a-service, so migrating them needs
  its own design pass rather than a mechanical extraction. Tracked as separate issues.
- `src/api/routes.py`'s three `/v1/trigger/*` routes (`lineup`, `waiver`, `draft-pick`)
  require an `X-API-Key` header (`require_trigger_api_key` dependency), checked against
  `TRIGGER_API_KEY` — OpenBao-injected in deployed environments, no default (issue #141:
  these routes cause real ESPN side effects and are called in-cluster by CronJobs and
  `fantasy-browser-worker`, bypassing the `oauth2-proxy` in front of `fantasy-web-ui`).
  `src/optimization/lineup_solver.py`, `draft_matrix.py`, and `waiver_evaluator.py` each
  read their own `TRIGGER_API_KEY` env var and set the header before posting.

## CORE-05: Dynamic VORP matrix

- `src/ingestion/sync_league.py` requires `ESPN_LEAGUE_ID`, `ESPN_S2`, and `ESPN_SWID`
  (sourced from OpenBao in deployed environments) — no defaults, and no credentials are
  committed to this repo.
- Ingestion job order matters: `sync_players` → `sync_id_crosswalk` → `sync_league` →
  `sync_kicker_dst` → `sync_skill_position_crosswalk` → `sync_projections` →
  `metrics_extractor` → `vorp_matrix`. `sync_id_crosswalk` must run after `sync_players`
  (it updates existing player rows) and before `sync_league` (roster matching needs
  `players.espn_id`) and `metrics_extractor` (needs a fresh `gsis_id`). `sync_kicker_dst`
  (#83) backfills `espn_id` and `weekly_stats.projected_points` for K/D-ST directly from
  ESPN, since `sync_id_crosswalk`'s DynastyProcess crosswalk has zero coverage for either
  position and Sleeper has no projections endpoint for them; it needs `sync_players`'s
  K/D-ST rows to match against and reuses `sync_league.build_league`'s ESPN connection, so
  it slots in right after `sync_league`.
- `sync_skill_position_crosswalk` (#150) backfills `players.espn_id` for the ~18% of
  QB/RB/WR/TE players `sync_id_crosswalk`'s DynastyProcess crosswalk still misses (see
  `src/models.py`'s coverage note). Unlike `sync_kicker_dst`, it can't use
  `League.free_agents()` — that only returns unowned players, which was fine for #83
  because it ran pre-draft; post-draft, most unmatched skill players are rostered
  somewhere. So it queries the raw `kona_player_info` view with no `filterStatus`
  ownership filter instead, matching by (name, team) with a high-cutoff `rapidfuzz`
  fallback for stale `players.team` values.

## CORE-06: PuLP lineup optimizer

- `src/optimization/lineup_solver.py` must run after `vorp_matrix` (CORE-05) for a given
  season/week — its objective function is each player's `weekly_stats.vorp_score`, which
  only CORE-05 populates. A week with no VORP computed yet just falls back to `0.0` per
  player (via `COALESCE`) rather than crashing, but that makes every player look equally
  replacement-level, so the lineup it produces won't be meaningful until `vorp_matrix` has
  actually run for that season/week.
- `weekly_stats` is keyed by `(player_id, season, week)`, not `week` alone — always pass
  both; `--week` without `--season` on the CLI (or filtering the roster-pool query by week
  only) can silently pick up a stale row from a different season once more than one season
  exists in the table.
- Players with `injury_status = 'Out'` are excluded from the optimizer's decision
  variables entirely; `Questionable`/`Doubtful` remain eligible to start (real judgment
  calls, not hard exclusions). In `load_roster_pool` (exact-week, used by backtests,
  `matchup_service`, `rumor_action_service`) a bye-week player simply has no `weekly_stats`
  row for that week, so the roster/stats join drops them.
- The scheduled run (`optimize_weekly_lineup`) evaluates an **upcoming** week (#178) via
  `load_pregame_roster_pool`, not `load_roster_pool`. Source order, always within the
  target season: (1) the target week's own `vorp_score` if any rostered player has one
  (pregame Sleeper projections written by `fantasy-draft-prep-refresher`, 05:00 CT daily);
  otherwise (2) each player's mean `vorp_score` over the up-to-3 prior weeks of the same
  season. Week 1 never reads the prior season. Mode (2) needs the newest usable week to be
  `target - 1` (else "stale"), and takes byes from the nflverse schedule
  (`load_teams_playing`), since prior-week rows say nothing about the target week. The
  source (`source_mode`, `source_season`, `source_weeks`, `held_slots`, plus `season`/`week`)
  is logged and merged into each swap's `decision_inputs` → `bot_decisions`.
- K and D/ST normally have no `vorp_score` (Sleeper projections are QB/RB/WR/TE only;
  `sync_kicker_dst` is free-agent-only and unscheduled). A `HOLDABLE_WITHOUT_METRICS` slot
  no pool player can fill is **held**: removed from the ILP, and its current starter is
  left out of the diff so it's never dropped. An unfillable skill slot is an error.
- Exit codes: `main()` returns 0 for a real decision (swaps published *or* lineup already
  optimal) and 1 for `InsufficientLineupInputError` (no roster, no usable/stale metrics,
  schedule unavailable in mode (2), infeasible solve), so the CronJob's Job fails and
  alerts (fantasy-infra#97). Publish/HTTP errors still raise. `--dry-run` computes and logs
  swaps without calling `/v1/trigger/lineup`.
- `metrics_extractor`'s CLI defaults to the last **completed** regular-season week (every
  game final, from the nflverse schedule), not nflreadpy's current week, which is already
  the upcoming, unplayed week by the Tuesday CronJob. Writing 0 rows for a resolved week
  exits 1; preseason (no completed week yet) exits 0.
- The solver does not publish to RabbitMQ directly. It POSTs to the existing
  `POST /v1/trigger/lineup` FastAPI route (`src/api/routes.py`, mounted under the `/v1`
  prefix in `src/api/main.py` — `CORE_API_BASE_URL` is just the host, the `/v1` prefix is
  appended at the call site), which publishes to the
  `lineup_updates` queue — this matches the `optimization → api → mq` flow documented in
  `README.md`'s architecture diagram instead of duplicating publish logic. The route's base
  URL is `CORE_API_BASE_URL`, defaulting to `http://fantasy-api.core.svc.cluster.local:8000`
  in code — **that default is stale**: `fantasy-infra` actually names the FastAPI k8s
  Service `fantasy-core-pipeline`, not `fantasy-api`, confirmed against
  `base/*/fastapi-deployment.yaml` in that repo. The env var default in `lineup_solver.py`
  and `draft_matrix.py` should be corrected (or always overridden via the deployment
  manifest) — tracked as a follow-up, not fixed in this pass since it touches deploy config
  outside this repo.

## CORE-08: Live draft-day board matrix

- `src/optimization/draft_matrix.py` replaced the earlier greedy-heuristic stub of the same
  name — it's now a long-running listener (`python -m src.optimization.draft_matrix
  --league-id --team-id --season [--week] [--auto-draft-delay-seconds]`), not a batch job,
  meant to run for the few hours of the actual draft.
- Consumes fantasy-browser-worker's `draft_picks:{leagueId}` **Redis Stream** via
  `XREADGROUP` (consumer group `draft_matrix`), not the plain `draft_picks` pub/sub channel
  the same worker also publishes to — a missed pub/sub message would silently corrupt the
  undrafted pool for the rest of the draft, whereas the Stream can resume from the last
  acked entry after a restart. `REDIS_HOST`/`REDIS_PORT` follow the same convention as
  `sync_players.py` (default `fantasy-cache.default.svc.cluster.local:6379`).
- Every pick event is persisted to a new `draft_picks` table (`(league_id, season,
  pick_number)` PK). `persist_draft_pick` uses `ON CONFLICT DO NOTHING ... RETURNING` and
  returns whether it actually inserted — `handle_pick_event` short-circuits (no recompute,
  no recommendation, no scheduled publish) when it didn't, because a Stream consumer group
  redelivers an entry if the process crashes before `XACK`, and reprocessing an
  already-recorded pick must be a true no-op, not just a suppressed insert that still
  re-triggers a duplicate auto-draft.
- Scarcity-adjusted VORP is **not** recomputed with new math — `recompute_scarcity_vorp`
  calls CORE-05's `vorp_matrix` functions directly against the shrinking undrafted pool.
  This is exactly `vorp_matrix`'s pre-draft/no-rosters-yet branch (real `league_rosters`
  rows don't exist until the regular season starts syncing from ESPN, long after the draft
  is over), just re-invoked once per pick instead of once per week.
- Per-position draft caps and "do we still need a starter here" checks are **derived from
  `league_settings.lineup_slot_counts`** (`compute_position_caps`), not the hardcoded
  QB≥1/RB≥3/WR≥3/TE≥1 thresholds from the original issue text or the old stub — so they stay
  correct if this league's roster construction ever changes. Bench slots are distributed
  across RB/WR/TE using the same `FLEX_DEFAULT_WEIGHTS` `vorp_matrix` uses for its own FLEX
  allocation; QB has no bench/FLEX share, so its cap is always exactly its starting need.
  `compute_draftable_roster_size` takes bench capacity from `lineup_slot_counts['BE']`
  specifically (falling back to `DEFAULT_ROSTER_SIZE` only when no league is synced yet) —
  summing every `lineup_slot_counts` value would double as "bench" the `IR` slot(s) this
  league's real settings also carry, which are never filled during the initial draft.
- The ILP's total-picks constraint is an **equality** (`== target_picks`), not `<=`.
  `vorp_score` is intentionally signed, so a `<=` budget constraint would let the solver
  maximize by selecting nobody once every remaining candidate is at/below replacement —
  correct as pure optimization, wrong for a draft, which still forces a pick every round
  regardless of how thin the pool has gotten. `target_picks` is `min(remaining_picks,
  pool size, fillable)`, where `fillable = sum(min(cap, pool_count) for pos in
  VORP_POSITIONS)` — a straight `sum of position caps` (the original formula) overstates
  what's actually achievable whenever a capped position (K/D-ST, capped at 1 each with no
  bench share — see #83) has fewer candidates left in the pool than its cap, which made
  CBC report the whole ILP infeasible instead of returning the picks that are genuinely
  available (#86).
- Publishing is **full autopilot with a cancellable delay window**, not instant, and it
  runs on a **background thread**, not inline in the Stream consumer loop — `handle_pick_event`
  only computes/persists/writes the recommendation to the `draft:{league_id}:board` Redis
  hash and returns immediately; `run_draft_listener` hands the actual wait to
  `_schedule_publish`, which spawns `hold_and_publish` on a `threading.Thread` so
  `XREADGROUP` keeps consuming new picks through the delay window instead of stalling.
  Each new opponent pick's `_schedule_publish` call sets a `threading.Event` that
  supersedes (cancels) any still-pending prior publish — only the freshest recommendation
  can ever actually fire. `wait_for_pause_or_timeout` checks the pause key/cancel event
  before every one-second sleep **and once more immediately after the last one** (a
  pause/cancel landing in that final window would otherwise be missed and the target
  published anyway). If uninterrupted, it POSTs to `POST /v1/trigger/draft-pick` (same
  shape as `/trigger/lineup`: Pydantic payload → `tracking_id` → `pika.BlockingConnection`
  → durable `queue_declare` → `basic_publish`), which publishes `{team_id, player_id,
  name}` to a **new `draft_targets` queue** — `draft_picks` was already taken by the Redis
  channel name. `player_id` here is `players.player_id` (Sleeper-namespaced, per
  `sync_id_crosswalk.py`) — **not** `players.espn_id`, a separate column; don't conflate
  the two when a consumer needs to resolve this ID against ESPN. `fantasy-browser-worker`'s
  `draft_clicker.js` already expects this `{player_id, name}` shape but isn't wired to
  consume the queue yet (WORKER-06, marked as a planned/dashed edge in `README.md`'s
  diagram, not a live one), and `fantasy-infra` hasn't declared `draft_targets` in
  `fantasy-broker-definitions` or added a KEDA scaler for it (INFRA-07) — both are
  follow-ups blocked *on* this work, not done here.
- On startup, `run_draft_listener` first drains this exact consumer name's own pending
  backlog (`XREADGROUP ... id="0"`) before switching to new entries (`id=">"`) — reading
  only `">"` would treat anything delivered-but-never-`XACK`'d before a crash as gone
  forever, defeating the whole point of using a consumer group. **Scope note**: this only
  recovers this listener's own backlog after its own restart, not another dead consumer's
  entries via `XAUTOCLAIM` — out of scope for a single-team listener (one consumer name per
  team per league).
- **Known limitation**: `handle_pick_event` has no real snake-draft turn-order tracking —
  the worker's event schema (`player_id`/`pick_number`/`team_id`/`round`) carries no "who's
  up next" data, and deriving it needs each team's draft-slot assignment, which isn't
  modeled anywhere yet. It currently recomputes/schedules a publish after *every* opponent
  pick (skipped only when the event's `team_id` is our own or the event is a replay), not
  specifically "the pick right before our turn" — harmless for recommendation freshness
  since each new schedule supersedes the last (see above), so only the most recent one ever
  publishes. Tracking real turn order is unimplemented, not a bug to work around.

## CORE-12: OpenTelemetry tracing

- `src/telemetry.py`'s `configure_tracing()` is a true no-op (no span processor attached
  at all, not a `ConsoleSpanExporter`) when `OTEL_EXPORTER_OTLP_ENDPOINT` is unset — local
  dev and the test suite never need a live Tempo collector and never print span JSON to
  stdout. Set `OTEL_CONSOLE_EXPORT=1` to opt into console span output for local debugging
  without a live collector.
- The OTLP exporter is **HTTP/protobuf** (`opentelemetry-exporter-otlp-proto-http`), not
  gRPC — matches this repo's existing httpx/requests-only stack, no `grpcio` dependency
  added. This means `OTEL_EXPORTER_OTLP_ENDPOINT` must point at Tempo's `:4318` HTTP port,
  not `:4317` (gRPC) — the opposite of the "primary" convention most OTel docs assume.
  `configure_tracing()` appends `/v1/traces` to the endpoint itself; set the env var to
  the bare `http://host:4318`, not the full traces path.
- `tracking_id` (UUID in the AMQP JSON body, human/log-facing, unchanged by this ticket)
  and the W3C `traceparent` (injected into `pika.BasicProperties(headers=...)` via
  `opentelemetry.propagate.inject()`, machine-facing) are two separate, deliberately
  non-unified identifiers in `src/api/routes.py` — don't conflate them. Consuming the
  `traceparent` header to link a `fantasy-browser-worker` span into this trace is
  WORKER-09, not implemented in this repo.
- `draft_matrix.py`'s `hold_and_publish` runs on a daemon `threading.Thread` spawned by
  `_schedule_publish`, and OTel span context does **not** cross OS thread boundaries
  automatically (unlike asyncio tasks). `_schedule_publish` must capture
  `context.get_current()` before spawning and pass it through; `hold_and_publish` must
  `context.attach()` it as its first action. Forgetting this produces no error — it just
  silently starts a disconnected trace, so treat any change to this thread-handoff
  signature with extra care.
- `FastAPIInstrumentor.instrument_app(app)` (`src/api/main.py`) is called as the very
  last line of the module, after `app.mount("/", mcp_asgi_app)` — instrumenting before
  the mount risks the `/mcp` routes showing up as an unlabeled catch-all span instead of
  getting accurate `http.route` templating.
- Wiring `OTEL_EXPORTER_OTLP_ENDPOINT` into actual k8s Deployment/CronJob manifests is
  `fantasy-infra`'s job (INFRA-10, already closed on their end) — not fixed in this repo,
  same follow-up pattern as the `CORE_API_BASE_URL` stale-default note above.
