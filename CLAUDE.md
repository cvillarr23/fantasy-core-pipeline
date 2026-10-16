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

- Adding or changing a route means regenerating `docs/openapi.json` and the endpoint tables in
  `docs/api-reference.md` (`DB_PASSWORD=test python scripts/export_openapi.py`) and adding a
  one-line entry for the route in that script's `DESCRIPTIONS`; `tests/test_api_docs.py` fails
  otherwise. Schema or queue changes also need `docs/data-model.md` / `docs/queue-publishing.md`.
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
- `chat_service.answer_chat_query` grounds the LLM in `roster_service.get_team_roster_players`
  (the calling team's own roster, player by player, with that week's
  `vorp_score`/`projected_points`) plus `compute_roster_deficits` (league-wide per-position
  aggregates). Other teams' full rosters are deliberately left out to keep the prompt small.
  `llm_client` has no tool calling, so chat calls these in process; the same function backs
  the `get_team_roster` MCP tool for remote callers.
  A question (or the previous one, for follow-ups) matching `WAIVER_QUESTION_PATTERN` also
  gets the top 5 free agents per position (`free_agent_service.browse_free_agents`, also the
  `get_free_agents` MCP tool) and compact `compute_waiver_recommendations` output. The
  latter plans only for `ESPN_TEAM_ID`, so another `team_id` gets `{"unavailable": ...}`, as
  does `WaiverInputsUnavailableError`; other questions skip both fetches.
- `trade_score_service` (`POST /v1/trade/score`, MCP `score_trade_proposal`) is the
  schedule-aware trade scorer, kept separate from `evaluate_trade`, whose raw-VORP
  contract the worker still calls and which the scorer reuses as `raw_vorp`. Request ids
  are ESPN ids only. Ownership comes from the live trade context (409 on mismatch). Each
  week is scored only from its own `weekly_stats` rows, never carried forward. The formula
  constants live in the service docstring and the README "Trade scoring" section; change
  `SCORING_VERSION` if they change.
- `GET /v1/trade/offers` returns `evaluation` (`ProposalEvaluation` in `src/schemas/trade.py`,
  reusing `TradeScore`), the worker's stored score evidence for generated proposals. It is
  display-only: keep it out of `compute_offer_key`, `_validated_offer` and the approval
  payload, since approval binds what the other manager receives, not our reasoning. An
  unknown version or invalid payload becomes `null` with a warning instead of failing the list.
- The incoming inbox uses `trade_inbox_service` and `schemas/trade_inbox.py`;
  all four inbox writes require the existing `X-API-Key` guard. Core publication
  claims live in `outcome_details.incoming_publications`, separate from worker-owned
  inbox status and decision. Never set `responding` when queuing: the executor rejects
  that state. `queued` means broker-confirmed, not ESPN-processed. Decision, counter
  and retry all require the listed row's `review_token` (fingerprint, both sides, other
  items, the proposer's exact note), checked against the locked `FOR UPDATE` row before
  any other check, claim or broker call; score evidence stays out of it. `evaluation`
  (`IncomingEvaluation`) is the worker's full score request and response, display-only:
  it is null unless it validates and matches the offer fingerprint, the row's teams and
  sides, and the `score` summary's timestamp, version, week and final score. See
  `docs/trade-inbox-api.md` for recovery and the worker contract.
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
  it slots in right after `sync_league`. It is also scheduled daily in
  `fantasy-draft-prep-refresher` (fantasy-infra) between `sync_projections` and
  `vorp_matrix`, for the upcoming target week (#183).
- `sync_skill_position_crosswalk` (#150) backfills `players.espn_id` for the ~18% of
  QB/RB/WR/TE players `sync_id_crosswalk`'s DynastyProcess crosswalk still misses (see
  `src/models.py`'s coverage note). It can't use `League.free_agents()`, which only returns
  unowned players; post-draft, most unmatched skill players are rostered somewhere. So it
  queries the raw `kona_player_info` view with no `filterStatus` ownership filter instead,
  matching by (name, team) with a high-cutoff `rapidfuzz` fallback for stale
  `players.team` values. `sync_kicker_dst` originally used `free_agents()` (fine pre-draft)
  and now uses the same unfiltered query with `scoringPeriodId` set to the target week, so
  rostered K/D-ST get projections too (#183). Kickers match by existing `espn_id` first,
  then (name, team); D/ST by team. A matched player with no projection for that week (a
  bye, or a since-released kicker) gets **no** `weekly_stats` row, never a 0.0 one, because
  the pregame pool reads a missing row as "on bye"; a row an earlier daily run wrote for
  that week is deleted (only `sync_kicker_dst` writes K/D-ST rows).
- `sync_projections` follows the same rule for QB/RB/WR/TE: Sleeper lists a player whose
  team is on bye with a `team` but `opponent: null` and no scoring stats. That row is
  skipped, never stored as 0.0, and a 0.0 row an earlier run wrote for him is deleted
  (unless actual scores were synced).

## Bye weeks across recommenders

- `src/nfl_byes.py` is the only bye lookup (nflverse schedule, cached per season). Display
  paths use `bye_weeks_or_empty`, so an unreadable schedule leaves `bye_week` null rather
  than failing.
- Every recommender judges a roster move over a multi-week horizon, never one week's VORP,
  so a player on bye is never dropped or traded away for that one idle week:
  - Trades: the trade score.
  - Waivers: `waiver_strategy.build_horizon_pools` gives the average VORP per week, with a
    bye week counted as 0.0, which is replacement level.
  - Fill-ins: `waiver_bye_fill`.
- `roster_move_scoring` holds the shared per-week lineup comparison.
- `bye_outlook` flags "crunch" weeks. A week is a crunch when a bye leaves a starter seat
  empty, or forces more below-replacement starters than usual.
- The schedule beats the data: a player whose team is idle that week is on bye even if a
  `weekly_stats` row exists (ESPN projects a bye-week K/D-ST at 0, and Sleeper can carry a
  stale team). `roster_move_scoring.exclusion`, `build_horizon_pools` and
  `sync_kicker_dst` all apply this. A K/D-ST slot held only because its starter is on bye
  counts as an empty seat, so it can be a crunch.
- Fill-ins score an empty seat at `EMPTY_SEAT_VORP` (minus vorp_matrix's default
  replacement baseline), not 0.0: an empty seat scores zero points. So a below-replacement
  free agent still beats an empty seat. Up to 3 alternatives are listed per crunch week.
- Bye fill-ins are recommend-only. The live waiver cron uses the horizon values with its
  usual margin and never publishes a `bye_fill` move.

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
  calls, not hard exclusions). In `load_roster_pool` (exact-week, now used only by the
  backtests) a bye-week player simply has no `weekly_stats` row for that week, so the
  roster/stats join drops them.
- Live lineup decisions evaluate an **upcoming** week (#178) through one shared, non-publishing
  `evaluate_lineup()` (pregame pool → held slots → ILP → diff): the weekly
  `optimize_weekly_lineup`, `matchup_service.suggest_lineup_swaps` (display-only: on
  `InsufficientLineupInputError` it logs and shows no badges), and `rumor_action_service`'s
  bench_watch path (lets the error raise, like the optimizer). It uses
  `load_pregame_roster_pool`, not `load_roster_pool`. It leaves IR-slot players out of both
  its roster and metrics queries (#219): they can't start without an activation, but mode (2)
  would otherwise keep their prior-week VORP. Source order, always within the
  target season: (1) the target week's own `vorp_score` if any rostered **non-K/D-ST**
  player has one (pregame projections written by `fantasy-draft-prep-refresher`, 05:00 CT
  daily; K/D-ST rows alone don't count, since ESPN's can land before Sleeper's, #183);
  otherwise (2) each player's mean `vorp_score` over the up-to-3 prior weeks of the same
  season. Week 1 never reads the prior season. Mode (2) needs the newest usable week to be
  `target - 1` (else "stale"), and takes byes from the nflverse schedule
  (`load_teams_playing`), since prior-week rows say nothing about the target week. The
  source (`source_mode`, `source_season`, `source_weeks`, `held_slots`, plus `season`/`week`)
  is logged and merged into each swap's `decision_inputs` → `bot_decisions`.
- K and D/ST get their target-week `vorp_score` from `sync_kicker_dst`'s ESPN projections
  (Sleeper's are QB/RB/WR/TE only), run in the daily refresher before `vorp_matrix` (#183).
  When that data is missing, a `HOLDABLE_WITHOUT_METRICS` slot no pool player can fill is
  **held**: removed from the ILP, and its current starter is
  left out of the diff so it's never dropped. An unfillable skill slot is an error.
- A defaulted week comes from `src/nfl_schedule.py`: the earliest regular-season week of
  the requested season with a game not yet final (week 1 before kickoff; `None` once the
  regular season is over). The nflverse schedule read retries with backoff (6 attempts,
  5s doubling to 30s) and then raises `WeekResolutionError`; nothing ever substitutes
  another week. The lineup optimizer and rumor-action CLIs exit 1 on that error and 0
  (no-op) when the season is over. `lineup_solver.resolve_default_week()` is kept for
  fantasy-infra's refresher/waiver scripts: it raises the same way and returns
  `SEASON_OVER_WEEK` (19) for "season over", which the refresher reads as offseason.
- Exit codes: `main()` returns 0 for a real decision (swaps published *or* lineup already
  optimal) and 1 for `InsufficientLineupInputError` (no roster, no usable/stale metrics,
  schedule unavailable in mode (2), infeasible solve), so the CronJob's Job fails and
  alerts (fantasy-infra#97). Publish/HTTP errors still raise. `--dry-run` computes and logs
  swaps without calling `/v1/trigger/lineup`.
- `waiver_evaluator.load_live_waiver_inputs` (the waiver CronJob and rumor boost_waiver path)
  keeps `waiver_strategy`'s exact-week loaders, which are shared with backtesting, but raises
  `InsufficientWaiverInputError` when the bench or free-agent side is empty (target-week
  projections not landed yet); `waiver_evaluator.main()` then exits 1. Both Tuesday CronJobs
  (lineup, waivers) are scheduled after the 05:00 CT refresher in fantasy-infra, so the
  target week's projections normally exist.
- `waiver_service` (read-only `GET /v1/waiver/recommendations`) prefers the exact-week
  pools. When either side is empty (before the refresh), it uses
  `load_prior_week_waiver_pools`: the same same-season, up-to-3-prior-week mean, staleness
  gate and schedule bye exclusion as the lineup fallback (#185). Our bench plus unrostered
  players only. The response's `source_mode`/`source_weeks` say which source was used,
  and `WaiverInputsUnavailableError` maps to 503 rather than an empty list.
- IR suggestions (#215) are display-only unless `AUTO_PUBLISH_IR_MOVES` is on (#219, below).
  IR eligibility is ESPN's call, so it reads ESPN's
  `injuryStatus` (`src/ir_eligibility.py`), never `players.injury_status` (Sleeper's labels,
  refreshed daily): `league_rosters.espn_injury_status` (written by `sync_league`) for waivers,
  live `Team.roster` for `/v1/matchup/roster`. `IR_ELIGIBLE_ESPN_STATUSES` (`OUT`,
  `INJURY_RESERVE`) is not read from the league; it was confirmed by live slot-21 moves on
  2026-10-06 (an ACTIVE player got 409 `TRAN_ROSTER_INELIGIBLE_IR_NOT_INJURED`; see the
  worker's `docs/espn-reverse-engineering.md`, "IR moves"). Open IR slots = `lineup_slot_counts['IR']` minus
  `league_teams.ir_slots_used` (counted off ESPN's raw roster, since `league_rosters` drops
  players with no espn_id match). A player in an IR slot is never a waiver drop. While a slot is
  open, `waiver_strategy.plan_waiver_moves` never drops an IR-eligible player, wherever they
  rank: each stash frees a spot, filled first by the best free agents (`kind: "ir_and_add"`, no
  drop), and only the remaining positions are swapped against healthy bench players. Both live
  publishers (the waiver CronJob, rumor boost_waiver) use the same planner via
  `waiver_evaluator.plan_live_waiver_moves` and refresh rosters first. With the flag off they
  publish only the swaps and log `ir_and_add` at WARNING. The lineup roster adds per-line `ir_suggestion` (`move_to_ir` /
  `activate_from_ir`) and `ir_blocking_transactions` (an active player in IR, which makes ESPN
  reject new moves).
- `/v1/trigger/lineup` accepts IR entries alongside swaps (#223): `{kind: "ir", player_id,
  slot: "IR"}` and `{kind: "activate_ir", player_id, slot: <non-IR, normally BENCH>}`, with ESPN
  ids. The union uses a callable discriminator because pydantic v2 won't apply a defaulted `kind`.
  An entry without `kind` is a swap, and its published body is unchanged (`kind` is excluded from
  the swap dump). The gateway checks slot-vs-kind and the id namespace, and writes `bot_decisions`
  rows as `lineup_ir_move` / `lineup_ir_activate` with no drop. It does **not** pre-check
  eligibility: the worker reads ESPN live and rejects ineligible players, a full IR slot or "not
  in IR" permanently. `activity_runner.reconcile_action` verifies an IR entry by that one player's
  slot.
- `ir_and_add` auto-publish (#219) is gated by `AUTO_PUBLISH_IR_MOVES` (read per call, off unless
  `1`/`true`/`yes`). Flag off is an exact no-op. With it on, `plan_live_waiver_moves` returns the
  IR + add moves after the swaps (so a failed one can't hold the week's swaps back), unless one of
  our IR slots holds a player ESPN no longer lists as injured, or an occupant `ir_slots_used`
  counts but `league_rosters` couldn't match: then it logs ERROR and withholds them (activating
  is a manager's call, so `activate_from_ir` and `move_to_ir` stay suggestions with no
  auto-publisher).
  `publish_waiver_claim` hands a target with `ir_player_id` to `publish_ir_and_add`, which posts
  the IR move to `/v1/trigger/lineup` (week-scoped `lineup_idempotency_key`, no Job name), then the
  add-only claim to `/v1/trigger/waiver` with `after_tracking_id` = the IR move's tracking id (its
  own key, with `ir` in the material). It raises, sending no add, if the flag is off, season/week
  are missing, or the IR response has no tracking id. The gateway checks the parent (exists, same
  team, has a `kind: "ir"` entry, not already failed, via `activity_service.require_ir_move`; 503
  if it can't read it)
  and stores the claim as a **held** outbox row (`activity_outbox.depends_on`, `hold_until` =
  `ACTIVITY_DEPENDENCY_HOLD_SECONDS`, default 3600) without delivering it. `publish_one`'s own
  SELECT only releases a held row while its parent is `verified_complete` and `hold_until` hasn't
  passed, so no caller can bypass it. `activity_runner.settle_held_actions` (start of every tick)
  cancels a held row for good (`skipped`/`cancelled`, `dependency_failed` event, idempotency key
  released so a claim held on a fresh IR move can be queued) when the parent is skipped,
  rejected, cancelled, expired, permanently failed or needs manual verification, or when
  `hold_until` passes; a transient parent failure keeps it held. Held claims skip
  `create_action`'s legacy `bot_decisions` dedupe, which matches on add/drop alone. A replay
  whose IR move has since failed gets 409 (loud), not "duplicate". The worker treats an IR move
  for a player already in IR as satisfied, so a replay after the 30-minute lineup key lapses
  verifies at once. `after_tracking_id` is excluded
  from the queue body. `tests/test_activity_hold_postgres.py` checks the SQL against a local
  Postgres when `ACTIVITY_TEST_DSN` is set.
- `metrics_extractor`'s CLI defaults to the last **completed** regular-season week (every
  game final, from the nflverse schedule), not nflreadpy's current week, which is already
  the upcoming, unplayed week by the Tuesday CronJob. Writing 0 rows for a resolved week
  exits 1; preseason (no completed week yet) exits 0. Schedule scores and play-by-play are
  published separately, so before writing it checks (`missing_final_games`) that the week's
  raw pbp covers every `game_id` the schedule marks final. If any are missing, or the
  schedule can't be loaded to check, it raises `IncompletePlayByPlayError` and exits 1,
  naming the games, rather than writing a partial week (#184). A rerun fills the week,
  because the upsert is idempotent.
- Swaps are computed on internal `players.player_id` but **published with `players.espn_id`**
  (#187): `lineup_solver.resolve_lineup_swaps` translates both ids through the shared
  `src/espn_identity.resolve_espn_identity` (also used by `waiver_evaluator`) and adds
  `add_player_name`/`drop_player_name` for the gateway's namespace guard. It runs at publish
  time in both publishers (`optimize_weekly_lineup`, before the dry-run log too, and
  `rumor_action_service`'s bench_watch path, after matching on internal ids), never inside
  `evaluate_lineup`, whose internal ids the matchup badge and rumor matching rely on. A swap
  with no espn_id is skipped with a warning; if swaps were proposed but none resolve,
  `InsufficientLineupInputError` makes the Job exit 1, distinct from a zero-swap "already
  optimal" run. `/v1/trigger/lineup` runs `_reject_internal_player_id` on every swap id
  (names optional, since the web UI sends bare ESPN ids).
- Publishing is **retried and idempotent** (#189). `publish_lineup_swaps` sends an
  `idempotency_key` from `lineup_idempotency_key` (hash of `JOB_NAME` + team + season/week +
  the resolved swaps) through `trigger_client.post_trigger_idempotent`. That retries
  connection errors, timeouts and 5xx (6 attempts, 5s doubling to 30s, which outlasts an API
  rollout), never a 4xx. `/v1/trigger/lineup` reuses draft-pick's claim/mark/release helpers
  under the `lineup:` Redis namespace (`LINEUP_TRIGGER_IDEMPOTENCY_TTL_SECONDS`, 30 min): a
  repeat returns `status: "duplicate"`, an in-flight claim 503s. `JOB_NAME` comes from the
  CronJob's downward API (fantasy-infra#110, which also moved the Job to
  `restartPolicy: Never`), so pod retries within one Job dedupe while every new Job
  publishes. Callers without a key (the web UI) are unchanged.
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
