# Copilot instructions for fantasy-core-pipeline

FastAPI gateway + ingestion/optimization scripts for an automated fantasy football pipeline. Talks to PostgreSQL (psycopg2), RabbitMQ (pika), Redis, and external APIs (Sleeper, ESPN, nflverse).

## System architecture

This is one of three repos that make up the platform:
- **`fantasy-core-pipeline`** (this repo): the brain. Ingests player/injury/stat data from external APIs into Postgres, runs optimization (VORP/lineup/draft) math, exposes a FastAPI gateway, and publishes action messages (`lineup_updates`, `waiver_actions`) onto RabbitMQ.
- **`fantasy-browser-worker`**: the hands. A Node/Playwright worker that consumes those RabbitMQ messages and executes the corresponding action against the ESPN UI. This repo's queue names, routing keys, and JSON payload shapes are a contract with that consumer — changing them without a corresponding change there breaks automation silently.
- **`fantasy-infra`**: the plumbing. Kustomize manifests (synced by Argo CD) that deploy this repo's FastAPI service plus the shared Postgres/Redis/RabbitMQ instances it depends on. Env var defaults in this repo's code (`os.getenv(..., "fantasy-x.<namespace>.svc.cluster.local")`) must match what's actually defined there.

## Stack conventions

- `src/database.py` opens a real `ThreadedConnectionPool` **at import time**. Anything importing it (directly or transitively) needs a reachable Postgres unless the pool class is mocked first — see how `tests/conftest.py` patches `psycopg2.pool.ThreadedConnectionPool` before collection.
- DB access goes through `get_db_connection()` / `get_db_cursor()` (`src/database.py`) — these context managers auto-commit on success and roll back on exception. Don't open raw `psycopg2.connect()` calls elsewhere.
- RabbitMQ publishes follow one pattern throughout the repo (see `src/api/routes.py`): `channel.queue_declare(queue=<name>, durable=True)`, then `basic_publish(exchange="", routing_key=<name>, ..., properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))`. Flag any publish that diverges without a stated reason.
- `src/api/main.py`'s startup event runs `run_migrations()`, which also needs a real DB — tests patch it at the `src.api.main` import site, not `src.migrations`.
- Schema lives in `src/models.py` as raw DDL strings, applied idempotently (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`) by `src/migrations.py`. A new column needs an explicit `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` migration, not just an edit to the `CREATE TABLE` string, or existing deployed databases won't pick it up.
- `requirements.txt` is pinned to what builds cleanly on `python:3.11-slim` (the Dockerfile's base image). A pin failing to build on a newer local Python isn't evidence the pin itself is wrong.

## Testing

- Tests live in `tests/`, use `pytest` + FastAPI's `TestClient`, and run with `DB_PASSWORD=test pytest tests/`.
- Mock `pika.BlockingConnection` at the *consuming* module's import site (e.g. `src.api.routes.pika.BlockingConnection`, `src.ingestion.sync_players.pika.BlockingConnection`), not `pika` globally.
- A package `__init__.py` that does `from pkg.module import name` where `name` collides with the submodule's own name (e.g. `from src.ingestion.sync_players import sync_players`) silently shadows the submodule attribute — `import pkg.module_name as x` then resolves to the wrong object. Prefer empty `__init__.py` files (the established pattern in `src/api/__init__.py`) unless there's a real reason to re-export.

## Things to double-check in review

- External API URLs/hostnames (Sleeper, ESPN, nflverse) — verify against real documentation rather than trusting a confident-sounding comment or docstring; this codebase has had placeholder/incorrect hosts checked in before.
- Service DNS defaults in `os.getenv(..., "fantasy-x.<namespace>.svc.cluster.local")` must match the actual namespace/Service name defined in `fantasy-infra` — a mismatched fallback (e.g. wrong namespace) fails silently at runtime, not at review time.
- Newly-added files — including ones that show up untracked before a PR — aren't automatically trustworthy. Check they actually import/run (syntax errors, undefined names, wrong function signatures) rather than assuming existing code is a safe base to build on.
- ID spaces differ between data sources (Sleeper `player_id` vs nflverse `gsis_id` vs ESPN IDs) — never assume two different providers' IDs are interchangeable or joinable by name.
