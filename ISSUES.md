# GitHub Issues Template Backlog: fantasy-core-pipeline

This tracking index logs functional requirements for the core python application engine.

---

## [CORE-01] Build FastAPI Core Controller & Endpoint Framework
*   **Priority**: High 🔥
*   **Milestone**: Phase 1 - Ingestion & Core API Setup
*   **Tags**: `area/api`, `component/fastapi`, `type/feature`

### Description
Implement the baseline structure for the FastAPI engine. This service serves as the core orchestration plane inside your cluster. It captures workflow manual triggers, handles background scheduling, and acts as the entry interface for your automation pipelines.

### Technical Tasks
- [ ] Initialize `src/api/main.py` configuring default routes and error middleware classes.
- [ ] Standardize a `/v1/health` status path mapping database and broker connection liveness.
- [ ] Connect transaction handlers converting internal HTTP requests straight into RabbitMQ message events.

---

## [CORE-02] Code Deterministic Sleeper Roster Sync & Status Differ
*   **Priority**: High 🔥
*   **Milestone**: Phase 1 - Ingestion & Core API Setup
*   **Tags**: `area/data`, `component/ingestion`, `type/feature`

### Description
Develop the logic to pull the flat Sleeper master players dictionary (`/v1/players/nfl`). The utility must isolate change vectors across runtime execution iterations to discover injury, team, and roster modifications with zero human bias.

### Technical Tasks
- [ ] Author `src/ingestion/sync_players.py` utilizing `polars` to slice status configurations.
- [ ] Program a delta-evaluator parsing changes across `injury_status` and active `status` blocks.
- [ ] On a verified status mismatch, fire a high-priority payload event onto RabbitMQ to instantly engage the browser sniper worker.

---

## [CORE-03] Implement nflverse Advanced Metric Usage Ingestor
*   **Priority**: Medium ⚡
*   **Milestone**: Phase 2 - Intelligence & Modeling
*   **Tags**: `area/data`, `component/analytics`, `type/feature`

### Description
Construct the ingestion routine tracking play-by-play execution arrays using `nflreadpy`. By aggregating true context volume parameters, the math solver shifts priority variables before basic fantasy stats update.

### Technical Tasks
- [ ] Draft `src/ingestion/metrics_extractor.py` parsing raw data dumps.
- [ ] Aggregate and map week-over-week trajectory data capturing **Snap Shares, Target Percentages, and Route Run Ratios**.
- [ ] Load results cleanly into the custom TimescaleDB schema tables.

---

## [CORE-04] Program Dynamic VORP Solver using PuLP Matrix Algebra
*   **Priority**: High 🔥
*   **Milestone**: Phase 2 - Intelligence & Modeling
*   **Tags**: `area/math`, `component/optimization`, `type/feature`

### Description
Develop the core VORP (Value Over Replacement Player) calculator and mathematical linear roster optimizer. This script executes multi-variable linear program structures to compute ideal starting constraints based on changing team attributes.

### Technical Tasks
- [ ] Initialize `src/optimization/lineup_solver.py`.
- [ ] Program a positional scanner determining replacement thresholds across individual layout types.
- [ ] Code the `pulp.LpProblem` system maximizing points across rigid configuration boundaries (1 QB, 2 RB, 2 WR, 1 TE, 1 FLEX).
