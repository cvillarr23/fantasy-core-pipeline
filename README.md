# fantasy-core-pipeline

The core intelligence layer of your automated fantasy football infrastructure. It performs data transformations, tracks utilization metrics, solves linear roster models, and exposes an internal control API.

## Repository Layout
*   `src/api/`: FastAPI Control Plane router, middleware definitions, and Model Context Protocol (MCP) data tools.
*   `src/ingestion/`: Scrapers managing deterministic JSON transformations from Sleeper and `nflverse`.
*   `src/optimization/`: `PuLP` matrix logic calculating dynamic VORP values and optimizing active lineups.

## Local Configuration
Ensure you match your system parameters before booting the engine:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
