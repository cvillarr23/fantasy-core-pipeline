---
title: Backtesting
description: What historical replays measure, their limits and how to run them.
sidebar_position: 11
---

## Replay inputs and ordering

`src.backtesting.season_replay` creates and wipes `backtest_<season>` and runs migrations there. A temporary connection pool redirects shared optimizers into that schema, restoring the original pool afterward. Run as a standalone process: the pool swap must not overlap API requests or another replay in the same process.

The player universe, crosswalk and actual weekly PPR scoring come from nflreadpy. FantasyPros preseason ECR is an ADP proxy; only a scrape before opening kickoff is eligible. If unavailable, use prior-season actual-point ranks. Projections are fixed synthetic preseason values `max(1, 30 − 0.15 × rank)`, with 0 for players outside the ranked pool in weekly shells. These are not historical weekly Sleeper forecasts. The replay uses a fixed standard league and scheduled playing teams, not historical ESPN ownership.

Simulate a snake draft with our matrix and opponents picking by ADP, seed rosters and initial starters, then repeat each regular-season week:

1. Recompute VORP, solve starters and persist starter flags.
2. Apply ordinary same-position >3 VORP waivers inside the isolated schema.
3. Reveal actual scores only after decisions, then score comparisons.

Current-week actual points cannot influence that week's decision phase. The replay uses the exact-week lineup loader and ordinary waiver strategy, not every live pregame fallback, IR action, rumor or trade path.

## What the report measures

| Comparison | Measurement |
| --- | --- |
| Lineup | Sum actual points of chosen starters versus actual-point-maximizing hindsight starters from that week's eligible roster pool and a fixed preseason-projection starting lineup |
| Waivers | Sum weekly actual points for the entire evolving roster versus the initial roster held without moves |
| Draft | Sum replay-period actual points for our drafted roster versus our team in an all-ADP snake draft |

Waiver/draft totals include bench players; they are roster production, not optimized starting points or wins. Hindsight is an upper comparison within the supplied eligible pool. Missing actual points count as 0. Draft scoring covers only replayed weeks. These comparisons do not model FAAB, waiver competition, trades, incoming policies or a calibrated championship probability.

## Run and inspect

From the repo root, with dependencies installed, reachable PostgreSQL and `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` configured for a development database:

```bash
python -m src.backtesting.season_replay --season 2024
```

This command downloads historical data, recreates the season's backtest schema and writes files; it does not publish actions to ESPN. Re-running erases that schema's previous replay. Final summary rows append to the original database's `backtest_results`, so reports survive the next schema wipe. Use a development database with schema creation privileges.

Defaults cover the season's regular-season weeks. Python callers can pass `weeks`, `rounds` and `out_dir` to `run_season_replay`; these are not CLI flags. Reports are `backtest_output/<season>/<season>_report.json` and `<season>_weekly.csv` (week, our_points, optimal_points, naive_points). Stored summaries are exposed by the backtest API through `backtest_service`.

For offline regression checks rather than a replay:

```bash
DB_PASSWORD=test pytest tests/
```

Implementation: [season_replay.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/backtesting/season_replay.py), [historical_data.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/backtesting/historical_data.py), [scoring.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/backtesting/scoring.py).
