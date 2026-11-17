---
title: Run a backtest
description: Run a historical season replay and verify its files and stored summary.
sidebar_position: 12
---

Run a standalone replay against a development database to compare historical decision results. Read [Backtesting](backtesting.md) before interpreting its scores.

:::warning[Prerequisites]
Install the repo's dependencies and configure `DB_HOST`, `DB_NAME`, `DB_USER` and `DB_PASSWORD` for a reachable development PostgreSQL database with schema creation privileges. Each run wipes `backtest_<season>`; run in a standalone process because its temporary connection-pool swap must not overlap API requests or another replay.
:::

## Run the replay

1. Open a terminal in the repo root.
2. Run the season replay:

   ```bash
   python -m src.backtesting.season_replay --season 2024
   ```

3. Verify the JSON summary and weekly CSV in `backtest_output/2024/`.
4. Verify the appended summary through the backtest results API or the development database's `backtest_results` table.

```mermaid
flowchart LR
    R[Replay CLI] -->|recreate and migrate| B[(backtest season schema)]
    B -->|isolated optimizer inputs| S[Season replay]
    S -->|summary and weekly records| F[JSON and CSV]
    S -->|restore original pool; append summary| P[(Original backtest_results)]
```

The command downloads historical data and writes reports without publishing actions to ESPN. The original connection pool is restored after the isolated replay.

## Tune the market weight

`ESPN_MARKET_WEIGHT` is chosen by replaying seasons with FantasyPros weekly ECR standing in for ESPN start%, over the rolling base projection:

1. Check the stand-in still agrees with ESPN for a week that has a snapshot. Dump that week's `espn_player_ownership` (`player_id`, `position`, `percent_started`) to CSV, then run `python -m src.backtesting.market_stand_in --season 2026 --week 10 --espn-csv espn.csv`. A position under 0.8 Spearman is a poor stand-in.
2. Run the sweep against a throwaway database (it wipes `backtest_<season>` once per weight and appends to `backtest_results`): `python -m src.backtesting.market_weight_sweep --seasons 2023 2024 2025`.
3. Read the printed table: waiver gain and lineup points per season and pooled, each against weight 0 in its group.

## Troubleshoot the run

| Symptom or need | Action |
| --- | --- |
| Cannot connect or create schema | Check development DB settings and schema privileges |
| Earlier replay data disappeared | Re-running wipes that season's isolated schema; inspect persistent reports/summaries |
| Need a shorter replay or custom output | Call `run_season_replay` with `weeks`, `rounds` or `out_dir`; these are not CLI flags |
| Need offline regression checks | Run `DB_PASSWORD=test pytest tests/` instead of a historical replay |
| Unsure what a score measures | Read [comparison definitions](backtesting.md#what-does-each-comparison-measure) |
