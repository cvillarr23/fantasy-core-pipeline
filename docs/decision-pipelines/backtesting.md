---
title: Backtesting
description: What historical replays measure, their limits and how to run them.
sidebar_position: 11
---

Historical replay compares the decision rules with fixed baselines and hindsight. Use this page to interpret the report; use [Run a backtest](run-backtest.md) for commands.

```mermaid
flowchart LR
    H[Historical players and preseason ranks] -->|synthetic projections| D[Simulated snake draft]
    D -->|initial rosters and starters| V[Weekly VORP]
    V -->|eligible roster| L[Choose starters]
    L -->|starter flags| W[Apply ordinary waivers]
    W -->|decisions complete| A[Reveal actual scores]
    A -->|actual points| S[Compare and report]
    S -->|next regular-season week| V
```

## What data goes into a replay?

| Input | Source or rule |
| --- | --- |
| Players, crosswalk, weekly PPR actuals | nflreadpy |
| ADP proxy | FantasyPros ECR scrape before opening kickoff |
| No eligible scrape | Prior-season actual-point ranks |
| Fixed preseason projection | `max(1, 30 − 0.15 × rank)` |
| Outside ranked pool | Weekly shell projection 0 |
| League and ownership | Fixed standard league and simulated draft; scheduled playing teams |
| Draft opponents | Pick by ADP |

:::note[This tests decision mechanics]
Synthetic preseason values are not historical weekly Sleeper forecasts. Current-week actuals are revealed only after decisions; the replay does not reconstruct historical ESPN ownership.
:::

## What does each comparison measure?

| Comparison | Our result | Baseline |
| --- | --- | --- |
| Lineup | Actual points from chosen starters | Hindsight-optimal starters in that week's eligible pool, plus a fixed preseason-projection lineup |
| Waivers | Whole evolving roster's weekly actual points | Initial roster held with no moves |
| Draft | Drafted roster's replay-period actual points | Our team in an all-ADP snake draft |

| Interpretation limit | Meaning |
| --- | --- |
| Waiver/draft totals | Include bench players; measure roster production, not starting points or wins |
| Hindsight | Upper comparison within the supplied eligible pool |
| Missing actual points | Count as 0 |
| Draft period | Only replayed weeks |
| Live-path scope | Exact-week lineup loader and ordinary same-position >3 waivers |
| Not modeled | Pregame fallbacks, IR actions, rumors, trades, FAAB, waiver competition or championship probability |

## Where do results live?

| Output | Location |
| --- | --- |
| JSON summary | `backtest_output/<season>/<season>_report.json` |
| Weekly CSV | `backtest_output/<season>/<season>_weekly.csv` |
| CSV columns | `week`, `our_points`, `optimal_points`, `naive_points` |
| Persistent summaries | Original database's `backtest_results`; survive the next schema wipe |
| Read API | Backtest routes through `backtest_service` |

## Related

- [Run and verify a backtest](run-backtest.md)
- [Draft rules](draft.md)
- [Waiver rules](waivers.md)
- Implementation: [season_replay.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/backtesting/season_replay.py), [historical_data.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/backtesting/historical_data.py), [scoring.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/backtesting/scoring.py)
