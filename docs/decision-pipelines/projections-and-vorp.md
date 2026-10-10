---
title: Projections and VORP
description: How weekly projections, replacement baselines and scarcity produce player values.
sidebar_position: 2
---

VORP measures a player's projected value above a replacement player. Use this page to predict how weekly inputs change lineup, waiver and draft values.

```mermaid
flowchart LR
    S[Sleeper skill projections] -->|pts_ppr| P[Exact-week stats]
    E[ESPN K and D/ST projections] -->|projected_points| P
    P -->|points and signals| C[Matchup coefficient]
    C -->|adjusted points| B[Replacement baseline]
    L[League slots and roster usage] -->|starter demand| B
    B -->|signed vorp_base| V[Scarcity adjustment]
    V -->|vorp_score| W[(weekly_stats)]
```

## When does the refresh run?

| Setting | Behavior |
| --- | --- |
| Schedule | Daily at 18:00 America/Chicago |
| Season | `CURRENT_YEAR` |
| Horizon | Upcoming week through `min(week + 3, 18)` |
| Week budget | 600 seconds shared across three stages |
| Target-week failure | Stop; fail the Job |
| Future-week failure | Skip that week's remaining stages; try other weeks; fail the Job |
| Completed work | Keep committed weeks; no automatic whole-run retry |

```mermaid
flowchart LR
    R[Resolve upcoming week] -->|current season| S[sync_projections]
    S -->|skill points committed| K[sync_kicker_dst]
    K -->|K and D/ST points committed| V[vorp_matrix]
    V -->|week complete| N[Next horizon week]
    N -->|through week 18| S
```

The resolver chooses the earliest regular-season week with an unfinished game. A finished Thursday game does not advance the week.

<details>
<summary>Week resolution and ingestion edge cases</summary>

| Condition | Result |
| --- | --- |
| Schedule download fails | Six attempts; backoff 5, 10, 20, 30, 30 seconds; then fail |
| Missing schedule or stale season configuration | Fail rather than guess |
| Current regular season complete | No upcoming week; scheduled jobs exit successfully |
| Explicit API week | Use the requested week; trade horizons stop at the league's regular-season end |
| Sleeper scoring field missing/null | Store 0 for a known player |
| Player absent from response or unknown locally | Do not create a projection row |

`sync_projections` reads regular-season `pts_ppr` for QB/RB/WR/TE and upserts only `projected_points`, preserving advanced metrics. `sync_kicker_dst` supplies ESPN K/D-ST projections.

</details>

## How is VORP calculated?

```text
coefficient = clamp(1 + 0.06 × market_z + 0.03 × signal_z, 0.85, 1.15)
adjusted_projected_points = projected_points × coefficient
vorp_base = adjusted_projected_points − replacement_baseline[position]
expected_depth = max(1, round(0.5 × league_starting_demand[position]))
ratio = clamp(positive_VORP_free_agent_count / expected_depth, 0, 1)
scarcity_multiplier = 1 + 0.35 × (1 − ratio)
vorp_score = vorp_base × applicable_scarcity_multiplier
```

| Input | Rule |
| --- | --- |
| `market_z` | Standardize distinct NFL team implied totals within season/week |
| `signal_z`: QB | Standardize negative pressure rate allowed |
| `signal_z`: WR/TE | Standardize separation |
| `signal_z`: RB | Standardize depth-chart improvement |
| Signal groups | Position/season/week; clamp z to ±2 |
| Missing/degenerate signal | Contribute 0 for null, fewer than two observations or zero standard deviation |
| Usage shares | Excluded because projections already include usage |
| Scarcity eligibility | Only unrostered players with positive base VORP; everyone else uses 1.0 |

:::tip[Predict the result]
12 adjusted points minus an 8-point baseline gives 4 VORP, or 5.4 at maximum scarcity. A 6-point player stays at −2, even when the position is thin.
:::

## Where is the replacement line?

| Roster state | Baseline |
| --- | --- |
| Populated rosters | Highest adjusted projection among non-starters/free agents at the position |
| Pre-draft | First player beyond structural starter demand: rank N+1 |
| Pool shorter than demand | Pool minimum |
| Empty position pool | QB 10; RB/WR 8; TE 5; K/D-ST 7 |

<details>
<summary>Starter demand, FLEX allocation and default league shape</summary>

Starter demand is positional slots × team count plus allocated FLEX slots.

| Allocation input | Rule |
| --- | --- |
| FLEX types | RB/WR/TE, RB/WR, WR/TE; restrict positions to the slot |
| At least ten observed eligible FLEX rows | Use observed position shares |
| Fewer than ten rows | RB 0.5, WR 0.4, TE 0.1; restrict and normalize |
| Integer rounding | Largest remainder; allocations sum to total FLEX demand |
| Missing league settings | Ten teams; QB 1, RB 2, WR 2, TE 1, RB/WR/TE 1 |

</details>

## What happens with missing data?

The pipeline updates existing exact-week rows with the coefficient, adjusted points, base VORP, scarcity, final VORP and defense multiplier. An empty week returns zero updates.

:::warning[Refresh coverage is not guaranteed]
A four-week refresh attempt does not guarantee four scored weeks. Trade scoring requires exact-week metrics; lineup and waiver reads have separate fallback rules.
:::

| Defense adjustment | Behavior |
| --- | --- |
| `MATCHUP_WEIGHTING_ENABLED=true`, week 1 | `clamp(1 + 0.05 × opponent_defense_strength_z, 0.9, 1.1)` |
| Missing strength | Neutral 1.0 |
| Disabled or another week | Persist null; lineup consumes it as 1.0 |
| Effect | Lineup objective only; canonical VORP stays unchanged |

Positive defense strength means a weaker defense.

## Related

- [Lineup data rules](lineup-optimization.md)
- [Waiver data rules](waivers.md)
- [Trade coverage](trade-scoring.md)
- Implementation: [vorp_matrix.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/optimization/vorp_matrix.py), [sync_projections.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/ingestion/sync_projections.py), [nfl_schedule.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/nfl_schedule.py)
- Schedule: [infra refresher](https://github.com/cvillarr23/fantasy-infra/blob/main/base/core-pipeline/draft-prep-refresher-cronjob.yaml)
