---
title: Projections and VORP
description: How weekly projections, replacement baselines and scarcity produce player values.
sidebar_position: 2
---

## Inputs and daily refresh

`sync_projections` fetches Sleeper regular-season `pts_ppr` for QB/RB/WR/TE. It upserts only `projected_points` for known players, preserving advanced metrics. A missing/null scoring field becomes 0; an absent player row is not created. `sync_kicker_dst` supplies ESPN K/D-ST projections.

The [infra refresher](https://github.com/cvillarr23/fantasy-infra/blob/main/base/core-pipeline/draft-prep-refresher-cronjob.yaml) runs daily at 18:00 America/Chicago. It resolves `CURRENT_YEAR`'s upcoming regular-season week, then refreshes that week through `min(week + 3, 18)`. Each week runs `sync_projections` → `sync_kicker_dst` → `vorp_matrix` in order within a shared 600-second budget. Target-week failure stops the run; a future-week failure skips its dependent stages but allows other weeks to proceed. Any failed week makes the Job fail. Earlier committed work remains, and there is no whole-run automatic retry.

`src.nfl_schedule` defines upcoming as the earliest regular-season week with any game lacking a final score. Thursday finishing does not advance the week. Downloads receive six attempts with delays of 5, 10, 20, 30 and 30 seconds. Missing schedules or stale season configuration fail rather than guess. A completed current regular season returns no upcoming week and scheduled jobs exit successfully without acting. APIs can supply a week explicitly; trade horizons default to four weeks and stop at the league's regular-season end.

## Formula

`vorp_matrix.run_vorp_pipeline(season, week)` reads exact-week `weekly_stats`, players, roster membership, starter flags and league slot counts.

```text
coefficient = clamp(1 + 0.06 × market_z + 0.03 × signal_z, 0.85, 1.15)
adjusted_projected_points = projected_points × coefficient
vorp_base = adjusted_projected_points − replacement_baseline[position]
expected_depth = max(1, round(0.5 × league_starting_demand[position]))
ratio = clamp(positive_VORP_free_agent_count / expected_depth, 0, 1)
scarcity_multiplier = 1 + 0.35 × (1 − ratio)
vorp_score = vorp_base × applicable_scarcity_multiplier
```

`market_z` standardizes distinct NFL team implied totals within season/week. `signal_z` standardizes by position/season/week: negative pressure rate allowed for QB, separation for WR/TE, depth-chart improvement for RB. Z scores clamp to ±2. Null signals, fewer than two observations or zero standard deviation contribute 0. Usage shares are excluded because projections already include usage.

Starter demand is positional slots × team count plus allocated FLEX slots. FLEX types RB/WR/TE, RB/WR and WR/TE restrict eligibility. At least ten observed eligible FLEX roster rows supply position weights; otherwise use RB 0.5, WR 0.4, TE 0.1, restricted and normalized. Largest-remainder rounding keeps integer allocations equal to total FLEX demand.

With populated rosters, replacement is the highest adjusted projection among non-starters/free agents at that position. Otherwise use the first player beyond structural starter demand (rank N+1); a short pool falls back to its minimum. Empty-position baselines are QB 10, RB/WR 8, TE 5, K/D-ST 7. No league settings falls back to ten teams with QB 1, RB 2, WR 2, TE 1 and RB/WR/TE 1.

VORP stays signed. Scarcity applies only to unrostered players with positive `vorp_base`; others use 1.0. For example, 12 adjusted points against an 8-point baseline gives 4 VORP, or 5.4 at maximum scarcity. A 6-point player stays −2 even in a thin pool.

## Outputs and missing data

The pipeline updates coefficient, adjusted points, base VORP, scarcity, final VORP and defense multiplier on existing exact-week rows. An empty week returns zero updates. It does not fill future weeks from prior data.

When `MATCHUP_WEIGHTING_ENABLED=true` and week is 1, the lineup-only defense multiplier is `clamp(1 + 0.05 × opponent_defense_strength_z, 0.9, 1.1)`. Positive strength means a weaker defense; missing strength is neutral. Other weeks/disabled mode persist null, consumed as 1.0. This does not change canonical VORP.

[Lineups](lineup-optimization.md) and the [waiver read service](waivers.md) have their own prior-week fallback. [Trade scoring](trade-scoring.md) requires exact-week metrics, so a four-week refresh attempt does not guarantee complete coverage.

Implementation: [vorp_matrix.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/optimization/vorp_matrix.py), [sync_projections.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/ingestion/sync_projections.py), [nfl_schedule.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/nfl_schedule.py).
