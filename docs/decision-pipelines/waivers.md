---
title: Waivers
description: How bench value, free agents and IR capacity determine recommendations and live claims.
sidebar_position: 4
---

## Inputs and drop selection

The shared pure planner is `src.backtesting.waiver_strategy`. Bench candidates are non-starters outside IR; free agents are unrostered across the whole league. Both pools join exact season/week metrics. Null VORP on an existing row becomes 0. Each position keeps its best free agent and compares against its worst bench player at the same position.

```text
swap_gain = add_vorp − drop_vorp
publish ordinary swap when swap_gain > 3.0
```

A gain of exactly 3 does not qualify. With bench VORP 2 and free-agent VORP 6, the gain of 4 qualifies. Starters and IR occupants are never ordinary drop candidates. There is no FAAB/budget model, cross-position ordinary drop search or explicit tie-break for equal-valued pool entries.

## IR stash and IR + add

IR eligibility uses ESPN status `OUT` or `INJURY_RESERVE`, case-insensitively. Open capacity is `max(0, configured_IR_slots − occupied_IR_slots)`; absent settings imply zero slots.

With open IR slots, protect every IR-eligible bench player from dropping. Rank stash candidates by descending VORP and take up to the open capacity. Pair those stashes with descending-value best free agents from any position whose own VORP exceeds the margin: nobody leaves, so gain is the add's entire VORP. Each best-per-position free agent is used once. Remaining positions can still generate ordinary swaps against healthy bench players. Eligible players beyond available IR capacity remain protected in that planning pass. Once IR is full, ordinary drop evaluation resumes.

## Recommendation service versus live evaluator

| Path | Metrics | Margin | Output |
| --- | --- | --- | --- |
| `waiver_service.compute_waiver_recommendations` | Exact week when both pools exist; otherwise prior three same-season weeks | Strictly above 0 | Sorted recommendations and standalone `ir_moves`; no publication |
| `waiver_evaluator.run_weekly_waivers_live` | Exact-week pools required | Strictly above 3 | Published claims and gated IR + add actions |

Both refresh stale roster ownership before planning. Read fallback averages available per-player VORP, requires the newest usable input week to be the immediately prior week and excludes target-week NFL byes. Week 1 does not use previous-season rows. Missing bench/free-agent pools or schedule failure raises an unavailable-input error. Live evaluation does not use the fallback. An empty successful plan means no qualifying improvement, not missing projections.

Recommendations expose `kind`, `position`, `vorp_delta`, free agent, bench/IR player and source weeks. Rumor fields on this read endpoint are currently null placeholders. Standalone stash hints can overlap an IR + add suggestion; they are options, not independent actions to execute twice.

## Auto-publication

Ordinary moves resolve internal IDs through ESPN identity; missing mappings skip the move. `POST /v1/trigger/waiver` carries the decision evidence. Its key uses team, season/week and add/drop identities, with IR identity when chained, excluding VORP and Job name so another run in the same waiver week cannot duplicate a claim.

`AUTO_PUBLISH_IR_MOVES` is off unless set to `1`, `true` or `yes` (trimmed, case-insensitive). With it off, IR + add is logged as a suggestion; ordinary swaps still publish. With it on, an ineligible or unknown-status IR occupant blocks unattended IR + add. The publisher rechecks the flag for saved rumor actions too.

IR + add first publishes a keyed IR lineup move, then an add-only waiver claim with `after_tracking_id`. The gateway holds the add until the IR move is `verified_complete`, and cancels it on failure. Missing season/week or IR tracking ID prevents the add. A duplicate publication is not another claim; worker execution and ESPN verification determine completion.

Implementation: [waiver_evaluator.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/optimization/waiver_evaluator.py), [waiver_service.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/services/waiver_service.py), [waiver_strategy.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/backtesting/waiver_strategy.py).
