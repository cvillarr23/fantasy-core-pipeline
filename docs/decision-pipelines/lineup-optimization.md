---
title: Lineup optimization
description: How eligible roster players fill starting slots and become published swaps.
sidebar_position: 3
---

The lineup solver fills starting slots with the highest total eligible VORP. Use this page to understand a suggested swap or an unavailable-lineup result.

```mermaid
flowchart LR
    R[Non-IR roster] -->|player IDs| P[Pregame metric pool]
    P -->|eligible players| H[Hold missing K and D/ST slots]
    H -->|remaining slot demand| S[Lineup ILP]
    S -->|optimal starters| D[Diff current starters]
    D -->|internal swaps| I[Resolve ESPN identities]
    I -->|POST /v1/trigger/lineup| G[Gateway]
    G -->|queued swaps| W[Worker and ESPN verification]
```

## Which metrics does the solver use?

`evaluate_lineup` serves the weekly optimizer, matchup suggestions and rumor `bench_watch` actions. Its pool excludes IR-slot players and Sleeper status `Out`.

| Source choice | Rule |
| --- | --- |
| Target week | Use non-null target-week VORP if any rostered player outside K/D-ST has it; omit players missing metrics |
| Prior-week fallback | Otherwise average available VORP over the previous three same-season weeks |
| Fallback freshness | Newest usable input week must be target week minus one |
| Fallback byes | Exclude target-week NFL byes using the schedule; discard prior defense multipliers |
| No usable source | Raise `InsufficientLineupInputError` |

:::warning[Missing data is not an optimal lineup]
K/D-ST rows alone do not select target-week mode. Week 1 never borrows the prior season; stale inputs, missing schedules and an empty eligible pool fail evaluation.
:::

Source evidence reports `metric_source`, `source_mode`, `source_season`, `source_weeks` and `held_slots`.

## How are slots filled?

```text
maximize Σ selected_player.vorp_score × defense_matchup_multiplier
subject to exactly one eligible player per starting slot
           each player occupies at most one slot
```

| Slot or result | Behavior |
| --- | --- |
| Position slot | Accept its position |
| FLEX | Accept the positions named by the slot |
| BE, Bench, IR | Exclude from starter demand |
| Missing K/D-ST capacity | Hold the slot; preserve its current starter |
| Missing skill-position capacity | Fail the solve |
| Negative VORP | Allow it when needed to fill a slot |
| Successful solve | CBC status must be `Optimal` |
| Equal optima | No explicit manager-preference tie-break |

The solver uses position eligibility, not arbitrary ESPN `eligible_slots`. Missing settings default to QB 1, RB 2, WR 2, TE 1, RB/WR/TE 1, K 1 and D/ST 1; the [defense multiplier](projections-and-vorp.md) defaults to 1.0.

## How does a swap reach ESPN?

| Stage | Rule |
| --- | --- |
| Diff | Compare optimal player IDs with current starters |
| Pair | Match adds with same-position removals first, then any remaining removal for FLEX |
| Slot | Use the removed starter's actual roster slot |
| Held starters | Never become removal candidates |
| Identity | Resolve internal IDs to ESPN IDs before publication |
| Evidence | Retain add/drop VORP, multiplier, season/week and source |
| Dry run | `--dry-run` evaluates without publishing |

:::note[IR is a separate action]
Stashed players must be activated before they can start. ESPN `OUT`/`INJURY_RESERVE` control IR hints; the pregame pool uses Sleeper `Out`. An ineligible IR occupant can block ESPN transactions.
:::

<details>
<summary>Publication keys and retry behavior</summary>

| Setting or outcome | Behavior |
| --- | --- |
| Idempotency material | Team, decision weeks, sorted swap identities and `JOB_NAME` when available |
| Same Job retry | Same key |
| Another Job | New decision |
| Transient gateway failure | Retry under the same key |
| Empty swaps or duplicate | Return false |
| Exhausted retries or 4xx | Raise |
| CLI week | Explicit `--week` or shared upcoming-week resolver |

</details>

## Related

- [Waivers and IR](waivers.md)
- [Rumor actions](rumor-actions.md)
- [Activity and execution evidence](../activity-api.md)
- Implementation: [lineup_solver.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/optimization/lineup_solver.py)
