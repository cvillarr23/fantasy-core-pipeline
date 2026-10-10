---
title: Lineup optimization
description: How eligible roster players fill starting slots and become published swaps.
sidebar_position: 3
---

## Pregame inputs

`lineup_solver.evaluate_lineup` is shared by the weekly optimizer, matchup suggestions and rumor `bench_watch` actions. It excludes IR-slot players and players with Sleeper status `Out`.

1. Use target-week non-null VORP if any rostered player outside K/D-ST has it. K/D-ST rows alone do not select this mode. Players missing target-week metrics are omitted.
2. Otherwise average each player's available VORP from the previous three same-season weeks. The newest usable week across the inputs must be target week minus one. Load the target week's NFL schedule to exclude byes; prior defense multipliers are discarded.
3. If neither source is usable, raise `InsufficientLineupInputError`. Week 1 never borrows the prior season. Missing schedules, stale inputs and an empty eligible pool are failures, distinct from an already-optimal lineup.

Outputs describe `metric_source`, `source_mode`, `source_season`, `source_weeks` and `held_slots`. This fallback is a VORP mean, not actual points or a newly generated projection.

## Slot rules and objective

League settings supply starter counts; missing settings use QB 1, RB 2, WR 2, TE 1, RB/WR/TE 1, K 1 and D/ST 1. BE, Bench and IR are excluded. Position slots accept their position; FLEX accepts its named positions. The ILP uses position eligibility, rather than arbitrary ESPN `eligible_slots`.

```text
maximize Σ selected_player.vorp_score × defense_matchup_multiplier
subject to exactly one eligible player per starting slot
           each player occupies at most one slot
```

The CBC solver must return `Optimal`. Signed VORP permits below-replacement starters when slots require them. A K or D/ST slot with insufficient eligible players is held out, preserving the current starter; missing skill-position capacity fails the solve. The [Week 1 defense multiplier](projections-and-vorp.md) defaults to 1.0.

## IR and publication

IR activation is a separate roster action; stashed players cannot enter the solve. ESPN `OUT`/`INJURY_RESERVE` determine [IR hints and waiver stashes](waivers.md), while the pregame pool uses the Sleeper `Out` filter. Managers must address an ineligible IR occupant before ESPN may allow new transactions.

The optimizer diffs selected player IDs against current starters. New starters pair with a removed starter of the same position where possible, otherwise a remaining removed starter (for a FLEX change). The swap's `slot` is the removed player's actual roster slot. Held K/D-ST starters never become drop candidates. Equal optimal solutions do not have an explicit manager preference tie-break.

Internal IDs resolve to ESPN IDs before `POST /v1/trigger/lineup`. Decision inputs retain add/drop VORP, multiplier, season/week and source evidence. `--dry-run` evaluates without publishing. The normal CLI uses an explicit `--week` or the shared upcoming-week resolver.

A stable idempotency key includes team, decision weeks, sorted swap identities and `JOB_NAME` when available. Retrying the same Job uses the same key; another Job is a new decision. The trigger client retries transient failures under that key. Empty swaps or a duplicate receipt return false; exhausted retries/4xx raise. Publication alone does not establish an ESPN lineup change.

Implementation: [lineup_solver.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/optimization/lineup_solver.py).
