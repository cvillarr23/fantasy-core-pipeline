---
title: Rumor-driven actions and checkpoints
description: How qualifying news signals reuse decision rules and survive retries.
sidebar_position: 9
---

## Qualification and actions

`rumor_action_service` consumes `rumor_signals` ordered by increasing ID after the team's durable watermark. The default `RUMOR_ACTION_CONFIDENCE_THRESHOLD` is 0.75; qualification is confidence at or above that value. Within an action's batch, the latest qualifying signal per player wins. Low-confidence and non-action rows still need to be accounted for so they cannot stall the cursor indefinitely.

| Suggested action | Evaluation | Published subset |
| --- | --- | --- |
| `boost_waiver_priority` | Normal live waiver pools, strict >3 margin and IR gates | Only moves adding a signaled player |
| `bench_watch` | Shared pregame lineup evaluation | Only swaps removing a signaled current starter |

Rumors do not add a score bonus or force a move. A high-confidence signal with no beneficial normal-rule move is accounted as `no_beneficial_move`. Other normal moves stay for scheduled evaluation. Missing inputs/identities are recorded separately from no improvement. Decision evidence adds rumor signal ID, sentiment and confidence to the underlying VORP/source evidence.

## Durable checkpoints

`rumor_action_checkpoints` stores per-team versioned state: watermark, per-signal progress, immutable prepared actions and failed-job markers. PostgreSQL owns progress; the legacy Redis `rumor_action:last_processed_id` initializes a team's first checkpoint and is subsequently a best-effort mirror.

Preparation commits exact payloads and stable gateway keys before HTTP publication. Attempts and outcomes are saved separately so a process death after delivery replays the same key, not a newly computed action. Optimistic version checks reject overlapping runs rather than overwrite progress. Transactions never span gateway calls.

The watermark advances in ordered ID sequence only over fully accounted signals; pending work stops it. Accepted and already-queued outcomes both complete an action. Terminal failures retain reasons in execution evidence, then checkpoint entries can be pruned below the watermark.

## Retry limits and outputs

Publication failures have a three-failure cap. Signal evaluation failures have their own three-failure cap and 24-hour window. Waiver actions have a 24-hour retry window; lineup actions have 20 minutes, shorter than the gateway's 30-minute key retention. Prepared actions for a different season/week are dropped rather than replayed. Failed-job markers live seven days. Inspect recorded drop reasons before deciding on recovery.

IR + add remains subject to `AUTO_PUBLISH_IR_MOVES` at delivery time. A gateway duplicate is recorded as already queued, not executed on ESPN.

The run returns newly published waiver claims/lineup swaps and watermark, and records `success`, `partial_failure` or `failure` in execution outcomes. `signals_unaccounted`, `actions_unaccounted`, missing identities and failed publications prevent a misleading success. The [Activity API](../activity-api.md) exposes accounting and worker outcomes.

Implementation: [rumor_action_service.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/services/rumor_action_service.py), [rumor_action_checkpoint.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/services/rumor_action_checkpoint.py).
