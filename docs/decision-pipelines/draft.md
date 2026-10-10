---
title: Draft matrix
description: How remaining roster capacity and scarcity select live draft targets.
sidebar_position: 10
---

## Inputs and recomputation

`draft_matrix` consumes browser-worker pick events from Redis Stream `draft_picks:{leagueId}` using a consumer group. It persists picks and resolves ESPN IDs through the player crosswalk. Its undrafted pool joins the requested season/week projections (week 1 by default) and excludes players already in `draft_picks`, rather than consulting post-draft roster ownership.

The matrix recomputes matchup coefficient, structural replacement baselines and scarcity over the remaining undrafted pool using the [pre-draft VORP rules](projections-and-vorp.md). As players disappear, both baselines and scarcity can change.

Roster capacity comes from configured starters plus BE slots, excluding IR. If BE is absent, a total size of 16 is the fallback. Per-team FLEX uses the default RB/WR/TE weights with largest-remainder rounding. Bench capacity uses those same weights across RB/WR/TE.

```text
starting_need[position] = max(0, positional_slots + FLEX_share − drafted_by_us)
position_cap[position] = max(0, positional_slots + FLEX_share + bench_share − drafted_by_us)
target_picks = min(remaining_picks, pool_size,
                   Σ min(position_cap[position], available_count[position]))
```

## Solve and immediate target

The binary ILP maximizes sum of scarcity-adjusted VORP with exactly `target_picks` players and per-position upper caps. Equality forces mandatory picks even if all remaining values are zero/negative. Empty pools, zero capacity or a non-optimal solve produce no recommendation.

This selects the ideal remaining roster from the current pool. To choose the immediate pick, rank selected players at positions still needing starters by descending VORP, then the remaining selected players by descending VORP. Keep up to five candidates. A lower-valued needed starter can therefore precede a higher-valued bench candidate. There is no additional equal-score preference.

Opponent picks trigger a recompute when our turn is next under the known snake order; our own confirmed pick records whether it matched the attempted target instead. The Redis board carries recommendation, candidates, VORP lookup and listener/timer evidence. Startup seeds the VORP lookup; the listener recovers its own unacknowledged backlog and acknowledges processed events.

## Clock, publication and recovery

The default hold delay is eight seconds, with a 90-second pick clock, five-second safety margin and minimum ten-second auto-publish window. Delay is clamped to the available window. Timing starts at the upstream Stream event, so backlog processing does not create a fresh clock. A newer filled pick cancels a pending publisher; draft completion stops the listener.

Publication uses the trigger API and worker draft execution path. Pause/autodraft state and mode gate the publisher. Transient publish failures retry every two seconds within the clock; three consecutive failed publications trip an auto-publish circuit breaker. A new pick cycle can re-enable a circuit-breaker disable; a manual disable remains in force. A confirmed ESPN rejection can advance to the next precomputed candidate after rejection readback; uncertainty is not permission to blindly choose another player. Board recommendation, queued target and landed ESPN pick are distinct evidence.

Run the listener with explicit `--league-id`, `--team-id`, `--season` and optionally `--week`/`--auto-draft-delay-seconds`; the API normally provisions its Job. IDs must name the intended draft. Implementation: [draft_matrix.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/optimization/draft_matrix.py).
