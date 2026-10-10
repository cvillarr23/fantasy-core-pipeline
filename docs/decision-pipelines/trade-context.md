---
title: Trade context and strength
description: How current ESPN ownership and completed fantasy matchups describe trade opportunities.
sidebar_position: 5
---

## Inputs and horizon

`GET /v1/trade/context` and MCP `get_league_trade_context_model` share `trade_context_service`. Requests name league, season, as-of week and horizon (default 4, range 1–18). The service supports the configured ESPN league. It reads current ESPN rosters/settings and stored score history, maps ESPN players to internal identities, and loads exact-week projection/VORP rows.

The horizon is inclusive from the requested week to `min(week + horizon − 1, regular_season_end_week)`. Ownership always describes the current snapshot, even for a historical/future as-of request. `roster_snapshot` reports fetch time, observed ESPN week and whether it matches the request. This is not a reconstructed historical roster.

## Fantasy opponent strength

Only completed regular-season matchup periods ending before both the requested week and ESPN's observed scoring week contribute. Byes, ambiguous/missing matches and incomplete totals do not contribute. Multiweek matchup points divide by their actual scoring-week count.

```text
points_per_week = sum(completed_matchup_points) / completed_scoring_weeks
strength_z = (points_per_week − league_mean) / population_standard_deviation
```

Mean and population standard deviation use teams with observations. With at least two observed teams and zero standard deviation, their z scores are 0; fewer than two observations leave z scores null. A team without completed history has null strength. Positive z means stronger fantasy scoring. This measures completed points, not win probability or NFL defense strength.

Each horizon scoring week reports pairing, matchup period, status (`scheduled`, `bye`, `unavailable`) and opponent strength. Schedule strength is the mean known opponent z only when every non-bye week has known pairing and strength and at least one strength exists. Missing opponents and fantasy byes are distinct; they are not converted to average-strength opponents.

## Outputs and failures

Teams include roster positions, slots, ESPN injury status, eligible slots, mapping, per-week `projected_points`/`vorp_score`, completed-history counts, schedule and coverage. Missing mappings and metrics remain explicit nulls with coverage counts. Request validation is 422; unavailable context is 503, ESPN timeout 504 and other ESPN read failure 502. Consumers must inspect snapshot and coverage before interpreting a schedule summary.

[Trade scoring](trade-scoring.md) uses this same snapshot to compare before/after lineups. Implementation: [trade_context_service.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/services/trade_context_service.py).
