---
title: Browse free agents
description: The free-agent browsing endpoint, its query parameters, response fields and how metrics are sourced.
sidebar_position: 8
---

`GET /v1/waiver/free-agents` lists all players absent from every team's `league_rosters` with a nonempty ESPN ID. It refreshes stale roster ownership before reading. It uses the gateway's existing authentication and never publishes a claim.

```text
/v1/waiver/free-agents?season=2026&week=4&position=RB&q=back&sort=vorp&limit=50&offset=0
```

## Query parameters

| Parameter | Behavior |
| --- | --- |
| `season`, `week` | Required; positive season and week 1–18 |
| `position` | Optional, case-insensitive exact position match |
| `q` | Optional, literal case-insensitive name substring |
| `sort` | `vorp` (default) or `projected`, descending; nulls last |
| `limit` | Default 50, range 1–200 |
| `offset` | Default 0, nonnegative |

Ties sort by case-insensitive name, then internal ID, so pagination is stable for an unchanged pool.

## Response

| Field | Meaning |
| --- | --- |
| `season`, `week` | The requested season and week |
| `free_agents` | The page of matching entries |
| `total` | All matching free agents before pagination |
| `limit`, `offset` | The applied pagination |
| `source_mode` | Where metrics came from; see [Metric source](#metric-source) |
| `source_weeks` | The weeks the metrics came from |

### Each entry

| Field | Meaning |
| --- | --- |
| `player_id`, `espn_id`, `name`, `position`, `pro_team`, `injury_status` | Player identity and display |
| `projected_points`, `vorp_score` | Metrics for the free agent |
| `bench_player` | The lowest-VORP player on our bench at that position, with the same identity, display and metric fields |
| `vorp_delta` | `free_agent.vorp_score - bench_player.vorp_score` |

- Starters, IR occupants, other teams' players, and players without an ESPN ID or usable VORP are excluded as drop candidates.
- ESPN roster injury status takes precedence for bench players.
- Negative and zero swaps remain visible.
- `bench_player` and `vorp_delta` are null if there is no usable same-position bench candidate.
- Missing free-agent VORP also makes the delta null.
- These are hypothetical ordinary swaps. The recommendations endpoint handles IR moves and open bench slots.

## Metric source

Source selection happens before filters and pagination. It doesn't require a bench player or a qualifying upgrade.

| `source_mode` | When | `source_weeks` |
| --- | --- | --- |
| `target_week` | Usable target-week rows exist in the free-agent/our-bench pool | `[week]` |
| `rolling_prior_weeks` | Otherwise. Metrics average each player's available rows with nonnull VORP over the prior three same-season weeks, as in waiver recommendations. | The actual source weeks |

- The immediately prior week must be present for the fallback, and Week 1 never borrows another season.
- Projections are averaged over the same usable rows as VORP.
- Players without metrics remain in the full pool with null metric fields.
- In fallback mode, players on a target-week bye stay browsable with null metrics rather than receiving misleading prior-week values.

## Errors

| Status | Cause |
| --- | --- |
| 422 | Invalid query parameters |
| 500 | Unexpected read or refresh failure |
| 503 | No usable source data, a stale fallback, or a failed fallback schedule lookup |
