# Browse free agents

`GET /v1/waiver/free-agents` lists all players absent from every team's
`league_rosters` with a nonempty ESPN ID. It refreshes stale roster ownership before
reading. It uses the gateway's existing authentication and never publishes a claim.

Example: `/v1/waiver/free-agents?season=2026&week=4&position=RB&q=back&sort=vorp&limit=50&offset=0`.

| Query parameter | Behavior |
| --- | --- |
| `season`, `week` | Required; positive season and week 1–18 |
| `position` | Optional, case-insensitive exact position match |
| `q` | Optional, literal case-insensitive name substring |
| `sort` | `vorp` (default) or `projected`, descending; nulls last |
| `limit` | Default 50, range 1–200 |
| `offset` | Default 0, nonnegative |

The response contains `season`, `week`, `free_agents`, `total`, `limit`, `offset`,
`source_mode`, and `source_weeks`. `total` counts all matching free agents before
pagination. Ties sort by case-insensitive name, then internal ID, so pagination is
stable for an unchanged pool.

Each entry contains `player_id`, `espn_id`, `name`, `position`, `pro_team`,
`injury_status`, `projected_points`, `vorp_score`, `bench_player`, and `vorp_delta`.
`bench_player` has the same identity, display, and metric fields and is the lowest
VORP player on our bench at that position. Starters, IR occupants, other teams'
players, and players without an ESPN ID or usable VORP are excluded as drop
candidates. ESPN roster injury status takes precedence for bench players.

`vorp_delta = free_agent.vorp_score - bench_player.vorp_score`. Negative and zero
swaps remain visible. If there is no usable same-position bench candidate,
`bench_player` and `vorp_delta` are null. Missing free-agent VORP also makes the
delta null. These are hypothetical ordinary swaps; IR moves and open bench slots
are handled by the recommendations endpoint.

When usable target-week rows exist in the free-agent/our-bench pool, the source is
`target_week` with `[week]`. Otherwise, metrics average each player's available
rows with nonnull VORP over the prior three same-season weeks, as in waiver
recommendations. The source is `rolling_prior_weeks` with the actual source weeks.
The immediately prior week must be present, and Week 1 never borrows another
season. Source selection happens before filters and pagination and does not
require a bench player or a qualifying upgrade.

Players without metrics remain in the full pool with null metric fields. In
fallback mode, players on a target-week bye remain browsable with null metrics
rather than receiving misleading prior-week values. Projections are averaged
over the same usable rows as VORP. A lack of usable source data, stale fallback,
or a failed fallback schedule lookup returns 503. Invalid query parameters
return 422; unexpected read/refresh failures return 500.
