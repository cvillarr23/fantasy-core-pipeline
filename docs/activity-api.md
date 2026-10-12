---
title: Activity submissions
description: How to list queued actions and read their delivery and verification state.
sidebar_position: 7
---

`GET /v1/activity/actions` returns `{ "items": [...] }` for the configured
league. `limit` defaults to 50 (maximum 100), and `offset` defaults to 0.

## Needs-attention filter

With `attention=true`, an action qualifies when any of these is true:

- Execution is `failed`.
- Outcome is `awaiting_approval`.
- Outcome is `unknown` and creation was more than five minutes ago.

Actions with an unpublished outbox row and a non-null `depends_on` are excluded before `LIMIT` and `OFFSET`.

:::note[How a held child is released]
A transient parent failure keeps its child held. Even a verified parent doesn't release the hold until the child is published. After publication or dependency cancellation closes the outbox row, the ordinary attention rules apply again. A skipped, cancelled claim alone doesn't qualify.
:::

## Dependency events

Each list item includes `events`, containing only dependency events in ascending event ID order.

- Each event has the same shape as a detail timeline event: `id`, `action_id`, `stage`, `detail` and `created_at`.
- Actions without dependency events return an empty array.
- The latest dependency event describes the stage:

| Event stage | Meaning |
| --- | --- |
| `held_for_dependency` | Waiting for the parent IR move |
| `dependency_verified` | Published after the parent was verified |
| `dependency_failed` | Cancelled before submission |

- `detail.depends_on` is the parent action's tracking ID.
- A `dependency_failed` event also includes `reason`, `parent_execution` and `parent_outcome`.
- Subsequent worker and status events don't replace the dependency stage.

Clients can display dependency badges and parent links from list data without requesting each claim's detail timeline.

## Detail endpoint

`GET /v1/activity/actions/{identity}` continues to return the full event timeline and linked sessions. The list's existing action fields and per-user `unread` state retain their meanings.

## Tests

The SQL regression tests use an explicitly supplied disposable local Postgres:

```sh
ACTIVITY_TEST_DSN='host=127.0.0.1 port=55444 dbname=postgres user=postgres password=activity-test' \
  DB_PASSWORD=test python -m pytest tests/test_activity_hold_postgres.py -q
```
