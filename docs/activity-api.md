# Activity submissions

`GET /v1/activity/actions` returns `{ "items": [...] }` for the configured
league. `limit` defaults to 50 (maximum 100), and `offset` defaults to 0.

With `attention=true`, actions qualify when execution is `failed`, outcome is
`awaiting_approval`, or outcome is `unknown` and creation was more than five
minutes ago. Actions with an unpublished outbox row and a non-null `depends_on`
are excluded before `LIMIT` and `OFFSET`. A transient parent failure keeps its
child held. Even a verified parent does not release the hold until the child is
published. After publication or dependency cancellation closes the outbox row,
the ordinary attention rules apply again; a skipped, cancelled claim alone does
not qualify.

Each list item includes `events`, containing only dependency events in ascending
event ID order. Each event has the same shape as a detail timeline event:
`id`, `action_id`, `stage`, `detail`, and `created_at`. Actions without dependency
events return an empty array. The latest dependency event describes the stage:

| Event stage | Meaning |
| --- | --- |
| `held_for_dependency` | Waiting for the parent IR move |
| `dependency_verified` | Published after the parent was verified |
| `dependency_failed` | Cancelled before submission |

`detail.depends_on` is the parent action's tracking ID. A `dependency_failed`
event also includes `reason`, `parent_execution`, and `parent_outcome`.
Subsequent worker and status events do not replace the dependency stage.
Clients can display dependency badges and parent links from list data without
requesting each claim's detail timeline.

`GET /v1/activity/actions/{identity}` continues to return the full event timeline
and linked sessions. The list's existing action fields and per-user `unread`
state retain their meanings.

The SQL regression tests use an explicitly supplied disposable local Postgres:

```sh
ACTIVITY_TEST_DSN='host=127.0.0.1 port=55444 dbname=postgres user=postgres password=activity-test' \
  DB_PASSWORD=test python -m pytest tests/test_activity_hold_postgres.py -q
```
