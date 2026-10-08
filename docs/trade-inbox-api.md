# Incoming trade inbox API

These endpoints read the browser worker's `execution_log` inbox and queue human
requests. No migration is needed. Publishing a request never means ESPN processed
it. `TRADE_RESPONSE_ENABLED` remains off in production; the worker also requires
its dry-run, approval, verified request-shape and verified pending-shape locks to open.

## Read contract

`GET /v1/trade/incoming` returns `{offers: IncomingTrade[], limit, offset}`.

| Query | Default | Values |
| --- | --- | --- |
| `status` | `live` | `live`, `all`, or one inbox state below |
| `visibility` | `visible` | `visible`, `hidden`, `all` |
| `league_id` | configured `ESPN_LEAGUE_ID`, if set | positive ESPN league ID |
| `season` | all stored seasons | 2000–2100 |
| `limit` | 50 | 1–200 |
| `offset` | 0 | zero or greater |

Inbox states: `open`, `unparsed`, `responding`, `accepted_pending_processing`,
`accepted`, `declined`, `countered`, `vetoed`, `expired`, `withdrawn`, `closed_unknown`.
`live` means only `open`, `unparsed`, `responding`; a processing accept is history.
Hidden means dismissed or snoozed strictly beyond now. At the snooze deadline it
becomes visible without a write. Missing/invalid snooze dates do not hide an entry.
`status=all&visibility=all` matches worker `rankInbox(includeHidden=true)`;
`status=live&visibility=all` is the UI's hidden toggle without adding history.

Ranking uses the stored numeric `score.final_score` descending, then expiration,
proposal date, and ESPN transaction ID as a string. Every numeric score precedes
every unscored entry; stale or incomplete evidence is not demoted. Raw delta VORP
is single-week evidence only and never ranks entries. Invalid/missing dates sort
last. Filtering and ranking happen before pagination.

Each offer exposes the following fields (the Pydantic models also define OpenAPI):

| Fields | Contract |
| --- | --- |
| `id`, `status`, `league_id`, `season`, `executed_at` | execution-log identity and authoritative status |
| `espn_transaction_id`, `related_transaction_id`, `thread_root_id` | string IDs; related/thread IDs may be null |
| `proposing_team_id`, `our_team_id`, `espn_type`, `espn_status` | worker-captured ESPN identity |
| `send_espn_ids`, `receive_espn_ids` | string ESPN player IDs, from our perspective |
| `send_players`, `receive_players` | `{espn_id, name: string|null}[]`, preserving side order |
| `other_items`, `player_names`, `comment`, `fingerprint`, `parse_issues` | stored offer details; comment is the proposer's note |
| `proposed_date`, `expiration_date`, `first_seen_at`, `last_seen_at` | UTC ISO timestamps or null; ESPN milliseconds are converted |
| `triage` | `{state, by?, at?, snooze_until?}` or null |
| `decision` | worker-owned JSON or null; includes its outcome and verification details |
| `score`, `shadow_policy` | nullable, validated advisory evidence |
| `missed_reads`, `exit_reason` | refresh retirement evidence |
| `publications` | core claims with payload omitted; distinct from worker outcomes |

Names resolve through `players.espn_id`, falling back to worker-captured names.
Unresolved names stay null. Internal `player_id` is never inferred from numeric
equality with an ESPN ID.

`score` is the worker's flattened summary: `state`, `scoring_version`, `as_of_week`,
`final_score`, `complete_horizon`, `unavailable_reasons`, `raw_delta_vorp`,
`exceeds_roster_limit`, `unmapped_espn_ids`, `partial_reasons`,
`players: {espnId: {player_id, name}}`, `scored_fingerprint`, `scored_at`, `retry`,
`stale`, `error`. States are `scored`, `incomplete`, `unavailable`, `unscorable`,
`stale_ownership`; `unparsed` is an inbox state, not a score state.
Absent, invalid or unknown-version score/shadow metadata returns null with a
bounded warning that omits stored values. The worker does not yet persist the full
score breakdown or detailed per-week horizon evidence. Do not invent those values.
Ranking follows the raw worker score even if an invalid advisory panel becomes null.

Worker outcomes are separate from inbox status: `decision.outcome=blocked` with
`closed_locks` displays response blocking, `pending` with inbox `responding`
displays execution, `unknown` with `verification` requires manual verification,
and confirmed outcomes include `accepted_pending_processing`, `accepted`, `declined`.
Use `last_seen_at` for per-offer refresh time; it is not a global job heartbeat.

## Writes

All three writes require `X-API-Key` matching `TRIGGER_API_KEY`. The existing guard
returns 422 for a missing header, 401 for an incorrect key and 500 when the server
key is not configured. `approved_by`/`by` are nonblank, trimmed human audit labels
(maximum 200 characters); they are not authenticated identities. Extra body fields
are rejected. Writes target the configured worker league/team.

`POST /v1/trade/incoming/{espn_transaction_id}/triage` accepts
`{state: new|seen|snoozed|dismissed, by, snooze_until?}`. Snoozing requires an aware
future timestamp; other states reject `snooze_until`. Returns the updated item
with 200. It is permitted in every inbox state and changes neither ESPN nor status.

`POST /v1/trade/incoming/{espn_transaction_id}/decision` accepts
`{action: accept|decline, approved_by, comment?}`. The comment is at most 2000
characters, trimmed exactly like JavaScript `String.trim`; empty becomes null.
It is the response comment, separate from the original proposer's note.

Only an open, parsed offer more than five minutes from expiry is actionable.
Accept also requires valid score evidence whose fingerprint matches the offer
and no roster-limit overage. The worker independently reads ESPN, checks the
approval key, expiry and live roster, and owns all external writes and outcomes.
The core score check mirrors its fingerprint check; stale/incomplete score badges
remain advisory. Decline does not require a score.

Core computes `decision_key` as SHA-256 of compact UTF-8 JSON:

```text
["trade-response-v1", league, season, our_team, transaction_id,
 action, fingerprint, trimmed-comment-or-null]
```

All four identifiers, including season, are strings. The published `trade_responses`
body contains `{tracking_id, espn_transaction_id, action, comment?,
approval: {decision_key, approved_by}}` plus tracing. Null comments are omitted.
Scores and shadow policy are never published as approval metadata.

`POST /v1/trade/incoming/{espn_transaction_id}/counter` accepts
`{approved_by, week: 1..18}`. Only open, parsed, unexpired entries are eligible.
It requires a score matching the offer fingerprint and requested week, and a
nonempty internal mapping for every player on both sides. The `trade_negotiations`
body exactly follows worker `buildNegotiationPayload`: `{tracking_id, season,
week, offering_team_id, incoming_player_ids, outgoing_player_ids}` plus tracing.
Incoming maps receive IDs; outgoing maps send IDs. Season comes from the row.
This starts negotiation and staging for later outgoing approval. It does not
submit a counter or mark the original offer `countered`. The existing payload
does not carry the original ESPN transaction/thread ID.

Both publication routes return 202 for a new broker-confirmed publication and 200
for a duplicate, with:

```json
{
  "espn_transaction_id": "123",
  "status": "queued",
  "kind": "decision",
  "tracking_id": "<uuid>",
  "approved_by": "human",
  "approved_at": "<UTC timestamp>",
  "duplicate": false,
  "decision_key": "<sha256>",
  "action": "decline"
}
```

Counter receipts have `kind=counter`, `decision_key=null`, `action=null`.
`queued` confirms only broker publication, including when the worker subsequently
blocks or rejects the request. Duplicate receipts retain the original approver,
timestamp and tracking ID, including after the worker settles the offer.

Other responses: 404 missing entry; 409 unsafe/non-open state, unmapped players,
duplicate rows, competing action or reconciliation needed; 422 invalid input;
503 broker publication failure. None certifies an ESPN response.

## Concurrency and recovery

Every mutation uses `pg_advisory_xact_lock(hashtext('trade-inbox:' + league + ':' + id))`
and re-reads the row `FOR UPDATE`. Identity matches the worker's league/transaction
lookup; season is not in that lookup or lock. Duplicate inbox rows fail closed.

Claims live under `outcome_details.incoming_publications[publication_key]` and
commit atomically with `bot_decisions` before broker access. Decision keys use the
worker digest. Counter keys hash `["trade-inbox-counter-v1", league, season,
our_team, transaction_id, fingerprint, week]` (season is a string).
Claims contain `kind`, `status`, `tracking_id`, `approved_by`, `approved_at`,
`fingerprint`, exact `payload`, and decision action/key where applicable.
Only the payload is omitted from list responses.

`pending` includes both in-flight and outcome-unknown publication. Confirmed sends
become `queued`; proven pre-send failures become `failed_before_publish` and may
be retried. Ambiguous failures and confirmation-save failures stay blocked without
a TTL. Finalization re-locks and verifies the claim tracking ID before updating.
Worker refreshes preserve `outcome_details`; core changes neither the worker's
`decision` nor inbox status when publishing.

Any outstanding/queued action blocks competing actions, even if a refresh changes
the offer fingerprint. Existing worker `responding`, `responded`, `response_unknown`
or `rejected` response rows also block new work when the inbox still looks open.
A repeat of the same confirmed request returns its receipt without another send.
Blocked or rejected worker execution does not cause core to republish automatically.
There is no reconciliation or reapproval endpoint in this change.

Reconcile using the stored payload, tracking ID, broker/worker logs, response rows,
Activity, and ESPN evidence. Reset a claim only after establishing that no action
was queued/sent and that no consumer can still execute it. An unknown result must
remain blocked. Any production write needs explicit operator authorization.

Worker follow-ups: [full score evidence](https://github.com/cvillarr23/fantasy-browser-worker/issues/152),
[response concurrency and recovery](https://github.com/cvillarr23/fantasy-browser-worker/issues/153),
and [counter thread linkage](https://github.com/cvillarr23/fantasy-browser-worker/issues/154).

Before enabling responses, the worker needs concurrency/recovery improvements:
its execution checks happen before its later inbox lock, and its initial
`responding` refusal precedes the redelivery reconciliation path. Core's lock alone
does not repair those worker behaviors. The response flag remains off.

Tests mock both storage and AMQP; never probe decision/counter endpoints on the
deployed stack. Hash fixtures were generated from worker `origin/main` at
`6e8b3e45c889213879639712e67f2866630277c5`.
