---
title: Incoming trade inbox API
description: Read contract, review token and publishing rules for the incoming trade inbox.
sidebar_position: 8
---

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
| `review_token` | opaque 64-hex version of the offer the human is reviewing; send it back on every publication (see [Review token](#review-token)) |
| `proposed_date`, `expiration_date`, `first_seen_at`, `last_seen_at` | UTC ISO timestamps or null; ESPN milliseconds are converted |
| `triage` | `{state, by?, at?, snooze_until?}` or null |
| `decision` | worker-owned JSON or null; includes its outcome and verification details |
| `score`, `shadow_policy` | nullable, validated advisory evidence |
| `evaluation` | nullable full score evidence bound to `score` (see below); display-only |
| `missed_reads`, `exit_reason` | refresh retirement evidence |
| `publications` | core claims with payload omitted; distinct from worker outcomes |
| `response` | `{id, status, action, decision_key}` of the newest worker `trade_response` row that blocks the offer (`responding`, `responded`, `response_unknown`, `rejected`), or null; `id` is what a retry names |
| `our_counter` | our counter to this offer as ESPN showed it, or null (see [Negotiation threads](#negotiation-threads)) |

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
bounded warning that omits stored values.
Ranking follows the raw worker score even if an invalid advisory panel becomes null.

`evaluation` is the worker's full breakdown behind `score`:
`{version: "incoming-trade-evaluation-v1", evaluated_at, scored_fingerprint, request, score}`.
`request` is the exact `POST /v1/trade/score` body the worker sent, validated as
`TradeScoreRequest`. The worker omits `horizon`, so `request.horizon` is null, which
means the scorer's default of four weeks applied, not that a value was sent. `score` is
the complete, unmodified `TradeScore` response: `score.our_weighted_gain` (G*),
`score.counterparty_weighted_benefit` (C*), `score.our_schedule_adjustment`, `schedule`,
both `roster_impact` sides, `horizon` (start/end week, `scored_weeks`, and
`unscored_weeks[].reasons`), `coverage`, `roster_snapshot` and `strength_metric`.

Core exposes it only while it describes the same offer version, refresh and score as
the summary, so a breakdown and summary can't silently disagree:

- `scored_fingerprint` equals both the offer `fingerprint` and `score.scored_fingerprint`;
  `evaluated_at` is the same instant as `score.scored_at`.
- The request's league and season match the row, its teams match `our_team_id` and
  `proposing_team_id`, and its two player sides match `send_espn_ids` and
  `receive_espn_ids` (order ignored, repeats counted).
- The response's league, season, `as_of_week`, both team IDs and `send`/`receive` ESPN
  IDs match the request.
- The response's `scoring_version`, `as_of_week` and `score.final_score` equal the
  summary's.

Absent or null evidence returns null silently (legacy rows, unscorable trades, permanent
score failures, or evidence over the worker's 64 KiB limit). Unknown-version, malformed or
mismatched evidence, or evidence beside a missing or invalid summary, returns null with a
warning naming the row and the failed check, never stored values. A stale summary
(`stale: true`) keeps its original evidence and timestamp, so both still match.
`evaluation` is not part of the review token, ranking, accept/counter checks or any
publication payload. Clients should show the breakdown as unavailable when it is null.

Worker outcomes are separate from inbox status: `decision.outcome=blocked` with
`closed_locks` displays response blocking, `pending` with inbox `responding`
displays execution, `unknown` with `verification` requires manual verification,
and confirmed outcomes include `accepted_pending_processing`, `accepted`, `declined`.
Use `last_seen_at` for per-offer refresh time; it is not a global job heartbeat.

## Negotiation threads

The worker records our counter to an incoming offer on that offer's row, as
`our_counter`. Core exposes it as `{espn_transaction_id, status, send_players,
receive_players, proposed_date, expiration_date, missed_reads, exit_reason, seen_at}`.
`status` uses the inbox states: `open` while the counter is pending, then how it
ended. The answered offer itself stays `countered`. An absent `our_counter` returns
null. An invalid one returns null with a warning that names the row and the
failing fields, never the stored values. `our_counter` is not part of the review
token.

`GET /v1/trade/incoming/{espn_transaction_id}/thread` returns the negotiation that
an offer, or one of our counters, belongs to. It has an optional `league_id`
parameter, which defaults to the configured `ESPN_LEAGUE_ID`; with neither set it
returns 409. It returns 404 when no row matches. The response is `{thread_root_id,
state, our_counters, counter_limit, turns}`:

- `turns`: their offers (`side: "theirs"`) and our counters (`side: "ours"`, with
  `counters` naming the offer they answered), ordered by proposal time, then ID.
- `state`: read from the latest turn. It is one of:
  - `awaiting_us`: their offer is open;
  - `awaiting_them`: our counter is pending;
  - `agreed`;
  - `rejected`;
  - `expired`;
  - `abandoned`: withdrawn or closed unexplained.
- `our_counters`: the counters ESPN has shown. Counters that were only staged
  don't count.
- `counter_limit`: the worker's per-thread cap. At the cap, the worker refuses
  another counter.

This mirrors the worker's `buildThread`.

## Writes

All four writes require `X-API-Key` matching `TRIGGER_API_KEY`. The existing guard
returns 422 for a missing header, 401 for an incorrect key and 500 when the server
key is not configured. `approved_by`/`by` are nonblank, trimmed human audit labels
(maximum 200 characters); they are not authenticated identities. Extra body fields
are rejected. Writes target the configured worker league/team.

`POST /v1/trade/incoming/{espn_transaction_id}/triage` accepts
`{state: new|seen|snoozed|dismissed, by, snooze_until?}`. Snoozing requires an aware
future timestamp; other states reject `snooze_until`. Returns the updated item
with 200. It is permitted in every inbox state and changes neither ESPN nor status.

### Review token

Decision, counter and retry each require `review_token`, copied from the listed row
the human reviewed. Missing or malformed tokens get 422. Under the offer lock, and
before any other check, claim, audit or broker access, core recomputes the token from
the row it just read `FOR UPDATE`. Any difference returns 409 "The offer changed since
it was reviewed" and nothing is queued. The 409 doesn't include the current token:
getting a new one takes a fresh read, so the human sees the offer again. Worker
refreshes take the same lock, so a refresh can't land between the check and the claim.

The token is SHA-256 of compact UTF-8 JSON:

```text
["trade-inbox-review-v1", league, season, our_team, transaction_id, proposer,
 fingerprint, sorted(send_ids), sorted(receive_ids), sorted(other_items), comment]
```

League and season are strings. Each list member is canonical JSON (sorted keys), so
item order doesn't matter. `comment` is the proposer's exact, untrimmed note, which the
worker fingerprint leaves out. Triage, refresh timestamps, `missed_reads`, player
names, `score`, `shadow_policy` and other score evidence are excluded. They change
without the trade changing, and accept and counter re-check score under the lock.
Change the version string whenever the covered fields change. Clients must treat the
token as opaque.

Because the token check comes first, replaying a request on an unchanged offer still
returns its duplicate receipt, and an unresolved publication still blocks with its
own 409. Once the offer has changed, any request gets the mismatch 409. The token is
stored on each claim (`publications[].review_token`, null on older claims) and in the
`bot_decisions` audit. It is not published to the broker.

`POST /v1/trade/incoming/{espn_transaction_id}/decision` accepts
`{action: accept|decline, approved_by, review_token, comment?}`. The comment is at most 2000
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
approval: {decision_key, approved_by, reviewed_note}}` plus tracing. Null response
comments are omitted. `reviewed_note` is always included, including on decision retries:
it is the exact proposer note from the locked inbox row whose review token was verified.
Whitespace is preserved, an empty string stays empty, and an absent note is null.
The worker compares this note against a fresh ESPN read before executing the response.
Scores and shadow policy are never published as approval metadata.

`POST /v1/trade/incoming/{espn_transaction_id}/counter` accepts
`{approved_by, review_token, week: 1..18}`. Only open, parsed, unexpired entries are eligible.
It requires a score matching the offer fingerprint and requested week, and a
nonempty internal mapping for every player on both sides. The `trade_negotiations`
body exactly follows worker `buildNegotiationPayload`: `{tracking_id, season,
week, offering_team_id, incoming_player_ids, outgoing_player_ids}` plus tracing.
Incoming maps receive IDs; outgoing maps send IDs. Season comes from the row.
This starts negotiation and staging for later outgoing approval. It does not
submit a counter or mark the original offer `countered`. The existing payload
does not carry the original ESPN transaction/thread ID.

`POST /v1/trade/incoming/{espn_transaction_id}/decision/retry` accepts
`{approved_by, review_token, of_claim_id, verified_not_answered?}`. It republishes an approved
response that the worker could not complete. `of_claim_id` is the listed
`response.id`. Under the offer lock it requires:
- the named row is the offer's only blocking worker response;
- its status is `rejected`, or `response_unknown` with `verified_not_answered: true`,
  meaning a human checked ESPN and the offer wasn't answered. `responding` (maybe in
  flight) and `responded` are never retried;
- the row's decision key still matches the stored offer, recomputed from its action
  and comment;
- the offer is `open` after a rejection, or `responding` with this decision after an
  unknown outcome, and otherwise actionable (parsed, more than five minutes from
  expiry; an accept still needs matching score evidence within the roster limit);
- every other publication is `failed_before_publish` or an earlier `queued` send of
  this same decision.

It publishes the original action, comment and decision key with a fresh
`tracking_id`, plus `retry: {of_claim_id, approved_by, verified_not_answered}`. The
worker re-checks all of this against a fresh ESPN read and supersedes the named row
in the same transaction as its new claim. The claim is stored under
`<decision_key>:retry:<of_claim_id>`, so the original receipt is kept, and a repeat
returns the retry's receipt. Receipts carry `retry_of`.

All three publication routes return 202 for a new broker-confirmed publication and 200
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

Other responses: 404 missing entry; 409 offer changed since review, unsafe/non-open state, unmapped players,
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
`fingerprint`, `review_token`, exact `payload`, and decision action/key where applicable.
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
Blocked or rejected worker execution does not cause core to republish automatically:
only an explicit human retry (above) does, and never for a response still in flight.

Reconcile using the stored payload, tracking ID, broker/worker logs, response rows,
Activity, and ESPN evidence. Reset a claim only after establishing that no action
was queued/sent and that no consumer can still execute it. An unknown result must
remain blocked. Any production write needs explicit operator authorization.

Worker follow-up: [counter thread linkage](https://github.com/cvillarr23/fantasy-browser-worker/issues/154).

The worker claims an offer atomically under the same `trade-inbox` lock (then the
decision-key lock), re-checking the stored entry. It reconciles an existing claim
by reading ESPN back before any fresh-offer check, and treats the same four
response statuses as blocking. The response flag remains off.

Tests mock both storage and AMQP; never probe decision/counter endpoints on the
deployed stack. Hash fixtures were generated from worker `origin/main` at
`6e8b3e45c889213879639712e67f2866630277c5`.
