---
title: Outgoing offers and evaluation
description: How raw evaluation, staged evidence and human approval produce outgoing trade requests.
sidebar_position: 7
---

## Evaluation paths

`POST /v1/trade/evaluate` and MCP `evaluate_proposed_trade` use `trade_service.evaluate_trade` with internal incoming/outgoing player IDs and exact season/week. Duplicate IDs and IDs on both sides are rejected.

```text
incoming_total = sum(available incoming VORP)
outgoing_total = sum(available outgoing VORP)
delta_vorp = incoming_total − outgoing_total
```

Absent rows contribute nothing to the sums and appear in `missing_player_ids`; therefore a positive partial total does not establish an improvement. This simple endpoint does not simulate roster slots, schedules or drops. Use [trade scoring](trade-scoring.md) for starter impact and weighted value.

The browser worker owns scanning, proposal generation, negotiation and staging. Core supplies deterministic context/evaluation and queues requests; language-model suggestions do not replace database VORP or approve an offer.

## Generated proposal selection

The [worker proposal generator](https://github.com/cvillarr23/fantasy-browser-worker/blob/main/src/bots/trade_proposal.js) chooses up to three deficit target teams and asks the LLM for at most six candidate packages. Player IDs must be distinct, correctly owned ESPN IDs, with no overlap; our outgoing side must include the target's deficit position. Core scores each package over four weeks before any pitch is written.

Eligibility requires at least one scored week, our raw lineup gain strictly above 0, counterparty raw lineup change at least 0, neither side over its roster limit and no unmapped traded player. It does not require a positive final weighted score or a complete horizon.

The [worker ranking](https://github.com/cvillarr23/fantasy-browser-worker/blob/main/src/lib/trade_scoring.js) puts packages with a numeric `final_score` ahead of packages using unweighted `our_lineup_gain`. Within each tier it ranks the chosen value divided by scored-week count, descending. Ties use more scored weeks, complete horizon, larger counterparty lineup change, then canonical package key. Thus weighted and unweighted values are never compared on the same scale. This differs from incoming inbox ranking, which uses total final score.

The winner's pitch uses deterministic score evidence and cited evidence IDs. Stored evaluation includes the exact request/response, target deficit, ranking basis/value/counts and pitch evidence paths (JSON Pointers into the score). No eligible package means no staged offer; unavailable scoring can fail the run rather than stage unscored claims.

## Staged offers and evidence

`GET /v1/trade/offers` reads staged execution-log offers. Their player IDs are ESPN IDs despite payload field names such as `incoming_player_ids`. `trade_offer_service` validates stored proposal evaluation version `proposal-evaluation-v1` before exposing it; absent or invalid evidence is unavailable, rather than inferred from an LLM message. This evidence is for review, not authorization.

`offer_key` is a canonical digest of the staged offer's identity and terms, shared with the worker. It includes the exact message and related incoming transaction ID for counters; thread root is tracing metadata outside the digest. Edits to keyed terms require a new key. The core checks configured league/team, both player sides and source before approval; conflicting staged copies fail closed.

## Human approval and publication

`POST /v1/trade/offers/{offer_key}/approve` requires a nonblank `approved_by` label. Unlike incoming decision routes, this route has no application-layer authentication and relies on the deployment trust boundary. The label is self-reported audit metadata. Under a PostgreSQL advisory lock, the core reads all rows for the key and accepts only an `awaiting_approval` offer. An earlier pending/submitting/submitted row blocks a new send.

The exact payload, approver, time, tracking ID and `bot_decisions` audit commit before broker access as `approval_pending`. Broker-confirmed publication to `trade_submissions` changes it to `approved` and returns 202. A repeated confirmed approval returns 200 with the original receipt and no new send. This receipt does not certify ESPN submission.

A proven pre-publication failure restores `awaiting_approval`; an ambiguous send or crash leaves `approval_pending` indefinitely. Reconcile tracking ID against broker, worker and ESPN evidence before releasing an uncertain claim. The worker independently applies its enable, dry-run, approval and execution gates.

[Incoming counters](incoming-trades.md) start negotiation and staging; they do not directly submit an outgoing counter. Implementation: [trade_service.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/services/trade_service.py), [trade_offer_service.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/services/trade_offer_service.py).
