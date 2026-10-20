---
title: Incoming trade inbox API
description: Entry point for the incoming trade decision and API contract.
sidebar_position: 20
---

The full contract now lives in [Incoming trade API reference](decision-pipelines/incoming-trade-api.md), alongside [trade context](decision-pipelines/trade-context.md), [scoring](decision-pipelines/trade-scoring.md) and [outgoing approval](decision-pipelines/outgoing-trades.md).


Outgoing proposal approval accepts `{approved_by, message?}` at
`POST /v1/trade/offers/{offer_key}/approve`. A changed `message` creates a new
`offer_key`, returned in the receipt, and supersedes the staged draft. Text must
contain a non-whitespace character and be at most 1000 characters; exact text is
preserved. Missing, null or unchanged text keeps existing approval behavior.
Repeating an edited approval returns 200 with `duplicate: true`; a different edit
after approval returns 409.

`GET /v1/trade/offers` includes `drafted_message` and `message_edited` (null/false
for unedited offers). Edited offers retain the generated draft and identify the
editor in `approval.approved_by`. `evaluation.pitch` remains generated evidence;
human-edited messages are not checked against score evidence. See
[outgoing approval](decision-pipelines/outgoing-trades.md#what-happens-when-you-approve)
for transaction, retry and publication details.
