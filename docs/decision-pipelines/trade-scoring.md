---
title: Trade scoring
description: How optimal starter changes and fantasy schedules produce G*, C* and the final score.
sidebar_position: 6
---

## Inputs and before/after lineups

`POST /v1/trade/score` and MCP `score_trade_proposal` use `trade_score_service.score_trade`. Inputs are league, season, week, horizon (default four), our team, counterparty, send ESPN IDs and receive ESPN IDs. Both sides must be nonempty with distinct IDs, no overlap and different teams. IDs must be owned by the sending team's current ESPN roster; ownership failure returns 409.

For each horizon week, exchange players in the [current trade-context snapshot](trade-context.md) and solve each team's optimal starters before and after using exact-week VORP and the lineup ILP. Bench players and IR supply no standalone value. The trade scorer does not pass the lineup-only defense multiplier.

For the as-of week, ESPN `OUT`, `INJURY_RESERVE` and IR-slot players are excluded. Future weeks do not assume current injuries/IR persist. Other exclusions are unmapped identity, unknown position, missing metrics and confirmed NFL bye. K/D-ST slots unfillable on either side are held on both, preventing an artificial gain from a data gap.

## Formula and signs

All signs are from our perspective. Let `ΔL_us,w` and `ΔL_them,w` be after minus before optimal starter VORP in week w.

```text
G = Σ ΔL_us,w
I = Σ ΔL_them,w
m(z) = 1 + 0.25 × clamp(z, −2, 2)
G* = Σ ΔL_us,w × m(our fantasy opponent's z)
C* = Σ −ΔL_them,w × counterparty_weight_w
final_score = G* + C*
our_schedule_adjustment = G* − G
```

`m` ranges from 0.5 to 1.5. When the teams face each other, counterparty weight equals our opponent multiplier. Otherwise it is `0.25 × m(their opponent's z)`; weakening another team's starters contributes only indirect value. A fantasy bye weighs 0. Unknown pairing or strength yields an unavailable weight rather than 1.

For example, our starter gain +4 against z=1 gives G*=5. Their gain +2 against a different z=0 opponent gives C*=−0.5 and final score 4.5. If we face each other and their z=1, C*=−2.5 and final score 2.5. Helping their starters reduces our score.

There is no built-in score threshold authorizing a trade. The [shadow policy](incoming-trades.md#shadow-policy) records hypothetical threshold verdicts; human approval and worker execution gates are separate.

## Coverage and roster impact

A week is unscored if a traded player is unmapped, lacks position/required metrics, neither team has usable week metrics, a starter slot is unsupported or either lineup is unfillable. A traded player's confirmed NFL bye or as-of-week OUT exclusion is legitimate exclusion, not missing evidence. Missing metrics without a readable NFL schedule cannot be classified as a bye.

Untraded players with missing mapping/position/metrics can be excluded while a week still scores; `coverage.partial_roster_weeks` records this. No prior-week metric is substituted. `horizon.complete_horizon` means no unscored horizon weeks; it does not certify complete roster coverage or known opponent strength.

Raw G/I sum only scored weeks and are null when none score. Any unknown weight in those scored weeks withholds that entire weighted term; `final_score` requires both G* and C*. An empty or entirely fantasy-bye-weighted scored horizon has no final score. Inspect `unscored_weeks[].reasons` and `final_score_unavailable_reasons`; a numeric partial-horizon score is not a full-horizon estimate.

`roster_impact` contains before/after starter lists and totals, deltas, held slots, exclusions and non-IR roster sizes. The limit sums configured non-IR slots. Unequal trades can exceed it; the scorer reports `exceeds_roster_limit` without simulating drops or withholding the numeric score.

`raw_vorp` preserves [single-week evaluation](outgoing-trades.md): mapped incoming total minus outgoing total, missing IDs and unmapped ESPN IDs. It is independent evidence, not G* or inbox ranking.

Implementation: [trade_score_service.py](https://github.com/cvillarr23/fantasy-core-pipeline/blob/main/src/services/trade_score_service.py).
