"""
How likely the counterparty is to accept a trade, estimated from our side's evidence.

It ranks candidates and picks alternatives. It never gates: classification and the
prefilter ignore it, and it is not a term in final_score. Pure over the trade context,
whose `acceptance_evidence` (read per call) and per-team `activity` (from ESPN's
`transactionCounter`) it reads.

logit(p) = logit(base) + activity + fairness + market, then p is clamped to
[MIN_LIKELIHOOD, MAX_LIKELIHOOD].

- base: our outgoing offers' outcomes, shrunk twice. The league rate is
  (accepted_all + PRIOR_WEIGHT * PRIOR_ACCEPTANCE) / (offers_all + PRIOR_WEIGHT), and this
  manager's rate is (accepted + MANAGER_WEIGHT * league rate) / (offers + MANAGER_WEIGHT).
  An offer counts once its resolution is accepted or vetoed (the manager said yes),
  declined or expired. Withdrawn and cancelled are our own doing and don't count.
- activity: ACTIVITY_WEIGHT * clamp(z, ±Z_CLAMP), z being this manager's completed trades
  this season plus the offers they sent us, standardized over every other team.
- fairness: FAIRNESS_WEIGHT * (I - G) / (|I| + |G| + 1), from our lineup model: G is our
  lineup gain and I their lineup change. A trade lopsided in our favor scores lower.
- market: MARKET_WEIGHT * clamp(Δ / 100, ±1), Δ being ESPN percent owned of what they
  receive minus what they send. Managers see ESPN's market more than our VORP.

An input that can't be read contributes 0 and is named in unavailable_inputs, so a
likelihood is always returned (at worst the prior). Change ACCEPTANCE_MODEL_VERSION when
a constant or term changes.
"""

import math
import statistics

ACCEPTANCE_MODEL_VERSION = "acceptance-v1"
PRIOR_ACCEPTANCE = 0.15
PRIOR_WEIGHT = 4.0
MANAGER_WEIGHT = 3.0
ACTIVITY_WEIGHT = 0.35
FAIRNESS_WEIGHT = 1.5
MARKET_WEIGHT = 1.0
Z_CLAMP = 2.0
MIN_LIKELIHOOD = 0.01
MAX_LIKELIHOOD = 0.95

# Resolution outcomes that count as the manager's answer, and which of those are a yes.
COUNTED_OUTCOMES = ("accepted", "vetoed", "declined", "expired")
ACCEPTED_OUTCOMES = ("accepted", "vetoed")


def _logit(p):
    return math.log(p / (1 - p))


def _clamp(value, bound):
    return max(-bound, min(bound, value))


def _history(evidence, counterparty_team_id):
    outcomes = (evidence or {}).get("offer_outcomes") or {}

    def totals(counts):
        accepted = sum(counts.get(o, 0) for o in ACCEPTED_OUTCOMES)
        return accepted, sum(counts.get(o, 0) for o in COUNTED_OUTCOMES)

    league_accepted = league_offers = 0
    for counts in outcomes.values():
        accepted, offers = totals(counts)
        league_accepted += accepted
        league_offers += offers
    league_rate = (league_accepted + PRIOR_WEIGHT * PRIOR_ACCEPTANCE) / (league_offers + PRIOR_WEIGHT)
    mine = outcomes.get(counterparty_team_id, {})
    accepted, offers = totals(mine)
    rate = (accepted + MANAGER_WEIGHT * league_rate) / (offers + MANAGER_WEIGHT)
    return rate, {
        "offers": offers,
        "accepted": accepted,
        "declined": mine.get("declined", 0),
        "expired": mine.get("expired", 0),
        "league_offers": league_offers,
        "league_accepted": league_accepted,
        "league_rate": league_rate,
        "rate": rate,
    }


def _activity(context, our_team_id, counterparty_team_id):
    offers_to_us = ((context.get("acceptance_evidence") or {}).get("offers_to_us")) or {}

    def level(team_id):
        trades = (context["teams"][team_id].get("activity") or {}).get("trades")
        if trades is None:
            return None
        return trades + offers_to_us.get(team_id, 0)

    trades = (context["teams"][counterparty_team_id].get("activity") or {}).get("trades")
    inputs = {"trades": trades, "offers_to_us": offers_to_us.get(counterparty_team_id, 0), "z": None}
    levels = [
        value for team_id in context["teams"]
        if team_id != our_team_id and (value := level(team_id)) is not None
    ]
    mine = level(counterparty_team_id)
    if mine is None or len(levels) < 2:
        return None, inputs
    stddev = statistics.pstdev(levels)
    inputs["z"] = 0.0 if stddev == 0 else (mine - statistics.fmean(levels)) / stddev
    return ACTIVITY_WEIGHT * _clamp(inputs["z"], Z_CLAMP), inputs


def _fairness(our_gain, their_change):
    inputs = {"our_lineup_gain": our_gain, "counterparty_lineup_change": their_change, "balance": None}
    if our_gain is None or their_change is None:
        return None, inputs
    inputs["balance"] = (their_change - our_gain) / (abs(their_change) + abs(our_gain) + 1)
    return FAIRNESS_WEIGHT * inputs["balance"], inputs


def _market(send, receive):
    """send/receive are from our side, so the counterparty receives `send`."""

    def owned(players):
        values = [(p.get("espn") or {}).get("percent_owned") for p in players]
        return None if None in values else sum(values)

    they_receive, they_send = owned(send), owned(receive)
    inputs = {"percent_owned_received": they_receive, "percent_owned_sent": they_send}
    if they_receive is None or they_send is None:
        return None, inputs
    return MARKET_WEIGHT * _clamp((they_receive - they_send) / 100, 1.0), inputs


def estimate_acceptance(context, our_team_id, counterparty_team_id, send, receive, our_gain, their_change):
    """The acceptance block for one proposal. send/receive are trade-context roster
    players from our side; our_gain/their_change are G and I (None when unscored)."""
    rate, history = _history(context.get("acceptance_evidence"), counterparty_team_id)
    unavailable = [] if context.get("acceptance_evidence") is not None else ["offer_history"]
    if history["offers"] == 0 and "offer_history" not in unavailable:
        unavailable.append("no_offer_history")
    terms = {"base_logit": _logit(rate)}
    inputs = {"history": history}
    for name, (term, term_inputs) in (
        ("activity", _activity(context, our_team_id, counterparty_team_id)),
        ("fairness", _fairness(our_gain, their_change)),
        ("market", _market(send, receive)),
    ):
        inputs[name] = term_inputs
        terms[name] = term
        if term is None:
            unavailable.append(name)
    logit = sum(value for value in terms.values() if value is not None)
    likelihood = min(MAX_LIKELIHOOD, max(MIN_LIKELIHOOD, 1 / (1 + math.exp(-logit))))
    return {
        "model_version": ACCEPTANCE_MODEL_VERSION,
        "likelihood": likelihood,
        "terms": terms,
        "inputs": inputs,
        "unavailable_inputs": unavailable,
        "weights": {
            "prior_acceptance": PRIOR_ACCEPTANCE,
            "prior_weight": PRIOR_WEIGHT,
            "manager_weight": MANAGER_WEIGHT,
            "activity_weight": ACTIVITY_WEIGHT,
            "fairness_weight": FAIRNESS_WEIGHT,
            "market_weight": MARKET_WEIGHT,
            "z_clamp": Z_CLAMP,
        },
    }
