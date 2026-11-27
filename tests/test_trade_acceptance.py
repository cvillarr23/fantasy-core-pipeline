import math

import pytest

from src.schemas.trade_score import AcceptanceEstimate
from src.services import trade_acceptance as acceptance


def team(trades=None):
    return {"activity": None if trades is None else {"trades": trades, "acquisitions": 0}}


def player(percent_owned):
    return {"espn": None if percent_owned is None else {"percent_owned": percent_owned}}


def context(outcomes=None, offers_to_us=None, trades=None, evidence=True, team_count=4):
    trades = trades or {}
    return {
        "teams": {str(i): team(trades.get(str(i))) for i in range(1, team_count + 1)},
        "acceptance_evidence": {
            "offer_outcomes": outcomes or {}, "offers_to_us": offers_to_us or {},
        } if evidence else None,
    }


def estimate(ctx=None, send=(50.0,), receive=(50.0,), gain=5.0, change=5.0, them="2"):
    return acceptance.estimate_acceptance(
        ctx or context(), "1", them, [player(p) for p in send], [player(p) for p in receive],
        gain, change,
    )


def test_without_any_evidence_the_likelihood_is_the_prior():
    result = estimate(context(evidence=False), send=(None,), gain=None, change=None)
    assert result["likelihood"] == pytest.approx(acceptance.PRIOR_ACCEPTANCE)
    assert result["unavailable_inputs"] == ["offer_history", "activity", "fairness", "market"]
    assert result["terms"]["activity"] is None and result["terms"]["market"] is None
    AcceptanceEstimate.model_validate(result)


def test_no_history_yet_is_named_but_still_uses_the_prior():
    result = estimate()
    assert result["unavailable_inputs"] == ["no_offer_history", "activity"]
    assert result["inputs"]["history"]["rate"] == pytest.approx(acceptance.PRIOR_ACCEPTANCE)


def test_history_is_shrunk_toward_the_league_rate():
    history = estimate(context({"2": {"accepted": 2}}))["inputs"]["history"]
    league = (2 + acceptance.PRIOR_WEIGHT * acceptance.PRIOR_ACCEPTANCE) / (2 + acceptance.PRIOR_WEIGHT)
    assert history["league_rate"] == pytest.approx(league)
    assert history["rate"] == pytest.approx(
        (2 + acceptance.MANAGER_WEIGHT * league) / (2 + acceptance.MANAGER_WEIGHT)
    )
    # Another manager who never saw an offer inherits the league rate, not the raw prior.
    other = estimate(context({"2": {"accepted": 2}}), them="3")["inputs"]["history"]
    assert other["rate"] == pytest.approx(league) and other["offers"] == 0


def test_declines_and_expiries_lower_it_and_our_own_withdrawals_dont_count():
    base = estimate()["likelihood"]
    assert estimate(context({"2": {"declined": 2, "expired": 1}}))["likelihood"] < base
    withdrawn = estimate(context({"2": {"withdrawn": 3, "cancelled": 1}}))
    assert withdrawn["likelihood"] == pytest.approx(base)
    assert withdrawn["inputs"]["history"]["offers"] == 0


def test_vetoed_counts_as_the_manager_saying_yes():
    vetoed = estimate(context({"2": {"vetoed": 1}}))["inputs"]["history"]
    assert vetoed["accepted"] == 1 and vetoed["offers"] == 1


def test_active_traders_rank_higher_and_offers_to_us_count_as_activity():
    trades = {"1": 9, "2": 3, "3": 0, "4": 0}
    active = estimate(context(trades=trades))
    quiet = estimate(context(trades=trades), them="3")
    assert active["inputs"]["activity"]["z"] > 0 > quiet["inputs"]["activity"]["z"]
    assert active["likelihood"] > quiet["likelihood"]
    # Our own team's trades don't move the league mean.
    assert active["inputs"]["activity"]["z"] == pytest.approx(math.sqrt(2))
    nudged = estimate(context(trades=trades, offers_to_us={"3": 2}), them="3")
    assert nudged["likelihood"] > quiet["likelihood"]


def test_activity_is_clamped_and_needs_the_counterpartys_counter():
    # Standardized over the other teams, |z| <= sqrt(others - 1), so the clamp (2) can only
    # fire with at least 6 other teams; use 8 teams in all (7 others, max |z| = sqrt(6)).
    trades = {"2": 1000, **{str(i): 0 for i in range(3, 9)}}
    result = estimate(context(trades=trades, team_count=8))
    assert result["inputs"]["activity"]["z"] == pytest.approx(math.sqrt(6))
    assert result["inputs"]["activity"]["z"] > acceptance.Z_CLAMP
    assert result["terms"]["activity"] == pytest.approx(
        acceptance.ACTIVITY_WEIGHT * acceptance.Z_CLAMP)
    quiet = {"2": 0, "3": 1000, **{str(i): 1000 for i in range(4, 9)}}
    low = estimate(context(trades=quiet, team_count=8))
    assert low["inputs"]["activity"]["z"] < -acceptance.Z_CLAMP
    assert low["terms"]["activity"] == pytest.approx(
        -acceptance.ACTIVITY_WEIGHT * acceptance.Z_CLAMP)
    missing = estimate(context(trades={"3": 1, "4": 2}))
    assert missing["terms"]["activity"] is None and "activity" in missing["unavailable_inputs"]


def test_a_trade_lopsided_in_our_favor_is_less_likely():
    even = estimate(gain=4.0, change=4.0)
    ours = estimate(gain=10.0, change=0.0)
    theirs = estimate(gain=1.0, change=9.0)
    assert theirs["likelihood"] > even["likelihood"] > ours["likelihood"]
    assert even["terms"]["fairness"] == 0.0
    assert -acceptance.FAIRNESS_WEIGHT < ours["terms"]["fairness"] < 0


def test_market_term_reads_what_they_receive_against_what_they_send():
    # From our side: send is what they receive.
    better_name = estimate(send=(90.0,), receive=(40.0,))
    worse_name = estimate(send=(40.0,), receive=(60.0, 30.0))
    assert better_name["terms"]["market"] == pytest.approx(0.5 * acceptance.MARKET_WEIGHT)
    assert worse_name["terms"]["market"] == pytest.approx(-0.5 * acceptance.MARKET_WEIGHT)
    clamped = estimate(send=(99.0, 99.0), receive=(1.0,))
    assert clamped["terms"]["market"] == pytest.approx(acceptance.MARKET_WEIGHT)
    unknown = estimate(send=(90.0,), receive=(None,))
    assert unknown["terms"]["market"] is None and "market" in unknown["unavailable_inputs"]


def test_likelihood_stays_within_bounds():
    high = estimate(
        context({"2": {"accepted": 50}}, trades={"2": 40, "3": 0, "4": 0}),
        send=(99.0, 99.0), receive=(1.0,), gain=0.0, change=20.0,
    )
    low = estimate(
        context({"2": {"declined": 50}}, trades={"2": 0, "3": 40, "4": 40}),
        send=(1.0,), receive=(99.0, 99.0), gain=20.0, change=-20.0,
    )
    assert high["likelihood"] == acceptance.MAX_LIKELIHOOD
    assert low["likelihood"] == acceptance.MIN_LIKELIHOOD
    AcceptanceEstimate.model_validate(high)
