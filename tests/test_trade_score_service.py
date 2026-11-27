from contextlib import contextmanager
from copy import deepcopy
from unittest.mock import MagicMock, patch

import pytest

from src.schemas.trade_score import TradeScore
from src.services import trade_score_service as service
from src.services.trade_service import evaluate_trade

WEEKS = (5, 6)
ALL_TEAMS = {"KC", "BUF", "DAL", "PHI"}


def player(espn_id, pos, vorp, nfl_team="KC", slot="BE", injury=None, player_id="auto"):
    """vorp is one value for every horizon week, or a {week: value} dict."""
    by_week = vorp if isinstance(vorp, dict) else {week: vorp for week in WEEKS}
    return {
        "espn_id": espn_id,
        "player_id": f"s{espn_id}" if player_id == "auto" else player_id,
        "name": f"Player {espn_id}",
        "position": pos,
        "nfl_team": nfl_team,
        "slot": slot,
        "eligible_slots": [],
        "injury_status": injury,
        "metrics": {
            str(week): {
                "vorp_score": by_week.get(week),
                "projected_points": None if by_week.get(week) is None else 10.0,
            }
            for week in WEEKS
        },
    }


def week_row(week, opponent, z, status="scheduled"):
    return {
        "week": week,
        "matchup_period": week,
        "status": status,
        "opponent_team_id": opponent if status == "scheduled" else None,
        "opponent_name": f"Team {opponent}" if status == "scheduled" else None,
        "opponent_strength_z": z if status == "scheduled" else None,
    }


def team(team_id, roster, schedule):
    return {
        "team_id": team_id,
        "name": f"Team {team_id}",
        "roster": roster,
        "strength": {
            "points_per_week": None,
            "completed_matchups": 0,
            "completed_scoring_weeks": 0,
            "unavailable_history_periods": 0,
            "z_score": None,
        },
        "schedule": schedule,
        "schedule_coverage": {
            "scheduled_weeks": 0,
            "bye_weeks": 0,
            "unavailable_weeks": 0,
            "strength_known_weeks": 0,
        },
        "schedule_strength_z": None,
    }


@pytest.fixture
def context():
    """Us (1) start QB + RB + FLEX = 5 + 10 + 4 = 19; them (2) = 6 + 8 + 7 = 21.

    Week 5: we play team 3 (z +1.0), they play team 4 (z -1.0).
    Week 6: we play each other; their z is +0.5, ours +0.2.
    """
    return {
        "league_id": "123",
        "season": 2026,
        "as_of_week": 5,
        "start_week": 5,
        "end_week": 6,
        "regular_season_end_week": 14,
        "lineup_slot_counts": {"QB": 1, "RB": 1, "RB/WR/TE": 1, "BE": 3, "IR": 1},
        "roster_snapshot": {
            "kind": "current",
            "source": "ESPN mRoster",
            "fetched_at": "2026-10-06T12:00:00+00:00",
            "observed_week": 5,
            "matches_as_of_week": True,
        },
        "strength_metric": {
            "name": "completed_fantasy_points_per_week_z",
            "source": "ESPN completed regular-season matchup totals",
            "league_mean": 100.0,
            "league_stddev": 10.0,
            "teams_with_strength": 4,
        },
        "coverage": {"unmapped_espn_ids": [], "missing_metric_count": 0},
        "teams": {
            "1": team(
                "1",
                [
                    player("101", "QB", 5.0, slot="QB"),
                    player("102", "RB", 10.0, slot="RB"),
                    player("103", "RB", 4.0, slot="RB/WR/TE"),
                    player("104", "WR", 3.0),
                ],
                [week_row(5, "3", 1.0), week_row(6, "2", 0.5)],
            ),
            "2": team(
                "2",
                [
                    player("201", "QB", 6.0, nfl_team="BUF", slot="QB"),
                    player("202", "RB", 8.0, nfl_team="BUF", slot="RB"),
                    player("203", "WR", 7.0, nfl_team="BUF", slot="RB/WR/TE"),
                    player("204", "RB", 1.0, nfl_team="BUF"),
                ],
                [week_row(5, "4", -1.0), week_row(6, "1", 0.2)],
            ),
            "3": team("3", [player("301", "QB", 1.0)], [week_row(5, "1", 0.2)]),
            "4": team("4", [player("401", "QB", 1.0)], [week_row(5, "2", 0.5)]),
        },
    }


RAW = {
    "incoming_total": 7.0,
    "outgoing_total": 4.0,
    "delta_vorp": 3.0,
    "missing_player_ids": [],
}


def score(context, send=("103",), receive=("203",), us="1", them="2", playing=None, raw=RAW):
    playing = (
        {week: set(ALL_TEAMS) for week in range(context["start_week"], context["end_week"] + 1)}
        if playing is None
        else playing
    )
    return service.shape_trade_score(
        context, us, them, list(send), list(receive), dict(raw), playing
    )


def m(z):
    return 1 + service.STRENGTH_WEIGHT * max(-service.Z_CLAMP, min(service.Z_CLAMP, z))


def test_formula_separates_our_gain_counterparty_impact_and_schedule_terms(context):
    result = score(context)

    us, them = result["roster_impact"]["us"], result["roster_impact"]["counterparty"]
    assert [(w["week"], w["before"], w["after"], w["delta"]) for w in us["weeks"]] == [
        (5, 19.0, 22.0, 3.0),
        (6, 19.0, 22.0, 3.0),
    ]
    assert [w["delta"] for w in them["weeks"]] == [-3.0, -3.0]
    s = result["score"]
    assert s["our_lineup_gain"] == pytest.approx(6.0)
    assert s["counterparty_lineup_change"] == pytest.approx(-6.0)
    our_weighted = 3 * m(1.0) + 3 * m(0.5)
    assert s["our_weighted_gain"] == pytest.approx(our_weighted)
    assert s["our_schedule_adjustment"] == pytest.approx(our_weighted - 6.0)
    # Week 5 is indirect (they play team 4); week 6 is head-to-head, so their lost
    # point counts exactly like our own gained point that week.
    benefit = 3 * service.INDIRECT_FACTOR * m(-1.0) + 3 * m(0.5)
    assert s["counterparty_weighted_benefit"] == pytest.approx(benefit)
    assert s["final_score"] == pytest.approx(our_weighted + benefit)
    assert s["final_score_unavailable_reasons"] == []
    schedule = {row["week"]: row for row in result["schedule"]["weeks"]}
    assert schedule[6]["head_to_head"] is True
    assert schedule[5]["head_to_head"] is False
    assert result["horizon"] == {
        "start_week": 5,
        "end_week": 6,
        "scored_weeks": [5, 6],
        "unscored_weeks": [],
        "complete_horizon": True,
    }
    assert result["weights"] == {
        "strength_weight": service.STRENGTH_WEIGHT,
        "z_clamp": service.Z_CLAMP,
        "indirect_factor": service.INDIRECT_FACTOR,
        "ros_regular_weight": service.ROS_REGULAR_WEIGHT,
        "playoff_week_weight": service.PLAYOFF_WEEK_WEIGHT,
        "questionable_availability": service.QUESTIONABLE_AVAILABILITY,
        "doubtful_availability": service.DOUBTFUL_AVAILABILITY,
    }
    # No rest-of-season window: R is 0, not unavailable.
    assert s["rest_of_season_value"] == 0.0
    assert result["rest_of_season"]["weeks"] == []
    assert result["rest_of_season"]["costs_playoff_value"] is False
    TradeScore.model_validate(result)


def test_starters_and_exclusions_are_structured_evidence(context):
    week = score(context)["roster_impact"]["us"]["weeks"][0]
    assert {p["espn_id"] for p in week["starters_before"]} == {"101", "102", "103"}
    assert {p["espn_id"] for p in week["starters_after"]} == {"101", "102", "203"}
    assert week["held_slots"] == []
    assert week["excluded_players"] == []


def test_raw_vorp_is_evaluate_trade_unchanged_plus_identity_scope(context):
    result = score(context)
    assert result["raw_vorp"] == {**RAW, "week": 5, "unmapped_espn_ids": []}


def test_bench_blocked_raw_gain_is_not_lineup_gain(context):
    context["teams"]["2"]["roster"].append(player("205", "QB", 4.0, nfl_team="BUF"))
    result = score(context, send=("104",), receive=("205",))
    assert result["score"]["our_lineup_gain"] == 0.0
    assert result["score"]["counterparty_lineup_change"] == 0.0


def ranked(context, mutate):
    context = deepcopy(context)
    mutate(context)
    return score(context)["score"]["final_score"]


def set_z(team_id, week, z):
    def mutate(context):
        row = next(r for r in context["teams"][team_id]["schedule"] if r["week"] == week)
        row["opponent_strength_z"] = z

    return mutate


def test_our_gain_counts_more_ahead_of_stronger_opponents(context):
    assert ranked(context, set_z("1", 5, 1.5)) > ranked(context, set_z("1", 5, -1.5))


def test_weakening_counterparty_counts_more_ahead_of_their_tougher_matchups(context):
    assert ranked(context, set_z("2", 5, 1.5)) > ranked(context, set_z("2", 5, -1.5))


def test_head_to_head_weakening_outweighs_indirect_weakening(context):
    def no_h2h(context):
        context["teams"]["1"]["schedule"][1] = week_row(6, "4", 0.5)
        context["teams"]["2"]["schedule"][1] = week_row(6, "3", 0.5)

    assert ranked(context, lambda c: None) > ranked(context, no_h2h)


def test_strength_is_clamped(context):
    assert ranked(context, set_z("1", 5, 3.0)) == ranked(context, set_z("1", 5, 2.0))


def test_same_trade_scored_from_the_other_side_swaps_team_terms(context):
    ours = score(context)
    theirs = score(context, send=("203",), receive=("103",), us="2", them="1")
    assert theirs["roster_impact"]["us"]["weeks"] == ours["roster_impact"]["counterparty"]["weeks"]
    assert theirs["roster_impact"]["counterparty"]["weeks"] == ours["roster_impact"]["us"]["weeks"]
    assert theirs["score"]["our_lineup_gain"] == ours["score"]["counterparty_lineup_change"]
    assert theirs["score"]["counterparty_lineup_change"] == ours["score"]["our_lineup_gain"]


def test_week_without_traded_player_metrics_is_unscored_not_zero(context):
    context["teams"]["2"]["roster"][2]["metrics"]["6"]["vorp_score"] = None
    result = score(context)
    assert result["horizon"]["scored_weeks"] == [5]
    assert result["horizon"]["unscored_weeks"] == [
        {"week": 6, "reasons": ["missing_traded_player_metrics"]}
    ]
    assert result["horizon"]["complete_horizon"] is False
    assert result["score"]["our_lineup_gain"] == 3.0
    assert [w["week"] for w in result["roster_impact"]["us"]["weeks"]] == [5]


def test_unknown_strength_keeps_lineup_terms_but_withholds_weighted_scores(context):
    set_z("1", 5, None)(context)
    s = score(context)["score"]
    assert s["our_lineup_gain"] == 6.0
    assert s["our_weighted_gain"] is None
    assert s["our_schedule_adjustment"] is None
    assert s["counterparty_weighted_benefit"] is not None
    assert s["final_score"] is None
    assert s["final_score_unavailable_reasons"] == ["unknown_our_opponent_strength"]


def test_final_score_is_the_sum_of_in_final_components(context):
    s = score(context)["score"]
    components = s["components"]
    assert set(components) == {
        "our_weighted_gain", "counterparty_weighted_benefit", "rest_of_season", "roster_spot",
        "acceptance_likelihood",
    }
    # No rest-of-season window in this context: R is 0 and counts.
    assert components["rest_of_season"] == {
        "value": 0.0, "in_final_score": True, "unavailable_reasons": [],
    }
    assert components["roster_spot"] == {
        "value": s["roster_spot_value"], "in_final_score": True, "unavailable_reasons": [],
    }
    assert components["our_weighted_gain"] == {
        "value": s["our_weighted_gain"], "in_final_score": True, "unavailable_reasons": [],
    }
    assert components["counterparty_weighted_benefit"]["value"] == s["counterparty_weighted_benefit"]
    assert s["final_score"] == pytest.approx(
        sum(c["value"] for c in components.values() if c["in_final_score"])
    )


def test_component_without_value_withholds_final_and_lends_its_reasons(context):
    set_z("1", 5, None)(context)
    components = score(context)["score"]["components"]
    assert components["our_weighted_gain"] == {
        "value": None, "in_final_score": True,
        "unavailable_reasons": ["unknown_our_opponent_strength"],
    }
    assert components["counterparty_weighted_benefit"]["value"] is not None


def test_final_from_components_ignores_reported_only_terms():
    components = {
        "a": service._component(2.0, []),
        "b": service._component(None, ["no_b"], in_final_score=False),
        "c": service._component(-0.5, []),
    }
    assert service._final_from_components(components) == (1.5, [])
    components["a"] = service._component(None, ["no_a", "shared"])
    components["c"] = service._component(None, ["shared"])
    assert service._final_from_components(components) == (None, ["no_a", "shared"])


def test_unavailable_pairing_is_unknown_not_a_bye(context):
    context["teams"]["2"]["schedule"][0] = week_row(5, None, None, status="unavailable")
    result = score(context)
    row = result["schedule"]["weeks"][0]
    assert row["counterparty_weight"] is None
    assert result["score"]["counterparty_weighted_benefit"] is None
    assert result["score"]["final_score_unavailable_reasons"] == [
        "unknown_counterparty_opponent_strength"
    ]


def test_unmapped_traded_player_is_reported_and_unscored(context):
    context["teams"]["2"]["roster"][2]["player_id"] = None
    result = score(context)
    assert result["horizon"]["scored_weeks"] == []
    assert result["coverage"]["unmapped_traded_espn_ids"] == ["203"]
    assert result["raw_vorp"]["unmapped_espn_ids"] == ["203"]
    assert result["score"]["our_lineup_gain"] is None
    assert result["score"]["final_score_unavailable_reasons"] == ["no_scored_weeks"]


def test_untraded_player_without_metrics_is_excluded_and_flagged(context):
    context["teams"]["1"]["roster"][3]["metrics"]["5"]["vorp_score"] = None
    result = score(context)
    week = result["roster_impact"]["us"]["weeks"][0]
    assert week["excluded_players"] == [{"espn_id": "104", "reason": "no_metrics"}]
    assert result["coverage"]["partial_roster_weeks"] == [5]
    assert result["horizon"]["scored_weeks"] == [5, 6]


def test_kicker_slot_is_held_identically_before_and_after(context):
    context["lineup_slot_counts"]["K"] = 1
    context["teams"]["1"]["roster"].append(player("105", "K", None))
    context["teams"]["2"]["roster"].append(player("206", "K", 2.0, nfl_team="BUF"))
    result = score(context, send=("103",), receive=("206",))
    us = result["roster_impact"]["us"]["weeks"][0]
    them = result["roster_impact"]["counterparty"]["weeks"][0]
    assert us["held_slots"] == them["held_slots"] == ["K"]
    assert (us["before"], us["after"]) == (19.0, 18.0)
    assert (them["before"], them["after"]) == (21.0, 21.0)


def test_unsupported_lineup_slot_is_unscored(context):
    context["lineup_slot_counts"]["OP"] = 1
    result = score(context)
    assert result["horizon"]["unscored_weeks"] == [
        {"week": 5, "reasons": ["unsupported_lineup_slot"]},
        {"week": 6, "reasons": ["unsupported_lineup_slot"]},
    ]


def test_unfillable_skill_slot_is_unscored(context):
    context["teams"]["1"]["roster"][0]["metrics"]["5"]["vorp_score"] = None
    result = score(context)
    assert result["horizon"]["unscored_weeks"] == [
        {"week": 5, "reasons": ["lineup_unfillable"]}
    ]


def test_out_players_sit_only_in_the_as_of_week(context):
    context["teams"]["2"]["roster"][0]["injury_status"] = "OUT"
    context["teams"]["2"]["roster"].append(player("205", "QB", 2.0, nfl_team="BUF"))
    weeks = score(context)["roster_impact"]["counterparty"]["weeks"]
    assert weeks[0]["excluded_players"] == [{"espn_id": "201", "reason": "out"}]
    assert weeks[0]["before"] == 17.0
    assert weeks[1]["excluded_players"] == []
    assert weeks[1]["before"] == 21.0


def test_traded_player_on_nfl_bye_sits_and_the_week_is_scored(context):
    # His own team, so the rest of team 2 (BUF) still plays week 5.
    context["teams"]["2"]["roster"][2]["nfl_team"] = "PHI"
    context["teams"]["2"]["roster"][2]["metrics"]["5"]["vorp_score"] = None
    playing = {5: ALL_TEAMS - {"PHI"}, 6: set(ALL_TEAMS)}
    result = score(context, playing=playing)
    week = result["roster_impact"]["us"]["weeks"][0]
    assert week["excluded_players"] == [{"espn_id": "203", "reason": "nfl_bye"}]
    assert week["delta"] == -1.0
    assert result["horizon"]["scored_weeks"] == [5, 6]


def test_washington_matches_the_nfl_schedule_code(context):
    context["teams"]["2"]["roster"][2]["nfl_team"] = "WSH"
    context["teams"]["2"]["roster"][2]["metrics"]["5"]["vorp_score"] = None
    result = score(context, playing={5: set(ALL_TEAMS) | {"WAS"}, 6: set(ALL_TEAMS)})
    assert result["horizon"]["unscored_weeks"] == [
        {"week": 5, "reasons": ["missing_traded_player_metrics"]}
    ]


def test_week_without_any_metrics_says_so_even_when_traded_players_are_on_bye(context):
    # Both traded players are on NFL byes, so they pass the traded-player checks, but the
    # week was never refreshed: that is missing data, not a roster that can't be filled.
    for team_data in context["teams"].values():
        for p in team_data["roster"]:
            p["metrics"]["6"]["vorp_score"] = None
    playing = {5: set(ALL_TEAMS), 6: ALL_TEAMS - {"KC", "BUF"}}
    result = score(context, playing=playing)
    assert result["horizon"]["unscored_weeks"] == [
        {"week": 6, "reasons": ["no_week_metrics"]}
    ]


def test_nfl_schedule_unavailable_cannot_confirm_a_bye(context):
    context["teams"]["2"]["roster"][2]["metrics"]["5"]["vorp_score"] = None
    result = score(context, playing={5: None, 6: None})
    assert result["horizon"]["unscored_weeks"] == [
        {"week": 5, "reasons": ["schedule_unavailable"]}
    ]
    assert result["coverage"]["nfl_schedule_unavailable_weeks"] == [5, 6]


def test_fantasy_bye_week_has_zero_weight(context):
    context["teams"]["1"]["schedule"][0] = week_row(5, None, None, status="bye")
    result = score(context)
    assert result["schedule"]["weeks"][0]["our_multiplier"] == 0.0
    assert result["score"]["our_weighted_gain"] == pytest.approx(3 * m(0.5))


def test_bye_only_horizon_has_no_final_score(context):
    for team_id in ("1", "2"):
        context["teams"][team_id]["schedule"] = [
            week_row(week, None, None, status="bye") for week in WEEKS
        ]
    s = score(context)["score"]
    assert s["final_score"] is None
    assert s["final_score_unavailable_reasons"] == ["no_matchup_weeks"]


def test_empty_postseason_horizon_has_no_scores(context):
    context.update(start_week=15, end_week=14, as_of_week=15)
    for team_data in context["teams"].values():
        team_data["schedule"] = []
    result = score(context, playing={})
    assert result["horizon"]["scored_weeks"] == []
    assert result["horizon"]["complete_horizon"] is False
    assert result["score"]["our_lineup_gain"] is None
    assert result["score"]["final_score_unavailable_reasons"] == ["no_scored_weeks"]
    TradeScore.model_validate(result)


def test_uneven_trade_flags_roster_limit_without_simulating_drops(context):
    context["lineup_slot_counts"]["BE"] = 1
    result = score(context, send=("103", "104"), receive=("203",))
    us, them = result["roster_impact"]["us"], result["roster_impact"]["counterparty"]
    assert (us["roster_size_before"], us["roster_size_after"]) == (4, 3)
    assert (them["roster_size_before"], them["roster_size_after"]) == (4, 5)
    assert them["roster_limit"] == 4
    assert them["exceeds_roster_limit"] is True
    assert us["exceeds_roster_limit"] is False


# --- service entry point: validation, ownership and identity -----------------------


@pytest.fixture
def live(context):
    with patch.object(service, "get_league_trade_context", return_value=context) as ctx, \
         patch.object(service, "evaluate_trade", return_value=dict(RAW)) as evaluate, \
         patch.object(service, "load_teams_playing", return_value=set(ALL_TEAMS)) as playing:
        yield ctx, evaluate, playing


def call(**overrides):
    args = {
        "league_id": "123",
        "season": 2026,
        "week": 5,
        "horizon": 2,
        "our_team_id": "1",
        "counterparty_team_id": "2",
        "send_espn_ids": ["103"],
        "receive_espn_ids": ["203"],
    }
    args.update(overrides)
    return service.score_trade(**args)


def test_service_uses_shared_context_and_evaluates_mapped_internal_ids(live):
    ctx, evaluate, playing = live
    result = call()
    ctx.assert_called_once_with("123", 2026, 5, 2)
    evaluate.assert_called_once_with(["s203"], ["s103"], 2026, 5)
    assert [c.args for c in playing.call_args_list] == [(2026, 5), (2026, 6)]
    assert result["score"]["final_score"] is not None


def test_schedule_load_failure_is_unknown(live):
    live[2].side_effect = RuntimeError("nflverse down")
    result = call()
    assert result["coverage"]["nfl_schedule_unavailable_weeks"] == [5, 6]
    assert result["horizon"]["scored_weeks"] == [5, 6]


@pytest.mark.parametrize(
    "overrides",
    [
        {"send_espn_ids": []},
        {"receive_espn_ids": []},
        {"send_espn_ids": ["103", "103"]},
        {"send_espn_ids": ["103"], "receive_espn_ids": ["103"]},
        {"counterparty_team_id": "1"},
        {"counterparty_team_id": "abc"},
        {"send_espn_ids": ["s103"]},
        {"receive_espn_ids": ["0"]},
    ],
)
def test_malformed_proposals_fail_before_fetching(live, overrides):
    with pytest.raises(ValueError):
        call(**overrides)
    live[0].assert_not_called()


def test_unknown_team_is_invalid(live):
    with pytest.raises(ValueError, match="team 9"):
        call(counterparty_team_id="9")


@pytest.mark.parametrize(
    "overrides,message",
    [
        ({"send_espn_ids": ["203"], "receive_espn_ids": ["101"]}, "203 is on team 2"),
        ({"receive_espn_ids": ["301"]}, "301 is on team 3"),
        ({"receive_espn_ids": ["999"]}, "999 is not on any"),
    ],
)
def test_ownership_mismatches_are_conflicts(live, overrides, message):
    with pytest.raises(service.TradeOwnershipError, match=message):
        call(**overrides)
    live[1].assert_not_called()


def test_numeric_id_collision_resolves_only_in_espn_namespace(live, context):
    # Their receiver's internal id is "104", which is also our WR's ESPN id. Asking to
    # receive ESPN id "104" means our own player, never their internal player.
    context["teams"]["2"]["roster"][2]["player_id"] = "104"
    with pytest.raises(service.TradeOwnershipError, match="104 is on team 1"):
        call(receive_espn_ids=["104"])
    call()
    live[1].assert_called_once_with(["104"], ["s103"], 2026, 5)


def test_d_st_negative_espn_ids_are_accepted(live, context):
    context["teams"]["2"]["roster"].append(player("-16002", "D/ST", 1.0, nfl_team="BUF"))
    result = call(receive_espn_ids=["-16002"])
    assert result["receive"][0]["espn_id"] == "-16002"


@contextmanager
def fake_cursor(cursor):
    yield cursor


def test_raw_vorp_parity_with_real_evaluate_trade(context):
    cursor = MagicMock()
    cursor.fetchall.return_value = [("s203", 7.0), ("s103", 4.0)]
    with patch("src.services.trade_service.get_db_cursor", return_value=fake_cursor(cursor)):
        direct = evaluate_trade(["s203"], ["s103"], 2026, 5)
    cursor.fetchall.return_value = [("s203", 7.0), ("s103", 4.0)]
    with patch.object(service, "get_league_trade_context", return_value=context), \
         patch.object(service, "load_teams_playing", return_value=set(ALL_TEAMS)), \
         patch("src.services.trade_service.get_db_cursor", return_value=fake_cursor(cursor)):
        result = call()
    raw = dict(result["raw_vorp"])
    assert raw.pop("week") == 5
    assert raw.pop("unmapped_espn_ids") == []
    assert {k: (sorted(v) if isinstance(v, list) else v) for k, v in raw.items()} == {
        k: (sorted(v) if isinstance(v, list) else v) for k, v in direct.items()
    }


def test_bye_coverage_reports_crunch_weeks_a_trade_fixes(context):
    # Both our RBs share a week-6 bye, leaving the RB seat empty; two BUF RBs cover it.
    for p in context["teams"]["1"]["roster"][1:3]:
        p["nfl_team"] = "DAL"
        p["metrics"]["6"]["vorp_score"] = None
    playing = {5: set(ALL_TEAMS), 6: ALL_TEAMS - {"DAL"}}

    result = score(context, send=("104",), receive=("202", "204"), playing=playing)

    assert result["bye_coverage"]["us"] == {
        "crunch_weeks_before": [6], "crunch_weeks_after": [], "fixed": [6], "created": [],
    }
    assert result["bye_coverage"]["counterparty"]["created"] == []
    TradeScore.model_validate(result)


def test_bye_coverage_reports_a_crunch_a_trade_creates(context):
    # Trading away our only healthy RB depth leaves week 6's bye uncovered.
    context["teams"]["1"]["roster"][1]["nfl_team"] = "DAL"
    context["teams"]["1"]["roster"][1]["metrics"]["6"]["vorp_score"] = None
    playing = {5: set(ALL_TEAMS), 6: ALL_TEAMS - {"DAL"}}

    result = score(context, send=("103",), receive=("201",), playing=playing)

    assert result["bye_coverage"]["us"]["created"] == [6]


def with_rest_of_season(context, kinds, overrides=None):
    """Adds rest-of-season weeks ({week: kind}); every player keeps his week-5 metrics in
    them unless overrides[(espn_id, week)] gives a VORP (None drops the metrics)."""
    overrides = overrides or {}
    context["rest_of_season"] = {
        "weeks": [{"week": week, "kind": kind} for week, kind in kinds.items()],
        "playoff_weeks": [week for week, kind in kinds.items() if kind == "playoff"],
    }
    for team in context["teams"].values():
        for p in team["roster"]:
            for week in kinds:
                vorp = overrides.get((p["espn_id"], week), p["metrics"]["5"]["vorp_score"])
                p["metrics"][str(week)] = {
                    "vorp_score": vorp, "projected_points": None if vorp is None else 10.0,
                }
    return context


def all_playing(*weeks):
    return {week: set(ALL_TEAMS) for week in weeks}


def test_rest_of_season_weights_playoff_weeks_over_regular_weeks(context):
    horizon_only = score(deepcopy(context))["score"]
    # Week 7 looks like the horizon (+3 for us). In playoff week 15 the RB we send is worth
    # 12 and the WR we receive 0: our FLEX drops from 12 to our WR's 3, so -9.
    with_rest_of_season(context, {7: "regular", 15: "playoff"},
                        {("103", 15): 12.0, ("203", 15): 0.0})
    result = score(context, playing=all_playing(5, 6, 7, 15))

    ros = result["rest_of_season"]
    assert [(w["week"], w["kind"], w["weight"], w["our_delta"], w["counterparty_delta"])
            for w in ros["weeks"]] == [
        (7, "regular", service.ROS_REGULAR_WEIGHT, 3.0, -3.0),
        (15, "playoff", service.PLAYOFF_WEEK_WEIGHT, -9.0, 11.0),
    ]
    assert ros["scored_weeks"] == [7, 15]
    assert ros["complete"] is True
    assert ros["our_lineup_gain"] == pytest.approx(-6.0)
    assert ros["playoff_lineup_gain"] == pytest.approx(-9.0)
    assert ros["counterparty_lineup_change"] == pytest.approx(8.0)
    expected = 0.5 * 3.0 + 1.0 * -9.0
    assert ros["value"] == pytest.approx(expected)
    assert ros["costs_playoff_value"] is True
    s = result["score"]
    assert s["rest_of_season_value"] == pytest.approx(expected)
    assert s["components"]["rest_of_season"]["value"] == pytest.approx(expected)
    assert s["components"]["rest_of_season"]["in_final_score"] is True
    # G*/C* are untouched; only our R is added, never the counterparty's ROS change.
    assert s["our_weighted_gain"] == pytest.approx(horizon_only["our_weighted_gain"])
    assert s["counterparty_weighted_benefit"] == pytest.approx(
        horizon_only["counterparty_weighted_benefit"]
    )
    assert s["final_score"] == pytest.approx(horizon_only["final_score"] + expected)
    # The horizon and its lineups stay the horizon's.
    assert result["horizon"]["scored_weeks"] == [5, 6]
    assert [w["week"] for w in result["roster_impact"]["us"]["weeks"]] == [5, 6]
    TradeScore.model_validate(result)


def test_playoff_gain_does_not_flag_costs_playoff_value(context):
    with_rest_of_season(context, {15: "playoff"})
    result = score(context, playing=all_playing(5, 6, 15))
    assert result["rest_of_season"]["playoff_lineup_gain"] == pytest.approx(3.0)
    assert result["rest_of_season"]["costs_playoff_value"] is False
    assert result["score"]["rest_of_season_value"] == pytest.approx(3.0)


def test_rest_of_season_without_projections_is_unavailable_not_zero(context):
    horizon_only = score(deepcopy(context))["score"]["final_score"]
    overrides = {
        (p["espn_id"], week): None
        for team in context["teams"].values() for p in team["roster"] for week in (7, 15)
    }
    with_rest_of_season(context, {7: "regular", 15: "playoff"}, overrides)
    result = score(context, playing=all_playing(5, 6, 7, 15))

    ros = result["rest_of_season"]
    assert ros["value"] is None
    assert ros["unavailable_reasons"] == ["no_scored_weeks"]
    assert [w["week"] for w in ros["unscored_weeks"]] == [7, 15]
    assert all(w["reasons"] for w in ros["unscored_weeks"])
    assert ros["playoff_lineup_gain"] is None
    assert ros["costs_playoff_value"] is False
    assert result["score"]["rest_of_season_value"] is None
    assert result["score"]["components"]["rest_of_season"] == {
        "value": None, "in_final_score": False, "unavailable_reasons": ["no_scored_weeks"],
    }
    # A lagging far-week refresh never withholds the horizon score.
    assert result["score"]["final_score"] == pytest.approx(horizon_only)
    assert result["score"]["final_score_unavailable_reasons"] == []
    TradeScore.model_validate(result)


def test_partly_scored_rest_of_season_counts_only_its_scored_weeks(context):
    with_rest_of_season(context, {7: "regular", 15: "playoff"}, {("203", 15): None})
    result = score(context, playing=all_playing(5, 6, 7, 15))
    ros = result["rest_of_season"]
    assert ros["scored_weeks"] == [7]
    assert [w["week"] for w in ros["unscored_weeks"]] == [15]
    assert ros["complete"] is False
    assert ros["value"] == pytest.approx(0.5 * 3.0)
    assert ros["playoff_lineup_gain"] is None


def test_rest_of_season_week_without_nfl_schedule_follows_horizon_rules(context):
    # As in the horizon: with every metric present the week still scores, but a traded
    # player missing his projection can't be told apart from a bye, so it is unscored.
    with_rest_of_season(context, {15: "playoff", 16: "playoff"}, {("203", 16): None})
    result = score(context, playing={**all_playing(5, 6), 15: None, 16: None})
    assert result["rest_of_season"]["scored_weeks"] == [15]
    assert [w["week"] for w in result["rest_of_season"]["unscored_weeks"]] == [16]


def test_service_reads_the_nfl_schedule_through_the_last_rest_of_season_week(live, context):
    with_rest_of_season(context, {7: "regular", 8: "playoff"})
    result = call()
    assert [c.args for c in live[2].call_args_list] == [
        (2026, 5), (2026, 6), (2026, 7), (2026, 8),
    ]
    assert result["rest_of_season"]["scored_weeks"] == [7, 8]
# ---- injury timelines (src/injury_timeline), opted in by an `injury` key on the player ----


def injure(context, espn_id, status, return_week="none", slot=None):
    for t in context["teams"].values():
        for p in t["roster"]:
            if p["espn_id"] == espn_id:
                p["injury_status"] = status
                if return_week != "absent":
                    p["injury"] = {"expected_return_week": None if return_week == "none" else return_week,
                                   "return_date": "2026-10-11", "injury_type": "Ankle",
                                   "reported_at": "2026-10-05T12:00:00+00:00"}
                if slot:
                    p["slot"] = slot


def our_deltas(result):
    return [w["delta"] for w in result["roster_impact"]["us"]["weeks"]]


def test_out_player_counts_from_his_return_week(context):
    injure(context, "203", "OUT", return_week=6)
    result = score(context)
    # Week 5 he sits, so FLEX falls to our WR 104 (3.0): 18 - 19. Week 6 he's back: +3.
    assert our_deltas(result) == [-1.0, 3.0]
    week5 = result["roster_impact"]["us"]["weeks"][0]
    assert {"espn_id": "203", "reason": "out"} in week5["excluded_players"]
    assert result["injuries"]["203"]["weeks_out"] == [5]
    assert result["injuries"]["203"]["expected_return_week"] == 6
    TradeScore.model_validate(result)


def test_injured_reserve_without_a_date_sits_the_whole_horizon(context):
    injure(context, "203", "INJURY_RESERVE")
    assert our_deltas(score(context)) == [-1.0, -1.0]


def test_injured_reserve_without_a_date_counts_in_rest_of_season_weeks(context):
    # He sits the horizon only: week 7 and playoff week 15 score him like a healthy player.
    with_rest_of_season(context, {7: "regular", 15: "playoff"})
    injure(context, "203", "INJURY_RESERVE")
    result = score(context, playing=all_playing(5, 6, 7, 15))
    assert our_deltas(result) == [-1.0, -1.0]
    assert [w["our_delta"] for w in result["rest_of_season"]["weeks"]] == [3.0, 3.0]
    assert result["injuries"]["203"]["weeks_out"] == [5, 6]
    TradeScore.model_validate(result)


def test_a_dated_return_after_the_horizon_holds_into_rest_of_season_weeks(context):
    with_rest_of_season(context, {7: "regular", 15: "playoff"})
    injure(context, "203", "OUT", return_week=15)
    result = score(context, playing=all_playing(5, 6, 7, 15))
    assert [w["our_delta"] for w in result["rest_of_season"]["weeks"]] == [-1.0, 3.0]
    # weeks_out lists every scored week he misses, horizon and rest of season alike.
    assert result["injuries"]["203"]["weeks_out"] == [5, 6, 7]


def test_player_without_an_injury_key_keeps_the_as_of_week_only_rule(context):
    # Callers that don't load injuries (waivers, outlooks) see the pre-timeline behaviour.
    injure(context, "203", "INJURY_RESERVE", return_week="absent")
    assert our_deltas(score(context)) == [-1.0, 3.0]


def test_questionable_discounts_positive_vorp_in_the_as_of_week_only(context):
    injure(context, "203", "QUESTIONABLE")
    result = score(context)
    q = service.QUESTIONABLE_AVAILABILITY
    week5, week6 = result["roster_impact"]["us"]["weeks"]
    assert week5["delta"] == pytest.approx(7.0 * q - 4.0)
    assert week6["delta"] == pytest.approx(3.0)
    assert week5["injury_adjustments"] == [{"espn_id": "203", "factor": q, "reason": "questionable"}]
    assert week6["injury_adjustments"] == []
    # The counterparty's "before" lineup starts him discounted too.
    their5 = result["roster_impact"]["counterparty"]["weeks"][0]
    assert their5["before"] == pytest.approx(6.0 + 8.0 + 7.0 * q)
    assert result["injuries"]["203"]["as_of_week_availability"] == q
    TradeScore.model_validate(result)


def test_a_risk_tag_never_raises_a_negative_vorp(context):
    context["teams"]["1"]["roster"][3]["metrics"]["5"]["vorp_score"] = -2.0
    injure(context, "104", "DOUBTFUL")
    week5 = score(context)["roster_impact"]["us"]["weeks"][0]
    assert week5["injury_adjustments"] == []


def test_healthy_traded_players_report_a_clean_timeline(context):
    result = score(context)
    assert result["injuries"]["103"] == {
        "injury_status": None, "slot": "RB/WR/TE", "return_date": None,
        "expected_return_week": None, "injury_type": None, "reported_at": None,
        "weeks_out": [], "as_of_week_availability": 1.0,
    }


# --- roster spot: the spot a 2-for-1 frees and the drop a 1-for-2 forces -------------------


def with_free_agents(context, *free_agents):
    context = deepcopy(context)
    context["free_agents"] = [
        {**fa, "slot": None} for fa in free_agents
    ]
    return context


def with_roster_size(context, size):
    """Pads our roster with zero-value bench players up to size (the limit here is 6)."""
    context = deepcopy(context)
    roster = context["teams"]["1"]["roster"]
    roster += [player(f"9{len(roster) + i}0", "TE", 0.0) for i in range(size - len(roster))]
    return context


def test_two_for_one_values_the_freed_spot_as_the_best_free_agents_lineup_gain(context):
    context = with_free_agents(
        with_roster_size(context, 6),
        player("9001", "RB", 8.0),  # beats the 7.0 WR we receive in the FLEX slot: +1 a week
        player("9002", "RB", 2.0),  # would only sit on the bench: worth nothing
        player("9003", "WR", 6.5),
    )
    result = score(context, send=("103", "104"), receive=("203",))

    spot = result["roster_spot"]
    assert spot["kind"] == "freed_spot"
    assert spot["player"]["espn_id"] == "9001"
    assert [w["delta"] for w in spot["weeks"]] == [1.0, 1.0]
    assert spot["total"] == pytest.approx(2.0)
    s = result["score"]
    assert s["roster_spot_value"] == pytest.approx(m(1.0) + m(0.5))
    # Reported separately and counted in the final score.
    assert s["final_score"] == pytest.approx(
        s["our_weighted_gain"] + s["counterparty_weighted_benefit"] + s["roster_spot_value"]
    )
    TradeScore.model_validate(result)


def test_freed_spot_is_worth_nothing_when_no_free_agent_would_start(context):
    context = with_free_agents(context, player("9002", "RB", 2.0), player("9004", "QB", 1.0))
    result = score(context, send=("103", "104"), receive=("203",))

    assert result["score"]["roster_spot_value"] == 0.0
    assert result["roster_spot"]["player"] is None


def test_freed_spot_ignores_a_free_agent_on_bye_that_week(context):
    on_bye = player("9005", "RB", {5: 30.0, 6: 30.0}, nfl_team="NYJ")
    context = with_free_agents(context, on_bye)
    playing = {week: set(ALL_TEAMS) for week in WEEKS}
    result = score(context, send=("103", "104"), receive=("203",), playing=playing)

    assert result["score"]["roster_spot_value"] == 0.0
    assert result["roster_spot"]["player"] is None


def test_a_spot_that_was_already_open_is_not_credited_to_the_trade(context):
    # 5 of 6 spots: a 2-for-1 ends at 4, but we could add the free agent without trading.
    context = with_free_agents(with_roster_size(context, 5), player("9001", "RB", 8.0))
    result = score(context, send=("103", "104"), receive=("203",))

    assert result["roster_impact"]["us"]["roster_size_before"] == 5
    assert result["roster_spot"]["kind"] == "none"
    assert result["score"]["roster_spot_value"] == 0.0


def test_empty_free_agent_pool_is_unavailable_not_zero(context):
    context = with_free_agents(with_roster_size(context, 6))
    assert context["free_agents"] == []
    result = score(context, send=("103", "104"), receive=("203",))

    s = result["score"]
    assert s["roster_spot_value"] is None and s["final_score"] is None
    assert "roster_spot_value_unavailable" in s["final_score_unavailable_reasons"]
    assert result["roster_spot"]["unavailable_reason"] == "free_agents_unavailable"
    TradeScore.model_validate(result)


def test_one_for_one_has_no_roster_spot_term(context):
    context = with_free_agents(context, player("9001", "RB", 8.0))
    result = score(context)

    assert result["score"]["roster_spot_value"] == 0.0
    assert result["roster_spot"] == {
        "kind": "none", "player": None, "weeks": [], "total": 0.0,
        "candidates_considered": 0, "unavailable_reason": None,
    }
    s = result["score"]
    assert s["final_score"] == pytest.approx(
        s["our_weighted_gain"] + s["counterparty_weighted_benefit"]
    )


def test_missing_free_agent_pool_withholds_the_final_score_instead_of_zeroing(context):
    assert "free_agents" not in context
    result = score(with_roster_size(context, 6), send=("103", "104"), receive=("203",))

    s = result["score"]
    assert s["roster_spot_value"] is None and s["final_score"] is None
    assert "roster_spot_value_unavailable" in s["final_score_unavailable_reasons"]
    assert result["roster_spot"]["unavailable_reason"] == "free_agents_unavailable"
    TradeScore.model_validate(result)


def test_one_for_two_that_overfills_the_roster_charges_the_cheapest_drop(context):
    # Limit is 6 (QB, RB, FLEX, 3 BE). Six rostered players plus one net add is seven.
    context = deepcopy(context)
    context["teams"]["1"]["roster"] += [player("105", "WR", 0.5), player("106", "TE", 0.4)]
    result = score(context, send=("103",), receive=("203", "204"))

    assert result["roster_impact"]["us"]["exceeds_roster_limit"] is True
    spot = result["roster_spot"]
    assert spot["kind"] == "forced_drop"
    assert spot["total"] <= 0
    # A zero-cost bench drop exists, so the lineup doesn't lose anything here.
    assert result["score"]["roster_spot_value"] == pytest.approx(0.0)
    assert spot["player"]["espn_id"] in {"104", "105", "106", "204"}


def test_forced_drop_costs_the_lineup_when_every_player_starts_in_some_week(context):
    context = deepcopy(context)
    context["lineup_slot_counts"] = {"RB": 1, "WR": 1, "RB/WR/TE": 1, "BE": 0, "IR": 1}
    # Week 5 everyone plays; week 6 KC is idle. Each of the five players we'd hold after the
    # trade starts in at least one of the two weeks, so no drop is free.
    context["teams"]["1"]["roster"] = [
        player("101", "RB", 9.0, nfl_team="KC", slot="RB"),
        player("102", "WR", 7.0, nfl_team="DAL", slot="WR"),
        player("103", "RB", 8.0, nfl_team="DAL", slot="RB/WR/TE"),
        player("104", "TE", 0.1, nfl_team="DAL"),
    ]
    context["teams"]["2"]["roster"] = [
        player("211", "WR", 10.0, nfl_team="KC"),
        player("212", "WR", 5.0, nfl_team="BUF"),
        player("213", "RB", 3.0, nfl_team="BUF"),
        player("214", "RB", 3.0, nfl_team="DAL"),
        player("215", "WR", 2.0, nfl_team="BUF"),
        player("216", "WR", 2.0, nfl_team="DAL"),
    ]
    playing = {5: set(ALL_TEAMS), 6: {"BUF", "DAL", "PHI"}}
    result = score(context, send=("104",), receive=("211", "212"), playing=playing)

    spot = result["roster_spot"]
    assert spot["kind"] == "forced_drop"
    assert spot["total"] < 0
    assert all(w["delta"] <= 0 for w in spot["weeks"])
    assert result["score"]["roster_spot_value"] < 0
    assert result["score"]["roster_spot_value"] == pytest.approx(
        sum(w["delta"] * m_ for w, m_ in zip(spot["weeks"], (m(1.0), m(0.5))))
    )


def test_acceptance_is_evidence_and_leaves_final_score_alone(context):
    from src.services import trade_acceptance

    baseline = score(context)
    context["acceptance_evidence"] = {"offer_outcomes": {"2": {"declined": 3}}, "offers_to_us": {}}
    result = score(context)
    estimate = result["acceptance"]
    component = result["score"]["components"]["acceptance_likelihood"]
    assert component == {
        "value": estimate["likelihood"], "in_final_score": False, "unavailable_reasons": [],
    }
    without = lambda terms: {**terms, "components": {
        k: v for k, v in terms["components"].items() if k != "acceptance_likelihood"}}
    assert without(result["score"]) == without(baseline["score"])
    assert estimate["model_version"] == trade_acceptance.ACCEPTANCE_MODEL_VERSION
    assert estimate["likelihood"] < baseline["acceptance"]["likelihood"]
    assert estimate["inputs"]["fairness"] == {
        "our_lineup_gain": result["score"]["our_lineup_gain"],
        "counterparty_lineup_change": result["score"]["counterparty_lineup_change"],
        "balance": estimate["inputs"]["fairness"]["balance"],
    }
    # A context without the per-call evidence (the candidate job's tests) still estimates.
    assert "offer_history" in baseline["acceptance"]["unavailable_inputs"]
