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
    }
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
    context["teams"]["2"]["roster"][2]["metrics"]["5"]["vorp_score"] = None
    playing = {5: ALL_TEAMS - {"BUF"}, 6: set(ALL_TEAMS)}
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
