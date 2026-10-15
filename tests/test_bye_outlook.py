from unittest.mock import patch

import pytest

from src.optimization import bye_outlook as pure
from src.services import roster_outlook_service as outlook

SLOTS = {"QB": 1, "RB": 2, "RB/WR/TE": 1, "BE": 4, "IR": 1}
WEEKS = [5, 6, 7, 8]
ALL = {"KC", "BUF", "DAL"}


def player(espn_id, pos, vorp, nfl_team="KC", bye=None, slot="BE"):
    """vorp for every week except `bye`, where the player has no row."""
    return {
        "espn_id": espn_id, "player_id": f"s{espn_id}", "name": f"P{espn_id}",
        "position": pos, "nfl_team": nfl_team, "slot": slot, "injury_status": None,
        "metrics": {str(w): {"vorp_score": None if w == bye else vorp} for w in WEEKS},
    }


def playing(byes):
    """byes: {week: {teams idle}}."""
    return {w: ALL - byes.get(w, set()) for w in WEEKS}


ROSTER = [
    player("1", "QB", 6.0),
    player("2", "RB", 10.0, nfl_team="BUF", bye=7),
    player("3", "RB", 8.0, nfl_team="BUF", bye=7),
    player("4", "WR", 5.0),
    player("5", "RB", 2.0),
]


def test_a_week_with_stacked_byes_is_a_crunch_with_its_byes_and_shortfall():
    result = pure.build_bye_outlook(ROSTER, WEEKS, 5, SLOTS, playing({7: {"BUF"}}))

    by_week = {w["week"]: w for w in result["weeks"]}
    assert result["crunch_weeks"] == [7]
    assert by_week[7]["on_bye"] == ["2", "3"]
    assert by_week[7]["unfilled_slots"] == ["RB"]
    assert by_week[7]["shortfall"] == pytest.approx(29.0 - 13.0)
    assert not by_week[6]["crunch"]


def test_a_single_bye_absorbed_by_depth_is_not_a_crunch():
    roster = ROSTER[:2] + [player("3", "RB", 8.0), ROSTER[3], player("5", "RB", 7.5)]

    result = pure.build_bye_outlook(roster, WEEKS, 5, SLOTS, playing({7: {"BUF"}}))

    assert result["crunch_weeks"] == []
    assert {w["week"]: w for w in result["weeks"]}[7]["on_bye"] == ["2"]


def test_a_dip_without_any_bye_is_not_blamed_on_byes():
    roster = [player("1", "QB", 6.0), player("2", "RB", 10.0)]
    roster[1]["metrics"]["7"]["vorp_score"] = 1.0  # a tough matchup, not a bye

    result = pure.build_bye_outlook(roster, WEEKS, 5, {"QB": 1, "RB": 1}, playing({}))

    assert result["crunch_weeks"] == []


def test_unknown_schedule_reports_unknowns_not_byes():
    result = pure.build_bye_outlook(ROSTER, WEEKS, 5, SLOTS, {w: None for w in WEEKS})

    week7 = {w["week"]: w for w in result["weeks"]}[7]
    assert week7["on_bye"] == []
    assert week7["unknown"] == ["2", "3"]
    assert result["crunch_weeks"] == []


def test_attach_bye_outlooks_adds_each_teams_outlook_and_player_bye_weeks():
    context = {
        "as_of_week": 5, "start_week": 5, "end_week": 8, "lineup_slot_counts": SLOTS,
        "teams": {"1": {"team_id": "1", "roster": [dict(p) for p in ROSTER]}},
    }

    pure.attach_bye_outlooks(context, {"BUF": 7, "KC": 10, "DAL": 9})

    team = context["teams"]["1"]
    assert team["bye_outlook"]["crunch_weeks"] == [7]
    assert {p["espn_id"]: p["nfl_bye_week"] for p in team["roster"]}["2"] == 7
    assert {p["espn_id"]: p["nfl_bye_week"] for p in team["roster"]}["1"] == 10


def test_attach_bye_outlooks_without_a_schedule_leaves_byes_unknown():
    context = {
        "as_of_week": 5, "start_week": 5, "end_week": 8, "lineup_slot_counts": SLOTS,
        "teams": {"1": {"team_id": "1", "roster": [dict(p) for p in ROSTER]}},
    }

    pure.attach_bye_outlooks(context, {})

    team = context["teams"]["1"]
    assert team["bye_outlook"]["crunch_weeks"] == []
    assert team["roster"][0]["nfl_bye_week"] is None


def test_get_bye_outlook_reads_the_shared_trade_context_for_one_team():
    context = {
        "league_id": "9", "season": 2026, "as_of_week": 5, "start_week": 5, "end_week": 8,
        "lineup_slot_counts": SLOTS,
        "teams": {"1": {"team_id": "1", "name": "Us", "roster": [dict(p) for p in ROSTER]}},
    }
    with patch("src.services.roster_outlook_service.get_league_trade_context", return_value=context) as ctx, \
         patch("src.services.roster_outlook_service.bye_weeks_or_empty", return_value={"BUF": 7, "KC": 10}):
        result = outlook.get_bye_outlook("9", 2026, 5, 4, "1")

    ctx.assert_called_once_with("9", 2026, 5, 4)
    assert result["team_id"] == "1"
    assert result["crunch_weeks"] == [7]
    assert result["players"][1] == {"espn_id": "2", "name": "P2", "position": "RB",
                                    "nfl_team": "BUF", "nfl_bye_week": 7}


def test_get_bye_outlook_rejects_an_unknown_team():
    context = {"as_of_week": 5, "start_week": 5, "end_week": 8, "lineup_slot_counts": SLOTS,
               "teams": {}}
    with patch("src.services.roster_outlook_service.get_league_trade_context", return_value=context), \
         patch("src.services.roster_outlook_service.bye_weeks_or_empty", return_value={}):
        with pytest.raises(ValueError):
            outlook.get_bye_outlook("9", 2026, 5, 4, "1")


def test_a_bye_forcing_a_below_replacement_starter_is_a_crunch():
    roster = ROSTER[:2] + [player("3", "RB", 8.0), ROSTER[3], player("5", "RB", -1.5)]

    result = pure.build_bye_outlook(roster, WEEKS, 5, SLOTS, playing({7: {"BUF"}}))

    week7 = {w["week"]: w for w in result["weeks"]}[7]
    assert result["crunch_weeks"] == [7]
    assert week7["unfilled_slots"] == []
    assert week7["replacement_level_starters"] == 1
