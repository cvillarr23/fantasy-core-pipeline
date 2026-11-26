import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src import espn_scoring as sc
from src.ingestion import live_games as lg

FIXTURES = Path(__file__).parent / "fixtures" / "espn_live"
NOW = datetime(2026, 11, 23, 18, 0, 0, tzinfo=timezone.utc)
TEAMS = {"8", "7"}


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture(scope="module")
def settings():
    """This league's real scoringSettings, as `sync_league` stores them."""
    return fixture("league_scoring_settings.json")


@pytest.fixture(scope="module")
def rules(settings):
    return sc.rule_points(settings)


@pytest.fixture(scope="module")
def players():
    """`live:player` values for every roster player in the recorded matchup payload, by name."""
    league = fixture("league_matchup.json")
    espn_map = {str(e["playerId"]): f"p{e['playerId']}" for _, e in lg.roster_entries(league, TEAMS)}
    shaped = lg.shape_roster(league, TEAMS, 2026, 11, espn_map, {}, NOW)
    return {p["full_name"]: p["value"] for p in shaped}


def lines_by_key(lines):
    return {line["stat_key"]: line for line in lines}


# ---------------------------------------------------------------- rule labels


@pytest.mark.parametrize("stat_id, points, label", [
    (3, 0.04, "1 pt per 25 passing yds"),
    (24, 0.1, "1 pt per 10 rushing yds"),
    (24, 1.0, "1 pt per rushing yd"),
    (24, 0.15, "0.15 pts per rushing yd"),
    (43, 6.0, "6 pts per receiving TD"),
    (53, 1.0, "1 pt per reception"),
    (53, 0.5, "0.5 pts per reception"),
    (72, -2.0, "−2 pts per fumble lost"),
    (85, -1.0, "−1 pt per missed field goal"),
    (89, 5.0, "5 pts for allowing 0 points"),
    (123, -1.0, "−1 pt for allowing 28–34 points"),
    (130, 2.0, "2 pts for allowing 200–299 yds"),
])
def test_rule_label_reads_as_plain_english(stat_id, points, label):
    assert sc.rule_label(sc.ESPN_STATS[stat_id], points) == label


# ---------------------------------------------------------------- league rules


def test_league_rules_cover_every_scored_stat(settings):
    rules = sc.scoring_rules(settings)
    by_key = {(r["stat_key"], r["position"]): r for r in rules}

    assert by_key[("passYd", None)] == {
        "stat_key": "passYd", "label": "Passing yards", "category": "Passing", "points_per": 0.04,
        "unit": "yds", "rule_label": "1 pt per 25 passing yds", "position": None,
    }
    assert by_key[("int", None)]["rule_label"] == "−2 pts per interception thrown"
    assert by_key[("rec", None)]["rule_label"] == "1 pt per reception"
    assert by_key[("fg50To59", None)]["rule_label"] == "5 pts per FG made from 50–59 yds"


def test_dst_rules_come_from_the_position_override(settings):
    rules = sc.scoring_rules(settings)
    dst = {r["stat_key"]: r for r in rules if r["position"] == "D/ST"}

    assert dst["sacks"]["rule_label"] == "1 pt per sack"
    assert dst["ptsAllowed0"]["rule_label"] == "5 pts for allowing 0 points"
    assert dst["ydsAllowed550Plus"]["rule_label"] == "−7 pts for allowing 550+ yds"
    # Base 0 rules aren't rules, and an override equal to the base isn't a second rule.
    assert ("sacks", None) not in {(r["stat_key"], r["position"]) for r in rules}
    assert [r["position"] for r in rules if r["stat_key"] == "onePtSafety"] == [None]


def test_rules_are_in_display_order(settings):
    categories = [r["category"] for r in sc.scoring_rules(settings)]

    assert categories.index("Passing") < categories.index("Rushing") < categories.index("Kicking") < categories.index("Defense")


def test_unknown_stat_ids_are_skipped_and_logged(caplog):
    settings = {"scoringItems": [
        {"statId": 4, "points": 4.0},
        {"statId": 9999, "points": 1.0},
        {"statId": "junk", "points": 1.0},
    ]}

    rules = sc.scoring_rules(settings)

    assert [r["stat_key"] for r in rules] == ["passTD"]
    assert "9999" in caplog.text


def test_missing_settings_have_no_rules():
    assert sc.scoring_rules(None) == []
    assert sc.scoring_rules({}) == []


# ---------------------------------------------------------------- breakdown, one sample per position


def test_qb_breakdown(players, rules):
    lines = sc.score_breakdown(players["Cam Ward"], rules)

    assert lines == [
        {"stat_key": "passYd", "label": "Passing yards", "qty": 248, "unit": "yds", "rule_label": "1 pt per 25 passing yds", "points": 9.92},
        {"stat_key": "passTD", "label": "Passing TD", "qty": 2, "unit": "TD", "rule_label": "4 pts per passing TD", "points": 8.0},
        {"stat_key": "rushYd", "label": "Rushing yards", "qty": 3, "unit": "yds", "rule_label": "1 pt per 10 rushing yds", "points": 0.3},
        {"stat_key": "rushTD", "label": "Rushing TD", "qty": 1, "unit": "TD", "rule_label": "6 pts per rushing TD", "points": 6.0},
        {"stat_key": "fumLost", "label": "Fumble lost", "qty": 1, "unit": "lost", "rule_label": "−2 pts per fumble lost", "points": -2.0},
    ]


def test_rb_breakdown(players, rules):
    lines = lines_by_key(sc.score_breakdown(players["Jahmyr Gibbs"], rules))

    assert lines["rushYd"]["qty"] == 98 and lines["rushYd"]["points"] == 9.8
    assert lines["rushTD"]["qty"] == 2 and lines["rushTD"]["points"] == 12.0
    assert lines["rec"]["rule_label"] == "1 pt per reception"


def test_wr_breakdown(players, rules):
    lines = lines_by_key(sc.score_breakdown(players["Mike Evans"], rules))

    assert set(lines) == {"rec", "recYd", "recTD"}
    assert lines["recYd"] == {
        "stat_key": "recYd", "label": "Receiving yards", "qty": 109, "unit": "yds",
        "rule_label": "1 pt per 10 receiving yds", "points": 10.9,
    }


def test_te_breakdown(players, rules):
    lines = lines_by_key(sc.score_breakdown(players["Brenton Strange"], rules))

    assert players["Brenton Strange"]["position"] == "TE"
    assert lines["rec"]["points"] == 3.0 and lines["recYd"]["points"] == 1.6


def test_kicker_breakdown_has_distance_buckets_and_a_negative_miss(players, rules):
    lines = lines_by_key(sc.score_breakdown(players["Tyler Bass"], rules))

    assert lines["fgUnder40"]["rule_label"] == "3 pts per FG made from under 40 yds"
    assert lines["fg40To49"]["qty"] == 2 and lines["fg40To49"]["points"] == 8.0
    assert lines["fg50To59"]["points"] == 5.0
    assert lines["fgMissed"] == {
        "stat_key": "fgMissed", "label": "Field goal missed", "qty": 1, "unit": "FG",
        "rule_label": "−1 pt per missed field goal", "points": -1.0,
    }
    assert lines["xpMade"]["qty"] == 2


def test_dst_breakdown_uses_dst_rules_and_skips_zero_buckets(players, rules):
    bears = players["Bears D/ST"]
    lines = lines_by_key(sc.score_breakdown(bears, rules))

    assert bears["position"] == "D/ST"
    assert set(lines) == {"sacks", "defInt", "fumRec", "safeties", "ptsAllowed7To13"}
    assert lines["sacks"] == {
        "stat_key": "sacks", "label": "Sacks", "qty": 6, "unit": "sacks", "rule_label": "1 pt per sack", "points": 6.0,
    }
    assert lines["ptsAllowed7To13"]["rule_label"] == "3 pts for allowing 7–13 points"


def test_negative_stats_stay_negative(players, rules):
    cousins = lines_by_key(sc.score_breakdown(players["Kirk Cousins"], rules))

    assert cousins["int"]["points"] == -2.0
    assert cousins["int"]["rule_label"] == "−2 pts per interception thrown"


def test_every_recorded_player_sums_to_live_points(players, rules):
    scored = {name: value for name, value in players.items() if value["live_points"] is not None}

    assert len(scored) > 15
    for name, value in scored.items():
        lines = sc.score_breakdown(value, rules)
        assert all(line["stat_key"] != sc.OTHER_KEY for line in lines), name
        assert abs(sum(line["points"] for line in lines) - value["live_points"]) <= sc.SUM_TOLERANCE, name


# ---------------------------------------------------------------- remainder and edge cases


def test_unknown_stat_lands_in_an_other_line(rules):
    player = {"live_points": 14.5, "position": "RB", "applied_stats": {"24": 8.5, "9999": 6}, "applied_qty": {"24": 85}}

    lines = sc.score_breakdown(player, rules)

    assert lines[-1] == {
        "stat_key": "other", "label": "Other scoring", "qty": None, "unit": None,
        "rule_label": "Not itemized", "points": 6.0,
    }
    assert sum(line["points"] for line in lines) == 14.5


def test_mismatched_total_gets_a_negative_remainder(rules):
    player = {"live_points": 5.0, "applied_stats": {"43": 6}, "applied_qty": {"43": 1}}

    assert sc.score_breakdown(player, rules)[-1]["points"] == -1.0


def test_rounding_noise_is_not_a_remainder(rules):
    player = {"live_points": 2.3, "applied_stats": {"24": 2.3000000000000003}, "applied_qty": {"24": 23}}

    assert sc.score_breakdown(player, rules) == [
        {"stat_key": "rushYd", "label": "Rushing yards", "qty": 23, "unit": "yds", "rule_label": "1 pt per 10 rushing yds", "points": 2.3},
    ]


def test_no_live_points_means_no_breakdown(players, rules):
    assert sc.score_breakdown(players["Chase Brown"], rules) is None


def test_played_with_no_points_is_an_empty_breakdown(rules):
    assert sc.score_breakdown({"live_points": 0, "applied_stats": {}}, rules) == []


def test_older_snapshot_without_quantities_still_labels_from_the_rules(rules):
    player = {"live_points": 9.8, "applied_stats": {"24": 9.8}}

    assert sc.score_breakdown(player, rules) == [
        {"stat_key": "rushYd", "label": "Rushing yards", "qty": None, "unit": "yds", "rule_label": "1 pt per 10 rushing yds", "points": 9.8},
    ]


def test_without_league_rules_the_label_comes_from_espns_own_points():
    player = {"live_points": 9.8, "applied_stats": {"24": 9.8}, "applied_qty": {"24": 98}}

    assert sc.score_breakdown(player, {})[0]["rule_label"] == "1 pt per 10 rushing yds"


def test_applied_quantities_keep_the_count_behind_each_scored_stat():
    row = {"appliedStats": {"24": 9.8, "25": 12, "53": 0}, "stats": {"24": 98, "25": 2, "23": 15}}
    assert sc.applied_quantities(row) == {"24": 98, "25": 2}
