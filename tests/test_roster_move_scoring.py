from src.optimization import roster_move_scoring as rms

SLOTS = {"QB": 1, "RB": 2, "RB/WR/TE": 1, "BE": 4, "IR": 1}


def player(espn_id, pos, vorp_by_week, nfl_team="KC", slot="BE", injury=None):
    return {
        "espn_id": espn_id, "player_id": f"s{espn_id}", "name": f"P{espn_id}",
        "position": pos, "nfl_team": nfl_team, "slot": slot, "injury_status": injury,
        "metrics": {str(w): {"vorp_score": v} for w, v in vorp_by_week.items()},
    }


def test_exclusion_tells_a_confirmed_bye_from_missing_data():
    p = player("1", "RB", {5: None}, nfl_team="WSH")

    assert rms.exclusion(p, 5, 5, playing={"KC"}) == "nfl_bye"
    assert rms.exclusion(p, 5, 5, playing={"WAS"}) == "no_metrics"
    assert rms.exclusion(p, 5, 5, playing=None) == "no_metrics"


def test_unfilled_starter_slots_lets_dedicated_slots_claim_players_before_flex():
    pool = [{"position": "QB"}, {"position": "RB"}, {"position": "RB"}]

    assert rms.unfilled_starter_slots(pool, SLOTS) == ["RB/WR/TE"]


def test_unfilled_starter_slots_counts_each_missing_seat():
    assert rms.unfilled_starter_slots([{"position": "QB"}], SLOTS) == ["RB", "RB", "RB/WR/TE"]


def test_lineup_week_solves_what_it_can_and_names_byes_and_empty_seats():
    roster = [
        player("1", "QB", {6: 5.0}),
        player("2", "RB", {6: 9.0}),
        player("3", "RB", {6: None}, nfl_team="BUF"),   # on bye
        player("4", "WR", {6: None}, nfl_team="BUF"),   # on bye
    ]

    week = rms.lineup_week(roster, 6, 5, SLOTS, playing={"KC"})

    assert week["value"] == 14.0
    assert week["on_bye"] == ["3", "4"]
    assert week["unfilled_slots"] == ["RB", "RB/WR/TE"]
    assert week["unknown"] == []


def test_lineup_week_without_any_metrics_is_unknown_not_zero():
    roster = [player("1", "QB", {6: None}), player("2", "RB", {6: None})]

    week = rms.lineup_week(roster, 6, 5, SLOTS, playing={"KC"})

    assert week["value"] is None
    assert week["unknown"] == ["1", "2"]
