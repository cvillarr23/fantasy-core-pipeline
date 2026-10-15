import pytest

from src.optimization import waiver_bye_fill as bf

WEEKS = [7, 8, 9, 10]
SLOTS = {"QB": 1, "RB": 2, "RB/WR/TE": 1, "BE": 4, "IR": 1}
ALL = {"KC", "BUF", "DAL"}


def entry(pid, pos, vorp, team="KC", bye=None, starter=False, slot="BE"):
    return {
        "espn_id": f"e{pid}", "player_id": pid, "name": pid, "position": pos,
        "nfl_team": team, "slot": slot, "injury_status": None, "is_starter": starter,
        "metrics": {str(w): {"vorp_score": vorp} for w in WEEKS if w != bye},
    }


def playing(idle):
    return {w: ALL - idle.get(w, set()) for w in WEEKS}


ROSTER = [
    entry("qb", "QB", 6.0, starter=True, slot="QB"),
    entry("rb1", "RB", 10.0, team="BUF", bye=8, starter=True, slot="RB"),
    entry("rb2", "RB", 8.0, team="BUF", bye=8, starter=True, slot="RB"),
    entry("wr", "WR", 5.0, starter=True, slot="RB/WR/TE"),
    entry("scrub", "TE", -2.0),
]


def value(roster, fas):
    return {p["player_id"]: min(v["vorp_score"] for v in p["metrics"].values()) for p in roster + fas}


def test_recommends_a_free_agent_that_covers_a_bye_crunch_and_drops_the_scrub():
    fa = entry("stream", "RB", 1.5, team="DAL")

    moves, outlook = bf.plan_bye_fill_moves(
        ROSTER, {"stream": fa}, value(ROSTER, [fa]), 7, WEEKS, SLOTS, playing({8: {"BUF"}}),
        open_roster_slots=0,
    )

    assert outlook["crunch_weeks"] == [8]
    assert len(moves) == 1
    move = moves[0]
    assert (move["kind"], move["add_player_id"], move["drop_player_id"]) == ("swap", "stream", "scrub")
    assert move["bye_fill"]["week"] == 8
    assert move["bye_fill"]["lineup_gain"] > 0
    assert move["horizon_lineup_gain"] > 0


def test_never_drops_a_player_worth_more_than_the_add_even_if_he_never_starts():
    roster = ROSTER[:4] + [entry("handcuff", "TE", 4.0)]
    fa = entry("stream", "RB", 1.5, team="DAL")

    moves, _ = bf.plan_bye_fill_moves(
        roster, {"stream": fa}, value(roster, [fa]), 7, WEEKS, SLOTS, playing({8: {"BUF"}}),
        open_roster_slots=0,
    )

    assert moves == []


def test_uses_an_open_roster_slot_instead_of_dropping():
    fa = entry("stream", "RB", 1.5, team="DAL")

    moves, _ = bf.plan_bye_fill_moves(
        ROSTER, {"stream": fa}, value(ROSTER, [fa]), 7, WEEKS, SLOTS, playing({8: {"BUF"}}),
        open_roster_slots=1,
    )

    assert (moves[0]["kind"], moves[0]["drop_player_id"]) == ("add", None)


def test_free_agent_on_bye_in_the_crunch_week_is_no_help():
    fa = entry("same_bye", "RB", 9.0, team="BUF", bye=8)

    moves, _ = bf.plan_bye_fill_moves(
        ROSTER, {"same_bye": fa}, value(ROSTER, [fa]), 7, WEEKS, SLOTS, playing({8: {"BUF"}}),
        open_roster_slots=1,
    )

    assert moves == []


def test_no_crunch_means_no_bye_fill_moves():
    fa = entry("stream", "RB", 1.5, team="DAL")

    moves, outlook = bf.plan_bye_fill_moves(
        ROSTER, {"stream": fa}, value(ROSTER, [fa]), 7, WEEKS, SLOTS, playing({}),
        open_roster_slots=1,
    )

    assert outlook["crunch_weeks"] == []
    assert moves == []


def test_skips_pairs_already_recommended():
    fa = entry("stream", "RB", 1.5, team="DAL")

    moves, _ = bf.plan_bye_fill_moves(
        ROSTER, {"stream": fa}, value(ROSTER, [fa]), 7, WEEKS, SLOTS, playing({8: {"BUF"}}),
        open_roster_slots=0, claimed_player_ids={"stream"},
    )

    assert moves == []


def test_lineup_gains_by_week_reports_unscorable_weeks_as_none():
    fa = entry("stream", "RB", 1.5, team="DAL")
    fa["metrics"].pop("10")  # no projection yet, and DAL plays: unknown, not zero

    gains = bf.lineup_gains_by_week(ROSTER, fa, ROSTER[4], 7, WEEKS, SLOTS, playing({8: {"BUF"}}))

    assert [g["week"] for g in gains] == WEEKS
    assert gains[-1]["delta"] is None
    assert gains[1]["delta"] == pytest.approx(1.5)
