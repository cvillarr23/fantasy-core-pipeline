from datetime import date

import pytest

from src.injury_timeline import (
    DOUBTFUL_AVAILABILITY,
    QUESTIONABLE_AVAILABILITY,
    availability,
    expected_return_week,
    weeks_out,
)

# A team that plays Sundays in weeks 12, 13 and 15, with a week 14 bye.
GAMES = [(12, date(2026, 11, 22)), (13, date(2026, 11, 29)), (15, date(2026, 12, 13))]


@pytest.mark.parametrize("return_date,week", [
    (date(2026, 11, 29), 13),          # the game day itself
    (date(2026, 11, 24), 13),          # midweek: the next game
    ("2026-11-30", 15),                # the week 14 bye is skipped, not counted as a return
    (date(2026, 11, 1), 12),           # a stale past date: the first remaining game
    (date(2027, 1, 20), 16),           # after the last game: out for the rest of the season
])
def test_return_week_is_the_first_game_on_or_after_the_date(return_date, week):
    assert expected_return_week(return_date, GAMES) == week


def test_return_week_is_unknown_without_a_date_or_schedule():
    assert expected_return_week(None, GAMES) is None
    assert expected_return_week(date(2026, 11, 29), []) is None


def player(status=None, slot="BE", return_week="absent"):
    entry = {"espn_id": "1", "injury_status": status, "slot": slot}
    if return_week != "absent":
        entry["injury"] = None if return_week is None else {"expected_return_week": return_week}
    return entry


def test_without_an_injury_key_only_the_as_of_week_is_out():
    # The pre-timeline rule, still used by callers that don't load injuries.
    for p in (player("OUT"), player("INJURY_RESERVE"), player(slot="IR")):
        assert availability(p, 5, 5) == (0.0, "out")
        assert availability(p, 6, 5) == (1.0, None)
    assert availability(player("QUESTIONABLE"), 5, 5) == (1.0, None)


def test_out_player_sits_until_his_return_week():
    p = player("OUT", return_week=7)
    assert [availability(p, w, 5)[0] for w in (5, 6, 7, 8)] == [0.0, 0.0, 1.0, 1.0]
    assert weeks_out(p, [5, 6, 7, 8], 5) == 2


def test_status_beats_a_stale_return_date_in_the_as_of_week():
    assert availability(player("OUT", return_week=4), 5, 5) == (0.0, "out")
    assert availability(player("OUT", return_week=4), 6, 5) == (1.0, None)


def test_without_a_return_week_ir_sits_the_horizon_and_out_returns():
    assert weeks_out(player("INJURY_RESERVE", return_week=None), [5, 6, 7], 5) == 3
    assert weeks_out(player("OUT", slot="IR", return_week=None), [5, 6, 7], 5) == 3
    assert weeks_out(player("OUT", return_week=None), [5, 6, 7], 5) == 1
    # Healthy but not yet activated: back the next week.
    assert weeks_out(player("ACTIVE", slot="IR", return_week=None), [5, 6, 7], 5) == 1


def test_suspension_uses_the_return_date():
    assert weeks_out(player("SUSPENSION", return_week=7), [5, 6, 7], 5) == 2


def test_questionable_and_doubtful_are_discounted_in_the_as_of_week_only():
    assert availability(player("QUESTIONABLE", return_week=None), 5, 5) == (QUESTIONABLE_AVAILABILITY, "questionable")
    assert availability(player("DOUBTFUL", return_week=5), 5, 5) == (DOUBTFUL_AVAILABILITY, "doubtful")
    assert availability(player("QUESTIONABLE", return_week=None), 6, 5) == (1.0, None)
    assert availability(player("ACTIVE", return_week=None), 5, 5) == (1.0, None)
