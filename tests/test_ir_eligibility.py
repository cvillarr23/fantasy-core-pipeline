from unittest.mock import MagicMock

import pytest

from src import ir_eligibility as ir


@pytest.mark.parametrize("status", ["OUT", "INJURY_RESERVE", "out", "Injury_Reserve"])
def test_is_ir_eligible_accepts_out_and_injury_reserve(status):
    assert ir.is_ir_eligible(status) is True


@pytest.mark.parametrize("status", ["ACTIVE", "QUESTIONABLE", "DOUBTFUL", "NORMAL", "", None, []])
def test_is_ir_eligible_rejects_everything_else(status):
    assert ir.is_ir_eligible(status) is False


def test_open_ir_slots_subtracts_occupied_and_never_goes_negative():
    assert ir.open_ir_slots({"IR": 2, "BE": 7}, occupied=1) == 1
    assert ir.open_ir_slots({"IR": 1}, occupied=3) == 0
    assert ir.open_ir_slots({"BE": 7}, occupied=0) == 0


def test_load_open_ir_slots_prefers_espn_ir_occupancy_from_league_teams():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [({"IR": 2, "BE": 7},), (1,)]

    assert ir.load_open_ir_slots(cursor, "8", 2026) == 1

    settings_sql, settings_params = cursor.execute.call_args_list[0].args
    assert "league_settings" in settings_sql and settings_params == (2026,)
    sql, params = cursor.execute.call_args_list[1].args
    assert "ir_slots_used" in sql and "league_teams" in sql
    assert params == ("8",)


def test_load_open_ir_slots_falls_back_to_league_rosters_before_occupancy_is_synced():
    cursor = MagicMock()
    cursor.fetchone.side_effect = [({"IR": 1},), None, (1,)]

    assert ir.load_open_ir_slots(cursor, "8", 2026) == 0

    sql, params = cursor.execute.call_args_list[2].args
    assert "slot_position = 'IR'" in sql
    assert params == ("8",)


def test_load_ir_slot_count_is_zero_without_synced_settings():
    """No league_settings row must not invent an IR slot ESPN would reject."""
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    assert ir.load_ir_slot_count(cursor, 2026) == 0
    assert ir.load_open_ir_slots(cursor, "8", 2026) == 0
    assert cursor.execute.call_count == 2  # the roster count is never needed


def test_load_lineup_slot_counts_row_is_none_without_synced_settings():
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    assert ir.load_lineup_slot_counts_row(cursor, 2026) is None


def test_load_lineup_slot_counts_row_parses_json_text():
    cursor = MagicMock()
    cursor.fetchone.return_value = ('{"BE": 7, "IR": 1}',)
    assert ir.load_lineup_slot_counts_row(cursor, 2026) == {"BE": 7, "IR": 1}


def test_load_ir_slot_count_parses_json_text():
    cursor = MagicMock()
    cursor.fetchone.return_value = ('{"IR": 1}',)
    assert ir.load_ir_slot_count(cursor, 2026) == 1


def _line(player_id, slot, status, net_vorp=None):
    return {"player_id": player_id, "slot": slot, "injury_status": status, "net_vorp": net_vorp}


def test_lineup_ir_hints_suggests_moving_an_out_player_into_an_open_ir_slot():
    lines = [_line("1", "RB", "ACTIVE"), _line("2", "BE", "OUT")]

    hints, blocking = ir.lineup_ir_hints(lines, ir_slot_count=1)

    assert set(hints) == {"2"}
    assert hints["2"]["action"] == "move_to_ir"
    assert blocking is False


def test_lineup_ir_hints_none_when_ir_is_full_or_league_has_no_ir():
    full = [_line("1", "IR", "INJURY_RESERVE"), _line("2", "BE", "OUT")]
    assert ir.lineup_ir_hints(full, ir_slot_count=1) == ({}, False)
    assert ir.lineup_ir_hints([_line("2", "BE", "OUT")], ir_slot_count=0) == ({}, False)


def test_lineup_ir_hints_flags_an_active_player_sitting_in_ir():
    """ESPN blocks every new transaction while an active player occupies IR."""
    lines = [_line("1", "IR", "ACTIVE"), _line("2", "BE", "OUT")]

    hints, blocking = ir.lineup_ir_hints(lines, ir_slot_count=1)

    assert hints["1"]["action"] == "activate_from_ir"
    assert blocking is True
    # The IR slot is still occupied until the activation happens.
    assert "2" not in hints


def test_lineup_ir_hints_prefers_starters_then_higher_value_when_slots_are_short():
    lines = [
        _line("bench_low", "BE", "OUT", net_vorp=1.0),
        _line("bench_high", "BE", "OUT", net_vorp=5.0),
        _line("starter", "WR", "INJURY_RESERVE", net_vorp=0.5),
    ]

    hints, _ = ir.lineup_ir_hints(lines, ir_slot_count=2)

    assert set(hints) == {"starter", "bench_high"}


def test_load_ineligible_ir_occupants_returns_only_players_who_no_longer_qualify():
    cursor = MagicMock()
    cursor.fetchone.return_value = (4,)
    cursor.fetchall.return_value = [
        ("hurt", "Still Hurt", "INJURY_RESERVE"),
        ("out", "Out Guy", "OUT"),
        ("healed", "Healed Guy", "ACTIVE"),
        ("unknown", "No Status", None),
    ]

    assert ir.load_ineligible_ir_occupants(cursor, 7) == [
        ("healed", "Healed Guy", "ACTIVE"),
        ("unknown", "No Status", None),
    ]
    sql, params = cursor.execute.call_args_list[0].args
    assert "slot_position = 'IR'" in sql
    assert params == ("7",)


@pytest.mark.parametrize("used,unmatched", [(3, 2), (1, 0), (None, 0)])
def test_ir_occupants_without_a_player_match_count_as_unknown(used, unmatched):
    """league_rosters drops players with no espn_id match, but ir_slots_used counts them;
    their ESPN status can't be checked, so they're reported rather than assumed eligible."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [("hurt", "Still Hurt", "OUT")]
    cursor.fetchone.return_value = None if used is None else (used,)

    assert ir.load_ineligible_ir_occupants(cursor, "7") == [(None, None, None)] * unmatched
