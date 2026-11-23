from unittest.mock import patch

from src import nfl_game_status
from src.nfl_game_status import game_status_for, load_team_game_status


def _event(state, *teams):
    return {
        "status": {"type": {"state": state}},
        "competitions": [{"competitors": [{"team": {"abbreviation": t}} for t in teams]}],
    }


def _board(*events, season=2026, week=12):
    return {"season": {"year": season, "type": 2}, "week": {"number": week}, "events": list(events)}


def _load(board):
    with patch("src.nfl_game_status.fetch_scoreboard", return_value=board):
        return load_team_game_status(2026, 12)


def test_maps_the_three_espn_states_for_both_teams_of_each_game():
    statuses = _load(_board(_event("pre", "GB", "DET"), _event("in", "TB", "ATL"), _event("post", "MIA", "BUF")))

    assert statuses == {
        "GB": "not_started", "DET": "not_started",
        "TB": "in_progress", "ATL": "in_progress",
        "MIA": "final", "BUF": "final",
    }


def test_team_codes_use_the_nflverse_spelling():
    statuses = _load(_board(_event("post", "WSH", "JAX")))

    assert statuses == {"WAS": "final", "JAX": "final"}


def test_a_failed_scoreboard_read_is_unknown_not_an_error(caplog):
    with patch("src.nfl_game_status.fetch_scoreboard", side_effect=RuntimeError("down")):
        assert load_team_game_status(2026, 12) is None
    assert "scoreboard" in caplog.text.lower()


def test_a_scoreboard_for_another_week_is_unknown():
    assert _load(_board(_event("post", "MIA", "BUF"), week=11)) is None


def test_an_event_with_an_unrecognised_state_is_unknown_for_its_teams_only():
    statuses = _load(_board(_event("weird", "GB", "DET"), _event("post", "MIA", "BUF")))

    assert statuses == {"MIA": "final", "BUF": "final"}


def test_game_status_for_looks_up_by_espn_pro_team_code():
    statuses = {"WAS": "in_progress"}

    assert game_status_for("WSH", statuses) == "in_progress"
    assert game_status_for("DAL", statuses) is None
    assert game_status_for("WSH", None) is None
    assert game_status_for(None, statuses) is None
