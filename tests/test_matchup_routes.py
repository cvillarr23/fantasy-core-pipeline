from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests
from espn_api.requests.espn_requests import ESPNAccessDenied

from src.services import matchup_service
from src.nfl_schedule import RegularSeasonOverError, WeekResolutionError
from src.services.matchup_service import NoMatchupFoundError

FAKE_MATCHUP = {
    "season": 2026,
    "week": 2,
    "our_team": {
        "team": {"name": "Our Team", "logo_url": "https://logo", "score": 100.0, "projected": 95.0},
        "starters": [],
        "bench": [],
    },
    "opponent": {
        "team": {"name": "Their Team", "logo_url": "https://logo2", "score": 90.0, "projected": 92.0},
        "starters": [],
        "bench": [],
    },
}


def test_get_matchup_success(client):
    with patch("src.api.matchup.get_current_team_matchup", return_value=FAKE_MATCHUP):
        response = client.get("/v1/matchup?season=2026&week=2")

    assert response.status_code == 200
    body = response.json()
    assert body["our_team"]["team"]["name"] == "Our Team"
    assert body["opponent"]["team"]["name"] == "Their Team"


def test_get_matchup_no_matchup_returns_404(client):
    with patch("src.api.matchup.get_current_team_matchup", side_effect=NoMatchupFoundError("bye week")):
        response = client.get("/v1/matchup?season=2026&week=2")

    assert response.status_code == 404


def test_get_matchup_espn_error_returns_502(client):
    with patch("src.api.matchup.get_current_team_matchup", side_effect=ESPNAccessDenied("denied")):
        response = client.get("/v1/matchup?season=2026&week=2")

    assert response.status_code == 502


def test_get_matchup_espn_timeout_returns_504(client):
    with patch("src.api.matchup.get_current_team_matchup", side_effect=matchup_service.ESPNTimeoutError("ESPN stalled")):
        response = client.get("/v1/matchup?season=2026&week=2")

    assert response.status_code == 504
    assert response.json() == {"detail": "ESPN request timed out"}


def test_get_matchup_unrelated_timeout_remains_500(client):
    with patch("src.api.matchup.get_current_team_matchup", side_effect=requests.Timeout("other service")):
        response = client.get("/v1/matchup?season=2026&week=2")

    assert response.status_code == 500


def test_get_matchup_unexpected_error_returns_500(client):
    with patch("src.api.matchup.get_current_team_matchup", side_effect=Exception("boom")):
        response = client.get("/v1/matchup?season=2026&week=2")

    assert response.status_code == 500


def test_get_matchup_requires_season_and_week(client):
    response = client.get("/v1/matchup")

    assert response.status_code == 422


def test_get_lineup_roster_success(client):
    fixture = {"starters": [], "bench": []}
    with patch("src.api.matchup.get_current_team_roster", return_value=fixture):
        response = client.get("/v1/matchup/roster?season=2026")

    assert response.status_code == 200
    assert response.json() == {**fixture, "ir_blocking_transactions": False, "lineup_slot_counts": None}


def test_get_lineup_roster_serializes_lineup_slot_counts(client):
    fixture = {"starters": [], "bench": [], "lineup_slot_counts": {"QB": 1, "BE": 7, "IR": 1}}
    with patch("src.api.matchup.get_current_team_roster", return_value=fixture):
        response = client.get("/v1/matchup/roster?season=2026")

    assert response.status_code == 200
    assert response.json()["lineup_slot_counts"] == {"QB": 1, "BE": 7, "IR": 1}


def test_get_lineup_roster_serializes_ir_suggestions(client):
    line = {
        "player_id": "5", "espn_id": "5", "name": "Hurt WR", "position": "WR", "jersey": None,
        "slot": "BE", "pro_team": "DET", "injury_status": "OUT", "headshot_url": "h",
        "deficit_flag": False,
        "ir_suggestion": {"action": "move_to_ir", "reason": "OUT with an open IR slot"},
    }
    fixture = {"starters": [], "bench": [line], "ir_blocking_transactions": False}
    with patch("src.api.matchup.get_current_team_roster", return_value=fixture):
        response = client.get("/v1/matchup/roster?season=2026")

    assert response.status_code == 200
    assert response.json()["bench"][0]["ir_suggestion"] == {"action": "move_to_ir", "reason": "OUT with an open IR slot"}


def test_get_lineup_roster_espn_error_returns_502(client):
    with patch("src.api.matchup.get_current_team_roster", side_effect=ESPNAccessDenied("denied")):
        response = client.get("/v1/matchup/roster?season=2026")

    assert response.status_code == 502


def test_get_lineup_roster_unresolvable_week_returns_503(client):
    with patch("src.api.matchup.get_current_team_roster", side_effect=WeekResolutionError("download failed")):
        response = client.get("/v1/matchup/roster?season=2026")

    assert response.status_code == 503
    assert "week" in response.json()["detail"]


def test_get_lineup_roster_after_the_regular_season_returns_409(client):
    with patch("src.api.matchup.get_current_team_roster", side_effect=RegularSeasonOverError("over")):
        response = client.get("/v1/matchup/roster?season=2026")

    assert response.status_code == 409


def test_get_lineup_roster_unexpected_error_returns_500(client):
    with patch("src.api.matchup.get_current_team_roster", side_effect=Exception("boom")):
        response = client.get("/v1/matchup/roster?season=2026")

    assert response.status_code == 500


def test_get_lineup_roster_requires_season(client):
    response = client.get("/v1/matchup/roster")

    assert response.status_code == 422


def test_scoreboard_timeout_returns_504_on_existing_and_alias_routes(client):
    with patch("src.api.matchup.get_league_scoreboard", side_effect=matchup_service.ESPNTimeoutError("ESPN stalled")):
        existing = client.get("/v1/matchup/scoreboard?season=2026&week=2")
        alias = client.get("/v1/scoreboard?season=2026&week=2")

    assert existing.status_code == alias.status_code == 504
    assert existing.json() == alias.json() == {"detail": "ESPN request timed out"}


def test_scoreboard_alias_preserves_existing_response_shape(client):
    payload = {"season": 2026, "week": 2, "matchups": [{"home": None, "away": None}]}
    with patch("src.api.matchup.get_league_scoreboard", return_value=payload):
        response = client.get("/v1/scoreboard?season=2026&week=2")

    assert response.status_code == 200
    assert response.json() == payload


def test_roster_current_week_projections_with_real_service(client, monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    players = [
        SimpleNamespace(playerId=pid, name=f"Player {pid}", position=position,
                        jersey=None, lineupSlot=slot, proTeam="DET", injuryStatus="ACTIVE")
        for pid, position, slot in [
            (1, "RB", "RB"), (2, "WR", "BE"), (3, "K", "K"),
            (4, "D/ST", "BE"), (5, "WR", "IR"), (6, "RB", "BE"),
        ]
    ]
    league = MagicMock()
    league.teams = [SimpleNamespace(team_id=8, roster=players)]
    league.box_scores.side_effect = AssertionError("Roster must not require box scores")
    cursor = MagicMock()
    # NULL from a missing weekly row or a NULL column stays unknown; an unmapped
    # live ESPN player (5) is absent from these database results altogether.
    cursor.fetchall.side_effect = [
        [("1", 4.0, 15.5, 16.0, 15.0), ("2", None, 9.2, None, 9.2), ("3", None, None, None, None),
         ("4", None, None, None, None), ("6", 0.0, 0.0, None, 0.0)],
        [("p1", "1"), ("p2", "2")],  # roster player_id -> espn_id
        [("p1", 5, 90.0, 75.0, 1.0)],  # ownership: only player 1 has a row
        [],  # rumor signals
    ]
    with patch("src.services.matchup_service.build_league", return_value=league), \
         patch("src.services.matchup_service.require_upcoming_week", return_value=2), \
         patch("src.services.matchup_service.compute_roster_deficits", return_value={"teams": {}}), \
         patch("src.services.matchup_service.suggest_lineup_swaps", return_value={}), \
         patch("src.services.matchup_service.player_ids_by_espn_id", return_value={"1": "p1"}), \
         patch("src.services.matchup_service.get_db_cursor") as db:
        db.return_value.__enter__.return_value = cursor
        response = client.get("/v1/matchup/roster?season=2026")

    assert response.status_code == 200
    body = response.json()
    assert [p["projected_points"] for p in body["starters"]] == [15.5, None]
    assert [p["projected_points"] for p in body["bench"]] == [9.2, None, None, 0.0]
    assert body["bench"][0]["net_vorp"] is None
    assert body["starters"][0]["projection_sources"] == {"espn": 16.0, "sleeper": 15.0}
    assert body["starters"][1]["projection_sources"] is None
    assert [p["core_player_id"] for p in body["starters"]] == ["p1", None]
    assert body["starters"][0]["espn"] == {
        "pos_rank": 5, "percent_owned": 90.0, "percent_started": 75.0,
        "percent_bench": 15.0, "percent_change": 1.0,
    }
    assert body["starters"][1]["espn"] is None
    assert all(p["espn"] is None for p in body["bench"])
    assert cursor.execute.call_args_list[0].args[1] == (2026, 2, "8")
    league.box_scores.assert_not_called()


def test_get_matchup_serializes_core_player_id(client):
    line = {
        "player_id": "5", "espn_id": "5", "core_player_id": "9221", "name": "Back", "position": "RB",
        "slot": "RB", "points": None, "projected_points": None, "headshot_url": "h", "deficit_flag": False,
    }
    team = {"name": "T", "logo_url": "l", "score": 0.0, "projected": None}
    side = {"team": team, "starters": [line], "bench": []}
    fake = {"season": 2026, "week": 2, "our_team": side, "opponent": side}
    with patch("src.api.matchup.get_current_team_matchup", return_value=fake):
        response = client.get("/v1/matchup?season=2026&week=2")

    assert response.status_code == 200
    assert response.json()["our_team"]["starters"][0]["core_player_id"] == "9221"
