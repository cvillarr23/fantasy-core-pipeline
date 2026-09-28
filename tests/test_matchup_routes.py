from unittest.mock import patch

import requests
from espn_api.requests.espn_requests import ESPNAccessDenied

from src.services import matchup_service
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
    assert response.json() == fixture


def test_get_lineup_roster_espn_error_returns_502(client):
    with patch("src.api.matchup.get_current_team_roster", side_effect=ESPNAccessDenied("denied")):
        response = client.get("/v1/matchup/roster?season=2026")

    assert response.status_code == 502


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
