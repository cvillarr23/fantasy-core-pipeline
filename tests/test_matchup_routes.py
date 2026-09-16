from unittest.mock import patch

from espn_api.requests.espn_requests import ESPNAccessDenied

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


def test_get_matchup_unexpected_error_returns_500(client):
    with patch("src.api.matchup.get_current_team_matchup", side_effect=Exception("boom")):
        response = client.get("/v1/matchup?season=2026&week=2")

    assert response.status_code == 500


def test_get_matchup_requires_season_and_week(client):
    response = client.get("/v1/matchup")

    assert response.status_code == 422
