from unittest.mock import patch


def test_get_team_names_success(client):
    with patch("src.api.league.get_team_names", return_value={"1": "The Dynasty"}):
        response = client.get("/v1/league/teams?league_id=1248158344")

    assert response.status_code == 200
    assert response.json() == {"1": "The Dynasty"}


def test_get_team_names_defaults_league_id_from_env(client, monkeypatch):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "1248158344")
    with patch("src.api.league.get_team_names", return_value={}) as mock_get:
        response = client.get("/v1/league/teams")

    assert response.status_code == 200
    mock_get.assert_called_once_with("1248158344")


def test_get_team_names_db_failure_returns_500(client):
    with patch("src.api.league.get_team_names", side_effect=Exception("connection refused")):
        response = client.get("/v1/league/teams?league_id=1248158344")

    assert response.status_code == 500


def test_get_current_week_success(client):
    with patch("src.api.league.resolve_default_week", return_value=3):
        response = client.get("/v1/league/current-week")

    assert response.status_code == 200
    assert response.json() == {"week": 3}
