from contextlib import contextmanager
from unittest.mock import MagicMock, patch

LEAGUE_SETTINGS_ROW = (10, {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 7})


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


def test_get_roster_deficits_success(client):
    cursor = MagicMock()
    cursor.fetchone.return_value = LEAGUE_SETTINGS_ROW
    cursor.fetchall.side_effect = [
        [("team1",)],
        [("team1", "RB", 1, 12.5)],
    ]

    with patch("src.services.roster_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        response = client.get("/v1/roster/deficits?season=2026&week=3")

    assert response.status_code == 200
    body = response.json()
    assert body["season"] == 2026
    assert body["week"] == 3
    assert body["roster_snapshot"] == "current"
    assert body["teams"]["team1"]["RB"] == {
        "rostered": 1,
        "starters_required": 3,
        "deficit": 2,
        "net_vorp": 12.5,
    }


def test_get_roster_deficits_db_failure_returns_500(client):
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.services.roster_service.get_db_cursor", _broken_cursor):
        response = client.get("/v1/roster/deficits?season=2026&week=3")

    assert response.status_code == 500


def test_get_roster_deficits_requires_season_and_week(client):
    response = client.get("/v1/roster/deficits")

    assert response.status_code == 422


OUTLOOK = {
    "team_id": "4", "team_name": "Us", "season": 2026, "start_week": 7, "end_week": 10,
    "schedule_known": True, "median_lineup_value": 30.0, "crunch_weeks": [8],
    "weeks": [{
        "week": 8, "value": 18.0, "shortfall": 12.0, "unfilled_slots": ["RB"], "held_slots": [],
        "on_bye": ["101"], "unknown": [], "replacement_level_starters": 0, "crunch": True,
    }],
    "players": [{"espn_id": "101", "name": "Back", "position": "RB", "nfl_team": "BUF",
                 "nfl_bye_week": 8}],
}


def test_bye_outlook_defaults_to_our_league_and_team(client, monkeypatch):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "123")
    monkeypatch.setenv("ESPN_TEAM_ID", "4")
    with patch("src.api.roster.get_bye_outlook", return_value=OUTLOOK) as mock_outlook:
        response = client.get("/v1/roster/bye-outlook?season=2026&week=7")

    assert response.status_code == 200
    assert response.json()["crunch_weeks"] == [8]
    mock_outlook.assert_called_once_with("123", 2026, 7, 4, "4")


def test_bye_outlook_unknown_team_is_422(client):
    with patch("src.api.roster.get_bye_outlook", side_effect=ValueError("Unknown ESPN team 9")):
        response = client.get("/v1/roster/bye-outlook?season=2026&week=7&league_id=123&team_id=9")

    assert response.status_code == 422
