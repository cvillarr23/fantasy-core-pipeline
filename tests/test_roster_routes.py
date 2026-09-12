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
