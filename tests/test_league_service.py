from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from src.services import league_service


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


def test_get_team_names_returns_team_id_to_name_mapping():
    cursor = MagicMock()
    cursor.fetchall.return_value = [("1", "The Dynasty"), ("2", "Team Two")]

    with patch("src.services.league_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        result = league_service.get_team_names("123456789")

    assert result == {"1": "The Dynasty", "2": "Team Two"}


def test_get_team_names_scopes_query_to_league_id():
    cursor = MagicMock()
    cursor.fetchall.return_value = []

    with patch("src.services.league_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        league_service.get_team_names("123456789")

    params = cursor.execute.call_args.args[1]
    assert params == ("123456789",)
