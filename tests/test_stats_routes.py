from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from src.api.stats import DEFAULT_VORP_LIMIT


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


VORP_ROW = (
    "p1",
    "Some Back",
    "RB",
    "KC",
    2026,
    3,
    15.2,
    12.4,
    1.05,
    15.96,
    4.2,
    1.1,
    0.98,
    4.62,
)

VORP_ROW_NULL_METRICS = (
    "p2",
    "Bye Week Guy",
    "WR",
    None,
    2026,
    3,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    None,
)


def test_browse_vorp_filters_and_defaults_to_vorp_sort(client):
    cursor = MagicMock()
    cursor.fetchall.return_value = [VORP_ROW]

    with patch("src.services.stats_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        response = client.get("/v1/stats/vorp?season=2026&week=3&position=rb")

    assert response.status_code == 200
    assert response.json() == [
        {
            "player_id": "p1",
            "name": "Some Back",
            "position": "RB",
            "team": "KC",
            "season": 2026,
            "week": 3,
            "projected_points": 15.2,
            "actual_points": 12.4,
            "matchup_coefficient": 1.05,
            "adjusted_projected_points": 15.96,
            "vorp_base": 4.2,
            "scarcity_multiplier": 1.1,
            "defense_matchup_multiplier": 0.98,
            "vorp_score": 4.62,
        }
    ]
    query, params = cursor.execute.call_args.args
    assert "s.season = %s AND s.week = %s AND p.position = %s" in query
    assert "ORDER BY s.vorp_score DESC NULLS LAST" in query
    assert "LIMIT %s" in query
    assert params == (2026, 3, "RB", DEFAULT_VORP_LIMIT)


def test_browse_vorp_supports_season_trend_sort(client):
    cursor = MagicMock()
    cursor.fetchall.return_value = []

    with patch("src.services.stats_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        response = client.get("/v1/stats/vorp?season=2026&sort=week")

    assert response.status_code == 200
    assert response.json() == []
    query, params = cursor.execute.call_args.args
    assert "ORDER BY s.season, s.week, p.name" in query
    assert params == (2026, DEFAULT_VORP_LIMIT)


def test_browse_vorp_supports_projected_points_sort(client):
    cursor = MagicMock()
    cursor.fetchall.return_value = [VORP_ROW]

    with patch("src.services.stats_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        response = client.get("/v1/stats/vorp?sort=projected_points")

    assert response.status_code == 200
    query, params = cursor.execute.call_args.args
    assert "ORDER BY s.projected_points DESC NULLS LAST" in query
    assert params == (DEFAULT_VORP_LIMIT,)


def test_browse_vorp_supports_actual_points_sort(client):
    cursor = MagicMock()
    cursor.fetchall.return_value = [VORP_ROW]

    with patch("src.services.stats_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        response = client.get("/v1/stats/vorp?sort=actual_points")

    assert response.status_code == 200
    query, params = cursor.execute.call_args.args
    assert "ORDER BY s.actual_points DESC NULLS LAST" in query
    assert params == (DEFAULT_VORP_LIMIT,)


def test_browse_vorp_serializes_null_metrics(client):
    cursor = MagicMock()
    cursor.fetchall.return_value = [VORP_ROW_NULL_METRICS]

    with patch("src.services.stats_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        response = client.get("/v1/stats/vorp")

    assert response.status_code == 200
    assert response.json() == [
        {
            "player_id": "p2",
            "name": "Bye Week Guy",
            "position": "WR",
            "team": None,
            "season": 2026,
            "week": 3,
            "projected_points": None,
            "actual_points": None,
            "matchup_coefficient": None,
            "adjusted_projected_points": None,
            "vorp_base": None,
            "scarcity_multiplier": None,
            "defense_matchup_multiplier": None,
            "vorp_score": None,
        }
    ]


def test_browse_vorp_rejects_unknown_sort(client):
    with patch("src.services.stats_service.get_db_cursor") as get_cursor:
        response = client.get("/v1/stats/vorp?sort=player_id")

    assert response.status_code == 422
    get_cursor.assert_not_called()


def test_browse_vorp_rejects_limit_out_of_bounds(client):
    with patch("src.services.stats_service.get_db_cursor") as get_cursor:
        response = client.get("/v1/stats/vorp?limit=0")

    assert response.status_code == 422
    get_cursor.assert_not_called()


def test_browse_vorp_db_failure_returns_500(client):
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.services.stats_service.get_db_cursor", _broken_cursor):
        response = client.get("/v1/stats/vorp")

    assert response.status_code == 500
    assert "connection refused" not in response.text
