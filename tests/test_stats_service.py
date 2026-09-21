from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from src.schemas.stats import VorpSort
from src.services import stats_service

VORP_ROW = ("p1", "Some Back", "RB", "KC", 2026, 3, 15.2, 12.4, 1.05, 15.96, 4.2, 1.1, 0.98, 4.62)


@contextmanager
def _fake_db_cursor(cursor):
    yield cursor


def test_browse_vorp_applies_filters_and_defaults_to_vorp_sort():
    cursor = MagicMock()
    cursor.fetchall.return_value = [VORP_ROW]

    with patch("src.services.stats_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        results = stats_service.browse_vorp(season=2026, week=3, position="rb", sort=VorpSort.vorp, limit=200)

    assert results == [
        {
            "player_id": "p1", "name": "Some Back", "position": "RB", "team": "KC",
            "season": 2026, "week": 3, "projected_points": 15.2, "actual_points": 12.4,
            "matchup_coefficient": 1.05, "adjusted_projected_points": 15.96, "vorp_base": 4.2,
            "scarcity_multiplier": 1.1, "defense_matchup_multiplier": 0.98, "vorp_score": 4.62,
        }
    ]
    query, params = cursor.execute.call_args.args
    assert "s.season = %s AND s.week = %s AND p.position = %s" in query
    assert "ORDER BY s.vorp_score DESC NULLS LAST" in query
    assert params == (2026, 3, "RB", 200)


def test_browse_vorp_supports_season_trend_sort():
    cursor = MagicMock()
    cursor.fetchall.return_value = []

    with patch("src.services.stats_service.get_db_cursor", lambda: _fake_db_cursor(cursor)):
        results = stats_service.browse_vorp(season=2026, week=None, position=None, sort=VorpSort.week, limit=200)

    assert results == []
    query, params = cursor.execute.call_args.args
    assert "ORDER BY s.season, s.week, p.name" in query
    assert params == (2026, 200)
