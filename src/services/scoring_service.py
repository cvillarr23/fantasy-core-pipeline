"""
The league's fantasy scoring rules, read from `league_settings.scoring_settings` (ESPN's
`scoringSettings`, written by `sync_league`) and explained by `src.espn_scoring`. Backs
`GET /v1/scoring/rules` and the `breakdown` on the live pages (`live_service`).
"""
import json

from src.database import get_db_cursor
from src import espn_scoring


class ScoringRulesNotFoundError(LookupError):
    """No `league_settings` row (for that season): `sync_league` hasn't run."""


def load_scoring_settings(cursor, season: int | None = None) -> tuple[int, dict]:
    """`(season, scoring_settings)` for `season`, or for the latest synced season when None."""
    cursor.execute(
        """
        SELECT season, scoring_settings
        FROM league_settings
        WHERE %s::int IS NULL OR season = %s
        ORDER BY season DESC, updated_at DESC
        LIMIT 1;
        """,
        (season, season),
    )
    row = cursor.fetchone()
    if not row:
        raise ScoringRulesNotFoundError(f"No league settings synced{f' for season {season}' if season else ''}.")
    settings = json.loads(row[1]) if isinstance(row[1], str) else row[1]
    return row[0], settings or {}


def load_rule_points(cursor, season: int | None = None) -> dict:
    """`espn_scoring.rule_points` for the season, the index `score_breakdown` labels from."""
    _, settings = load_scoring_settings(cursor, season)
    return espn_scoring.rule_points(settings)


def get_scoring_rules(season: int | None = None) -> dict:
    with get_db_cursor() as cursor:
        resolved, settings = load_scoring_settings(cursor, season)
    return {"season": resolved, "rules": espn_scoring.scoring_rules(settings)}
