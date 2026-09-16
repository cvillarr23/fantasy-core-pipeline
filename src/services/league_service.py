"""League-level lookups synced from ESPN (see src.ingestion.sync_league)."""
from src.database import get_db_cursor


def get_team_names(league_id: str) -> dict:
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT team_id, team_name FROM league_teams WHERE league_id = %s;",
            (league_id,),
        )
        return {team_id: team_name for team_id, team_name in cursor.fetchall()}
