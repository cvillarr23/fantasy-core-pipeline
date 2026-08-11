"""
Syncs ESPN league configuration (team count, scoring, lineup slot counts) and roster
assignments for the real fantasy league this pipeline serves. Requires ESPN_LEAGUE_ID,
ESPN_S2, and ESPN_SWID (sourced from OpenBao in deployed environments) - no defaults, and
no credentials are committed here.

Must run after sync_id_crosswalk (roster rows are matched via players.espn_id). Tolerates
an all-empty roster response as a valid, logged outcome - true pre-draft.
"""
import argparse
import json
import logging
import os

from espn_api.football import League
from espn_api.football.constant import POSITION_MAP

from src.database import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LeagueSync")

BENCH_AND_IR_SLOT_IDS = {20, 21}


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} environment variable is required for ESPN league sync but is not set.")
    return value


def build_league(season: int) -> League:
    """Constructs an authenticated espn_api League client from OpenBao-sourced env vars."""
    league_id = _require_env("ESPN_LEAGUE_ID")
    espn_s2 = _require_env("ESPN_S2")
    swid = _require_env("ESPN_SWID")
    return League(league_id=int(league_id), year=season, espn_s2=espn_s2, swid=swid)


def fetch_raw_league_data(league: League) -> dict:
    """
    espn_api's typed Settings/Player classes don't expose roster lineup slot counts or
    per-player lineup slot IDs (gaps in espn-api==0.33.0), so those are read directly off
    the raw mSettings/mRoster/mTeam payload instead.
    """
    return league.espn_request.league_get(params={"view": ["mSettings", "mRoster", "mTeam"]})


def fetch_league_config(league: League, raw: dict) -> dict:
    """team_count/scoring_type/name come from espn_api's typed Settings object;
    lineup_slot_counts and the full scoring settings block are only in the raw JSON."""
    settings = raw.get("settings", {})
    return {
        "team_count": league.settings.team_count,
        "scoring_type": league.settings.scoring_type,
        "name": league.settings.name,
        "lineup_slot_counts": settings.get("rosterSettings", {}).get("lineupSlotCounts", {}),
        "scoring_settings": settings.get("scoringSettings", {}),
    }


def persist_league_settings(conn, league_id: str, season: int, config: dict) -> None:
    upsert_query = """
        INSERT INTO league_settings
            (league_id, season, name, team_count, scoring_type, lineup_slot_counts, scoring_settings, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (league_id) DO UPDATE SET
            season = EXCLUDED.season,
            name = EXCLUDED.name,
            team_count = EXCLUDED.team_count,
            scoring_type = EXCLUDED.scoring_type,
            lineup_slot_counts = EXCLUDED.lineup_slot_counts,
            scoring_settings = EXCLUDED.scoring_settings,
            updated_at = CURRENT_TIMESTAMP;
    """
    with conn.cursor() as cursor:
        cursor.execute(upsert_query, (
            league_id, season, config["name"], config["team_count"], config["scoring_type"],
            json.dumps(config["lineup_slot_counts"]), json.dumps(config["scoring_settings"]),
        ))


def _extract_espn_player_id(entry: dict) -> str | None:
    if entry.get("playerId") is not None:
        return str(entry["playerId"])
    player = entry.get("playerPoolEntry", {}).get("player") or entry.get("player") or {}
    if player.get("id") is not None:
        return str(player["id"])
    return None


def build_roster_rows(raw_teams: list, espn_to_player_id: dict) -> list:
    """
    Pure function: maps each ESPN roster entry -> (team_id, player_id, slot_position,
    is_starter), via the espn_id -> player_id crosswalk populated by sync_id_crosswalk.
    Unmatched entries (no espn_id crosswalk hit) are skipped, not errors.
    """
    rows = []
    skipped = 0
    for team in raw_teams:
        team_id = str(team.get("id"))
        entries = team.get("roster", {}).get("entries", [])
        for entry in entries:
            espn_player_id = _extract_espn_player_id(entry)
            player_id = espn_to_player_id.get(espn_player_id) if espn_player_id else None
            if player_id is None:
                skipped += 1
                continue
            slot_id = entry.get("lineupSlotId")
            slot_position = POSITION_MAP.get(slot_id, str(slot_id))
            is_starter = slot_id not in BENCH_AND_IR_SLOT_IDS
            rows.append((team_id, player_id, slot_position, is_starter))
    if skipped:
        logger.warning(f"Skipped {skipped} roster entr(y/ies) with no espn_id -> player_id match.")
    return rows


def persist_rosters(conn, rows: list) -> int:
    """Full refresh inside one transaction: dropped players must actually disappear, not
    linger from a prior week's roster snapshot."""
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM league_rosters;")
        if rows:
            cursor.executemany(
                """
                INSERT INTO league_rosters (team_id, player_id, slot_position, is_starter)
                VALUES (%s, %s, %s, %s);
                """,
                rows,
            )
    return len(rows)


def sync_league(season: int) -> int:
    league_id = os.getenv("ESPN_LEAGUE_ID") or _require_env("ESPN_LEAGUE_ID")
    logger.info(f"Syncing ESPN league {league_id} config and rosters for season {season}...")

    league = build_league(season)
    raw = fetch_raw_league_data(league)
    config = fetch_league_config(league, raw)
    logger.info(
        f"League config: team_count={config['team_count']}, scoring_type={config['scoring_type']}"
    )

    with get_db_connection() as conn:
        persist_league_settings(conn, league_id, season, config)

        with conn.cursor() as cursor:
            cursor.execute("SELECT espn_id, player_id FROM players WHERE espn_id IS NOT NULL;")
            espn_to_player_id = {row[0]: row[1] for row in cursor.fetchall()}

        rows = build_roster_rows(raw.get("teams", []), espn_to_player_id)
        written = persist_rosters(conn, rows)

    if written == 0:
        logger.info("Persisted 0 roster rows - all rosters are empty (expected pre-draft).")
    else:
        logger.info(f"Persisted {written} roster row(s).")
    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync ESPN league settings and rosters.")
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    sync_league(season=args.season)
