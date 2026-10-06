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

from src.ingestion.espn_client import TimedEspnFantasyRequests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LeagueSync")

BENCH_AND_IR_SLOT_IDS = {20, 21}


def get_db_connection():
    # Read-only ESPN tools must not open the database pool just to build a client.
    from src.database import get_db_connection as connect
    return connect()


def get_redis_client():
    from src.database import get_redis_client as connect
    return connect()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} environment variable is required for ESPN league sync but is not set.")
    return value


def build_league(season: int, request_timeout_seconds: int | None = None) -> League:
    """Constructs an authenticated espn_api League client from OpenBao-sourced env vars."""
    league_id = _require_env("ESPN_LEAGUE_ID")
    espn_s2 = _require_env("ESPN_S2")
    swid = _require_env("ESPN_SWID")
    kwargs = {"league_id": int(league_id), "year": season, "espn_s2": espn_s2, "swid": swid}
    if request_timeout_seconds is None:
        return League(**kwargs)

    league = League(**kwargs, fetch_league=False)
    league.espn_request = TimedEspnFantasyRequests(
        sport="nfl",
        year=season,
        league_id=int(league_id),
        cookies={"espn_s2": espn_s2, "SWID": swid},
        logger=league.logger,
        timeout_seconds=request_timeout_seconds,
    )
    league.fetch_league()
    return league


def fetch_raw_league_data(league: League, include_schedule: bool = False) -> dict:
    """
    espn_api's typed Settings/Player classes don't expose roster lineup slot counts or
    per-player lineup slot IDs (gaps in espn-api==0.33.0), so those are read directly off
    the raw mSettings/mRoster/mTeam payload instead.
    """
    views = ["mSettings", "mRoster", "mTeam"]
    if include_schedule:
        views.append("mMatchup")
    return league.espn_request.league_get(params={"view": views})


def _lineup_slot_counts_by_label(raw_counts: dict) -> dict:
    """The raw mSettings payload keys lineupSlotCounts by ESPN slot ID (e.g. "2" -> RB
    count, "23" -> FLEX count). Downstream VORP baseline/rank math looks positions up by
    label ("RB", "RB/WR/TE"), so translate via POSITION_MAP before persisting - leaving
    it slot-ID-keyed would make every lineup_slot_counts.get(pos, 0) silently return 0."""
    return {POSITION_MAP.get(int(slot_id), slot_id): count for slot_id, count in raw_counts.items()}


def fetch_league_config(league: League, raw: dict) -> dict:
    """team_count/scoring_type/name come from espn_api's typed Settings object;
    lineup_slot_counts and the full scoring settings block are only in the raw JSON."""
    settings = raw.get("settings", {})
    raw_slot_counts = settings.get("rosterSettings", {}).get("lineupSlotCounts", {})
    return {
        "team_count": league.settings.team_count,
        "scoring_type": league.settings.scoring_type,
        "name": league.settings.name,
        "lineup_slot_counts": _lineup_slot_counts_by_label(raw_slot_counts),
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


def _extract_espn_injury_status(entry: dict) -> str | None:
    player = entry.get("playerPoolEntry", {}).get("player") or entry.get("player") or {}
    status = player.get("injuryStatus")
    return status if isinstance(status, str) and status else None


def build_roster_rows(raw_teams: list, espn_to_player_id: dict) -> list:
    """
    Pure function: maps each ESPN roster entry -> (team_id, player_id, slot_position,
    is_starter, espn_injury_status), via the espn_id -> player_id crosswalk populated by
    sync_id_crosswalk. Unmatched entries (no espn_id crosswalk hit) are skipped, not errors.
    espn_injury_status is ESPN's own label, which decides IR eligibility (src/ir_eligibility).
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
            rows.append((team_id, player_id, slot_position, is_starter, _extract_espn_injury_status(entry)))
    if skipped:
        logger.warning(f"Skipped {skipped} roster entr(y/ies) with no espn_id -> player_id match.")
    return rows


def _extract_team_name(team: dict) -> str:
    """Replicates espn_api's own Team.team_name derivation exactly (team.py), so the
    name stored here matches what box_scores()/League.teams would show elsewhere."""
    name = team.get("name", "Unknown")
    if name != "Unknown":
        return name
    return "%s %s" % (team.get("location", "Unknown"), team.get("nickname", "Unknown"))


def build_team_name_rows(raw_teams: list) -> list:
    """Pure function: maps each raw ESPN team -> (team_id, team_name)."""
    return [(str(team.get("id")), _extract_team_name(team)) for team in raw_teams]


IR_SLOT_ID = 21


def build_ir_occupancy_rows(raw_teams: list) -> list:
    """Pure function: (team_id, IR slots in use) per team, counted from ESPN's entries
    directly. league_rosters can't answer this: build_roster_rows skips players with no
    espn_id crosswalk match, and one of them may well be the player sitting in IR."""
    return [
        (str(team.get("id")), sum(
            1 for entry in team.get("roster", {}).get("entries", []) if entry.get("lineupSlotId") == IR_SLOT_ID
        ))
        for team in raw_teams
    ]


def persist_ir_occupancy(conn, league_id: str, rows: list) -> None:
    """Runs after persist_team_names, which creates the league_teams rows."""
    with conn.cursor() as cursor:
        if rows:
            cursor.executemany(
                """
                UPDATE league_teams SET ir_slots_used = %s, updated_at = CURRENT_TIMESTAMP
                WHERE league_id = %s AND team_id = %s;
                """,
                [(count, league_id, team_id) for team_id, count in rows],
            )


def persist_team_names(conn, league_id: str, rows: list) -> int:
    """Upsert, not a full-refresh - a team doesn't disappear mid-season the way a
    roster's contents do, so there's no equivalent need to DELETE first."""
    with conn.cursor() as cursor:
        if rows:
            cursor.executemany(
                """
                INSERT INTO league_teams (league_id, team_id, team_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (league_id, team_id) DO UPDATE SET
                    team_name = EXCLUDED.team_name,
                    updated_at = CURRENT_TIMESTAMP;
                """,
                [(league_id, team_id, team_name) for team_id, team_name in rows],
            )
    return len(rows)


def persist_rosters(conn, rows: list) -> int:
    """Full refresh inside one transaction: dropped players must actually disappear, not
    linger from a prior week's roster snapshot."""
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM league_rosters;")
        if rows:
            cursor.executemany(
                """
                INSERT INTO league_rosters (team_id, player_id, slot_position, is_starter, espn_injury_status)
                VALUES (%s, %s, %s, %s, %s);
                """,
                rows,
            )
    return len(rows)


# Roster changes (pickups, drops, trades) happen all week, but sync_league only runs from
# the weekly ingestor; reads that decide waiver moves re-sync rosters at most this often.
ROSTER_REFRESH_SECONDS = 600


def sync_rosters(season: int, request_timeout_seconds: int = 10) -> int:
    """Rosters-only refresh of league_rosters from live ESPN data, cheap enough to run on
    request. Refuses to replace the table with an empty snapshot when ESPN returned no
    teams, since persist_rosters is a full DELETE+INSERT."""
    league = build_league(season, request_timeout_seconds=request_timeout_seconds)
    raw = league.espn_request.league_get(params={"view": ["mRoster", "mTeam"]})
    teams = raw.get("teams") or []
    if not teams:
        raise RuntimeError("ESPN returned no teams; keeping the existing roster snapshot")
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT espn_id, player_id FROM players WHERE espn_id IS NOT NULL;")
            espn_to_player_id = {row[0]: row[1] for row in cursor.fetchall()}
        written = persist_rosters(conn, build_roster_rows(teams, espn_to_player_id))
        league_id = _require_env("ESPN_LEAGUE_ID")
        persist_team_names(conn, league_id, build_team_name_rows(teams))
        persist_ir_occupancy(conn, league_id, build_ir_occupancy_rows(teams))
        return written


def refresh_rosters_if_stale(season: int, max_age_seconds: int = ROSTER_REFRESH_SECONDS) -> bool:
    """Re-sync league_rosters unless another request did so within max_age_seconds.

    A Redis SET NX key elects one refresher per window across gateway workers. Failures
    are logged and swallowed: callers fall back to the existing snapshot, and the key is
    left in place so an ESPN outage doesn't turn into one slow retry per request.
    Returns True when this call refreshed the table."""
    key = f"league_rosters:fresh:{os.getenv('ESPN_LEAGUE_ID')}:{season}"
    try:
        if not get_redis_client().set(key, "1", nx=True, ex=max_age_seconds):
            return False
    except Exception:
        logger.warning("Redis unavailable for the roster refresh gate; refreshing anyway")
    try:
        written = sync_rosters(season)
    except Exception as exc:
        logger.warning(f"Roster refresh failed; using the existing snapshot: {exc}")
        return False
    logger.info(f"Refreshed league_rosters from ESPN ({written} row(s)).")
    return True


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

        team_name_rows = build_team_name_rows(raw.get("teams", []))
        persist_team_names(conn, league_id, team_name_rows)
        persist_ir_occupancy(conn, league_id, build_ir_occupancy_rows(raw.get("teams", [])))

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
