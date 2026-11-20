"""
Stores ESPN's league-wide market signal - percent owned, percent started, the 24h change
in percent owned, and positional rank - per player per week in `espn_player_ownership`.

Same ESPN auth and raw `kona_player_info` request as sync_skill_position_crosswalk, sorted
by percent owned with no ownership filter, so the batch covers every rostered player plus
the most-owned free agents. ESPN ids map to `players.player_id` through `players.espn_id`,
so this runs after the crosswalk syncs; ids with no match are counted and skipped.

Runs in the daily draft-prep refresher before vorp_matrix, for the upcoming week. A rerun
within the week upserts that week's row; earlier weeks are left as history.
"""
import argparse
import json
import logging
import sys

import nflreadpy as nfl
from espn_api.utils.utils import json_parsing
from psycopg2.extras import execute_values

from src.database import get_db_connection
from src.ingestion.sync_league import build_league
from src.nfl_schedule import WeekResolutionError, resolve_upcoming_week
from src.services import run_log_service

logger = logging.getLogger("EspnOwnershipSync")

# Covers every rostered player in a 10-12 team league plus the relevant free agents.
OWNERSHIP_FETCH_LIMIT = 1000


def fetch_ownership_entries(league) -> list:
    """The raw kona_player_info entries for the most-owned players, all positions."""
    filters = {
        "players": {
            "limit": OWNERSHIP_FETCH_LIMIT,
            "sortPercOwned": {"sortPriority": 1, "sortAsc": False},
        }
    }
    data = league.espn_request.league_get(
        params={"view": "kona_player_info"},
        headers={"x-fantasy-filter": json.dumps(filters)},
    )
    return data.get("players", [])


def _none_if_missing(value):
    """ESPN sends -1 (or nothing) when it has no figure; that is unknown, not zero. Other
    negatives are real: percentChange is a signed delta."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value == -1:
        return None
    return value


def _positional_rank(entry: dict):
    """Same source espn_api's Player.posRank reads (the nested `positionalRanking`)."""
    rank = _none_if_missing(json_parsing(entry, "positionalRanking"))
    return None if rank is None else int(rank)


def _ownership_row(entry: dict) -> dict | None:
    """One ESPN entry as an `espn_player_ownership` row keyed by ESPN id, or None when the
    entry has no player id."""
    player = entry.get("player") or {}
    espn_id = player.get("id", entry.get("id"))
    if espn_id is None:
        return None
    ownership = player.get("ownership") or {}
    return {
        "espn_id": str(espn_id),
        "percent_owned": _none_if_missing(ownership.get("percentOwned")),
        "percent_started": _none_if_missing(ownership.get("percentStarted")),
        "percent_change": _none_if_missing(ownership.get("percentChange")),
        "pos_rank": _positional_rank(entry),
    }


def load_espn_id_map(cursor) -> dict:
    cursor.execute("SELECT espn_id, player_id FROM players WHERE espn_id IS NOT NULL;")
    return {row[0]: row[1] for row in cursor.fetchall()}


def build_ownership_rows(entries: list, espn_to_player_id: dict, season: int, week: int) -> tuple:
    """Returns (rows ready to upsert, count of entries skipped for having no matching
    player)."""
    rows, unmatched = [], 0
    for entry in entries:
        parsed = _ownership_row(entry)
        if parsed is None:
            continue
        player_id = espn_to_player_id.get(parsed.pop("espn_id"))
        if player_id is None:
            unmatched += 1
            continue
        rows.append({"player_id": player_id, "season": season, "week": week, **parsed})
    return rows, unmatched


def persist_ownership(conn, rows: list) -> int:
    """Upserts the rows in one batch; a repeat within the week replaces that week's row."""
    if not rows:
        return 0
    values = [
        (r["player_id"], r["season"], r["week"], r["percent_owned"], r["percent_started"],
         r["percent_change"], r["pos_rank"])
        for r in rows
    ]
    with conn.cursor() as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO espn_player_ownership
                (player_id, season, week, percent_owned, percent_started, percent_change, pos_rank)
            VALUES %s
            ON CONFLICT (player_id, season, week) DO UPDATE SET
                percent_owned = EXCLUDED.percent_owned,
                percent_started = EXCLUDED.percent_started,
                percent_change = EXCLUDED.percent_change,
                pos_rank = EXCLUDED.pos_rank,
                captured_at = now();
            """,
            values,
        )
    return len(rows)


def sync_espn_ownership(season: int, week: int) -> dict:
    logger.info(f"Syncing ESPN ownership for season {season} week {week}...")
    league = build_league(season)
    entries = fetch_ownership_entries(league)
    if not entries:
        raise RuntimeError("ESPN returned no players; keeping the existing ownership snapshot")
    logger.info(f"Fetched {len(entries)} ESPN player(s).")

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            espn_to_player_id = load_espn_id_map(cursor)
        rows, unmatched = build_ownership_rows(entries, espn_to_player_id, season, week)
        written = persist_ownership(conn, rows)

    logger.info(f"Ownership sync complete: {written} row(s) written, {unmatched} ESPN id(s) unmatched.")
    return {"written": written, "unmatched": unmatched}


def _resolve_season_week(season: int | None, week: int | None) -> tuple:
    """Defaults to the current season and its upcoming week; week is None when the regular
    season is over. Raises WeekResolutionError when the schedule can't be read."""
    if season is None:
        season = int(nfl.get_current_season())
    if week is None:
        week = resolve_upcoming_week(season)
    return season, week


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Store ESPN ownership, start% and positional rank.")
    parser.add_argument("--season", type=int, default=None, help="defaults to the current NFL season")
    parser.add_argument("--week", type=int, default=None, help="defaults to the upcoming week")
    args = parser.parse_args(argv)

    try:
        season, week = _resolve_season_week(args.season, args.week)
    except WeekResolutionError as exc:
        logger.error(f"Could not resolve the upcoming week: {exc}")
        return 1
    if week is None:
        logger.info(f"The {season} regular season is over; no ownership to capture.")
        return 0

    sync_espn_ownership(season=season, week=week)
    return 0


if __name__ == "__main__":
    sys.exit(run_log_service.run_main("sync_espn_ownership", main))
