"""
Syncs ESPN's weekly projections for QB/RB/WR/TE into weekly_stats.espn_projected_points, and
recomputes projected_points as the ESPN+Sleeper consensus (#462, src/projection_consensus.py).

Same ESPN path as sync_kicker_dst: the authenticated espn_api League from sync_league, and
the raw kona_player_info view for an explicit scoringPeriodId with no filterStatus, so
rostered players come back as well as free agents. Players are matched by players.espn_id
only, never by name: a player with no espn_id simply keeps his Sleeper-only consensus.

It only UPDATEs rows Sleeper projected (sleeper_projected_points IS NOT NULL), never inserts.
sync_projections (which runs first in each week's draft_prep_refresh stages) decides which
players have a row for the week, so "no row = bye", the lineup pool and vorp_matrix's
free-agent baseline pool are unchanged by this sync; only the values blend. The same filter
keeps it off rows from before #462, so they need no backfill. A player ESPN doesn't return, or has no projection for (or whose team is on bye
per the NFL schedule, since ESPN projects some byes at 0), gets espn_projected_points = NULL,
so the consensus falls back to Sleeper's value instead of keeping a stale ESPN one.

Must run after sync_projections and before vorp_matrix for the same season/week. An empty or
short ESPN response fails the run before anything is written. draft_prep_refresh treats a
failure as non-fatal: the consensus already falls back to Sleeper.
"""
import argparse
import json
import logging

from espn_api.football import Player
from espn_api.football.constant import POSITION_MAP

from src.database import get_db_connection
from src.ingestion.sync_kicker_dst import _projected_points
from src.ingestion.sync_league import build_league
from src.ingestion.sync_skill_position_crosswalk import PLAYER_POOL_FETCH_SIZE
from src.nfl_byes import bye_week_for, bye_weeks_or_empty
from src.projection_consensus import recompute_consensus
from src.services import run_log_service

logger = logging.getLogger("EspnProjectionsSync")

SKILL_POSITIONS = ["QB", "RB", "WR", "TE"]
# Per-request timeout. draft_prep_refresh treats this stage as optional and caps its budget,
# so a slow ESPN never holds back the week's vorp_matrix.
ESPN_REQUEST_TIMEOUT_SECONDS = 30
# Fewer players than this back for a position means a broken or truncated response, never a
# real universe (one starter-depth player per NFL team at least; the real unfiltered counts
# are several times higher). The whole sync then fails before writing anything, so stored
# ESPN values are never nulled by a bad fetch.
MIN_PLAYERS_PER_POSITION = {"QB": 32, "RB": 64, "WR": 96, "TE": 32}


class IncompleteEspnFetchError(RuntimeError):
    """ESPN returned an empty or short player list for a position."""


def fetch_espn_position_players(league, position: str, week: int) -> list:
    """Every ESPN player at `position`, rostered or free agent, with `week`'s projection in
    player.stats[week]. Same request as sync_kicker_dst.fetch_espn_kicker_dst."""
    params = {"view": "kona_player_info", "scoringPeriodId": week}
    filters = {
        "players": {
            "filterSlotIds": {"value": [POSITION_MAP[position]]},
            "limit": PLAYER_POOL_FETCH_SIZE,
            "sortPercOwned": {"sortPriority": 1, "sortAsc": False},
        }
    }
    data = league.espn_request.league_get(params=params, headers={"x-fantasy-filter": json.dumps(filters)})
    return [Player(raw_player, league.year) for raw_player in data.get("players", [])]


def projection_entries(espn_players: list, week: int, bye_weeks: dict) -> dict:
    """Pure. ESPN id -> `week`'s projection, or None when there is none or the player's team
    is on bye per the NFL schedule (ESPN projects some byes at 0)."""
    entries = {}
    for espn_player in espn_players:
        points = _projected_points(espn_player, week)
        if bye_week_for(espn_player.proTeam, bye_weeks) == week:
            points = None
        entries[str(espn_player.playerId)] = points
    return entries


def fetch_projection_entries(league, week: int, bye_weeks: dict) -> dict:
    """Fetches one position at a time and keeps only (espn_id -> points), so at most one
    position's Player objects are alive at once. Raises IncompleteEspnFetchError for an
    empty or short position, before anything is written."""
    entries = {}
    for position in SKILL_POSITIONS:
        espn_players = fetch_espn_position_players(league, position, week)
        if len(espn_players) < MIN_PLAYERS_PER_POSITION[position]:
            raise IncompleteEspnFetchError(
                f"ESPN returned {len(espn_players)} {position} player(s) for week {week}; "
                f"expected at least {MIN_PLAYERS_PER_POSITION[position]}. Writing nothing."
            )
        logger.info(f"Fetched {len(espn_players)} ESPN {position} player(s).")
        entries.update(projection_entries(espn_players, week, bye_weeks))
        del espn_players
    return entries


def load_skill_players_with_espn_ids(cursor) -> list:
    cursor.execute(
        "SELECT player_id, espn_id FROM players "
        "WHERE position IN ('QB', 'RB', 'WR', 'TE') AND espn_id IS NOT NULL;"
    )
    return [{"player_id": r[0], "espn_id": str(r[1])} for r in cursor.fetchall()]


def build_espn_projection_values(player_rows: list, entries: dict) -> tuple:
    """Pure. Returns (values, not_returned): values is a list of (player_id, points or None)
    for every row, matched by espn_id only; not_returned lists the player_ids ESPN didn't
    return at all."""
    values, not_returned = [], []
    for row in player_rows:
        if row["espn_id"] not in entries:
            not_returned.append(row["player_id"])
        values.append((row["player_id"], entries.get(row["espn_id"])))
    return values, not_returned


def persist_espn_projections(conn, values: list, season: int, week: int) -> int:
    """Sets espn_projected_points on the week's rows Sleeper projected (no inserts, and never
    a row whose sleeper_projected_points is NULL, so no row becomes ESPN-only) and recomputes
    the consensus. Returns the number of rows changed."""
    if not values:
        return 0
    player_ids = [player_id for player_id, _ in values]
    with conn.cursor() as cursor:
        cursor.execute(
            """
            UPDATE weekly_stats s
            SET espn_projected_points = v.points
            FROM unnest(%s::text[], %s::real[]) AS v(player_id, points)
            WHERE s.player_id = v.player_id AND s.season = %s AND s.week = %s
              AND s.sleeper_projected_points IS NOT NULL
              AND s.espn_projected_points IS DISTINCT FROM v.points;
            """,
            (player_ids, [points for _, points in values], season, week),
        )
        updated = cursor.rowcount
        recompute_consensus(cursor, season, week, player_ids)
    return updated


def sync_espn_projections(season: int, week: int) -> dict:
    logger.info(f"Syncing ESPN QB/RB/WR/TE projections for season {season} week {week}...")
    league = build_league(season, request_timeout_seconds=ESPN_REQUEST_TIMEOUT_SECONDS)
    entries = fetch_projection_entries(league, week, bye_weeks_or_empty(season))

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            player_rows = load_skill_players_with_espn_ids(cursor)
        values, not_returned = build_espn_projection_values(player_rows, entries)
        updated = persist_espn_projections(conn, values, season, week)

    projected = sum(1 for _, points in values if points is not None)
    if not_returned:
        logger.warning(f"ESPN returned no player for {len(not_returned)} of {len(player_rows)} espn_id(s).")
    logger.info(
        f"ESPN projections: {projected} of {len(player_rows)} player(s) projected for season {season} "
        f"week {week}; {updated} weekly_stats row(s) changed."
    )
    return {"players": len(player_rows), "projected": projected, "not_returned": len(not_returned), "updated": updated}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync ESPN QB/RB/WR/TE weekly projections into weekly_stats.")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int, required=True)
    args = parser.parse_args()
    with run_log_service.run_scope("sync_espn_projections", trigger={"season": args.season, "week": args.week}):
        sync_espn_projections(season=args.season, week=args.week)
