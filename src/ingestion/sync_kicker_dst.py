"""
Backfills `players.espn_id` and `weekly_stats.projected_points` for K and D/ST (#83).

sync_id_crosswalk's DynastyProcess crosswalk has zero coverage for either position (see
src/models.py's note: "covers 845 of 1027 players and no K or D/ST at all"), and Sleeper
has no projections endpoint for them either (sync_projections.py is skill-position-only).
So this talks to ESPN directly via the same authenticated espn_api.football.League client
sync_league.py uses (ESPN_LEAGUE_ID/ESPN_S2/ESPN_SWID from OpenBao, no new credential
surface). It reads the raw kona_player_info view for the target scoringPeriodId with no
filterStatus - the same ownership-agnostic query sync_skill_position_crosswalk uses (#150).
#83 originally read League.free_agents(), which hardcodes a FREEAGENT/WAIVERS filter: fine
pre-draft, but post-draft it never saw a rostered K/D-ST, so lineup_solver had to hold those
slots every week (#183). Free agents still come back too, and vorp_matrix needs them for its
post-draft replacement baseline.

Must run after sync_players (creates the K/D-ST player_id rows this matches against) and
before vorp_matrix for the same season/week; fantasy-draft-prep-refresher runs it daily
between sync_projections and vorp_matrix.
Writing weekly_stats.projected_points here is what lets these rows pass
draft_matrix.load_undrafted_pool's inner join to weekly_stats and actually enter the
undrafted candidate pool - everything downstream (vorp_matrix's baseline/scarcity math,
draft_matrix's position caps) already handles a resolved K/D-ST row correctly on its own.
"""
import argparse
import json
import logging

from espn_api.football import Player
from espn_api.football.constant import POSITION_MAP

from src.database import get_db_connection
from src.ingestion.sync_league import build_league

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KickerDSTSync")

# ESPN's PRO_TEAM_MAP spells Washington "WSH"; Sleeper - and therefore players.team and
# players.player_id for D/ST, which IS the team abbreviation - spells it "WAS". Every
# other one of the 32 abbreviations matches exactly between the two (confirmed live
# against both APIs), so this is the one team that needs normalizing, or its D/ST
# silently reproduces #83 for that team alone.
ESPN_TO_INTERNAL_TEAM = {"WSH": "WAS"}

# Comfortably above each position's whole ESPN universe (32 D/ST, ~50-60 K), owned or
# not, so one request per position captures all of it.
PLAYER_POOL_FETCH_SIZE = {"K": 100, "D/ST": 50}


def _normalize_team(pro_team: str) -> str:
    return ESPN_TO_INTERNAL_TEAM.get(pro_team, pro_team)


def _normalize_name(name: str) -> str:
    return " ".join((name or "").split()).strip().lower()


def fetch_espn_kicker_dst(league, week: int) -> list:
    """Returns (position, Player) pairs for every K and D/ST, rostered or free agent, with
    `week`'s projection in player.stats[week]. scoringPeriodId is explicit rather than
    ESPN's current week: the refresher targets the upcoming week, which ESPN may not have
    rolled over to yet when it runs early Tuesday."""
    players = []
    for position, size in PLAYER_POOL_FETCH_SIZE.items():
        params = {"view": "kona_player_info", "scoringPeriodId": week}
        filters = {
            "players": {
                "filterSlotIds": {"value": [POSITION_MAP[position]]},
                "limit": size,
                "sortPercOwned": {"sortPriority": 1, "sortAsc": False},
            }
        }
        data = league.espn_request.league_get(params=params, headers={"x-fantasy-filter": json.dumps(filters)})
        for raw_player in data.get("players", []):
            players.append((position, Player(raw_player, league.year)))
    return players


def load_kicker_dst_players(cursor) -> list:
    """The `players` rows this backfill tries to resolve an espn_id for."""
    cursor.execute("SELECT player_id, position, team, name, espn_id FROM players WHERE position IN ('K', 'D/ST');")
    return [
        {"player_id": r[0], "position": r[1], "team": r[2], "name": r[3], "espn_id": r[4]}
        for r in cursor.fetchall()
    ]


def match_dst_espn_ids(player_rows: list, espn_dst_players: list) -> tuple:
    """Matches D/ST players.player_id (== team abbreviation, Sleeper's own convention) to
    its ESPN BoxPlayer by team, normalizing ESPN's "WSH"/Sleeper's "WAS" mismatch. Returns
    (matches, unmatched): matches is a list of (player_id, espn_player) pairs, unmatched is
    the list of internal player_ids with no ESPN counterpart found."""
    espn_by_team = {_normalize_team(p.proTeam): p for p in espn_dst_players}
    matches, unmatched = [], []
    for row in player_rows:
        if row["position"] != "D/ST":
            continue
        espn_player = espn_by_team.get(row["team"])
        if espn_player is None:
            unmatched.append(row["player_id"])
        else:
            matches.append((row["player_id"], espn_player))
    return matches, unmatched


def match_kicker_espn_ids(player_rows: list, espn_k_players: list) -> tuple:
    """Matches K players by an already-resolved players.espn_id first, then by (normalized
    name, team) - both sources carry a real name/team for kickers (unlike D/ST), so this is
    a small, mostly-exact join rather than fuzzy matching infrastructure. The espn_id pass
    keeps a rostered kicker matched when players.team is stale after a signing. Same
    (matches, unmatched) shape as match_dst_espn_ids."""
    espn_by_id = {str(p.playerId): p for p in espn_k_players}
    espn_by_name_team = {(_normalize_name(p.name), _normalize_team(p.proTeam)): p for p in espn_k_players}
    matches, unmatched = [], []
    for row in player_rows:
        if row["position"] != "K":
            continue
        espn_player = espn_by_id.get(str(row.get("espn_id"))) or espn_by_name_team.get(
            (_normalize_name(row["name"]), row["team"])
        )
        if espn_player is None:
            unmatched.append(row["player_id"])
        else:
            matches.append((row["player_id"], espn_player))
    return matches, unmatched


def _projected_points(espn_player, week: int):
    """`week`'s ESPN projection, or None when there isn't one (e.g. a bye)."""
    points = espn_player.stats.get(week, {}).get("projected_points")
    return None if points is None else float(points)


def persist_espn_ids(conn, matches: list) -> int:
    """Backfills players.espn_id for resolved K/D-ST rows - the crosswalk this normally
    goes through (sync_id_crosswalk) has zero coverage for either position."""
    if not matches:
        return 0
    with conn.cursor() as cursor:
        cursor.executemany(
            "UPDATE players SET espn_id = %s WHERE player_id = %s;",
            [(str(espn_player.playerId), player_id) for player_id, espn_player in matches],
        )
    return len(matches)


def persist_projections(conn, matches: list, season: int, week: int) -> int:
    """Writes weekly_stats.projected_points (and team) for resolved K/D-ST rows, sourced
    from ESPN's own per-player projection. Same upsert shape as
    sync_projections.persist_projections, plus `team`: draft_matrix.load_undrafted_pool
    selects s.team (not players.team) and publish_draft_target forwards it as the
    required pro_team field, so leaving it NULL here left every K/D-ST recommendation
    rejected with a 422 the moment the worker tried to publish one. espn_player.proTeam,
    normalized, is ESPN's current team for the matched player, so there's no separate
    lookup needed.

    A match with no `week` projection (a bye) gets no row: lineup_solver's pregame pool
    reads "no target-week row" as "on bye", so a 0.0 row would make it look startable."""
    rows = [
        (player_id, season, week, points, _normalize_team(espn_player.proTeam))
        for player_id, espn_player in matches
        if (points := _projected_points(espn_player, week)) is not None
    ]
    if not rows:
        return 0
    upsert_query = """
        INSERT INTO weekly_stats (player_id, season, week, projected_points, team)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (player_id, season, week) DO UPDATE SET
            projected_points = EXCLUDED.projected_points,
            team = EXCLUDED.team;
    """
    with conn.cursor() as cursor:
        cursor.executemany(upsert_query, rows)
    return len(rows)


def sync_kicker_dst(season: int, week: int) -> dict:
    logger.info(f"Syncing ESPN K/D-ST espn_id + projections for season {season} week {week}...")
    league = build_league(season)
    espn_players = fetch_espn_kicker_dst(league, week)
    espn_dst = [p for pos, p in espn_players if pos == "D/ST"]
    espn_k = [p for pos, p in espn_players if pos == "K"]
    logger.info(f"Fetched {len(espn_dst)} ESPN D/ST and {len(espn_k)} ESPN K (rostered and free agent).")

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            player_rows = load_kicker_dst_players(cursor)

        dst_matches, dst_unmatched = match_dst_espn_ids(player_rows, espn_dst)
        k_matches, k_unmatched = match_kicker_espn_ids(player_rows, espn_k)
        all_matches = dst_matches + k_matches

        espn_id_written = persist_espn_ids(conn, all_matches)
        projections_written = persist_projections(conn, all_matches, season, week)

    if dst_unmatched:
        logger.warning(f"Could not resolve an ESPN D/ST for team(s): {sorted(dst_unmatched)}")
    if k_unmatched:
        logger.warning(f"Could not resolve an ESPN kicker for player_id(s): {sorted(k_unmatched)}")

    logger.info(
        f"K/D-ST sync complete: {espn_id_written} espn_id(s) backfilled, "
        f"{projections_written} weekly_stats projection row(s) written for season {season} week {week} "
        f"({len(all_matches) - projections_written} matched player(s) had no projection, e.g. on bye)."
    )
    return {
        "espn_id_written": espn_id_written,
        "projections_written": projections_written,
        "dst_unmatched": dst_unmatched,
        "k_unmatched": k_unmatched,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill ESPN espn_id + projections for K/D-ST.")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int, required=True)
    args = parser.parse_args()
    sync_kicker_dst(season=args.season, week=args.week)
