"""
Backfills `players.espn_id` and `weekly_stats.projected_points` for K and D/ST (#83).

sync_id_crosswalk's DynastyProcess crosswalk has zero coverage for either position (see
src/models.py's note: "covers 845 of 1027 players and no K or D/ST at all"), and Sleeper
has no projections endpoint for them either (sync_projections.py is skill-position-only).
So this talks to ESPN directly via the same authenticated espn_api.football.League client
sync_league.py uses (ESPN_LEAGUE_ID/ESPN_S2/ESPN_SWID from OpenBao, no new credential
surface), reading league.free_agents() rather than raw JSON since BoxPlayer.proTeam/
.stats[week]['projected_points'] are exactly what's needed and espn_api already types them.

Must run after sync_players (creates the K/D-ST player_id rows this matches against).
Writing weekly_stats.projected_points here is what lets these rows pass
draft_matrix.load_undrafted_pool's inner join to weekly_stats and actually enter the
undrafted candidate pool - everything downstream (vorp_matrix's baseline/scarcity math,
draft_matrix's position caps) already handles a resolved K/D-ST row correctly on its own.
"""
import argparse
import logging

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

# Pre-draft, ESPN's free-agent endpoint returns the entire undrafted universe for a
# position, so a size comfortably above the real pool (32 D/ST, ~50-60 rostered K) is
# enough to capture all of it in one call.
FREE_AGENT_FETCH_SIZE = {"K": 100, "D/ST": 50}


def _normalize_team(pro_team: str) -> str:
    return ESPN_TO_INTERNAL_TEAM.get(pro_team, pro_team)


def _normalize_name(name: str) -> str:
    return " ".join((name or "").split()).strip().lower()


def fetch_espn_kicker_dst(league, week: int) -> list:
    """Returns (position, BoxPlayer) pairs for every free-agent K and D/ST for `week` -
    explicit rather than left to League.free_agents' own current_week default, since a
    pre-draft run targets whatever season/week the draft's projections are being built
    for, not "whatever week it happens to be right now"."""
    players = []
    for position, size in FREE_AGENT_FETCH_SIZE.items():
        for player in league.free_agents(week=week, size=size, position=position):
            players.append((position, player))
    return players


def load_kicker_dst_players(cursor) -> list:
    """The `players` rows this backfill tries to resolve an espn_id for."""
    cursor.execute("SELECT player_id, position, team, name FROM players WHERE position IN ('K', 'D/ST');")
    return [{"player_id": r[0], "position": r[1], "team": r[2], "name": r[3]} for r in cursor.fetchall()]


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
    """Matches K players by (normalized name, team) - both sources carry a real name/team
    for kickers (unlike D/ST), so this is a small, mostly-exact join rather than fuzzy
    matching infrastructure. Same (matches, unmatched) shape as match_dst_espn_ids."""
    espn_by_name_team = {(_normalize_name(p.name), _normalize_team(p.proTeam)): p for p in espn_k_players}
    matches, unmatched = [], []
    for row in player_rows:
        if row["position"] != "K":
            continue
        espn_player = espn_by_name_team.get((_normalize_name(row["name"]), row["team"]))
        if espn_player is None:
            unmatched.append(row["player_id"])
        else:
            matches.append((row["player_id"], espn_player))
    return matches, unmatched


def _projected_points(espn_player, week: int) -> float:
    return float(espn_player.stats.get(week, {}).get("projected_points", 0.0) or 0.0)


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
    """Writes weekly_stats.projected_points for resolved K/D-ST rows, sourced from ESPN's
    own per-player projection. Same upsert shape as sync_projections.persist_projections."""
    if not matches:
        return 0
    rows = [
        (player_id, season, week, _projected_points(espn_player, week))
        for player_id, espn_player in matches
    ]
    upsert_query = """
        INSERT INTO weekly_stats (player_id, season, week, projected_points)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (player_id, season, week) DO UPDATE SET projected_points = EXCLUDED.projected_points;
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
    logger.info(f"Fetched {len(espn_dst)} ESPN D/ST and {len(espn_k)} ESPN K free agent(s).")

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
        f"{projections_written} weekly_stats projection row(s) written for season {season} week {week}."
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
