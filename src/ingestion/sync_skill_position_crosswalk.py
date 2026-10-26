"""
Backfills `players.espn_id` for the QB/RB/WR/TE players the DynastyProcess crosswalk
(sync_id_crosswalk.py) never resolved (#150) - it covers 845 of 1027 players and zero
K/D-ST (see src/models.py's note); #83's sync_kicker_dst.py closed the K/D-ST slice of
that gap, this closes the remaining skill-position slice.

Unlike #83, this can't lean on League.free_agents() - that endpoint only returns players
unowned in the ESPN league, and #83 only got away with it because it ran pre-draft, when
"unowned" meant "the entire universe". The 2026 draft has already happened, so most
still-unmatched skill players are now rostered somewhere. Instead this calls
league.espn_request.league_get() directly with the same raw kona_player_info view
sync_league.py's fetch_raw_league_data already uses for other raw payloads, but omitting
the filterStatus ownership filter free_agents() hardcodes - that's what surfaces owned
players too.

Must run after sync_players (creates the base player rows) and after sync_kicker_dst
(same build_league client reuse precedent) - order relative to sync_league doesn't matter
since this only touches players.espn_id, not rosters.
"""
import argparse
import json
import logging

from espn_api.football import Player
from espn_api.football.constant import POSITION_MAP
from rapidfuzz import fuzz

from src.database import get_db_connection
from src.ingestion.sync_league import build_league
from src.services import run_log_service

from src.telemetry import configure_trace_logging

configure_trace_logging()
logger = logging.getLogger("SkillPositionCrosswalkSync")

SKILL_POSITIONS = ["QB", "RB", "WR", "TE"]

# Comfortably above the real skill-position universe (~1300 rostered/relevant players
# across all 32 teams) so one request per position captures everyone, owned or not.
PLAYER_POOL_FETCH_SIZE = 2500

# Below this rapidfuzz token_sort_ratio score, a name pair is treated as no match rather
# than a guess - mirrors sync_rumors.match_player's "no below-threshold guess" discipline.
FUZZY_MATCH_THRESHOLD = 90


def _normalize_name(name: str) -> str:
    return " ".join((name or "").split()).strip().lower()


def fetch_espn_skill_position_players(league, positions: list = SKILL_POSITIONS) -> list:
    """Returns every ESPN player (owned or not) for `positions`, via the raw
    kona_player_info view with no filterStatus - the ownership filter is what makes
    League.free_agents() miss rostered players."""
    players = []
    for position in positions:
        slot_filter = [POSITION_MAP[position]]
        params = {"view": "kona_player_info"}
        filters = {
            "players": {
                "filterSlotIds": {"value": slot_filter},
                "limit": PLAYER_POOL_FETCH_SIZE,
                "sortPercOwned": {"sortPriority": 1, "sortAsc": False},
            }
        }
        data = league.espn_request.league_get(params=params, headers={"x-fantasy-filter": json.dumps(filters)})
        for raw_player in data.get("players", []):
            players.append(Player(raw_player, league.year))
    return players


def load_skill_position_players(cursor) -> list:
    """The `players` rows this backfill tries to resolve an espn_id for - only the actual
    gap, not the whole table."""
    cursor.execute(
        "SELECT player_id, position, team, name FROM players "
        "WHERE position IN ('QB', 'RB', 'WR', 'TE') AND espn_id IS NULL;"
    )
    return [{"player_id": r[0], "position": r[1], "team": r[2], "name": r[3]} for r in cursor.fetchall()]


def match_skill_position_espn_ids(player_rows: list, espn_players: list) -> tuple:
    """Two-tier match: exact (normalized name, team) first, falling back to a high-cutoff
    fuzzy name match (position-scoped, no team requirement - a mid-season trade/waiver
    move can make players.team stale relative to ESPN's current team) for exact misses.
    Returns (matches, unmatched): matches is a list of (player_id, espn_player) pairs,
    unmatched is the list of internal player_ids with no confident ESPN counterpart."""
    matches, unmatched = [], []
    espn_by_position = {}
    for p in espn_players:
        espn_by_position.setdefault(p.position, []).append(p)

    for row in player_rows:
        if row["position"] not in SKILL_POSITIONS:
            continue
        candidates = espn_by_position.get(row["position"], [])
        espn_player = _match_exact(row, candidates) or _match_fuzzy(row, candidates)
        if espn_player is None:
            unmatched.append(row["player_id"])
        else:
            matches.append((row["player_id"], espn_player))
    return matches, unmatched


def _match_exact(row: dict, candidates: list):
    target = (_normalize_name(row["name"]), row["team"])
    for candidate in candidates:
        if (_normalize_name(candidate.name), candidate.proTeam) == target:
            return candidate
    return None


def _match_fuzzy(row: dict, candidates: list):
    if not candidates:
        return None
    target_name = _normalize_name(row["name"])
    best_candidate, best_score = None, 0
    for candidate in candidates:
        score = fuzz.token_sort_ratio(target_name, _normalize_name(candidate.name))
        if score > best_score:
            best_candidate, best_score = candidate, score
    if best_score >= FUZZY_MATCH_THRESHOLD:
        return best_candidate
    return None


def persist_espn_ids(conn, matches: list) -> int:
    """Backfills players.espn_id for resolved skill-position rows."""
    if not matches:
        return 0
    with conn.cursor() as cursor:
        cursor.executemany(
            "UPDATE players SET espn_id = %s WHERE player_id = %s;",
            [(str(espn_player.playerId), player_id) for player_id, espn_player in matches],
        )
    return len(matches)


def sync_skill_position_crosswalk(season: int) -> dict:
    logger.info(f"Syncing ESPN espn_id for remaining QB/RB/WR/TE players, season {season}...")
    league = build_league(season)
    espn_players = fetch_espn_skill_position_players(league)
    logger.info(f"Fetched {len(espn_players)} ESPN skill-position player(s).")

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            player_rows = load_skill_position_players(cursor)

        matches, unmatched = match_skill_position_espn_ids(player_rows, espn_players)
        espn_id_written = persist_espn_ids(conn, matches)

    if unmatched:
        logger.warning(f"Could not resolve an ESPN id for {len(unmatched)} player_id(s): {sorted(unmatched)}")

    logger.info(
        f"Skill-position crosswalk sync complete: {espn_id_written} espn_id(s) backfilled, "
        f"{len(unmatched)} still unmatched."
    )
    return {"espn_id_written": espn_id_written, "unmatched": unmatched}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill ESPN espn_id for remaining QB/RB/WR/TE players.")
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()
    with run_log_service.run_scope("sync_skill_position_crosswalk"):
        sync_skill_position_crosswalk(season=args.season)
