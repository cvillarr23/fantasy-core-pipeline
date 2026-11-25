"""
Player summary/search read logic, split out of src/api/players.py so the router stays a
thin HTTP controller over this module doing the actual DB work.
"""
from src.database import get_db_cursor
from src.nfl_byes import bye_week_for, bye_weeks_or_empty
from src.services import live_service
from src.services.espn_ownership import load_latest_espn_ownership

RECENT_NEWS_LIMIT = 5
WEEKLY_LOG_WEEKS = 4
SEARCH_RESULT_LIMIT = 20


class PlayerNotFoundError(Exception):
    """No players row exists for the requested player_id."""


def _player_row(cursor, player_id: str):
    cursor.execute("""
        SELECT player_id, name, position, team, status, injury_status, espn_id
        FROM players
        WHERE player_id = %s;
    """, (player_id,))
    return cursor.fetchone()


def _recent_news(cursor, player_id: str, limit: int) -> list:
    cursor.execute("""
        SELECT source, raw_text, url, published_at
        FROM raw_signals
        WHERE player_id = %s
        ORDER BY published_at DESC NULLS LAST
        LIMIT %s;
    """, (player_id, limit))
    return [
        {"source": row[0], "raw_text": row[1], "url": row[2], "published_at": row[3].isoformat() if row[3] else None}
        for row in cursor.fetchall()
    ]


def _played_weeks(cursor, player_id: str) -> list:
    """`(week, actual_points)` for the weeks of the player's latest season that have final actual
    scores, oldest first. `actual_points` defaults to 0.0, so only rows an actual-scores sync
    stamped (`actual_points_source`) count as played.

    That sync scores only players on our league's rosters (no table holds actuals for every
    player), so a player never rostered here has no weeks. It also stores a bye-week starter
    as 0.0, so weeks the player's team was on bye are left out; that week's `weekly_stats.team`
    is preferred over `players.team` for a player who changed teams."""
    cursor.execute("""
        SELECT ws.season, ws.week, ws.actual_points, COALESCE(ws.team, p.team)
        FROM weekly_stats ws
        JOIN players p ON p.player_id = ws.player_id
        WHERE ws.player_id = %s AND ws.actual_points_source IS NOT NULL AND ws.actual_points IS NOT NULL
        ORDER BY ws.season DESC, ws.week DESC;
    """, (player_id,))
    rows = cursor.fetchall()
    if not rows:
        return []
    latest = rows[0][0]
    byes = bye_weeks_or_empty(latest)
    return sorted(
        (week, points)
        for season, week, points, team in rows
        if season == latest and bye_week_for(team, byes) != week
    )


def _season_avg(played: list):
    return round(sum(points for _, points in played) / len(played), 2) if played else None


def _weekly_log(played: list) -> list:
    return [{"week": week, "points": points} for week, points in played[-WEEKLY_LOG_WEEKS:]]


def search_players(query: str) -> list:
    """Name lookup for the web UI's manual-pick picker, so a draft-day override can be typed
    as a player name instead of a raw id.

    Returns both ids deliberately. player_id is Sleeper's - it is the key for /summary and for
    every projection/VORP join. espn_id is what trigger-draft-pick requires, because the browser
    worker clicks button.Button--draft[data-player-id=...] in the ESPN room. The two spaces
    overlap numerically (ESPN 12483 is Matthew Stafford; internal 12483 is Jack Bech), so
    sending the wrong one silently drafts a different, real player."""
    with get_db_cursor() as cursor:
        # LATERAL pulls each player's single most-recent weekly_stats row rather than
        # requiring the caller to know which season/week is "current" - vorp_score/
        # projected_points are simply null pre-draft, not an error.
        cursor.execute("""
            SELECT p.player_id, p.espn_id, p.name, p.position, p.team, p.injury_status,
                   w.vorp_score, w.projected_points
            FROM players p
            LEFT JOIN LATERAL (
                SELECT vorp_score, projected_points
                FROM weekly_stats
                WHERE player_id = p.player_id
                ORDER BY season DESC, week DESC
                LIMIT 1
            ) w ON true
            WHERE p.name ILIKE %s
            ORDER BY p.name
            LIMIT %s;
        """, (f"%{query}%", SEARCH_RESULT_LIMIT))
        rows = cursor.fetchall()
        espn_by_player = load_latest_espn_ownership(cursor, [row[0] for row in rows])

    return [
        {
            "player_id": row[0], "espn_id": row[1], "name": row[2], "position": row[3], "team": row[4],
            "injury_status": row[5], "vorp_score": row[6], "projected_points": row[7],
            "espn": espn_by_player.get(row[0]),
        }
        for row in rows
    ]


def get_player_summary(player_id: str) -> dict:
    """Includes espn_id so a caller translating an internal id to ESPN's (the browser worker's
    trade staging) needs one lookup, not a summary-then-search round trip bounded by
    SEARCH_RESULT_LIMIT. Null when no espn_id is resolved yet; see search_players for why the
    two id spaces must never be substituted for each other."""
    with get_db_cursor() as cursor:
        row = _player_row(cursor, player_id)
        if row is None:
            raise PlayerNotFoundError(f"Unknown player_id '{player_id}'.")
        recent_news = _recent_news(cursor, player_id, RECENT_NEWS_LIMIT)
        espn = load_latest_espn_ownership(cursor, [player_id]).get(player_id)
        played = _played_weeks(cursor, player_id)
        live = live_service.player_live_summary(cursor, player_id)

    return {
        "player_id": row[0],
        "name": row[1],
        "position": row[2],
        "team": row[3],
        "status": row[4],
        "injury_status": row[5],
        "espn_id": row[6],
        "recent_news": recent_news,
        "espn": espn,
        "live": live,
        "season_avg": _season_avg(played),
        "weekly_log": _weekly_log(played),
    }
