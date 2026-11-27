"""
Stores ESPN's current NFL injury report in `player_injuries`, from ESPN's public injuries
feed (one request, every team). It is the only source of an expected return date: ESPN's
fantasy roster payload carries an injuryStatus but no timeline, and Sleeper has neither.

Each non-Active entry has a status ("Questionable", "Out", "Injured Reserve", ...),
`details.fantasyStatus` (IR, IR-R, OUT, ...) and `details.returnDate`. For Questionable
that date is just the next game; for Out and IR it is ESPN's estimate. The athlete id is
not a field, but every `athlete.links` href carries `/id/<espn id>/`, the same id as
`players.espn_id`. Rows are keyed by that ESPN id and never mapped through player_id.

Each run replaces the table in one transaction, since a player missing from the feed is
healthy. A feed with no entries, or entries whose ids none parse, fails the run and keeps
the previous snapshot.
"""
import logging
import re
import sys
from datetime import date, datetime, timezone

import requests
from psycopg2.extras import execute_values

from src.database import get_db_connection
from src.services import run_log_service

logger = logging.getLogger("InjurySync")

INJURIES_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/injuries"
FETCH_TIMEOUT_SECONDS = (5, 60)
FETCH_ATTEMPTS = 3
_ATHLETE_ID = re.compile(r"/id/(\d+)(?:/|$)")


def fetch_injury_feed(get=requests.get, attempts=FETCH_ATTEMPTS) -> dict:
    """The raw feed, retrying any failure; the last one raises."""
    for attempt in range(1, attempts + 1):
        try:
            response = get(INJURIES_URL, timeout=FETCH_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            if attempt == attempts:
                raise
            logger.warning(f"Injury feed fetch attempt {attempt}/{attempts} failed: {exc}")


def athlete_espn_id(athlete: dict) -> str | None:
    for link in athlete.get("links") or []:
        match = _ATHLETE_ID.search(link.get("href") or "")
        if match:
            return match.group(1)
    return None


def _parse_time(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _parse_date(value):
    try:
        return date.fromisoformat(value) if value else None
    except ValueError:
        return None


def parse_injury_feed(feed: dict) -> tuple[list[dict], dict]:
    """(rows, counts). One row per injured athlete, from his newest report; Active entries
    are news, not injuries, and are skipped."""
    entries = [entry for team in feed.get("injuries") or [] for entry in team.get("injuries") or []]
    newest, unparsed, injured = {}, 0, 0
    for entry in entries:
        if (entry.get("status") or "Active") == "Active":
            continue
        injured += 1
        athlete = entry.get("athlete") or {}
        espn_id = athlete_espn_id(athlete)
        if espn_id is None:
            unparsed += 1
            continue
        details = entry.get("details") or {}
        row = {
            "espn_id": espn_id,
            "status": entry["status"],
            "fantasy_status": (details.get("fantasyStatus") or {}).get("abbreviation"),
            "nfl_team": (athlete.get("team") or {}).get("abbreviation"),
            "injury_type": details.get("type"),
            "injury_detail": details.get("detail"),
            "return_date": _parse_date(details.get("returnDate")),
            "reported_at": _parse_time(entry.get("date")),
        }
        current = newest.get(espn_id)
        if current is None or (row["reported_at"] or datetime.min.replace(tzinfo=timezone.utc)) > (
            current["reported_at"] or datetime.min.replace(tzinfo=timezone.utc)
        ):
            newest[espn_id] = row
    counts = {"entries": len(entries), "injured_entries": injured, "unparsed_ids": unparsed}
    return sorted(newest.values(), key=lambda r: r["espn_id"]), counts


def replace_injuries(conn, rows: list) -> int:
    """Swaps the whole snapshot in the caller's transaction."""
    columns = ("espn_id", "status", "fantasy_status", "nfl_team", "injury_type",
               "injury_detail", "return_date", "reported_at")
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM player_injuries;")
        if rows:
            execute_values(
                cursor,
                f"INSERT INTO player_injuries ({', '.join(columns)}) VALUES %s;",
                [tuple(row[c] for c in columns) for row in rows],
            )
    return len(rows)


def sync_injuries(fetch=fetch_injury_feed) -> dict:
    feed = fetch()
    rows, counts = parse_injury_feed(feed)
    if not counts["entries"]:
        raise RuntimeError("ESPN's injury feed had no entries; keeping the existing snapshot")
    if counts["injured_entries"] and not rows:
        raise RuntimeError("No injured athlete's ESPN id could be parsed; keeping the existing snapshot")
    if counts["unparsed_ids"]:
        logger.warning(f"Skipped {counts['unparsed_ids']} injury entr(ies) with no parsable ESPN id.")
    with get_db_connection() as conn:
        written = replace_injuries(conn, rows)
    dated = sum(1 for row in rows if row["return_date"] is not None)
    logger.info(f"Injury sync complete: {written} injured player(s), {dated} with a return date.")
    run_log_service.add_summary(written=written, with_return_date=dated, **counts)
    return {"written": written, "with_return_date": dated, **counts}


def main(argv=None) -> int:
    sync_injuries()
    return 0


if __name__ == "__main__":
    sys.exit(run_log_service.run_main("sync_injuries", main))
