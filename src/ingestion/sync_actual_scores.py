"""Persist ESPN scoring weeks and schedule evidence; dry-run never imports the DB."""

import argparse
from collections import defaultdict
import json
import logging
import math
import os
import re

import requests
from espn_api.football.constant import POSITION_MAP
from espn_api.requests.espn_requests import (
    ESPNAccessDenied,
    ESPNInvalidLeague,
    ESPNUnknownError,
)

from src.espn_positions import DEFAULT_POSITIONS
from src.ingestion.sync_league import (
    build_league,
    fetch_raw_league_data,
    _extract_team_name,
)

logger = logging.getLogger(__name__)


class IncompleteScoresError(ValueError):
    """The source cannot safely describe a complete scoring week."""


def integer(value):
    if isinstance(value, bool) or not re.fullmatch(r"[1-9]\d*", str(value)):
        raise IncompleteScoresError("Invalid ESPN period or identity")
    return int(value)


def number(value):
    if value is None:
        return None
    if isinstance(value, bool):
        raise IncompleteScoresError("Invalid score")
    try:
        value = float(value)
    except (TypeError, ValueError) as exc:
        raise IncompleteScoresError("Invalid score") from exc
    if not math.isfinite(value):
        raise IncompleteScoresError("Non-finite score")
    return value


def validate_scope(raw, league_id, season):
    if str(raw.get("id")) != league_id or raw.get("seasonId") != season:
        raise IncompleteScoresError("ESPN returned the wrong league or season")


def period_settings(raw):
    try:
        settings = raw["settings"]["scheduleSettings"]
        regular_count = integer(settings["matchupPeriodCount"])
        periods = {
            integer(p): [integer(w) for w in weeks]
            for p, weeks in settings["matchupPeriods"].items()
        }
        final = integer(raw["status"]["finalScoringPeriod"])
        flat = [w for p in sorted(periods) for w in periods[p]]
        if (
            not flat
            or flat != list(range(1, final + 1))
            or final > 18
            or not all(p in periods for p in range(1, regular_count + 1))
        ):
            raise IncompleteScoresError("Incomplete scoring-period mapping")
        return periods, regular_count
    except (KeyError, TypeError, AttributeError) as exc:
        raise IncompleteScoresError("Missing ESPN schedule settings") from exc


def completed_weeks(raw):
    periods, _ = period_settings(raw)
    observed = integer(raw.get("scoringPeriodId"))
    weeks = [w for ws in periods.values() for w in ws if w < observed]
    # ESPN can remain on the final scoring period after the season ends. A decided
    # period makes it a candidate; the NFL scoreboard still gates actual persistence.
    if observed == max(w for ws in periods.values() for w in ws):
        period = next(p for p, ws in periods.items() if observed in ws)
        games = [
            g for g in raw.get("schedule", []) if g.get("matchupPeriodId") == period
        ]
        if games and all(g.get("winner") in {"HOME", "AWAY", "TIE"} for g in games):
            weeks.append(observed)
    return sorted(weeks)


def shape_schedule(raw, league_id, season):
    validate_scope(raw, league_id, season)
    periods, regular_count = period_settings(raw)
    teams = {str(integer(t["id"])) for t in raw.get("teams", [])}
    if not teams or len(teams) != len(raw.get("teams", [])):
        raise IncompleteScoresError("Missing ESPN teams")
    rows = {}
    for game in raw.get("schedule", []):
        period = integer(game.get("matchupPeriodId"))
        if period not in periods:
            raise IncompleteScoresError("Unknown matchup period")
        for side, other in (("home", "away"), ("away", "home")):
            own = game.get(side) or {}
            if own.get("teamId") in (None, 0, -1):
                continue
            team_id = str(integer(own["teamId"]))
            opponent = (game.get(other) or {}).get("teamId")
            opponent = (
                None if opponent in (None, 0, -1, "0", "-1") else str(integer(opponent))
            )
            status = "bye" if opponent is None else "scheduled"
            if team_id not in teams or (
                opponent is not None and (opponent not in teams or opponent == team_id)
            ):
                raise IncompleteScoresError("Unknown or conflicting matchup team")
            key = (period, team_id)
            if key in rows:
                raise IncompleteScoresError("Ambiguous matchup pairing")
            rows[key] = {
                "team_id": team_id,
                "period": period,
                "weeks": periods[period],
                "opponent_team_id": opponent,
                "status": status,
                "is_home": side == "home",
                "regular": period <= regular_count,
                "winner": game.get("winner"),
            }
    # Missing entries remain unavailable, never inferred to be byes. Persisting them
    # also removes a stale pairing if a later ESPN snapshot withdraws that evidence.
    for period, weeks in periods.items():
        for team_id in teams:
            rows.setdefault(
                (period, team_id),
                {
                    "team_id": team_id,
                    "period": period,
                    "weeks": weeks,
                    "opponent_team_id": None,
                    "status": "unavailable",
                    "is_home": False,
                    "regular": period <= regular_count,
                    "winner": None,
                },
            )
    return list(rows.values())


def validate_scoreboard(board, season, week, expected_ids=None):
    if (
        board.get("season", {}).get("year") != season
        or board.get("season", {}).get("type") != 2
        or board.get("week", {}).get("number") != week
    ):
        raise IncompleteScoresError("Wrong NFL scoreboard season/week")
    events = board.get("events") or []
    ids = {str(e.get("id")) for e in events}
    if (
        not events
        or len(ids) != len(events)
        or any(
            e.get("status", {}).get("type", {}).get("completed") is not True
            for e in events
        )
        or (expected_ids is not None and ids != set(expected_ids))
    ):
        raise IncompleteScoresError("NFL games are missing or not final")


def shape_player(entry, season, week, team_id, bye_teams=frozenset()):
    raw = (
        (entry.get("playerPoolEntry") or {}).get("player") or entry.get("player") or {}
    )
    espn_id = str(entry.get("playerId", raw.get("id")))
    if not re.fullmatch(r"-?[1-9]\d*", espn_id):
        raise IncompleteScoresError("Missing player identity")
    slot = POSITION_MAP.get(entry.get("lineupSlotId"))
    if slot is None or not raw.get("fullName"):
        raise IncompleteScoresError("Missing historical player/slot")
    scores = {}
    for stat in raw.get("stats", []):
        if (
            stat.get("seasonId") == season
            and stat.get("scoringPeriodId") == week
            and stat.get("statSplitTypeId") == 1
            and stat.get("statSourceId") in (0, 1)
        ):
            source = stat["statSourceId"]
            if source in scores:
                raise IncompleteScoresError("Ambiguous player score")
            scores[source] = number(stat.get("appliedTotal"))
    actual = scores.get(0)
    if actual is None:
        actual = number(entry.get("appliedStatTotal"))
    if actual is None and slot not in {"BE", "IR"}:
        # ESPN writes no actual stat line for a player whose NFL team is idle, so a bye
        # starter is a true zero. Anyone else missing a score is still unknown.
        if raw.get("proTeamId") not in bye_teams:
            raise IncompleteScoresError("Missing starter actual score")
        actual = 0.0
    return {
        "espn_id": espn_id,
        "player_id": espn_id,
        "team_id": team_id,
        "name": raw["fullName"],
        "position": DEFAULT_POSITIONS.get(raw.get("defaultPositionId"), ""),
        "jersey": raw.get("jersey") if isinstance(raw.get("jersey"), str) else None,
        "slot": slot,
        "actual_points": actual,
        "points": actual,
        "projected_points": scores.get(1),
        "headshot_url": f"https://a.espncdn.com/i/headshots/nfl/players/full/{espn_id}.png",
        "deficit_flag": False,
        "suggested_swap": None,
    }


def shape_week(snapshot, box, board, league_id, season, week):
    validate_scope(snapshot, league_id, season)
    validate_scope(box, league_id, season)
    if week not in completed_weeks(snapshot):
        raise IncompleteScoresError("Requested scoring week is upcoming or in progress")
    # A queried box-score response reports the requested week, unlike the unfiltered
    # snapshot's observed current week. Never use it as the completion watermark.
    if box.get("scoringPeriodId") != week:
        raise IncompleteScoresError("ESPN substituted another scoring period")
    expected = snapshot.get("_nfl_game_ids_by_week", {}).get(str(week))
    if "_nfl_game_ids_by_week" in snapshot and not expected:
        raise IncompleteScoresError("Missing professional schedule for scoring week")
    if (
        "_nfl_official_weeks" in snapshot
        and week not in snapshot["_nfl_official_weeks"]
    ):
        raise IncompleteScoresError("ESPN professional statistics are not official")
    validate_scoreboard(board, season, week, expected)
    schedule = shape_schedule(snapshot, league_id, season)
    target = {r["team_id"]: r for r in schedule if week in r["weeks"]}
    teams = {str(t["id"]): t for t in snapshot["teams"]}
    bye_teams = frozenset(snapshot.get("_nfl_bye_teams_by_week", {}).get(str(week), []))
    result, players, seen = [], [], set()
    for game in box.get("schedule", []):
        period = integer(game.get("matchupPeriodId"))
        for side in ("home", "away"):
            own = game.get(side) or {}
            if own.get("teamId") in (None, 0, -1):
                continue
            team_id = str(integer(own["teamId"]))
            row = target.get(team_id)
            if row is None or row["period"] != period or team_id in seen:
                raise IncompleteScoresError("Wrong or duplicate box-score pairing")
            other = (game.get("away" if side == "home" else "home") or {}).get("teamId")
            other = None if other in (None, 0, -1) else str(other)
            if other != row["opponent_team_id"] or row["status"] == "unavailable":
                raise IncompleteScoresError("Box-score and schedule disagree")
            total = number(own.get("totalPoints"))
            score = number((own.get("pointsByScoringPeriod") or {}).get(str(week)))
            if score is None and len(row["weeks"]) == 1:
                score = total
            if score is None or total is None:
                raise IncompleteScoresError("Missing scoring-week team score")
            if (
                (
                    max(row["weeks"]) < snapshot["scoringPeriodId"]
                    or week == snapshot["scoringPeriodId"] == max(row["weeks"])
                )
                and row["status"] != "bye"
                and game.get("winner") not in {"HOME", "AWAY", "TIE"}
            ):
                raise IncompleteScoresError("Fantasy matchup result is not final")
            entries = own.get("rosterForCurrentScoringPeriod", {}).get("entries")
            if not isinstance(entries, list) or not entries:
                raise IncompleteScoresError("Missing historical lineup")
            lineup = [shape_player(e, season, week, team_id, bye_teams) for e in entries]
            seen.add(team_id)
            starters = [p for p in lineup if p["slot"] not in {"BE", "IR"}]
            bench = [p for p in lineup if p["slot"] in {"BE", "IR"}]
            projected = (
                sum(p["projected_points"] for p in starters)
                if all(p["projected_points"] is not None for p in starters)
                else None
            )
            result.append(
                {
                    "team_id": team_id,
                    "opponent_team_id": row["opponent_team_id"],
                    "is_home": row["is_home"],
                    "period": period,
                    "score": score,
                    "team": {
                        "name": _extract_team_name(teams[team_id]),
                        "logo_url": teams[team_id].get("logo", ""),
                        "score": score,
                        "projected": projected,
                    },
                    "starters": starters,
                    "bench": bench,
                }
            )
            players.extend(lineup)
    if seen != set(teams) or len({p["espn_id"] for p in players}) != len(players):
        raise IncompleteScoresError(
            "Incomplete team coverage or conflicting player ownership"
        )
    return {
        "league_id": league_id,
        "season": season,
        "week": week,
        "schedule": schedule,
        "teams": result,
        "players": players,
    }


def fetch_snapshot(league):
    raw = fetch_raw_league_data(league, include_schedule=True)
    pro = league.espn_request.get_pro_schedule()
    ids = defaultdict(set)
    official = defaultdict(list)
    byes = defaultdict(list)
    for team in pro.get("settings", {}).get("proTeams", []):
        if team.get("byeWeek"):
            byes[str(team["byeWeek"])].append(team["id"])
        for week, games in team.get("proGamesByScoringPeriod", {}).items():
            for game in games:
                ids[str(week)].add(str(game["id"]))
                official[int(week)].append(game.get("statsOfficial") is True)
    if not ids:
        raise IncompleteScoresError("Missing ESPN professional schedule")
    raw["_nfl_game_ids_by_week"] = {week: sorted(games) for week, games in ids.items()}
    raw["_nfl_official_weeks"] = [
        week for week, flags in official.items() if all(flags)
    ]
    raw["_nfl_bye_teams_by_week"] = dict(byes)
    return raw


def fetch_week(league, week, period):
    return league.espn_request.league_get(
        params={"view": ["mMatchupScore", "mScoreboard"], "scoringPeriodId": week},
        headers={
            "x-fantasy-filter": json.dumps(
                {"schedule": {"filterMatchupPeriodIds": {"value": [period]}}}
            )
        },
    )


def fetch_scoreboard(season, week):
    response = requests.get(
        "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
        params={"dates": season, "seasontype": 2, "week": week, "limit": 100},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def persist_schedule(cursor, league_id, season, rows):
    cursor.executemany(
        """INSERT INTO fantasy_matchups
        (league_id, season, matchup_period, team_id, scoring_weeks, opponent_team_id,
         status, is_home, is_regular_season, winner)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (league_id, season, matchup_period, team_id) DO UPDATE SET
        scoring_weeks=EXCLUDED.scoring_weeks, opponent_team_id=EXCLUDED.opponent_team_id,
        status=EXCLUDED.status, is_home=EXCLUDED.is_home,
        is_regular_season=EXCLUDED.is_regular_season, winner=EXCLUDED.winner,
        fetched_at=CURRENT_TIMESTAMP;""",
        [
            (
                league_id,
                season,
                r["period"],
                r["team_id"],
                r["weeks"],
                r["opponent_team_id"],
                r["status"],
                r["is_home"],
                r["regular"],
                r["winner"],
            )
            for r in rows
        ],
    )


def persist_week(conn, data):
    league_id, season, week = data["league_id"], data["season"], data["week"]
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s));",
            (f"espn-scores:{league_id}:{season}",),
        )
        persist_schedule(cursor, league_id, season, data["schedule"])
        cursor.execute(
            "SELECT espn_id, player_id FROM players WHERE espn_id IS NOT NULL;"
        )
        mappings = defaultdict(set)
        for espn, internal in cursor.fetchall():
            mappings[espn].add(internal)
        identities = {
            espn: next(iter(ids)) for espn, ids in mappings.items() if len(ids) == 1
        }
        cursor.execute(
            """UPDATE weekly_stats SET actual_points=NULL, actual_points_source=NULL,
            actual_points_updated_at=NULL WHERE season=%s AND week=%s AND actual_points_source=%s;""",
            (season, week, f"espn:{league_id}"),
        )
        cursor.execute(
            "DELETE FROM fantasy_player_week_scores WHERE league_id=%s AND season=%s AND week=%s;",
            (league_id, season, week),
        )
        cursor.execute(
            "DELETE FROM fantasy_team_week_scores WHERE league_id=%s AND season=%s AND week=%s;",
            (league_id, season, week),
        )
        cursor.executemany(
            """INSERT INTO fantasy_team_week_scores
            (league_id, season, week, team_id, matchup_period, actual_points, payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (league_id, season, week, team_id) DO UPDATE SET
            matchup_period=EXCLUDED.matchup_period, actual_points=EXCLUDED.actual_points,
            payload=EXCLUDED.payload, fetched_at=CURRENT_TIMESTAMP;""",
            [
                (
                    league_id,
                    season,
                    week,
                    r["team_id"],
                    r["period"],
                    r["score"],
                    json.dumps(r),
                )
                for r in data["teams"]
            ],
        )
        cursor.executemany(
            """INSERT INTO fantasy_player_week_scores
            (league_id, season, week, team_id, espn_id, player_id, actual_points, payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (league_id, season, week, team_id, espn_id) DO UPDATE SET
            player_id=EXCLUDED.player_id, actual_points=EXCLUDED.actual_points,
            payload=EXCLUDED.payload, fetched_at=CURRENT_TIMESTAMP;""",
            [
                (
                    league_id,
                    season,
                    week,
                    p["team_id"],
                    p["espn_id"],
                    identities.get(p["espn_id"]),
                    p["actual_points"],
                    json.dumps(p),
                )
                for p in data["players"]
            ],
        )
        cursor.executemany(
            """UPDATE weekly_stats SET actual_points=%s,
            actual_points_source=%s, actual_points_updated_at=CURRENT_TIMESTAMP
            WHERE player_id=%s AND season=%s AND week=%s;""",
            [
                (
                    p["actual_points"],
                    f"espn:{league_id}",
                    identities[p["espn_id"]],
                    season,
                    week,
                )
                for p in data["players"]
                if p["espn_id"] in identities and p["actual_points"] is not None
            ],
        )


def write_snapshot(data):
    from src.database import get_db_connection
    from src.services.read_cache import invalidate_league

    with get_db_connection() as conn:
        persist_week(conn, data)
    invalidate_league(data["league_id"])


def write_schedule_snapshot(snapshot, league_id, season):
    from src.database import get_db_connection
    from src.services.read_cache import invalidate_league

    rows = shape_schedule(snapshot, league_id, season)
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s));",
                (f"espn-scores:{league_id}:{season}",),
            )
            persist_schedule(cursor, league_id, season, rows)
    invalidate_league(league_id)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int)
    parser.add_argument("--start-week", type=int)
    parser.add_argument("--end-week", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if not 2000 <= args.season <= 2100 or (
        args.week is not None
        and (args.start_week is not None or args.end_week is not None)
    ):
        parser.error("Invalid season or conflicting week/range arguments")
    try:
        league_id = os.environ["ESPN_LEAGUE_ID"]
        integer(league_id)
        league = build_league(args.season, request_timeout_seconds=10)
        snapshot = fetch_snapshot(league)
        validate_scope(snapshot, league_id, args.season)
        eligible = completed_weeks(snapshot)
        if args.week is not None:
            weeks = [args.week]
        elif args.start_week is not None or args.end_week is not None:
            start = args.start_week if args.start_week is not None else 1
            end = (
                args.end_week
                if args.end_week is not None
                else (max(eligible) if eligible else 0)
            )
            if start < 1 or end < start or end > 18:
                raise IncompleteScoresError("Invalid week range")
            weeks = list(range(start, end + 1))
        else:
            weeks = eligible
        if any(w not in eligible for w in weeks):
            raise IncompleteScoresError("Requested week is not completed")
        for week in weeks:
            periods, _ = period_settings(snapshot)
            period = next(p for p, ws in periods.items() if week in ws)
            data = shape_week(
                snapshot,
                fetch_week(league, week, period),
                fetch_scoreboard(args.season, week),
                league_id,
                args.season,
                week,
            )
            if not args.dry_run:
                write_snapshot(data)
            logger.info(
                "%s season=%s week=%s teams=%s players=%s",
                "Validated" if args.dry_run else "Persisted",
                args.season,
                week,
                len(data["teams"]),
                len(data["players"]),
            )
        if not weeks:
            if not args.dry_run:
                write_schedule_snapshot(snapshot, league_id, args.season)
            logger.info("No completed scoring weeks yet")
        return 0
    except (
        IncompleteScoresError,
        requests.RequestException,
        RuntimeError,
        KeyError,
        ESPNAccessDenied,
        ESPNInvalidLeague,
        ESPNUnknownError,
    ) as exc:
        # HTTP exception text can contain request information; log only its class.
        logger.error(
            "Score ingestion failed: %s",
            str(exc) if isinstance(exc, IncompleteScoresError) else type(exc).__name__,
        )
        return 1


if __name__ == "__main__":
    from src.telemetry import configure_trace_logging

    configure_trace_logging()
    # Imported here: run_log opens the database pool, which --dry-run must never do.
    from src.services import run_log_service

    raise SystemExit(run_log_service.run_main("sync_actual_scores", main))
