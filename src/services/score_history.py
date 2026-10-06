"""Completed ESPN scoring history; a missing row is unknown, never a zero score."""

from collections import defaultdict
import os

from src.database import get_db_cursor


def load_completed_week(season, week):
    with get_db_cursor() as cursor:
        cursor.execute(
            """SELECT payload FROM fantasy_team_week_scores
            WHERE league_id=%s AND season=%s AND week=%s ORDER BY team_id;""",
            (os.getenv("ESPN_LEAGUE_ID"), season, week),
        )
        return [row[0] for row in cursor.fetchall()]


def shape_stored_events(rows):
    grouped = defaultdict(list)
    for team, period, weeks, opponent, status, week, score in rows:
        grouped[(team, period)].append((weeks, opponent, status, week, score))
    events = {}
    for key, values in grouped.items():
        weeks, opponent, status, _, _ = values[0]
        actuals = {week: score for _, _, _, week, score in values if score is not None}
        complete = set(actuals) == set(weeks)
        events[key] = [
            {
                "opponent_team_id": opponent,
                "status": status,
                "points": sum(actuals.values()) if complete else None,
                "completed": complete,
            }
        ]
    return events


def load_stored_events(league_id, season):
    with get_db_cursor() as cursor:
        cursor.execute(
            """SELECT m.team_id, m.matchup_period, m.scoring_weeks,
                m.opponent_team_id, m.status, s.week,
                CASE WHEN m.winner IN ('HOME','AWAY','TIE') THEN s.actual_points END
            FROM fantasy_matchups m
            LEFT JOIN fantasy_team_week_scores s ON s.league_id=m.league_id
                AND s.season=m.season AND s.team_id=m.team_id
                AND s.matchup_period=m.matchup_period AND s.week=ANY(m.scoring_weeks)
            WHERE m.league_id=%s AND m.season=%s AND m.is_regular_season
            ORDER BY m.matchup_period, m.team_id, s.week;""",
            (league_id, season),
        )
        return shape_stored_events(cursor.fetchall())


def stored_scoreboard(sides, season, week):
    by_id = {s["team_id"]: s for s in sides}
    seen, matchups = set(), []
    for side in sides:
        if side["team_id"] in seen:
            continue
        opponent = by_id.get(side["opponent_team_id"])
        home, away = (side, opponent) if side["is_home"] else (opponent, side)

        def shape(s):
            return {"team_id": s["team_id"], **s["team"]} if s else None

        matchups.append({"home": shape(home), "away": shape(away)})
        seen.add(side["team_id"])
        if opponent:
            seen.add(opponent["team_id"])
    return {"season": season, "week": week, "matchups": matchups}
