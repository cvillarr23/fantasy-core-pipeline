"""Current league rosters and fantasy head-to-head schedule evidence for trades."""

import math
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from statistics import mean, pstdev

from espn_api.football.constant import POSITION_MAP, PRO_TEAM_MAP

from src.database import get_db_cursor
from src.nfl_byes import bye_weeks_or_empty
from src.optimization.bye_outlook import attach_bye_outlooks
from src.espn_positions import DEFAULT_POSITIONS
from src.ingestion.sync_league import (
    _extract_espn_player_id,
    _extract_team_name,
    _lineup_slot_counts_by_label,
    build_league,
    fetch_raw_league_data,
)
from src.services.read_cache import cached
from src.services.score_history import load_stored_events


class TradeContextUnavailable(Exception):
    """Source context is incomplete or does not describe the requested league."""


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def _integer(value):
    if isinstance(value, bool) or not re.fullmatch(r"\d+", str(value)):
        raise TradeContextUnavailable("Invalid ESPN scoring-period settings.")
    return int(value)


def _regular_periods(raw):
    try:
        settings = raw["settings"]["scheduleSettings"]
        count = _integer(settings["matchupPeriodCount"])
        final_week = min(18, _integer(raw["status"]["finalScoringPeriod"]))
        if not 1 <= count <= 18 or final_week < 1:
            raise TradeContextUnavailable("Invalid regular-season bounds.")
        mapping = settings["matchupPeriods"]
        periods = {}
        for period in range(1, count + 1):
            weeks = mapping[str(period)]
            if not isinstance(weeks, list) or not weeks:
                raise TradeContextUnavailable("Missing regular-season scoring periods.")
            periods[period] = [_integer(week) for week in weeks]
        flat = [week for weeks in periods.values() for week in weeks]
        if not flat or flat != list(range(1, max(flat) + 1)) or max(flat) > final_week:
            raise TradeContextUnavailable("Invalid regular-season scoring periods.")
        return periods
    except (KeyError, TypeError) as error:
        raise TradeContextUnavailable("Missing ESPN schedule settings.") from error


def _source_rosters(raw, league_id, season):
    if str(raw.get("id")) != league_id or raw.get("seasonId") != season:
        raise TradeContextUnavailable(
            "ESPN snapshot does not match the requested league/season."
        )
    teams = raw.get("teams")
    if not isinstance(teams, list) or not teams:
        raise TradeContextUnavailable("Missing ESPN teams.")
    owners = set()
    team_ids = set()
    for team in teams:
        team_id = str(team.get("id"))
        if not re.fullmatch(r"[1-9]\d*", team_id) or team_id in team_ids:
            raise TradeContextUnavailable("Invalid ESPN team identity.")
        team_ids.add(team_id)
        entries = team.get("roster", {}).get("entries")
        if not isinstance(entries, list):
            raise TradeContextUnavailable("Missing ESPN roster snapshot.")
        for entry in entries:
            player_id = _extract_espn_player_id(entry)
            if player_id is None or player_id in owners:
                raise TradeContextUnavailable(
                    "Missing or conflicting ESPN player ownership."
                )
            owners.add(player_id)
    return teams, owners


def _schedule_events(raw, teams, periods):
    events = defaultdict(list)
    schedule = raw.get("schedule", [])
    if not isinstance(schedule, list):
        schedule = []
    for game in schedule:
        try:
            period = _integer(game.get("matchupPeriodId"))
        except TradeContextUnavailable:
            continue
        if period not in periods:
            continue
        for side, other in (("home", "away"), ("away", "home")):
            own = game.get(side) or {}
            team_id = str(own.get("teamId"))
            if team_id not in teams:
                continue
            opponent_id = (game.get(other) or {}).get("teamId")
            # A missing side or ESPN's sentinel is an explicit bye. A missing game
            # entry is different: its pairing is unknown, never inferred to be a bye.
            opponent_id = (
                None if opponent_id in (None, 0, -1, "0", "-1") else str(opponent_id)
            )
            status = (
                "bye"
                if opponent_id is None
                else (
                    "scheduled"
                    if opponent_id in teams and opponent_id != team_id
                    else "unavailable"
                )
            )
            events[(team_id, period)].append(
                {
                    "opponent_team_id": opponent_id,
                    "status": status,
                    "points": _number(own.get("totalPoints")),
                    "completed": game.get("winner") in ("HOME", "AWAY", "TIE"),
                }
            )
    return events


def shape_trade_context(
    raw,
    identities,
    metrics,
    league_id,
    season,
    week,
    horizon,
    fetched_at,
    stored_events=None,
):
    """Shape current ownership with schedule/history evidence and no future leakage.

    Strength is mean fantasy points per scoring week from completed regular-season
    matchups, standardized across teams with observations. Positive z means stronger.
    Multiweek matchup totals are divided by their actual scoring-week counts. A team's
    schedule average is available only when every non-bye week has a known pairing
    and opponent strength. This is descriptive evidence, not a win-probability model.
    """
    source_teams, _ = _source_rosters(raw, league_id, season)
    periods = _regular_periods(raw)
    observed_week = _integer(raw.get("scoringPeriodId"))
    if observed_week < 1:
        raise TradeContextUnavailable("ESPN snapshot has no observed scoring period.")
    end_week = min(
        week + horizon - 1, max(week for weeks in periods.values() for week in weeks)
    )
    weeks = list(range(week, end_week + 1))
    period_by_week = {
        week: period for period, weeks in periods.items() for week in weeks
    }
    teams = {
        str(team["id"]): {"team_id": str(team["id"]), "name": _extract_team_name(team)}
        for team in source_teams
    }
    live_events = _schedule_events(raw, teams, periods)

    def completed_history_keys(events):
        return {
            (team_id, period)
            for team_id in teams
            for period, period_weeks in periods.items()
            if max(period_weeks) < min(week, observed_week)
            and len(matches := events.get((team_id, period), [])) == 1
            and matches[0]["status"] == "scheduled"
            and matches[0]["completed"]
            and _number(matches[0]["points"]) is not None
        }

    if stored_events is not None:
        stored_history = completed_history_keys(stored_events)
        # Use one score source for the entire league so strengths remain comparable.
        # A scheduled row alone is not history; ESPN may have finished before sync.
        if not stored_history or completed_history_keys(live_events) - stored_history:
            stored_events = None
    events = live_events if stored_events is None else defaultdict(list, stored_events)
    unmapped = set()
    missing_metrics = 0
    for source in source_teams:
        team_id = str(source["id"])
        roster = []
        for entry in source["roster"]["entries"]:
            espn_id = _extract_espn_player_id(entry)
            player_id = identities.get(espn_id)
            if player_id is None:
                unmapped.add(espn_id)
            player = (
                (entry.get("playerPoolEntry") or {}).get("player")
                or entry.get("player")
                or {}
            )
            player_metrics = {}
            for target_week in weeks:
                values = (
                    metrics.get((player_id, target_week), {})
                    if player_id is not None
                    else {}
                )
                values = {
                    key: _number(values.get(key))
                    for key in ("vorp_score", "projected_points")
                }
                missing_metrics += int(any(value is None for value in values.values()))
                player_metrics[str(target_week)] = values
            roster.append(
                {
                    "espn_id": espn_id,
                    "player_id": player_id,
                    "name": player.get("fullName"),
                    "position": DEFAULT_POSITIONS.get(player.get("defaultPositionId")),
                    "nfl_team": PRO_TEAM_MAP.get(player.get("proTeamId")),
                    "slot": POSITION_MAP.get(entry.get("lineupSlotId")),
                    "eligible_slots": [
                        POSITION_MAP[slot]
                        for slot in (player.get("eligibleSlots") or [])
                        if slot in POSITION_MAP
                    ],
                    "injury_status": player.get("injuryStatus"),
                    "metrics": player_metrics,
                }
            )
        teams[team_id]["roster"] = roster
        points, scoring_weeks, completed_matchups = 0.0, 0, 0
        unavailable_history_periods = 0
        for period, period_weeks in periods.items():
            # Also respect ESPN's observed scoring week: a future planning request
            # cannot make an in-progress source snapshot look like completed history.
            if max(period_weeks) >= min(week, observed_week):
                continue
            matches = events[(team_id, period)]
            if len(matches) != 1:
                unavailable_history_periods += 1
                continue
            event = matches[0]
            if event["status"] == "bye":
                continue
            if (
                event["status"] != "scheduled"
                or not event["completed"]
                or event["points"] is None
            ):
                unavailable_history_periods += 1
                continue
            points += event["points"]
            scoring_weeks += len(period_weeks)
            completed_matchups += 1
        teams[team_id]["strength"] = {
            "points_per_week": points / scoring_weeks if scoring_weeks else None,
            "completed_matchups": completed_matchups,
            "completed_scoring_weeks": scoring_weeks,
            "z_score": None,
            "unavailable_history_periods": unavailable_history_periods,
        }
    observed = [
        team["strength"]["points_per_week"]
        for team in teams.values()
        if team["strength"]["points_per_week"] is not None
    ]
    league_mean = mean(observed) if observed else None
    league_stddev = pstdev(observed) if len(observed) >= 2 else None
    for team in teams.values():
        strength = team["strength"]
        if strength["points_per_week"] is not None and league_stddev is not None:
            strength["z_score"] = (
                (strength["points_per_week"] - league_mean) / league_stddev
                if league_stddev
                else 0.0
            )
    for team in teams.values():
        schedule = []
        coverage = {
            "scheduled_weeks": 0,
            "bye_weeks": 0,
            "unavailable_weeks": 0,
            "strength_known_weeks": 0,
        }
        scores = []
        for target_week in weeks:
            period = period_by_week[target_week]
            matches = events[(team["team_id"], period)]
            event = (
                matches[0]
                if len(matches) == 1
                else {"status": "unavailable", "opponent_team_id": None}
            )
            status = event["status"]
            opponent = (
                teams.get(event["opponent_team_id"]) if status == "scheduled" else None
            )
            z_score = opponent["strength"]["z_score"] if opponent else None
            coverage[
                {
                    "scheduled": "scheduled_weeks",
                    "bye": "bye_weeks",
                    "unavailable": "unavailable_weeks",
                }[status]
            ] += 1
            if z_score is not None:
                coverage["strength_known_weeks"] += 1
                scores.append(z_score)
            schedule.append(
                {
                    "week": target_week,
                    "matchup_period": period,
                    "status": status,
                    "opponent_team_id": opponent["team_id"] if opponent else None,
                    "opponent_name": opponent["name"] if opponent else None,
                    "opponent_strength_z": z_score,
                }
            )
        team["schedule"] = schedule
        team["schedule_coverage"] = coverage
        team["schedule_strength_z"] = (
            mean(scores)
            if scores
            and not coverage["unavailable_weeks"]
            and len(scores) == coverage["scheduled_weeks"]
            else None
        )
    raw_counts = (
        raw.get("settings", {}).get("rosterSettings", {}).get("lineupSlotCounts")
    )
    if not isinstance(raw_counts, dict) or not raw_counts:
        raise TradeContextUnavailable("Missing ESPN lineup slot settings.")
    return {
        "league_id": league_id,
        "season": season,
        "as_of_week": week,
        "start_week": week,
        "end_week": end_week,
        "regular_season_end_week": max(period_by_week),
        "lineup_slot_counts": _lineup_slot_counts_by_label(raw_counts),
        "roster_snapshot": {
            "kind": "current",
            "source": "ESPN mRoster",
            "fetched_at": fetched_at,
            "observed_week": observed_week,
            "matches_as_of_week": observed_week == week,
        },
        "strength_metric": {
            "name": "completed_fantasy_points_per_week_z",
            "source": (
                "ESPN completed regular-season matchup totals"
                if stored_events is None
                else "Stored ESPN completed regular-season scoring weeks"
            ),
            "league_mean": league_mean,
            "league_stddev": league_stddev,
            "teams_with_strength": len(observed),
        },
        "coverage": {
            "unmapped_espn_ids": sorted(unmapped),
            "missing_metric_count": missing_metrics,
        },
        "teams": teams,
    }


def get_league_trade_context(
    league_id: str, season: int, week: int, horizon: int = 4
) -> dict:
    """Shared REST/MCP read; current ESPN ownership plus exact-week database metrics."""
    if (
        not re.fullmatch(r"[1-9]\d*", league_id)
        or not 2000 <= season <= 2100
        or not 1 <= week <= 18
        or not 1 <= horizon <= 18
    ):
        raise ValueError("Invalid league, season, week or horizon.")
    configured = os.getenv("ESPN_LEAGUE_ID")
    if not configured:
        raise TradeContextUnavailable("ESPN league is not configured.")
    if league_id != configured:
        raise ValueError("Requested league does not match the configured ESPN league.")

    def load():
        league = build_league(season, request_timeout_seconds=10)
        raw = fetch_raw_league_data(league, include_schedule=True)
        fetched_at = datetime.now(timezone.utc).isoformat()
        _, espn_ids = _source_rosters(raw, league_id, season)
        periods = _regular_periods(raw)
        end_week = min(
            week + horizon - 1,
            max(week for weeks in periods.values() for week in weeks),
        )
        identities, metrics = {}, {}
        if espn_ids:
            with get_db_cursor() as cursor:
                cursor.execute(
                    "SELECT player_id, espn_id FROM players WHERE espn_id = ANY(%s);",
                    (sorted(espn_ids),),
                )
                rows = cursor.fetchall()
                by_espn = defaultdict(set)
                for player_id, espn_id in rows:
                    by_espn[espn_id].add(player_id)
                identities = {
                    espn: next(iter(ids))
                    for espn, ids in by_espn.items()
                    if len(ids) == 1
                }
                if week <= end_week:
                    cursor.execute(
                        """SELECT player_id, week, vorp_score, projected_points FROM weekly_stats
                        WHERE season = %s AND week BETWEEN %s AND %s AND player_id = ANY(%s);""",
                        (season, week, end_week, list(identities.values())),
                    )
                    metrics = {
                        (player_id, target_week): {
                            "vorp_score": vorp,
                            "projected_points": projected,
                        }
                        for player_id, target_week, vorp, projected in cursor.fetchall()
                    }
        context = shape_trade_context(
            raw,
            identities,
            metrics,
            league_id,
            season,
            week,
            horizon,
            fetched_at,
            stored_events=load_stored_events(league_id, season),
        )
        # Both teams' NFL byes and bye-crunch weeks, so trade proposals and negotiations
        # can target a partner whose byes complement ours.
        return attach_bye_outlooks(context, bye_weeks_or_empty(season))

    return cached(["trade-context", league_id, season, week, horizon], 60, load)
