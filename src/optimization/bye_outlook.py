"""
Pure bye outlook: for each week of a horizon, the best lineup a roster can field, who is
on an NFL bye, and whether those byes leave a hole worth fixing (a "crunch"). Shared by
the trade context (every team's outlook) and roster_outlook_service (one team's).
"""
import statistics

from src.nfl_byes import bye_week_for, teams_playing_from_byes
from src.optimization.roster_move_scoring import lineup_week


def build_bye_outlook(roster, weeks, as_of_week, slot_counts, playing_by_week):
    """Pure. playing_by_week maps each week to the NFL teams with a game, or None when the
    schedule is unknown (byes then stay unknown, never assumed).

    A week is a crunch when players on bye leave a starter seat empty, or force more
    below-replacement starters (VORP <= 0, i.e. worse than a typical free agent) than
    the roster's median week. That is the hole a waiver add or trade can actually fill;
    a stud's ordinary bye backed by a decent bench is not one."""
    rows = []
    for week in weeks:
        row = lineup_week(roster, week, as_of_week, slot_counts, playing_by_week.get(week))
        starters = row.pop("starters")
        row["replacement_level_starters"] = (
            sum(1 for s in starters if s["vorp_score"] <= 0) if row["value"] is not None else None
        )
        rows.append(row)
    known = [row["value"] for row in rows if row["value"] is not None]
    median = statistics.median(known) if known else None
    weak_counts = [r["replacement_level_starters"] for r in rows if r["replacement_level_starters"] is not None]
    typical_weak = statistics.median(weak_counts) if weak_counts else None
    for row in rows:
        row["shortfall"] = (
            max(0.0, median - row["value"])
            if median is not None and row["value"] is not None
            else None
        )
        forced_weak = (
            row["replacement_level_starters"] is not None
            and row["replacement_level_starters"] > typical_weak
        )
        # Only byes make a crunch: a dip from tough matchups isn't something to fix now.
        row["crunch"] = bool(row["on_bye"]) and (bool(row["unfilled_slots"]) or forced_weak)
    return {
        "median_lineup_value": median,
        "crunch_weeks": [row["week"] for row in rows if row["crunch"]],
        "weeks": rows,
    }


def horizon_weeks(context):
    return list(range(context["start_week"], context["end_week"] + 1))


def attach_bye_outlooks(context, bye_weeks):
    """Adds nfl_bye_week to every rostered player and bye_outlook to every team of a
    trade context, in place. An empty bye map leaves both unknown."""
    weeks = horizon_weeks(context)
    playing_by_week = {week: teams_playing_from_byes(bye_weeks, week) for week in weeks}
    for team in context["teams"].values():
        for player in team["roster"]:
            player["nfl_bye_week"] = bye_week_for(player.get("nfl_team"), bye_weeks)
        team["bye_outlook"] = build_bye_outlook(
            team["roster"], weeks, context["as_of_week"], context["lineup_slot_counts"],
            playing_by_week,
        )
    return context
