"""
Deterministic schedule-aware trade score, shared by POST /v1/trade/score and the
score_trade_proposal MCP tool.

Perspective is the proposing team ("us"); positive numbers are good for us. For every
horizon week with exact-week metrics, each team's optimal starting lineup is solved before
and after the trade from its current ESPN roster (the lineup optimizer's ILP, maximizing
weekly_stats.vorp_score; bench and IR are worth nothing). ΔL is after minus before.

- our_lineup_gain G = sum of our ΔL; counterparty_lineup_change I = sum of theirs.
- A week's points matter more against a stronger fantasy opponent:
  m(z) = 1 + STRENGTH_WEIGHT * clamp(z, ±Z_CLAMP), z being that opponent's completed
  fantasy points-per-week z score from the trade context. A fantasy bye weighs 0.
- our_weighted_gain G* = sum of our ΔL * m(our opponent's z).
- counterparty_weighted_benefit C* = sum of -(their ΔL) * c, where c is our own m in a
  week they play us (a point taken from our opponent equals a point added to us), and
  INDIRECT_FACTOR * m(their opponent's z) otherwise: weakening a rival only helps our
  standings indirectly, and more so ahead of their tougher matchups.
- rest_of_season (R) = sum of our ΔL * k over the rest-of-season weeks: every week after
  the horizon through the last fantasy playoff week (the trade context's rest_of_season,
  from ESPN's playoff settings). k = PLAYOFF_WEEK_WEIGHT for a playoff week and
  ROS_REGULAR_WEIGHT for a regular-season week the horizon didn't reach: projections that far
  out are noisier, and those games only decide seeding. No opponent multiplier applies, since
  those pairings aren't known yet. Only our side counts; the counterparty's rest-of-season
  change is evidence. costs_playoff_value flags a package whose playoff weeks lose us lineup
  value (informational, never a gate).
- final_score is the sum of score.components whose in_final_score is true: our_weighted_gain
  (G*), counterparty_weighted_benefit (C*) and rest_of_season (R), so final_score = G* + C* + R.
  Each component is {value, in_final_score, unavailable_reasons}. An in-final component with
  no value withholds final_score and lends it its reasons. A new term is one more entry.

R is an additive refinement, so it never withholds final_score: with no rest-of-season week
scored (the far-week projections not refreshed yet) its component has no value and is left
out of final_score (in_final_score false, reason no_scored_weeks), so final_score = G* + C*.
With no rest-of-season weeks left, R is 0.

Injuries (trade-score-v4) shape each week's inputs rather than adding a term, by the rules in
src/injury_timeline: an Out or IR player sits until his expected return week (ESPN's injuries
feed, mapped onto his team's schedule), an IR player with no date sits the whole horizon,
and a Questionable/Doubtful player's positive VORP is scaled by QUESTIONABLE_AVAILABILITY
(0.75) or DOUBTFUL_AVAILABILITY (0.25) in the as-of week only. Each week lists its
injury_adjustments, and `injuries` gives every traded player's timeline.

bye_coverage lists the bye-crunch weeks (src/optimization/bye_outlook) each side has
before and after, so a proposal can say which bye weeks it fixes. It is evidence only;
the lineup terms above already count every bye week.

Nothing is filled in. A week lacking a traded player's metrics, mapping or a fillable
lineup is unscored rather than counted as zero, and unknown opponent strength or pairing
withholds the weighted terms instead of treating that opponent as average. The raw
single-week VORP totals come from trade_service.evaluate_trade unchanged.
"""

import logging
import re

from src.injury_timeline import DOUBTFUL_AVAILABILITY, QUESTIONABLE_AVAILABILITY, availability
from src.optimization.bye_outlook import build_bye_outlook
from src.optimization.lineup_solver import load_teams_playing
from src.optimization.roster_move_scoring import compare_week
from src.schemas.trade_score import KNOWN_SCORING_VERSIONS
from src.services.trade_context_service import get_league_trade_context
from src.services.trade_service import evaluate_trade

logger = logging.getLogger("TradeScoreService")

SCORING_VERSION = KNOWN_SCORING_VERSIONS[-1]
STRENGTH_WEIGHT = 0.25
Z_CLAMP = 2.0
INDIRECT_FACTOR = 0.25
ROS_REGULAR_WEIGHT = 0.5
PLAYOFF_WEEK_WEIGHT = 1.0
ROS_WEIGHTS = {"regular": ROS_REGULAR_WEIGHT, "playoff": PLAYOFF_WEEK_WEIGHT}
_TOLERANCE = 1e-9

_ESPN_PLAYER_ID = re.compile(r"-?[1-9]\d*")
_ESPN_TEAM_ID = re.compile(r"[1-9]\d*")


class TradeOwnershipError(Exception):
    """A proposed player is not currently on the side of the trade that would send them."""


def _validate_proposal(our_team_id, counterparty_team_id, send_espn_ids, receive_espn_ids):
    for team_id in (our_team_id, counterparty_team_id):
        if not isinstance(team_id, str) or not _ESPN_TEAM_ID.fullmatch(team_id):
            raise ValueError(f"Invalid ESPN team id {team_id!r}.")
    if our_team_id == counterparty_team_id:
        raise ValueError("A trade needs two different teams.")
    for side, ids in (("send_espn_ids", send_espn_ids), ("receive_espn_ids", receive_espn_ids)):
        if not ids:
            raise ValueError(f"{side} must name at least one player.")
        for espn_id in ids:
            if not isinstance(espn_id, str) or not _ESPN_PLAYER_ID.fullmatch(espn_id):
                raise ValueError(f"{side} contains an invalid ESPN player id {espn_id!r}.")
        if len(set(ids)) != len(ids):
            raise ValueError(f"{side} contains duplicate ESPN player id(s).")
    overlap = set(send_espn_ids) & set(receive_espn_ids)
    if overlap:
        raise ValueError(f"ESPN player id(s) {sorted(overlap)} cannot be on both sides.")


def _traded_players(context, our_team_id, counterparty_team_id, send_espn_ids, receive_espn_ids):
    """Resolves request ids in the ESPN namespace only, against current ESPN ownership."""
    for team_id in (our_team_id, counterparty_team_id):
        if team_id not in context["teams"]:
            raise ValueError(f"Unknown ESPN team {team_id} in this league.")
    owners = {
        player["espn_id"]: (team_id, player)
        for team_id, team in context["teams"].items()
        for player in team["roster"]
    }

    def resolve(ids, expected_team):
        players = []
        for espn_id in ids:
            owner, player = owners.get(espn_id, (None, None))
            if owner is None:
                raise TradeOwnershipError(f"ESPN player {espn_id} is not on any roster.")
            if owner != expected_team:
                raise TradeOwnershipError(
                    f"ESPN player {espn_id} is on team {owner}, not team {expected_team}."
                )
            players.append(player)
        return players

    return resolve(send_espn_ids, our_team_id), resolve(receive_espn_ids, counterparty_team_id)


def _multiplier(z):
    if z is None:
        return None
    return 1 + STRENGTH_WEIGHT * max(-Z_CLAMP, min(Z_CLAMP, z))


def _team_week(before_roster, after_roster, traded_ids, week, context, playing):
    """One team's before/after lineup for one week, or (None, reasons) if unscorable."""
    return compare_week(
        before_roster, after_roster, traded_ids, week, context["as_of_week"],
        context["lineup_slot_counts"], playing,
    )


def _roster_size(roster):
    return sum(1 for player in roster if player.get("slot") != "IR")


def _schedule_row(context, our_team_id, counterparty_team_id, week):
    def row(team_id):
        for entry in context["teams"][team_id]["schedule"]:
            if entry["week"] == week:
                return entry
        return {"status": "unavailable", "opponent_team_id": None,
                "opponent_strength_z": None, "matchup_period": None}

    ours, theirs = row(our_team_id), row(counterparty_team_id)
    head_to_head = (
        ours["status"] == "scheduled" and ours["opponent_team_id"] == counterparty_team_id
    )

    def weight(entry):
        if entry["status"] == "bye":
            return 0.0
        if entry["status"] != "scheduled":
            return None
        return _multiplier(entry["opponent_strength_z"])

    our_multiplier = weight(ours)
    if head_to_head:
        counterparty_weight = our_multiplier
    else:
        counterparty_weight = weight(theirs)
        if counterparty_weight:
            counterparty_weight *= INDIRECT_FACTOR
    return {
        "week": week,
        "matchup_period": ours.get("matchup_period"),
        "head_to_head": head_to_head,
        "our_status": ours["status"],
        "our_opponent_team_id": ours["opponent_team_id"],
        "our_opponent_z": ours["opponent_strength_z"],
        "our_multiplier": our_multiplier,
        "counterparty_status": theirs["status"],
        "counterparty_opponent_team_id": theirs["opponent_team_id"],
        "counterparty_opponent_z": theirs["opponent_strength_z"],
        "counterparty_weight": counterparty_weight,
    }


def _component(value, reasons, in_final_score=True):
    return {"value": value, "in_final_score": in_final_score, "unavailable_reasons": reasons}


def _final_from_components(components):
    """(final_score, reasons): the sum of the in-final components, or None with the reasons
    of every in-final component that has no value."""
    reasons = []
    for component in components.values():
        if component["in_final_score"] and component["value"] is None:
            reasons.extend(r for r in component["unavailable_reasons"] if r not in reasons)
    if reasons:
        return None, reasons
    return sum(c["value"] for c in components.values() if c["in_final_score"]), []


def _ros_entries(context):
    """The context's rest-of-season weeks; a context without the window has none."""
    return (context.get("rest_of_season") or {}).get("weeks", [])


def scored_week_range(context):
    """(first, last) week the scorer reads: the horizon plus the rest-of-season weeks, for
    load_teams_playing_by_week."""
    ros_weeks = [entry["week"] for entry in _ros_entries(context)]
    return context["start_week"], max([context["end_week"], *ros_weeks])


def _rest_of_season(context, rosters, traded, teams_playing_by_week):
    """The R term and its per-week evidence. Each week is scored like a horizon week, from
    its own metrics; a week either side can't solve is unscored, never zero."""
    weeks, scored, unscored = [], [], []
    for entry in _ros_entries(context):
        week, kind = entry["week"], entry["kind"]
        row = {"week": week, "kind": kind, "weight": ROS_WEIGHTS[kind],
               "our_delta": None, "counterparty_delta": None}
        weeks.append(row)
        playing = teams_playing_by_week.get(week)
        deltas, reasons = {}, set()
        for side, (before_roster, after_roster) in rosters.items():
            result, side_reasons = _team_week(
                before_roster, after_roster, traded, week, context, playing
            )
            reasons |= side_reasons
            if result is not None:
                deltas[side] = result["delta"]
        if reasons or len(deltas) != len(rosters):
            unscored.append({"week": week, "reasons": sorted(reasons)})
            continue
        row["our_delta"], row["counterparty_delta"] = deltas["us"], deltas["counterparty"]
        scored.append(row)

    def total(rows, key):
        return sum(row[key] for row in rows) if rows else None

    playoff_rows = [row for row in scored if row["kind"] == "playoff"]
    playoff_gain = total(playoff_rows, "our_delta")
    if not weeks:
        value, reasons = 0.0, []
    elif not scored:
        value, reasons = None, ["no_scored_weeks"]
    else:
        value, reasons = sum(row["our_delta"] * row["weight"] for row in scored), []
    return {
        "weeks": weeks,
        "scored_weeks": [row["week"] for row in scored],
        "unscored_weeks": unscored,
        "complete": not unscored,
        "our_lineup_gain": total(scored, "our_delta"),
        "playoff_lineup_gain": playoff_gain,
        "counterparty_lineup_change": total(scored, "counterparty_delta"),
        "value": value,
        "unavailable_reasons": reasons,
        "costs_playoff_value": playoff_gain is not None and playoff_gain < -_TOLERANCE,
    }


def _traded_injury(player, weeks, as_of_week):
    injury = player.get("injury") or {}
    return {
        "injury_status": player.get("injury_status"),
        "slot": player.get("slot"),
        "return_date": injury.get("return_date"),
        "expected_return_week": injury.get("expected_return_week"),
        "injury_type": injury.get("injury_type"),
        "reported_at": injury.get("reported_at"),
        "weeks_out": [w for w in weeks if availability(player, w, as_of_week)[0] == 0.0],
        "as_of_week_availability": availability(player, as_of_week, as_of_week)[0],
    }


def _traded_summary(player):
    return {key: player.get(key) for key in ("espn_id", "player_id", "name", "position", "nfl_team")}


def shape_trade_score(
    context, our_team_id, counterparty_team_id, send_espn_ids, receive_espn_ids,
    raw_vorp, teams_playing_by_week,
):
    """Pure scorer over one trade-context snapshot. teams_playing_by_week maps each week
    to the NFL teams with a game, or None when the NFL schedule couldn't be read."""
    send, receive = _traded_players(
        context, our_team_id, counterparty_team_id, send_espn_ids, receive_espn_ids
    )
    send_ids, receive_ids = set(send_espn_ids), set(receive_espn_ids)
    our_roster = context["teams"][our_team_id]["roster"]
    their_roster = context["teams"][counterparty_team_id]["roster"]
    rosters = {
        "us": (our_roster, [p for p in our_roster if p["espn_id"] not in send_ids] + receive),
        "counterparty": (
            their_roster,
            [p for p in their_roster if p["espn_id"] not in receive_ids] + send,
        ),
    }
    traded = send_ids | receive_ids
    weeks = list(range(context["start_week"], context["end_week"] + 1))
    lineup_weeks = {"us": [], "counterparty": []}
    scored, unscored, partial = [], [], set()
    for week in weeks:
        playing = teams_playing_by_week.get(week)
        results, reasons = {}, set()
        for side, (before_roster, after_roster) in rosters.items():
            result, side_reasons = _team_week(
                before_roster, after_roster, traded, week, context, playing
            )
            results[side] = result
            reasons |= side_reasons
        if reasons:
            unscored.append({"week": week, "reasons": sorted(reasons)})
            continue
        scored.append(week)
        for side, result in results.items():
            lineup_weeks[side].append(result)
            if any(e["reason"] in ("no_metrics", "unmapped", "unknown_position")
                   for e in result["excluded_players"]):
                partial.add(week)

    schedule = [
        _schedule_row(context, our_team_id, counterparty_team_id, week) for week in weeks
    ]
    schedule_by_week = {row["week"]: row for row in schedule}
    our_deltas = {w["week"]: w["delta"] for w in lineup_weeks["us"]}
    their_deltas = {w["week"]: w["delta"] for w in lineup_weeks["counterparty"]}
    gain = sum(our_deltas.values()) if scored else None
    change = sum(their_deltas.values()) if scored else None

    rest_of_season = _rest_of_season(context, rosters, traded, teams_playing_by_week)
    ros_value = rest_of_season["value"]
    our_weighted = benefit = None
    our_reasons, their_reasons = [], []
    if not scored:
        our_reasons = their_reasons = ["no_scored_weeks"]
        our_multipliers = their_weights = []
    else:
        our_multipliers = [schedule_by_week[w]["our_multiplier"] for w in scored]
        their_weights = [schedule_by_week[w]["counterparty_weight"] for w in scored]
        if None in our_multipliers:
            our_reasons = ["unknown_our_opponent_strength"]
        else:
            our_weighted = sum(our_deltas[w] * m for w, m in zip(scored, our_multipliers))
        if None in their_weights:
            their_reasons = ["unknown_counterparty_opponent_strength"]
        else:
            benefit = sum(-their_deltas[w] * c for w, c in zip(scored, their_weights))
    components = {
        "our_weighted_gain": _component(our_weighted, our_reasons),
        "counterparty_weighted_benefit": _component(benefit, their_reasons),
        # Left out of final_score, not withholding it, while no rest-of-season week scores.
        "rest_of_season": _component(
            ros_value, rest_of_season["unavailable_reasons"], in_final_score=ros_value is not None,
        ),
    }
    final, reasons = _final_from_components(components)
    if final is not None and not any(our_multipliers) and not any(their_weights):
        final, reasons = None, ["no_matchup_weeks"]

    slot_counts = context["lineup_slot_counts"]
    roster_limit = sum(int(count) for slot, count in slot_counts.items() if slot != "IR")

    def impact(side):
        before_roster, after_roster = rosters[side]
        size_after = _roster_size(after_roster)
        return {
            "total_delta": gain if side == "us" else change,
            "roster_size_before": _roster_size(before_roster),
            "roster_size_after": size_after,
            "roster_limit": roster_limit,
            "exceeds_roster_limit": size_after > roster_limit,
            "weeks": lineup_weeks[side],
        }

    def bye_coverage(side):
        before_roster, after_roster = rosters[side]
        before, after = (
            set(build_bye_outlook(
                roster, weeks, context["as_of_week"], slot_counts, teams_playing_by_week,
            )["crunch_weeks"])
            for roster in (before_roster, after_roster)
        )
        return {
            "crunch_weeks_before": sorted(before),
            "crunch_weeks_after": sorted(after),
            "fixed": sorted(before - after),
            "created": sorted(after - before),
        }

    unmapped = sorted(p["espn_id"] for p in send + receive if p.get("player_id") is None)
    teams = context["teams"]
    return {
        "scoring_version": SCORING_VERSION,
        "league_id": context["league_id"],
        "season": context["season"],
        "as_of_week": context["as_of_week"],
        "our_team": {"team_id": our_team_id, "name": teams[our_team_id]["name"]},
        "counterparty_team": {
            "team_id": counterparty_team_id, "name": teams[counterparty_team_id]["name"],
        },
        "send": [_traded_summary(p) for p in send],
        "receive": [_traded_summary(p) for p in receive],
        "raw_vorp": {**raw_vorp, "week": context["as_of_week"], "unmapped_espn_ids": unmapped},
        "horizon": {
            "start_week": context["start_week"],
            "end_week": context["end_week"],
            "scored_weeks": scored,
            "unscored_weeks": unscored,
            "complete_horizon": bool(weeks) and not unscored,
        },
        "weights": {
            "strength_weight": STRENGTH_WEIGHT,
            "z_clamp": Z_CLAMP,
            "indirect_factor": INDIRECT_FACTOR,
            "ros_regular_weight": ROS_REGULAR_WEIGHT,
            "playoff_week_weight": PLAYOFF_WEEK_WEIGHT,
            "questionable_availability": QUESTIONABLE_AVAILABILITY,
            "doubtful_availability": DOUBTFUL_AVAILABILITY,
        },
        "roster_impact": {"us": impact("us"), "counterparty": impact("counterparty")},
        # Evidence for proposals and pitches, not a score term: final_score already counts
        # every bye week through the per-week lineups.
        "bye_coverage": {"us": bye_coverage("us"), "counterparty": bye_coverage("counterparty")},
        "injuries": {
            p["espn_id"]: _traded_injury(p, weeks, context["as_of_week"]) for p in send + receive
        },
        "schedule": {"weeks": schedule},
        "rest_of_season": rest_of_season,
        "score": {
            "our_lineup_gain": gain,
            "counterparty_lineup_change": change,
            "our_weighted_gain": our_weighted,
            "our_schedule_adjustment": None if our_weighted is None else our_weighted - gain,
            "counterparty_weighted_benefit": benefit,
            "rest_of_season_value": ros_value,
            "final_score": final,
            "final_score_unavailable_reasons": reasons,
            "components": components,
        },
        "coverage": {
            "unmapped_traded_espn_ids": unmapped,
            "partial_roster_weeks": sorted(partial),
            "nfl_schedule_unavailable_weeks": [
                week for week in weeks if teams_playing_by_week.get(week) is None
            ],
        },
        "roster_snapshot": context["roster_snapshot"],
        "strength_metric": context["strength_metric"],
    }


def score_trade(
    league_id: str,
    season: int,
    week: int,
    horizon: int,
    our_team_id: str,
    counterparty_team_id: str,
    send_espn_ids: list[str],
    receive_espn_ids: list[str],
) -> dict:
    """Validates, reads the shared trade context, and scores the proposal from our side.

    Raises ValueError for malformed proposals or unknown teams, TradeOwnershipError when a
    player isn't on the sending team's current ESPN roster; trade-context failures
    (TradeContextUnavailable, ESPN errors, timeouts) propagate unchanged."""
    _validate_proposal(our_team_id, counterparty_team_id, send_espn_ids, receive_espn_ids)
    context = get_league_trade_context(league_id, season, week, horizon)
    send, receive = _traded_players(
        context, our_team_id, counterparty_team_id, send_espn_ids, receive_espn_ids
    )
    raw_vorp = evaluate_trade(
        [p["player_id"] for p in receive if p.get("player_id")],
        [p["player_id"] for p in send if p.get("player_id")],
        season,
        context["as_of_week"],
    )
    teams_playing = load_teams_playing_by_week(season, *scored_week_range(context))
    return shape_trade_score(
        context, our_team_id, counterparty_team_id, send_espn_ids, receive_espn_ids,
        raw_vorp, teams_playing,
    )


def load_teams_playing_by_week(season: int, start_week: int, end_week: int) -> dict:
    """{week: NFL teams with a game} for shape_trade_score, None for a week whose
    schedule couldn't be read."""
    teams_playing = {}
    for target_week in range(start_week, end_week + 1):
        try:
            teams_playing[target_week] = load_teams_playing(season, target_week)
        except Exception as error:
            # Without the NFL schedule a missing projection can't be told apart from a
            # bye, so those players stay unknown rather than being treated as idle.
            logger.warning(f"NFL schedule unavailable for {season} week {target_week}: {error}")
            teams_playing[target_week] = None
    return teams_playing
