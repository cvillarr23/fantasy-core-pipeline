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
- final_score = G* + C*.

Nothing is filled in. A week lacking a traded player's metrics, mapping or a fillable
lineup is unscored rather than counted as zero, and unknown opponent strength or pairing
withholds the weighted terms instead of treating that opponent as average. The raw
single-week VORP totals come from trade_service.evaluate_trade unchanged.
"""

import logging
import re

from src.ingestion.metrics_extractor import normalize_team
from src.optimization.lineup_solver import (
    BENCH_AND_IR_SLOT_NAMES,
    FLEX_SLOT_ELIGIBLE_POSITIONS,
    build_and_solve_lineup,
    hold_slots_without_metrics,
    load_teams_playing,
)
from src.services.trade_context_service import get_league_trade_context
from src.services.trade_service import evaluate_trade

logger = logging.getLogger("TradeScoreService")

SCORING_VERSION = "trade-score-v1"
STRENGTH_WEIGHT = 0.25
Z_CLAMP = 2.0
INDIRECT_FACTOR = 0.25
OUT_STATUSES = {"OUT", "INJURY_RESERVE"}
SUPPORTED_STARTER_SLOTS = {"QB", "RB", "WR", "TE", "K", "D/ST", *FLEX_SLOT_ELIGIBLE_POSITIONS}
# ESPN's pro-team codes that differ from the nflverse schedule's.
ESPN_TO_NFLVERSE_TEAM = {"WSH": "WAS"}

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


def _exclusion(player, week, as_of_week, playing):
    """Why a player can't be in the lineup pool this week, or None if they can."""
    if week == as_of_week and (
        player.get("injury_status") in OUT_STATUSES or player.get("slot") == "IR"
    ):
        return "out"
    if player.get("player_id") is None:
        return "unmapped"
    if player.get("position") is None:
        return "unknown_position"
    vorp = (player["metrics"].get(str(week)) or {}).get("vorp_score")
    if vorp is not None:
        return None
    nfl_team = player.get("nfl_team")
    nfl_team = normalize_team(ESPN_TO_NFLVERSE_TEAM.get(nfl_team, nfl_team))
    if playing is not None and nfl_team is not None and nfl_team not in playing:
        return "nfl_bye"
    return "no_metrics"


def _pool(roster, week, as_of_week, playing):
    pool, excluded = [], {}
    for player in roster:
        reason = _exclusion(player, week, as_of_week, playing)
        if reason:
            excluded[player["espn_id"]] = reason
            continue
        pool.append(
            {
                "player_id": player["player_id"],
                "espn_id": player["espn_id"],
                "name": player.get("name"),
                "position": player["position"],
                "vorp_score": float(player["metrics"][str(week)]["vorp_score"]),
            }
        )
    return pool, excluded


def _solve(pool, slot_counts):
    """Optimal starter VORP and starters, or None when the starter slots can't be filled."""
    demand = sum(
        int(count) for slot, count in slot_counts.items()
        if slot not in BENCH_AND_IR_SLOT_NAMES and int(count) > 0
    )
    starters = build_and_solve_lineup(pool, lineup_slot_counts=slot_counts) if demand else []
    if len(starters) != demand:
        return None
    by_id = {player["player_id"]: player for player in pool}
    shaped = sorted(
        (
            {
                "espn_id": by_id[s["player_id"]]["espn_id"],
                "player_id": s["player_id"],
                "name": s["name"],
                "position": s["position"],
                "vorp_score": s["vorp_score"],
            }
            for s in starters
        ),
        key=lambda starter: starter["espn_id"],
    )
    return sum(s["vorp_score"] for s in shaped), shaped


def _team_week(before_roster, after_roster, traded_ids, week, context, playing):
    """One team's before/after lineup for one week, or (None, reasons) if unscorable."""
    as_of_week = context["as_of_week"]
    before_pool, before_excluded = _pool(before_roster, week, as_of_week, playing)
    after_pool, after_excluded = _pool(after_roster, week, as_of_week, playing)
    excluded = {**before_excluded, **after_excluded}
    reasons = set()
    for espn_id in traded_ids:
        reason = excluded.get(espn_id)
        if reason == "unmapped":
            reasons.add("unmapped_traded_player")
        elif reason == "unknown_position":
            reasons.add("unknown_traded_player_position")
        elif reason == "no_metrics":
            reasons.add("schedule_unavailable" if playing is None else "missing_traded_player_metrics")
    slot_counts = context["lineup_slot_counts"]
    if any(
        slot not in BENCH_AND_IR_SLOT_NAMES and int(count) > 0 and slot not in SUPPORTED_STARTER_SLOTS
        for slot, count in slot_counts.items()
    ):
        reasons.add("unsupported_lineup_slot")
    if reasons:
        return None, reasons
    # A K/D-ST slot that can't be filled on either side is held on both, so the delta
    # never credits a trade with filling a slot that was only empty for want of data.
    _, held_before = hold_slots_without_metrics(slot_counts, before_pool)
    _, held_after = hold_slots_without_metrics(slot_counts, after_pool)
    held = sorted(set(held_before) | set(held_after))
    solve_counts = {slot: count for slot, count in slot_counts.items() if slot not in held}
    before, after = _solve(before_pool, solve_counts), _solve(after_pool, solve_counts)
    if before is None or after is None:
        return None, {"lineup_unfillable"}
    return {
        "week": week,
        "before": before[0],
        "after": after[0],
        "delta": after[0] - before[0],
        "starters_before": before[1],
        "starters_after": after[1],
        "held_slots": held,
        "excluded_players": [
            {"espn_id": espn_id, "reason": reason}
            for espn_id, reason in sorted(excluded.items())
        ],
    }, set()


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

    reasons = []
    our_weighted = benefit = final = None
    if not scored:
        reasons.append("no_scored_weeks")
    else:
        our_multipliers = [schedule_by_week[w]["our_multiplier"] for w in scored]
        their_weights = [schedule_by_week[w]["counterparty_weight"] for w in scored]
        if None in our_multipliers:
            reasons.append("unknown_our_opponent_strength")
        else:
            our_weighted = sum(our_deltas[w] * m for w, m in zip(scored, our_multipliers))
        if None in their_weights:
            reasons.append("unknown_counterparty_opponent_strength")
        else:
            benefit = sum(-their_deltas[w] * c for w, c in zip(scored, their_weights))
        if not reasons:
            if not any(our_multipliers) and not any(their_weights):
                reasons.append("no_matchup_weeks")
            else:
                final = our_weighted + benefit

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
        },
        "roster_impact": {"us": impact("us"), "counterparty": impact("counterparty")},
        "schedule": {"weeks": schedule},
        "score": {
            "our_lineup_gain": gain,
            "counterparty_lineup_change": change,
            "our_weighted_gain": our_weighted,
            "our_schedule_adjustment": None if our_weighted is None else our_weighted - gain,
            "counterparty_weighted_benefit": benefit,
            "final_score": final,
            "final_score_unavailable_reasons": reasons,
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
    teams_playing = {}
    for target_week in range(context["start_week"], context["end_week"] + 1):
        try:
            teams_playing[target_week] = load_teams_playing(season, target_week)
        except Exception as error:
            # Without the NFL schedule a missing projection can't be told apart from a
            # bye, so those players stay unknown rather than being treated as idle.
            logger.warning(f"NFL schedule unavailable for {season} week {target_week}: {error}")
            teams_playing[target_week] = None
    return shape_trade_score(
        context, our_team_id, counterparty_team_id, send_espn_ids, receive_espn_ids,
        raw_vorp, teams_playing,
    )
