"""
Bye fill-in waiver suggestions: when our bye outlook shows a crunch week (byes leave a
starter seat empty or force a below-replacement starter), find a free agent who plays
that week and is worth the roster spot.

Recommend-only. Nothing here publishes; the live waiver cron never claims these.

Two guards keep a fill-in from costing season-long value:
- the move must raise our optimal lineup summed over the whole horizon (roster_move_scoring),
  not just in the crunch week, so cutting someone who starts in other weeks fails;
- the dropped player's horizon VORP may not exceed the add's (floored at replacement
  level), so a valuable bench player who rarely starts, e.g. a handcuff, is never cut
  for a one-week streamer.
"""
from src.optimization.bye_outlook import build_bye_outlook
from src.optimization.lineup_solver import BENCH_AND_IR_SLOT_NAMES, FLEX_SLOT_ELIGIBLE_POSITIONS
from src.optimization.roster_move_scoring import lineup_week
from src.optimization.vorp_matrix import DEFAULT_BASELINES

# Each candidate pair costs two lineup solves per horizon week; these keep a request fast.
CANDIDATES_PER_CRUNCH_WEEK = 6
DROPS_PER_CRUNCH_WEEK = 2
# Alternatives listed per crunch week, so there's a choice when the top add is gone.
OPTIONS_PER_CRUNCH_WEEK = 3

# What an empty starter seat is worth in VORP terms. It scores zero points, which is the
# replacement baseline below replacement level, not replacement level itself. Without
# this, a seat left empty by byes looked as good as a replacement starter, so filling it
# with a typical (below-replacement) free agent read as a loss. FLEX uses its costliest
# eligible position. These are vorp_matrix's default baselines, an approximation of the
# week's real ones, which aren't stored.
EMPTY_SEAT_VORP = {
    **{pos: -base for pos, base in DEFAULT_BASELINES.items()},
    **{slot: -max(DEFAULT_BASELINES.get(p, 0.0) for p in positions)
       for slot, positions in FLEX_SLOT_ELIGIBLE_POSITIONS.items()},
}


def _week_vorp(player, week):
    return ((player.get("metrics") or {}).get(str(week)) or {}).get("vorp_score")


def _seated_value(week_row):
    """A week's lineup VORP with each empty seat counted at EMPTY_SEAT_VORP."""
    if week_row["value"] is None:
        return None
    return week_row["value"] + sum(EMPTY_SEAT_VORP.get(slot, 0.0) for slot in week_row["unfilled_slots"])


def lineup_gains_by_week(roster, add, drop, as_of_week, weeks, slot_counts, playing_by_week):
    """[{week, delta}] of our best lineup after adding `add` (and dropping `drop`, if
    any). Unlike roster_move_scoring.compare_week, a lineup with an empty seat still
    scores, with the seat at EMPTY_SEAT_VORP, since filling that seat is the point of a
    fill-in.
    delta is None for a week with no metrics, or one where a moved player's projection
    is missing for a reason other than a confirmed bye."""
    after = [p for p in roster if drop is None or p["player_id"] != drop["player_id"]] + [add]
    moved = {add["espn_id"]} | ({drop["espn_id"]} if drop is not None else set())
    gains = []
    for week in weeks:
        playing = playing_by_week.get(week)
        before_week = lineup_week(roster, week, as_of_week, slot_counts, playing)
        after_week = lineup_week(after, week, as_of_week, slot_counts, playing)
        unknown = moved & (set(before_week["unknown"]) | set(after_week["unknown"]))
        before_value, after_value = _seated_value(before_week), _seated_value(after_week)
        if before_value is None or after_value is None or unknown:
            gains.append({"week": week, "delta": None})
        else:
            gains.append({"week": week, "delta": after_value - before_value})
    return gains


def _needed_positions(roster, crunch, as_of_week, slot_counts, playing):
    """Positions that could fill the crunch week's empty seats or replace its
    below-replacement starters."""
    needed = set()
    for slot in crunch["unfilled_slots"]:
        needed.update(FLEX_SLOT_ELIGIBLE_POSITIONS.get(slot, (slot,)))
    week = lineup_week(roster, crunch["week"], as_of_week, slot_counts, playing)
    needed.update(s["position"] for s in week["starters"] if s["vorp_score"] <= 0)
    starter_slots = {s for s, c in slot_counts.items() if s not in BENCH_AND_IR_SLOT_NAMES and int(c) > 0}
    return {p for p in needed if any(p in FLEX_SLOT_ELIGIBLE_POSITIONS.get(s, (s,)) for s in starter_slots)}


def plan_bye_fill_moves(roster, free_agents, value_by_player, as_of_week, weeks, slot_counts,
                        playing_by_week, open_roster_slots, claimed_player_ids=frozenset()):
    """(moves, outlook). Up to OPTIONS_PER_CRUNCH_WEEK alternative moves per crunch week,
    best horizon lineup gain first. Options for one week are alternatives, so they may
    share a drop; an add is never offered for two weeks, and moves never reuse a player
    already claimed by another recommendation.

    A below-replacement free agent qualifies: when byes empty a seat, he still beats it."""
    outlook = build_bye_outlook(roster, weeks, as_of_week, slot_counts, playing_by_week)
    claimed = set(claimed_player_ids)
    moves = []
    add_only_weeks = 0
    for crunch in (w for w in outlook["weeks"] if w["crunch"]):
        week = crunch["week"]
        needed = _needed_positions(roster, crunch, as_of_week, slot_counts, playing_by_week.get(week))
        candidates = sorted(
            (fa for pid, fa in free_agents.items()
             if pid not in claimed and fa["position"] in needed
             and _week_vorp(fa, week) is not None and fa.get("injury_status") is None),
            key=lambda fa: -_week_vorp(fa, week),
        )[:CANDIDATES_PER_CRUNCH_WEEK]
        uses_open_slot = open_roster_slots > add_only_weeks
        if uses_open_slot:
            drops = [None]
        else:
            drops = sorted(
                (p for p in roster
                 if not p.get("is_starter") and p.get("slot") != "IR"
                 and p["player_id"] not in claimed and p["player_id"] in value_by_player),
                key=lambda p: value_by_player[p["player_id"]],
            )[:DROPS_PER_CRUNCH_WEEK]
        options = []
        for fa in candidates:
            best = None
            add_value = value_by_player.get(fa["player_id"], 0.0)
            for drop in drops:
                drop_value = None if drop is None else value_by_player[drop["player_id"]]
                if drop_value is not None and drop_value > max(add_value, 0.0):
                    continue
                gains = lineup_gains_by_week(
                    roster, fa, drop, as_of_week, weeks, slot_counts, playing_by_week,
                )
                crunch_gain = next(g["delta"] for g in gains if g["week"] == week)
                total = sum(g["delta"] for g in gains if g["delta"] is not None)
                if not crunch_gain or crunch_gain <= 0 or total <= 0:
                    continue
                if best is None or total > best["horizon_lineup_gain"]:
                    best = {
                        "kind": "add" if drop is None else "swap",
                        "position": fa["position"],
                        "add_player_id": fa["player_id"],
                        "drop_player_id": None if drop is None else drop["player_id"],
                        "add_vorp_score": add_value,
                        "drop_vorp_score": drop_value,
                        "bye_fill": {"week": week, "lineup_gain": crunch_gain},
                        "weekly_lineup_gains": gains,
                        "horizon_lineup_gain": total,
                    }
            if best is not None:
                options.append(best)
        options = sorted(options, key=lambda m: -m["horizon_lineup_gain"])[:OPTIONS_PER_CRUNCH_WEEK]
        if options and uses_open_slot:
            add_only_weeks += 1
        moves.extend(options)
        claimed.update(m["add_player_id"] for m in options)
    return moves, outlook
