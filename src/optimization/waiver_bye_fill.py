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

# Each candidate pair costs two lineup solves per horizon week; these keep a request fast.
CANDIDATES_PER_CRUNCH_WEEK = 2
DROPS_PER_CRUNCH_WEEK = 2


def _week_vorp(player, week):
    return ((player.get("metrics") or {}).get(str(week)) or {}).get("vorp_score")


def lineup_gains_by_week(roster, add, drop, as_of_week, weeks, slot_counts, playing_by_week):
    """[{week, delta}] of our best lineup after adding `add` (and dropping `drop`, if
    any). Unlike roster_move_scoring.compare_week, a lineup with an empty seat still
    scores (the seat adds nothing), since filling that seat is the point of a fill-in.
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
        if before_week["value"] is None or after_week["value"] is None or unknown:
            gains.append({"week": week, "delta": None})
        else:
            gains.append({"week": week, "delta": after_week["value"] - before_week["value"]})
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
    """(moves, outlook). At most one move per crunch week, best horizon lineup gain first;
    moves never reuse a player already claimed by another recommendation."""
    outlook = build_bye_outlook(roster, weeks, as_of_week, slot_counts, playing_by_week)
    claimed = set(claimed_player_ids)
    moves = []
    for crunch in (w for w in outlook["weeks"] if w["crunch"]):
        week = crunch["week"]
        needed = _needed_positions(roster, crunch, as_of_week, slot_counts, playing_by_week.get(week))
        candidates = sorted(
            (fa for pid, fa in free_agents.items()
             if pid not in claimed and fa["position"] in needed
             and (_week_vorp(fa, week) or 0.0) > 0 and fa.get("injury_status") is None),
            key=lambda fa: -_week_vorp(fa, week),
        )[:CANDIDATES_PER_CRUNCH_WEEK]
        if open_roster_slots > len([m for m in moves if m["drop_player_id"] is None]):
            drops = [None]
        else:
            drops = sorted(
                (p for p in roster
                 if not p.get("is_starter") and p.get("slot") != "IR"
                 and p["player_id"] not in claimed and p["player_id"] in value_by_player),
                key=lambda p: value_by_player[p["player_id"]],
            )[:DROPS_PER_CRUNCH_WEEK]
        best = None
        for fa in candidates:
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
            moves.append(best)
            claimed.update(pid for pid in (best["add_player_id"], best["drop_player_id"]) if pid)
    return moves, outlook
