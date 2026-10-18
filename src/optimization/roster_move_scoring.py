"""
Per-week optimal-lineup comparison of a roster before and after a move: a trade, a
waiver add/drop, or no move at all (a bye outlook). Shared by trade_score_service,
waiver_service and roster_outlook_service so every recommender judges a move by the
starting lineup it produces in each week of a horizon, not by one week's raw VORP.

That is what keeps byes from distorting a recommendation in either direction: a player
on bye in one week simply sits that week (reason "nfl_bye") and still counts in every
other week, so a stud is never dropped or traded away for one idle week, while a week in
which byes leave the lineup thin shows up as a real lineup loss that a move can repair.

Roster entries use the trade context's player shape: espn_id, player_id, name, position,
nfl_team, injury_status, slot, metrics {str(week): {vorp_score, ...}}. Nothing is filled
in: a missing projection is "no_metrics" (or "nfl_bye" when the NFL schedule confirms
the team is idle), never a zero.
"""
from src.nfl_byes import to_nflverse_team
from src.optimization.lineup_solver import (
    BENCH_AND_IR_SLOT_NAMES,
    FLEX_SLOT_ELIGIBLE_POSITIONS,
    build_and_solve_lineup,
    hold_slots_without_metrics,
)

OUT_STATUSES = {"OUT", "INJURY_RESERVE"}
SUPPORTED_STARTER_SLOTS = {"QB", "RB", "WR", "TE", "K", "D/ST", *FLEX_SLOT_ELIGIBLE_POSITIONS}
# Reasons a player sits a week without making the roster's lineup unknowable.
SITS_KNOWINGLY = {"out", "nfl_bye"}


def exclusion(player, week, as_of_week, playing):
    """Why a player can't be in the lineup pool this week, or None if they can."""
    if week == as_of_week and (
        player.get("injury_status") in OUT_STATUSES or player.get("slot") == "IR"
    ):
        return "out"
    if player.get("player_id") is None:
        return "unmapped"
    if player.get("position") is None:
        return "unknown_position"
    # The schedule is checked before the row: ESPN projects a kicker on bye at 0 rather
    # than leaving him out, and Sleeper can carry a stale team, so a bye-week row exists.
    nfl_team = to_nflverse_team(player.get("nfl_team"))
    if playing is not None and nfl_team is not None and nfl_team not in playing:
        return "nfl_bye"
    vorp = ((player.get("metrics") or {}).get(str(week)) or {}).get("vorp_score")
    if vorp is not None:
        return None
    return "no_metrics"


def lineup_pool(roster, week, as_of_week, playing):
    """(solver pool, {espn_id: exclusion reason}) for one week."""
    pool, excluded = [], {}
    for player in roster:
        reason = exclusion(player, week, as_of_week, playing)
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


def solve_lineup(pool, slot_counts):
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


def has_unsupported_slot(slot_counts):
    return any(
        slot not in BENCH_AND_IR_SLOT_NAMES and int(count) > 0 and slot not in SUPPORTED_STARTER_SLOTS
        for slot, count in slot_counts.items()
    )


def compare_week(before_roster, after_roster, moved_ids, week, as_of_week, slot_counts, playing):
    """One roster's before/after optimal lineup for one week, or (None, reasons) if the
    week can't be scored. A moved player missing metrics for a reason other than a
    confirmed bye or Out makes the week unscorable rather than counting him as zero."""
    before_pool, before_excluded = lineup_pool(before_roster, week, as_of_week, playing)
    after_pool, after_excluded = lineup_pool(after_roster, week, as_of_week, playing)
    excluded = {**before_excluded, **after_excluded}
    reasons = set()
    for espn_id in moved_ids:
        reason = excluded.get(espn_id)
        if reason == "unmapped":
            reasons.add("unmapped_traded_player")
        elif reason == "unknown_position":
            reasons.add("unknown_traded_player_position")
        elif reason == "no_metrics":
            reasons.add("schedule_unavailable" if playing is None else "missing_traded_player_metrics")
    if not before_pool and not after_pool:
        # Nobody has a row for this week (typically not refreshed yet); say so rather than
        # blaming roster construction, even when the moved players happen to be on bye.
        reasons.add("no_week_metrics")
    if has_unsupported_slot(slot_counts):
        reasons.add("unsupported_lineup_slot")
    if reasons:
        return None, reasons
    # A K/D-ST slot that can't be filled on either side is held on both, so the delta
    # never credits a move with filling a slot that was only empty for want of data.
    _, held_before = hold_slots_without_metrics(slot_counts, before_pool)
    _, held_after = hold_slots_without_metrics(slot_counts, after_pool)
    held = sorted(set(held_before) | set(held_after))
    solve_counts = {slot: count for slot, count in slot_counts.items() if slot not in held}
    before, after = solve_lineup(before_pool, solve_counts), solve_lineup(after_pool, solve_counts)
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


def unfilled_starter_slots(pool, slot_counts):
    """Starter slots (FLEX included) the pool can't cover, as a sorted list of names
    repeated per missing seat. Greedy by specificity: dedicated slots claim players
    before FLEX slots draw on what's left."""
    supply = {}
    for player in pool:
        supply[player["position"]] = supply.get(player["position"], 0) + 1
    short = []
    starter_slots = [
        (slot, int(count)) for slot, count in slot_counts.items()
        if slot not in BENCH_AND_IR_SLOT_NAMES and int(count) > 0
    ]
    for slot, count in sorted(starter_slots, key=lambda item: len(FLEX_SLOT_ELIGIBLE_POSITIONS.get(item[0], (item[0],)))):
        for _ in range(count):
            for position in FLEX_SLOT_ELIGIBLE_POSITIONS.get(slot, (slot,)):
                if supply.get(position, 0) > 0:
                    supply[position] -= 1
                    break
            else:
                short.append(slot)
    return sorted(short)


def lineup_week(roster, week, as_of_week, slot_counts, playing):
    """One roster's best lineup for one week, with byes and empty seats spelled out.

    value is the optimal starters' VORP over the slots the roster can fill (None when
    nobody has metrics that week). Unfillable seats are listed, not solved as an error,
    because "two RBs on bye leaves FLEX empty" is exactly what a bye outlook reports."""
    pool, excluded = lineup_pool(roster, week, as_of_week, playing)
    on_bye = sorted(espn_id for espn_id, reason in excluded.items() if reason == "nfl_bye")
    unknown = sorted(
        espn_id for espn_id, reason in excluded.items() if reason not in SITS_KNOWINGLY
    )
    if not pool:
        return {
            "week": week, "value": None, "starters": [], "unfilled_slots": [],
            "held_slots": [], "on_bye": on_bye, "unknown": unknown,
        }
    solve_counts, held = hold_slots_without_metrics(slot_counts, pool)
    # A K or D/ST slot is held when nobody has metrics for it. When that's because our
    # player there is on bye, the seat is really empty, and the outlook should say so.
    bye_positions = {p["position"] for p in roster if excluded.get(p["espn_id"]) == "nfl_bye"}
    emptied = [slot for slot in held if slot in bye_positions]
    held = [slot for slot in held if slot not in emptied]
    short = unfilled_starter_slots(pool, solve_counts)
    for slot in short:
        solve_counts[slot] = int(solve_counts[slot]) - 1
    unfilled = sorted(short + [slot for slot in emptied for _ in range(int(slot_counts[slot]))])
    solved = solve_lineup(pool, solve_counts)
    value, starters = solved if solved is not None else (None, [])
    return {
        "week": week, "value": value, "starters": starters, "unfilled_slots": unfilled,
        "held_slots": held, "on_bye": on_bye, "unknown": unknown,
    }
