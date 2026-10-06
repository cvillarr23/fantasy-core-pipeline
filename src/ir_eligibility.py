"""
Injured-reserve rules shared by the waiver recommendations, the live waiver publishers
and the lineup page's IR hints.

Eligibility uses ESPN's own injuryStatus (stored on league_rosters.espn_injury_status by
sync_league, or read live off espn_api roster players), never players.injury_status:
that column is Sleeper's, uses different labels, and refreshes at most daily, while ESPN
is the one that accepts or rejects the IR move.

IR_ELIGIBLE_ESPN_STATUSES is not read from the league: rosterSettings carries no IR
eligibility field that espn-api exposes. OUT and INJURY_RESERVE are ESPN's default; it
has not been confirmed against an accepted/rejected slot-21 move for this league yet, so
suggestions built on it stay display-only until fantasy-browser-worker captures one.
"""
import json

IR_SLOT = "IR"
IR_ELIGIBLE_ESPN_STATUSES = frozenset({"OUT", "INJURY_RESERVE"})


def is_ir_eligible(status) -> bool:
    return isinstance(status, str) and status.upper() in IR_ELIGIBLE_ESPN_STATUSES


def open_ir_slots(lineup_slot_counts: dict, occupied: int) -> int:
    return max(0, int(lineup_slot_counts.get(IR_SLOT, 0) or 0) - occupied)


def load_ir_slot_count(cursor, season: int) -> int:
    """This league's IR slot count, from the lineup_slot_counts sync_league stores. Same
    row lineup_solver.load_lineup_slot_counts reads, but no row means 0 here: its 9-starter
    default carries no IR, and assuming a slot exists would suggest moves ESPN rejects."""
    cursor.execute(
        """
        SELECT lineup_slot_counts
        FROM league_settings
        WHERE season = %s
        ORDER BY updated_at DESC
        LIMIT 1;
        """,
        (season,),
    )
    row = cursor.fetchone()
    counts = row[0] if row else None
    if isinstance(counts, str):
        counts = json.loads(counts)
    return open_ir_slots(counts, 0) if isinstance(counts, dict) else 0


def load_open_ir_slots(cursor, team_id: str, season: int) -> int:
    """IR slots this team can still fill, from league_settings and our league_rosters rows."""
    slot_count = load_ir_slot_count(cursor, season)
    if slot_count == 0:
        return 0
    cursor.execute(
        "SELECT COUNT(*) FROM league_rosters WHERE team_id = %s AND slot_position = 'IR';",
        (str(team_id),),
    )
    row = cursor.fetchone()
    return max(0, slot_count - (int(row[0]) if row else 0))


def lineup_ir_hints(lines: list, ir_slot_count: int) -> tuple:
    """Pure: ({player_id: hint}, ir_blocking_transactions) for one team's roster lines
    (dicts with player_id, slot, injury_status = ESPN's, net_vorp).

    A player sitting in IR who is no longer eligible gets "activate_from_ir": ESPN blocks
    every new transaction until they leave IR, so that also sets the blocking flag. Open
    slots go to eligible players outside IR, starters first (they hold a lineup spot), then
    the more valuable player, since the one left over is the one worth dropping."""
    hints = {}
    in_ir = [line for line in lines if line["slot"] == IR_SLOT]
    for line in in_ir:
        if not is_ir_eligible(line["injury_status"]):
            hints[line["player_id"]] = {
                "action": "activate_from_ir",
                "reason": f"{line['injury_status'] or 'No injury status'} on ESPN; an active player in IR blocks new transactions",
            }
    blocking = bool(hints)

    candidates = [
        line for line in lines
        if line["slot"] != IR_SLOT and is_ir_eligible(line["injury_status"])
    ]
    candidates.sort(key=lambda line: (line["slot"] in ("BE", "Bench"), -(line.get("net_vorp") or 0.0)))
    for line in candidates[:max(0, ir_slot_count - len(in_ir))]:
        hints[line["player_id"]] = {
            "action": "move_to_ir",
            "reason": f"{line['injury_status']} on ESPN with an open IR slot",
        }
    return hints, blocking
