"""League-scored ESPN read models. Never substitute VORP for fantasy points."""

import json
import os
import time
from datetime import datetime, timezone
from espn_api.football.constant import POSITION_MAP, PRO_TEAM_MAP
from src.ingestion.sync_league import build_league, fetch_raw_league_data
from src.services.read_cache import cached, league_key


# ESPN player position identifiers are distinct from lineup slot identifiers.
DEFAULT_POSITIONS = {1: "QB", 2: "RB", 3: "WR", 4: "TE", 5: "K", 16: "D/ST"}
LEADER_FETCH_BUDGET_SECONDS = 12

def stamp():
    return datetime.now(timezone.utc).isoformat()


def current_context(season):
    def load():
        league = build_league(season, request_timeout_seconds=10)
        return {
            "week": max(1, min(18, int(league.current_week))),
            "season": season,
            "min_week": 1,
            "max_week": min(18, int(getattr(league, "finalScoringPeriod", 18))),
            "source": "ESPN",
            "updated_at": stamp(),
        }

    return cached(["current-week", *league_key(season)], 300, load)


def standings(season):
    def load():
        league = build_league(season, request_timeout_seconds=10)
        items = [
            {
                "rank": getattr(t, "standing", None),
                "id": str(t.team_id),
                "name": t.team_name,
                "wins": t.wins,
                "losses": t.losses,
                "ties": t.ties,
                "points": t.points_for,
                "points_against": t.points_against,
            }
            for t in league.teams
        ]
        items.sort(
            key=lambda r: (r["rank"] if r["rank"] is not None else 999, r["name"])
        )
        return {
            "items": items,
            "total": len(items),
            "updated_at": stamp(),
            "scoring": "ESPN league scoring",
        }

    # Standings only move when games finalise, and verified moves bump the league version.
    return cached(["standings", *league_key(season)], 300, load)


def stat_value(player, season, week, source):
    values = [
        s.get("appliedTotal")
        for s in player.get("stats", [])
        if s.get("seasonId") == season
        and s.get("scoringPeriodId") == week
        and s.get("statSourceId") == source
        and s.get("statSplitTypeId") != 2
    ]
    return next((float(v) for v in values if v is not None), None)


def leaders(season, week, totals=False):
    def load():
        league = build_league(season, request_timeout_seconds=10)
        owners = {str(t.team_id): t.team_name for t in league.teams}
        items = {}
        offset = 0
        coverage = None
        # Explicit page offsets prevent silently ranking only the first ESPN page.
        deadline = time.monotonic() + LEADER_FETCH_BUDGET_SECONDS
        for _ in range(20):
            if time.monotonic() >= deadline:
                coverage = "Player fetch time budget reached; rankings cover returned players only."
                break
            filters = {
                "players": {
                    "limit": 500,
                    "offset": offset,
                    "sortPercOwned": {"sortPriority": 1, "sortAsc": False},
                    "filterStatsForTopScoringPeriodIds": {
                        "value": 18,
                        "additionalValue": [f"00{season}", f"10{season}"],
                    },
                }
            }
            raw = league.espn_request.league_get(
                params={"view": "kona_player_info", "scoringPeriodId": week},
                headers={"x-fantasy-filter": json.dumps(filters)},
            )
            rows = raw.get("players", [])
            added = 0
            for row in rows:
                player = row.get(
                    "player", row.get("playerPoolEntry", {}).get("player", {})
                )
                if not player:
                    continue
                pid = str(player["id"])
                if pid in items:
                    continue
                owner_id = str(
                    row.get(
                        "onTeamId", row.get("playerPoolEntry", {}).get("onTeamId", 0)
                    )
                )
                if totals:
                    vals = [
                        stat_value(player, season, w, 0)
                        for w in range(1, min(week, 18) + 1)
                    ]
                    pts = (
                        sum(v for v in vals if v is not None)
                        if any(v is not None for v in vals)
                        else None
                    )
                else:
                    pts = stat_value(player, season, week, 0)
                items[pid] = {
                    "id": pid,
                    "name": player.get("fullName", "Unknown player"),
                    "position": DEFAULT_POSITIONS.get(player.get("defaultPositionId"), "—"),
                    "team": PRO_TEAM_MAP.get(player.get("proTeamId"), "—"),
                    "owner": owners.get(owner_id),
                    "owner_id": owner_id,
                    "points": pts,
                    "projected": None
                    if totals
                    else stat_value(player, season, week, 1),
                }
                added += 1
            if len(rows) < 500:
                break
            if not added:
                coverage = "ESPN did not return additional pages; rankings cover returned players only."
                break
            offset += len(rows)
        else:
            coverage = (
                "Player source limit reached; rankings cover returned players only."
            )
        result = sorted(
            items.values(),
            key=lambda p: (p["points"] is None, -(p["points"] or 0), p["name"]),
        )
        previous = None
        rank = 0
        for index, item in enumerate(result):
            if index == 0 or item["points"] != previous:
                rank = index + 1
            item["rank"] = rank
            previous = item["points"]
        return {
            "items": result,
            "total": len(result),
            "updated_at": stamp(),
            "scoring": "ESPN league scoring",
            "coverage": coverage,
        }

    # Player points change during live games; two minutes keeps leaders close to ESPN.
    return cached(["leaders", *league_key(season), week, totals], 120, load)


def shape_player(p):
    return {
        "espn_id": str(p.playerId),
        "name": p.name,
        "position": p.position,
        "pro_team": p.proTeam,
        "injury_status": p.injuryStatus,
        "slot": getattr(p, "lineupSlot", ""),
        "eligible_slots": p.eligibleSlots,
        # ESPN's onTeamId is 0 for free agents; lets clients tell a waiver target from a
        # player that can only be traded for.
        "on_team_id": str(p.onTeamId) if getattr(p, "onTeamId", 0) not in (None, 0, "0") else None,
    }


def normalize_card(p):
    # espn_api's json_parsing() returns [] (not None) for absent keys, and player_info()
    # keys schedule by the raw JSON scoring period strings; downstream code expects
    # scalar statuses and int weeks.
    if not isinstance(p.injuryStatus, str) or not p.injuryStatus:
        p.injuryStatus = None
    if not isinstance(getattr(p, "onTeamId", 0), (int, str)):
        p.onTeamId = 0
    p.schedule = {
        int(k): v for k, v in (p.schedule or {}).items() if str(k).lstrip("-").isdigit()
    }
    return p


def point_projection(p, week, current):
    status = p.injuryStatus if isinstance(p.injuryStatus, str) else None
    if status in {
        "OUT",
        "Out",
        "INJURY_RESERVE",
        "IR",
        "SUSPENSION",
        "DOUBTFUL",
    }:
        return 0.0, "Unavailable"
    if week not in p.schedule:
        return (0.0, "Bye") if p.schedule else (None, "Schedule unavailable")
    stat = p.stats.get(week, {})
    if stat.get("projected_points") is not None:
        return float(stat["projected_points"]), "Published projection"
    prior = [
        p.stats[w]["points"]
        for w in sorted(p.stats, reverse=True)
        if 0 < w < min(week, current)
        and p.stats[w].get("breakdown")
        and p.stats[w].get("points") is not None
    ][:3]
    return (
        (sum(prior) / len(prior), "Estimated from recent games")
        if prior
        else (None, "Projection unavailable")
    )


def solve_points(players, slots, week, current):
    import pulp

    values = {str(p.playerId): point_projection(p, week, current) for p in players}
    available = [
        p
        for p in players
        if values[str(p.playerId)][1] not in {"Bye", "Unavailable"}
        and values[str(p.playerId)][0] is not None
        and getattr(p, "lineupSlot", "") != "IR"
    ]
    # Unknown players might change the optimal lineup; do not present a false complete total.
    missing = any(v[0] is None for v in values.values())
    model = pulp.LpProblem("preview", pulp.LpMaximize)
    slot_list = [
        name
        for name, count in slots.items()
        if name not in {"BE", "IR", "Bench"}
        for _ in range(int(count))
    ]
    variables = {
        (str(p.playerId), i): pulp.LpVariable(
            f"p_{str(p.playerId).replace('-', 'n')}_{i}", cat="Binary"
        )
        for p in available
        for i, slot in enumerate(slot_list)
        if slot in p.eligibleSlots
    }
    for i in range(len(slot_list)):
        model += pulp.lpSum(v for (pid, j), v in variables.items() if j == i) == 1
    for p in available:
        model += (
            pulp.lpSum(v for (pid, j), v in variables.items() if pid == str(p.playerId))
            <= 1
        )
    model += pulp.lpSum(v * values[pid][0] for (pid, i), v in variables.items())
    model.solve(pulp.PULP_CBC_CMD(msg=False, timeLimit=5))
    if pulp.LpStatus[model.status] != "Optimal":
        return None, [], "Incomplete eligible lineup"
    lineup = [
        {**shape_player(p), "slot": slot_list[i]}
        for p in available
        for (pid, i), v in variables.items()
        if pid == str(p.playerId) and v.value() == 1
    ]
    total = sum(values[p["espn_id"]][0] for p in lineup)
    source = (
        "Estimated from recent games"
        if any("Estimated" in values[p["espn_id"]][1] for p in lineup)
        else "Published projections"
    )
    return (
        (None if missing else total),
        lineup,
        ("Incomplete projection coverage" if missing else source),
    )


def move_preview(payload):
    incoming = payload.incoming_espn_ids
    outgoing = payload.outgoing_espn_ids
    if (
        len(set(incoming)) != len(incoming)
        or len(set(outgoing)) != len(outgoing)
        or set(incoming) & set(outgoing)
    ):
        raise ValueError("Players must be distinct on each side.")
    if payload.end_week < payload.start_week:
        raise ValueError("End week must follow start week.")
    league = build_league(payload.season, request_timeout_seconds=10)
    team = next(
        (t for t in league.teams if str(t.team_id) == os.getenv("ESPN_TEAM_ID")), None
    )
    if not team:
        raise ValueError("Configured team not found.")
    roster_ids = {str(p.playerId) for p in team.roster}
    if not set(outgoing) <= roster_ids:
        raise ValueError("Outgoing players are no longer on your roster.")
    if set(incoming) & roster_ids:
        raise ValueError("Incoming players are already on your roster.")
    cards = league.player_info(
        playerId=[int(pid) for pid in roster_ids | set(incoming)]
    )
    cards = cards if isinstance(cards, list) else [cards] if cards else []
    by_id = {str(p.playerId): normalize_card(p) for p in cards}
    if not roster_ids | set(incoming) <= by_id.keys():
        raise ValueError("Some player details are unavailable from ESPN.")
    for p in team.roster:
        by_id[str(p.playerId)].lineupSlot = p.lineupSlot
    before = [by_id[pid] for pid in sorted(roster_ids)]
    after = [by_id[pid] for pid in sorted((roster_ids - set(outgoing)) | set(incoming))]
    raw = fetch_raw_league_data(league)
    raw_slots = (
        raw.get("settings", {}).get("rosterSettings", {}).get("lineupSlotCounts", {})
    )
    if not raw_slots:
        raise ValueError("League lineup settings unavailable.")
    slots = {POSITION_MAP.get(int(k), str(k)): v for k, v in raw_slots.items()}
    warnings = [
        "Current roster simulated across weeks. Current injury status is assumed to persist. Future lineup locks are not predicted."
    ]
    for pid in incoming:
        owner = getattr(by_id[pid], "onTeamId", 0)
        if payload.kind == "waiver" and owner not in (None, 0, "0"):
            warnings.append(f"{by_id[pid].name} is currently rostered by another team.")
    for p in after:
        if p.injuryStatus not in (None, "ACTIVE", "NORMAL", "Healthy"):
            warnings.append(f"{p.name}: {p.injuryStatus}")
    if len(after) > len(before):
        warnings.append(
            "Roster size increases. Confirm available roster capacity before submitting."
        )
    weeks = []
    for week in range(payload.start_week, payload.end_week + 1):
        b, bl, bs = solve_points(before, slots, week, league.current_week)
        a, al, asource = solve_points(after, slots, week, league.current_week)
        weeks.append(
            {
                "week": week,
                "before": b,
                "after": a,
                "delta": None if a is None or b is None else a - b,
                "before_lineup": bl,
                "after_lineup": al,
                "source": bs if bs == asource else f"{bs}; {asource}",
            }
        )
        byes = [p.name for p in after if p.schedule and week not in p.schedule]
        if byes:
            warnings.append(f"Week {week} byes: {', '.join(byes)}")
    return {
        "before": [shape_player(p) for p in before],
        "after": [shape_player(p) for p in after],
        "incoming": [shape_player(by_id[p]) for p in incoming],
        "outgoing": [shape_player(by_id[p]) for p in outgoing],
        "weeks": weeks,
        "warnings": warnings,
        "updated_at": stamp(),
    }
