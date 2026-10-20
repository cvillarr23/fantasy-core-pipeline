"""
League-wide trade candidates: every 1-for-1, 2-for-1 and 1-for-2 package between our team
and each counterparty, prefiltered cheaply, scored with the schedule-aware trade score and
classified with the browser worker's staging gates. Written by `python -m
src.jobs.trade_candidates`, read by GET /v1/trade/candidates and the list_trade_candidates
MCP tool. Shapes are named from our side: "2for1" means we send two and receive one.

Classification uses the worker's gates (fantasy-browser-worker eligibilityReasons):

- proposable: our lineup gain G > 0, their lineup change I >= 0, at least one scored
  week, no unmapped traded player, neither roster over its limit.
- needs_drop: only exceeds_roster_limit fails, so the offer would need a drop.
- one_sided: anything else; reasons lists the failed gates.

The prefilter must never drop a package that full scoring would mark proposable. It
leans on the lineup being a matroid (players matched into slots), so each week's optimal
lineup changes by exchanges:

- Adding a player can raise a lineup by at most his VORP minus the weakest starter in
  his position group (his position plus every position sharing a FLEX slot with it).
  Removing players never raises it. So G > 0 needs, in some week, an incoming player at
  or above that threshold on our side.
- Their side may also break even: I = 0 still passes. A package is dropped for them only
  when, in every week, nobody incoming reaches their threshold and some outgoing starter
  has no bench or incoming replacement as good as he is, so every week they could be
  scored in is a loss.

A week in which a team's current roster can't fill its lineup (typically a week with no
projections yet) is skipped: the scorer can't score that week for any package involving
the team.

Ties count as passing, so float noise in the solver can't make the prefilter unsound.
"""

import hashlib
import itertools
import logging
import math
import uuid
from datetime import datetime, timezone

from psycopg2.extras import Json

from src.database import get_db_cursor
from src.optimization.lineup_solver import (
    BENCH_AND_IR_SLOT_NAMES,
    FLEX_SLOT_ELIGIBLE_POSITIONS,
    HOLDABLE_WITHOUT_METRICS,
    hold_slots_without_metrics,
)
from src.optimization.roster_move_scoring import (
    OUT_STATUSES,
    exclusion,
    lineup_pool,
    lineup_solve_cache,
    solve_lineup,
)
from src.services.trade_context_service import get_league_trade_context
from src.services.trade_score_service import load_teams_playing_by_week, shape_trade_score
from src.services.trade_service import load_vorp_by_player, summarize_raw_vorp

logger = logging.getLogger("TradeCandidateService")

DEFAULT_MAX_CANDIDATES = 400
SHAPES = ("1for1", "2for1", "1for2")
STATUSES = ("proposable", "needs_drop", "one_sided")
SORT_COLUMNS = {
    "final_score": "final_score",
    "our_gain": "our_lineup_gain",
    "their_benefit": "counterparty_lineup_change",
}
_TOLERANCE = 1e-9
# A team whose current roster can't field a lineup in a week makes that week unscorable for
# every package it's in, so the prefilter ignores the week rather than keeping everything.
UNSCORABLE = "unscorable"


class TradeCandidatesNotFoundError(Exception):
    """The candidate job has not run for this league, season and week."""


def candidate_key(league_id, season, our_team_id, counterparty_team_id, send, receive):
    """Stable across runs and weeks, so a package can be followed from one run to the next."""
    material = "|".join(
        [str(league_id), str(season), our_team_id, counterparty_team_id,
         ",".join(sorted(send)), ",".join(sorted(receive))]
    )
    return hashlib.sha256(material.encode()).hexdigest()


def shape_of(send, receive):
    return f"{len(send)}for{len(receive)}"


def eligibility_reasons(score):
    """The worker's staging gates, in its order and with its reason codes."""
    reasons = []
    terms = score["score"]
    if not score["horizon"]["scored_weeks"]:
        reasons.append("no_scored_weeks")
    if terms["our_lineup_gain"] is None or not terms["our_lineup_gain"] > 0:
        reasons.append("no_gain_for_us")
    if terms["counterparty_lineup_change"] is None or terms["counterparty_lineup_change"] < 0:
        reasons.append("hurts_counterparty")
    impact = score["roster_impact"]
    if impact["us"]["exceeds_roster_limit"] or impact["counterparty"]["exceeds_roster_limit"]:
        reasons.append("exceeds_roster_limit")
    if score["coverage"]["unmapped_traded_espn_ids"]:
        reasons.append("unmapped_traded_player")
    return reasons


def classify(reasons):
    if not reasons:
        return "proposable"
    if reasons == ["exceeds_roster_limit"]:
        return "needs_drop"
    return "one_sided"


def position_groups(slot_counts):
    """{position: positions an added or removed player could trade places with}, the
    closure over every FLEX slot this league actually starts."""
    groups = {}
    for slot, count in slot_counts.items():
        if slot in BENCH_AND_IR_SLOT_NAMES or int(count) <= 0:
            continue
        positions = set(FLEX_SLOT_ELIGIBLE_POSITIONS.get(slot, (slot,)))
        for position in list(positions):
            positions |= groups.get(position, {position})
        for position in positions:
            groups[position] = positions
    return groups


def tradeable(player):
    """In the package universe: mapped, positioned, and neither Out nor in an IR slot."""
    return (
        player.get("slot") != "IR"
        and player.get("injury_status") not in OUT_STATUSES
        and player.get("player_id") is not None
        and player.get("position") is not None
    )


def _baseline(roster, week, context, playing, groups):
    """One team's current best lineup for one week, reduced to what the prefilter needs:
    the weakest starter and best non-starter per position group.

    UNSCORABLE when the roster can't fill its lineup even with every K/D-ST slot held: the
    scorer then fails that week's "before" solve for any trade. None when it only fails
    with fewer slots held, since the scorer may hold more; the prefilter keeps everything
    for such a week."""
    slot_counts = context["lineup_slot_counts"]
    pool, _ = lineup_pool(roster, week, context["as_of_week"], playing)
    solve_counts, _ = hold_slots_without_metrics(slot_counts, pool)
    solved = solve_lineup(pool, solve_counts) if pool else None
    if solved is None:
        fewest = {slot: count for slot, count in slot_counts.items() if slot not in HOLDABLE_WITHOUT_METRICS}
        if not pool or solve_lineup(pool, fewest) is None:
            return UNSCORABLE
        return None
    starters = {s["espn_id"] for s in solved[1]}
    weakest, best_bench = {}, {}
    for player in pool:
        target = weakest if player["espn_id"] in starters else best_bench
        pick = min if target is weakest else max
        for position in groups.get(player["position"], {player["position"]}):
            value = player["vorp_score"]
            target[position] = pick(target.get(position, value), value)
    return {"starters": starters, "weakest": weakest, "best_bench": best_bench}


def _week_vorp(player, week, context, playing):
    if exclusion(player, week, context["as_of_week"], playing):
        return None
    return float(player["metrics"][str(week)]["vorp_score"])


def _gain_bound(baseline, incoming, week, context, playing):
    """Upper bound on what the incoming players add to this week's lineup, and whether
    any of them reaches a starter at all (ties included)."""
    if baseline is None:
        return math.inf, True
    bound, reaches = 0.0, False
    for player in incoming:
        value = _week_vorp(player, week, context, playing)
        if value is None:
            continue
        threshold = baseline["weakest"].get(player["position"], -math.inf)
        if value - threshold >= -_TOLERANCE:
            reaches = True
            bound += max(0.0, value - threshold)
    return bound, reaches


def _loss_bound(baseline, outgoing, incoming, week, context, playing, groups):
    """Lower bound on what losing the outgoing players costs this week's lineup: the
    largest gap between a departing starter and the best bench or incoming player in his
    group. K/D-ST are skipped, since the scorer holds a slot the trade leaves empty
    instead of counting the loss. A starter nobody can replace makes the week
    unscorable, so his gap is taken against 0 just for ranking."""
    if baseline is None:
        return 0.0
    incoming_values = [
        (player["position"], value)
        for player in incoming
        if (value := _week_vorp(player, week, context, playing)) is not None
    ]
    loss = 0.0
    for player in outgoing:
        if player["espn_id"] not in baseline["starters"] or player["position"] in HOLDABLE_WITHOUT_METRICS:
            continue
        value = float(player["metrics"][str(week)]["vorp_score"])
        group = groups.get(player["position"], {player["position"]})
        replacements = [baseline["best_bench"].get(player["position"], -math.inf)] + [
            v for position, v in incoming_values if position in group
        ]
        best = max(replacements)
        loss = max(loss, value - (best if math.isfinite(best) else 0.0))
    return loss


def _roster_size(roster):
    return sum(1 for player in roster if player.get("slot") != "IR")


def enumerate_packages(context, our_team_id):
    """Every (counterparty, send, receive) in the candidate universe, in a fixed order."""
    teams = context["teams"]
    ours = [p for p in teams[our_team_id]["roster"] if tradeable(p)]
    for counterparty_team_id in sorted(teams, key=int):
        if counterparty_team_id == our_team_id:
            continue
        theirs = [p for p in teams[counterparty_team_id]["roster"] if tradeable(p)]
        for send_count, receive_count in ((1, 1), (2, 1), (1, 2)):
            for send in itertools.combinations(ours, send_count):
                for receive in itertools.combinations(theirs, receive_count):
                    yield counterparty_team_id, list(send), list(receive)


class Prefilter:
    """Per-team, per-week baselines computed once, then a cheap check per package."""

    def __init__(self, context, teams_playing_by_week):
        self.context = context
        self.playing = teams_playing_by_week
        self.weeks = list(range(context["start_week"], context["end_week"] + 1))
        self.groups = position_groups(context["lineup_slot_counts"])
        slot_counts = context["lineup_slot_counts"]
        self.roster_limit = sum(int(count) for slot, count in slot_counts.items() if slot != "IR")
        self.roster_sizes = {team_id: _roster_size(team["roster"]) for team_id, team in context["teams"].items()}
        self.baselines = {
            (team_id, week): _baseline(team["roster"], week, context, self.playing.get(week), self.groups)
            for team_id, team in context["teams"].items()
            for week in self.weeks
        }

    def check(self, our_team_id, counterparty_team_id, send, receive):
        """(keep, rank key) for one package. The rank key orders the shortlist: packages
        that fit both rosters first, then those not estimated to hurt the counterparty,
        then by our estimated gain and theirs. Estimates are each side's gain bound minus
        its loss bound; they only rank, never decide what is kept."""
        our_estimate = their_estimate = 0.0
        we_gain = they_can_hold = False
        for week in self.weeks:
            playing = self.playing.get(week)
            ours = self.baselines[(our_team_id, week)]
            theirs = self.baselines[(counterparty_team_id, week)]
            if UNSCORABLE in (ours, theirs):
                continue
            gain, reaches = _gain_bound(ours, receive, week, self.context, playing)
            we_gain |= reaches
            loss = _loss_bound(ours, send, receive, week, self.context, playing, self.groups)
            our_estimate += (gain if math.isfinite(gain) else 0.0) - loss
            gain, reaches = _gain_bound(theirs, send, week, self.context, playing)
            loss = _loss_bound(theirs, receive, send, week, self.context, playing, self.groups)
            they_can_hold |= reaches or loss <= _TOLERANCE
            their_estimate += (gain if math.isfinite(gain) else 0.0) - loss
        overflows = (
            self.roster_sizes[our_team_id] - len(send) + len(receive) > self.roster_limit
            or self.roster_sizes[counterparty_team_id] - len(receive) + len(send) > self.roster_limit
        )
        rank = (overflows, their_estimate < -_TOLERANCE, -our_estimate, -their_estimate)
        return we_gain and they_can_hold, rank


def _candidate_player(player, as_of_week):
    vorp = ((player.get("metrics") or {}).get(str(as_of_week)) or {}).get("vorp_score")
    return {
        "espn_id": player["espn_id"],
        "player_id": player.get("player_id"),
        "name": player.get("name"),
        "position": player.get("position"),
        "nfl_team": player.get("nfl_team"),
        "vorp": vorp,
        "nfl_bye_week": player.get("nfl_bye_week"),
    }


def _interleave_by_counterparty(ranked):
    """Round-robin over counterparties, each in rank order, so the cap leaves every team
    its best packages instead of filling up with one deep roster's."""
    by_team = {}
    for item in ranked:
        by_team.setdefault(item[2], []).append(item)
    queues = [by_team[team_id] for team_id in sorted(by_team, key=int)]
    return [item for group in itertools.zip_longest(*queues) for item in group if item is not None]


def build_candidates(context, our_team_id, teams_playing_by_week, vorp_by_player,
                     max_candidates=DEFAULT_MAX_CANDIDATES):
    """Pure over one trade-context snapshot: (scored candidate rows, counts)."""
    with lineup_solve_cache():
        return _build_candidates(context, our_team_id, teams_playing_by_week, vorp_by_player, max_candidates)


def _build_candidates(context, our_team_id, teams_playing_by_week, vorp_by_player, max_candidates):
    prefilter = Prefilter(context, teams_playing_by_week)
    enumerated, shortlist = 0, []
    for counterparty_team_id, send, receive in enumerate_packages(context, our_team_id):
        enumerated += 1
        keep, rank = prefilter.check(our_team_id, counterparty_team_id, send, receive)
        if not keep:
            continue
        send_ids = [p["espn_id"] for p in send]
        receive_ids = [p["espn_id"] for p in receive]
        key = candidate_key(
            context["league_id"], context["season"], our_team_id, counterparty_team_id,
            send_ids, receive_ids,
        )
        shortlist.append((rank, key, counterparty_team_id, send, receive))
    prefiltered = len(shortlist)
    shortlist = _interleave_by_counterparty(sorted(shortlist, key=lambda item: (item[0], item[1])))
    if len(shortlist) > max_candidates:
        logger.info(f"Shortlist capped at {max_candidates}; {len(shortlist) - max_candidates} prefiltered package(s) not scored")
    shortlist = shortlist[:max_candidates]

    teams = context["teams"]
    team_counts = {
        team_id: {"team_id": team_id, "name": team["name"], **{status: 0 for status in STATUSES}}
        for team_id, team in sorted(teams.items(), key=lambda item: int(item[0]))
        if team_id != our_team_id
    }
    rows = []
    for _, key, counterparty_team_id, send, receive in shortlist:
        send_ids = [p["espn_id"] for p in send]
        receive_ids = [p["espn_id"] for p in receive]
        raw_vorp = summarize_raw_vorp(
            vorp_by_player,
            [p["player_id"] for p in receive],
            [p["player_id"] for p in send],
        )
        score = shape_trade_score(
            context, our_team_id, counterparty_team_id, send_ids, receive_ids,
            raw_vorp, teams_playing_by_week,
        )
        reasons = eligibility_reasons(score)
        status = classify(reasons)
        team_counts[counterparty_team_id][status] += 1
        rows.append({
            "candidate_key": key,
            "counterparty_team_id": counterparty_team_id,
            "counterparty_name": teams[counterparty_team_id]["name"],
            "shape": shape_of(send, receive),
            "send": [_candidate_player(p, context["as_of_week"]) for p in send],
            "receive": [_candidate_player(p, context["as_of_week"]) for p in receive],
            "status": status,
            "reasons": reasons,
            "score": score,
        })
    counts = {
        "enumerated": enumerated,
        "prefiltered": prefiltered,
        "scored": len(rows),
        **{status: sum(1 for row in rows if row["status"] == status) for status in STATUSES},
    }
    return rows, {**counts, "teams": list(team_counts.values())}


def generate_trade_candidates(league_id, season, week, horizon, our_team_id,
                              max_candidates=DEFAULT_MAX_CANDIDATES):
    """Reads the trade context, the NFL schedule and as-of-week VORP once each, then
    builds the candidates. Trade-context errors propagate unchanged."""
    context = get_league_trade_context(league_id, season, week, horizon)
    if our_team_id not in context["teams"]:
        raise ValueError(f"Unknown ESPN team {our_team_id} in this league.")
    teams_playing = load_teams_playing_by_week(season, context["start_week"], context["end_week"])
    player_ids = sorted({
        p["player_id"]
        for team in context["teams"].values()
        for p in team["roster"]
        if p.get("player_id")
    })
    vorp_by_player = load_vorp_by_player(season, context["as_of_week"], player_ids)
    rows, counts = build_candidates(context, our_team_id, teams_playing, vorp_by_player, max_candidates)
    return context, rows, counts


def store_run(league_id, season, week, horizon, our_team_id, rows, counts, run_id=None):
    """Replaces this league/season/week's candidates with one run's, in one transaction."""
    run_id = run_id or str(uuid.uuid4())
    computed_at = datetime.now(timezone.utc)
    with get_db_cursor() as cursor:
        cursor.execute(
            """INSERT INTO trade_candidate_runs
                   (league_id, season, week, horizon, run_id, computed_at, our_team_id, counts)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (league_id, season, week) DO UPDATE SET
                   horizon = EXCLUDED.horizon, run_id = EXCLUDED.run_id,
                   computed_at = EXCLUDED.computed_at, our_team_id = EXCLUDED.our_team_id,
                   counts = EXCLUDED.counts;""",
            (league_id, season, week, horizon, run_id, computed_at, our_team_id, Json(counts)),
        )
        cursor.execute(
            "DELETE FROM trade_candidates WHERE league_id = %s AND season = %s AND week = %s;",
            (league_id, season, week),
        )
        if rows:
            cursor.executemany(
                """INSERT INTO trade_candidates
                       (league_id, season, week, horizon, run_id, computed_at, our_team_id,
                        candidate_key, counterparty_team_id, counterparty_name, shape,
                        send_espn_ids, receive_espn_ids, send_positions, receive_positions,
                        send_players, receive_players, status, reasons, final_score,
                        our_lineup_gain, our_weighted_gain, counterparty_lineup_change,
                        counterparty_weighted_benefit, our_schedule_adjustment,
                        scored_week_count, complete_horizon, score)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                [_row_params(league_id, season, week, horizon, run_id, computed_at, our_team_id, row)
                 for row in rows],
            )
    return run_id, computed_at


def _row_params(league_id, season, week, horizon, run_id, computed_at, our_team_id, row):
    terms = row["score"]["score"]
    return (
        league_id, season, week, horizon, run_id, computed_at, our_team_id,
        row["candidate_key"], row["counterparty_team_id"], row["counterparty_name"], row["shape"],
        [p["espn_id"] for p in row["send"]], [p["espn_id"] for p in row["receive"]],
        sorted({p["position"] for p in row["send"]}), sorted({p["position"] for p in row["receive"]}),
        Json(row["send"]), Json(row["receive"]), row["status"], row["reasons"],
        terms["final_score"], terms["our_lineup_gain"], terms["our_weighted_gain"],
        terms["counterparty_lineup_change"], terms["counterparty_weighted_benefit"],
        terms["our_schedule_adjustment"], len(row["score"]["horizon"]["scored_weeks"]),
        row["score"]["horizon"]["complete_horizon"], Json(row["score"]),
    )


_ITEM_COLUMNS = (
    "candidate_key, counterparty_team_id, counterparty_name, shape, send_players, "
    "receive_players, status, reasons, final_score, our_lineup_gain, our_weighted_gain, "
    "counterparty_lineup_change, counterparty_weighted_benefit, our_schedule_adjustment, "
    "score->'horizon', score->'bye_coverage'"
)


def list_trade_candidates(
    league_id: str,
    season: int,
    week: int,
    counterparty_team_id: str | None = None,
    receive_position: str | None = None,
    send_position: str | None = None,
    shape: str | None = None,
    status: str = "proposable",
    sort: str = "final_score",
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """The latest run's candidates for one league/season/week, filtered and paged.
    team_counts covers the whole run regardless of filters."""
    if status != "all" and status not in STATUSES:
        raise ValueError(f"Unknown candidate status {status!r}.")
    if shape is not None and shape not in SHAPES:
        raise ValueError(f"Unknown candidate shape {shape!r}.")
    if sort not in SORT_COLUMNS:
        raise ValueError(f"Unknown candidate sort {sort!r}.")
    where = ["league_id = %s", "season = %s", "week = %s"]
    params = [league_id, season, week]
    for clause, value in (
        ("counterparty_team_id = %s", counterparty_team_id),
        ("%s = ANY(receive_positions)", receive_position),
        ("%s = ANY(send_positions)", send_position),
        ("shape = %s", shape),
        ("status = %s", None if status == "all" else status),
    ):
        if value is not None:
            where.append(clause)
            params.append(value)
    where_sql = " AND ".join(where)
    with get_db_cursor() as cursor:
        cursor.execute(
            """SELECT run_id, computed_at, horizon, counts FROM trade_candidate_runs
               WHERE league_id = %s AND season = %s AND week = %s;""",
            (league_id, season, week),
        )
        run = cursor.fetchone()
        if run is None:
            raise TradeCandidatesNotFoundError(
                f"No trade candidates for league {league_id}, {season} week {week}: "
                "the trade_candidates job has not run for that week."
            )
        run_id, computed_at, horizon, counts = run
        cursor.execute(f"SELECT count(*) FROM trade_candidates WHERE {where_sql};", params)
        total = cursor.fetchone()[0]
        cursor.execute(
            f"""SELECT {_ITEM_COLUMNS} FROM trade_candidates WHERE {where_sql}
                ORDER BY {SORT_COLUMNS[sort]} DESC NULLS LAST, candidate_key
                LIMIT %s OFFSET %s;""",
            [*params, limit, offset],
        )
        items = [_item(row) for row in cursor.fetchall()]
    return {
        "league_id": league_id,
        "season": season,
        "week": week,
        "horizon": horizon,
        "run_id": run_id,
        "computed_at": computed_at,
        "total": total,
        "team_counts": (counts or {}).get("teams", []),
        "items": items,
    }


def _item(row):
    (key, counterparty_team_id, counterparty_name, shape, send, receive, status, reasons,
     final_score, gain, weighted_gain, change, benefit, adjustment, horizon, bye_coverage) = row
    return {
        "candidate_key": key,
        "counterparty": {"team_id": counterparty_team_id, "name": counterparty_name},
        "shape": shape,
        "receive": receive,
        "send": send,
        "final_score": final_score,
        "our_lineup_gain": gain,
        "our_weighted_gain": weighted_gain,
        "counterparty_lineup_change": change,
        "counterparty_weighted_benefit": benefit,
        "our_schedule_adjustment": adjustment,
        "scored_weeks": horizon["scored_weeks"],
        "horizon_weeks": list(range(horizon["start_week"], horizon["end_week"] + 1)),
        "complete_horizon": horizon["complete_horizon"],
        "bye_coverage": bye_coverage,
        "status": status,
        "reasons": reasons,
    }
