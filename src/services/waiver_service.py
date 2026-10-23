"""
Waiver-recommendation read endpoint backing the REST controller in src/api/waiver.py.

Reuses src/backtesting/waiver_strategy.py's pure decision functions unchanged - the same
ones season_replay.py backtests and waiver_evaluator.py publishes live from - rather than
re-deriving the VORP-delta math. margin=0.0 here, not waiver_strategy.MARGIN_THRESHOLD:
that threshold gates live auto-publish, not what's worth showing a manager to decide on.

Before the target week's projections land (draft-prep-refresher, Tuesday 05:00 CT) the
exact-week pools are empty, so the page falls back to prior same-season weeks, the same
source rules as lineup_solver.load_pregame_roster_pool (#185).

While an IR slot is open, a recommendation that would drop an IR-eligible player (ESPN's
status, src/ir_eligibility) becomes kind "ir_and_add": stash that player on IR and add the
free agent with no drop. Display-only; nothing here publishes.

Once the target week's projections exist, players are valued over a horizon (default 4
weeks) by waiver_strategy.build_horizon_pools: average VORP per week, a bye week counting
as replacement level. A stud on bye this week is therefore never the drop, and a free
agent on bye this week is still a candidate. Every recommendation then carries its
week-by-week effect on our best lineup, and waiver_bye_fill adds fill-ins for weeks where
byes leave our lineup short (bye_fill).
"""
import logging
import os

from src.backtesting import waiver_strategy
from src.database import get_db_cursor
from src.ingestion.metrics_extractor import normalize_team
from src.ingestion.sync_league import refresh_rosters_if_stale
from src.ir_eligibility import load_open_ir_slots
from src.nfl_byes import bye_week_for, bye_weeks_or_empty, teams_playing_from_byes
from src.optimization.lineup_solver import (
    METRIC_SOURCE,
    PREGAME_WINDOW_WEEKS,
    load_lineup_slot_counts,
    load_teams_playing,
)
from src.optimization.waiver_bye_fill import lineup_gains_by_week, plan_bye_fill_moves

logger = logging.getLogger("WaiverService")


class WaiverInputsUnavailableError(Exception):
    """Neither the target week nor the prior-week fallback has usable data - distinct from
    an empty recommendation list, which means no free agent beats a bench player."""


def load_prior_week_waiver_pools(cursor, team_id: str, season: int, week: int,
                                 window: int = PREGAME_WINDOW_WEEKS,
                                 add_candidates: list | None = None, allow_empty_bench: bool = False) -> tuple:
    """(bench_by_position, free_agents_by_position, source) from each player's mean
    vorp_score over the up-to-`window` prior weeks of the same season, in
    waiver_strategy's shapes. An optional add_candidates list receives every usable free
    agent, including multiple players at one position. An open roster slot permits an
    empty bench. Our bench and unrostered players only; players whose team
    is on bye in `week` are dropped. Week 1 never reads the prior season. Raises
    WaiverInputsUnavailableError when there's no usable or only stale data."""
    if week <= 1:
        raise WaiverInputsUnavailableError(
            f"no usable {METRIC_SOURCE} for Season {season} Week 1: no pregame projections yet, "
            "and prior seasons are deliberately not used"
        )
    first_week = max(1, week - window)
    cursor.execute(
        """
        SELECT p.player_id, p.position, p.team, s.week, s.vorp_score,
               (r.player_id IS NOT NULL) AS on_our_bench,
               CASE WHEN r.player_id IS NULL THEN p.injury_status
                    ELSE r.espn_injury_status END AS injury_status
        FROM weekly_stats s
        JOIN players p ON p.player_id = s.player_id
        LEFT JOIN league_rosters r ON r.player_id = p.player_id
        WHERE s.season = %s AND s.week BETWEEN %s AND %s AND s.vorp_score IS NOT NULL
          AND (r.player_id IS NULL OR (r.team_id = %s AND (r.is_starter IS NULL OR r.is_starter = FALSE)
                                       AND r.slot_position <> 'IR'));
        """,
        (season, first_week, week - 1, team_id),
    )
    rows = cursor.fetchall()
    if not rows:
        raise WaiverInputsUnavailableError(
            f"no usable {METRIC_SOURCE} for Season {season} Week {week} or Weeks {first_week}-{week - 1}"
        )
    weeks = sorted({row[3] for row in rows})
    if weeks[-1] < week - 1:
        raise WaiverInputsUnavailableError(
            f"stale {METRIC_SOURCE}: newest usable week is Season {season} Week {weeks[-1]}, "
            f"expected Week {week - 1} before evaluating Week {week}"
        )
    try:
        teams_playing = {normalize_team(t) for t in load_teams_playing(season, week)}
    except Exception as exc:
        raise WaiverInputsUnavailableError(
            f"could not load the nflverse schedule for Season {season} Week {week} "
            f"to exclude bye-week players: {exc}"
        ) from exc

    players: dict = {}
    for player_id, position, team, _, vorp_score, on_our_bench, injury_status in rows:
        entry = players.setdefault(player_id, {
            "position": position, "team": team, "bench": on_our_bench,
            "injury_status": injury_status, "vorp": [],
        })
        entry["vorp"].append(vorp_score)

    bench_by_position: dict = {}
    free_agents_by_position: dict = {}
    for player_id, entry in players.items():
        if normalize_team(entry["team"]) not in teams_playing:
            continue
        candidate = {"player_id": player_id, "vorp_score": sum(entry["vorp"]) / len(entry["vorp"])}
        if entry["bench"]:
            bench_by_position.setdefault(entry["position"], []).append(
                {**candidate, "espn_injury_status": entry["injury_status"]}
            )
        else:
            if add_candidates is not None:
                add_candidates.append({"position": entry["position"], **candidate,
                                       "injury_status": entry["injury_status"]})
            best = free_agents_by_position.get(entry["position"])
            if best is None or candidate["vorp_score"] > best["vorp_score"]:
                free_agents_by_position[entry["position"]] = candidate

    missing = [
        label for label, pool in (("bench", bench_by_position), ("free-agent", free_agents_by_position))
        if not pool and (label != "bench" or not allow_empty_bench)
    ]
    if missing:
        raise WaiverInputsUnavailableError(
            f"no {' or '.join(missing)} players with usable {METRIC_SOURCE} in Season {season} "
            f"Weeks {weeks} after excluding Week {week} byes"
        )
    return bench_by_position, free_agents_by_position, {"source_mode": "rolling_prior_weeks", "source_weeks": weeks}


def _load_player_display(cursor, player_ids: set, bye_weeks: dict | None = None) -> dict:
    """player_id -> {espn_id, name, position, pro_team, injury_status, bye_week}, one batched query."""
    if not player_ids:
        return {}
    cursor.execute(
        """
        SELECT player_id, espn_id, name, position, team, injury_status
        FROM players
        WHERE player_id = ANY(%s);
        """,
        (list(player_ids),),
    )
    return {
        player_id: {"espn_id": espn_id, "name": name, "position": position, "pro_team": team,
                    "injury_status": injury_status,
                    "bye_week": bye_week_for(team, bye_weeks or {})}
        for player_id, espn_id, name, position, team, injury_status in cursor.fetchall()
    }


def _shape_recommendation(move: dict, display_by_player_id: dict) -> dict:
    add_display = display_by_player_id[move["add_player_id"]]
    if move.get("kind") == "add":
        vorp_delta = move["add_vorp_score"]
        bench_player = None
        ir_player = None
    elif move.get("kind") == "ir_and_add":
        # Nobody leaves the roster, so the gain is the add's whole value.
        vorp_delta = move["add_vorp_score"]
        bench_player = None
        ir_player = {**display_by_player_id[move["ir_player_id"]], "net_vorp": move["ir_vorp_score"]}
    else:
        vorp_delta = move["add_vorp_score"] - move["drop_vorp_score"]
        bench_player = {**display_by_player_id[move["drop_player_id"]], "net_vorp": move["drop_vorp_score"]}
        ir_player = None
    gains = move.get("weekly_lineup_gains")
    known = [g["delta"] for g in gains or [] if g["delta"] is not None]
    return {
        "kind": move.get("kind", "swap"),
        "position": move["position"],
        "vorp_delta": vorp_delta,
        "free_agent": {**add_display, "net_vorp": move["add_vorp_score"]},
        "bench_player": bench_player,
        "ir_player": ir_player,
        # Change to our best starting lineup's VORP in each horizon week (None: unscorable).
        "weekly_lineup_gains": gains,
        "horizon_lineup_gain": sum(known) if known else None,
        "bye_fill": move.get("bye_fill"),
        # Placeholders until a future ticket wires the rumor-signal feed into this
        # endpoint, mirroring how it was added to GET /v1/matchup/roster.
        "rumor_sentiment": None,
        "rumor_confidence": None,
        "rumor_suggested_action": None,
    }


def _attach_lineup_gains(moves: list, pools: dict, week: int, weeks: list, slot_counts: dict,
                         playing_by_week: dict) -> None:
    roster_by_id = {p["player_id"]: p for p in pools["roster"]}
    for move in moves:
        add = pools["free_agents"].get(move["add_player_id"])
        drop_id = move.get("drop_player_id")
        drop = roster_by_id.get(drop_id) if drop_id else None
        if add is None or (drop_id and drop is None):
            continue
        move["weekly_lineup_gains"] = lineup_gains_by_week(
            pools["roster"], add, drop, week, weeks, slot_counts, playing_by_week,
        )


def rostered_team_for_espn_id(espn_id: str):
    """team_id currently holding this ESPN player in league_rosters, or None."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT r.team_id FROM league_rosters r
            JOIN players p ON p.player_id = r.player_id
            WHERE p.espn_id = %s LIMIT 1;
            """,
            (str(espn_id),),
        )
        row = cursor.fetchone()
    return str(row[0]) if row else None


def compute_waiver_recommendations(season: int, week: int,
                                   horizon: int = waiver_strategy.HORIZON_WEEKS) -> dict:
    our_team_id = os.getenv("ESPN_TEAM_ID")
    # The free-agent pool is "not in league_rosters"; a week-old snapshot suggests
    # players other managers have since picked up.
    refresh_rosters_if_stale(season)
    weeks = waiver_strategy.horizon_weeks(week, horizon)
    bye_weeks = bye_weeks_or_empty(season)
    pools = None
    with get_db_cursor() as cursor:
        open_roster_slots = waiver_strategy.load_open_roster_slots(cursor, our_team_id, season)
        add_candidates = []
        if len(weeks) > 1:
            pools = waiver_strategy.load_horizon_waiver_pools(
                cursor, our_team_id, season, week, weeks, bye_weeks,
            )
            bench_by_position = pools["bench_by_position"]
            free_agents_by_position = pools["free_agents_by_position"]
        else:
            bench_by_position = waiver_strategy.load_bench_by_position(cursor, our_team_id, season, week)
            free_agents_by_position = waiver_strategy.load_best_free_agent_by_position(cursor, season, week)
        if free_agents_by_position and (bench_by_position or open_roster_slots > 0):
            if open_roster_slots > 0:
                add_candidates = (
                    pools["add_candidates"] if pools is not None
                    else waiver_strategy.load_add_candidates(cursor, season, week)
                )
            source = {"source_mode": "target_week", "source_weeks": [week]}
        else:
            pools = None
            bench_by_position, free_agents_by_position, source = load_prior_week_waiver_pools(
                cursor, our_team_id, season, week,
                add_candidates=add_candidates, allow_empty_bench=open_roster_slots > 0,
            )
            logger.info(
                f"No Season {season} Week {week} rows yet; waiver recommendations use "
                f"{METRIC_SOURCE} means from Weeks {source['source_weeks']}."
            )
        adds = []
        if open_roster_slots > 0:
            adds, free_agents_by_position = waiver_strategy.plan_add_only_moves(
                add_candidates, open_roster_slots,
                rostered_positions=waiver_strategy.load_rostered_positions(cursor, our_team_id),
            )
        open_ir_slots = load_open_ir_slots(cursor, our_team_id, season)
        swaps, ir_and_adds = waiver_strategy.plan_waiver_moves(
            bench_by_position, free_agents_by_position, open_ir_slots, margin=0.0,
        )
        # Bench only: an IR-eligible starter is the lineup page's move_to_ir hint.
        stash = waiver_strategy.ir_stash_candidates(bench_by_position, open_ir_slots)
        moves = adds + swaps + ir_and_adds

        bye_fills, crunch_weeks = [], []
        if pools is not None:
            slot_counts = load_lineup_slot_counts(cursor, season)
            playing_by_week = {w: teams_playing_from_byes(bye_weeks, w) for w in weeks}
            _attach_lineup_gains(moves, pools, week, weeks, slot_counts, playing_by_week)
            claimed = {pid for m in moves for pid in (m["add_player_id"], m.get("drop_player_id")) if pid}
            bye_fills, outlook = plan_bye_fill_moves(
                pools["roster"], pools["free_agents"], pools["value_by_player"], week, weeks,
                slot_counts, playing_by_week, open_roster_slots - len(adds), claimed,
            )
            crunch_weeks = outlook["crunch_weeks"]

        player_ids = (
            {m["add_player_id"] for m in moves + bye_fills}
            | {m["drop_player_id"] for m in swaps + bye_fills if m["drop_player_id"]}
            | {m["ir_player_id"] for m in ir_and_adds}
            | {p["player_id"] for p in stash}
        )
        display_by_player_id = _load_player_display(cursor, player_ids, bye_weeks)

    recommendations = [_shape_recommendation(m, display_by_player_id) for m in moves]
    recommendations.sort(key=lambda r: (r["kind"] != "add", -r["vorp_delta"]))
    # Fill-ins come after the season-value moves: they patch one week, so they are
    # listed (recommend-only) but never outrank a move that's better over the horizon.
    recommendations += sorted(
        (_shape_recommendation(m, display_by_player_id) for m in bye_fills),
        key=lambda r: -r["horizon_lineup_gain"],
    )
    ir_moves = [
        {
            "action": "move_to_ir",
            "position": p["position"],
            "player": {**display_by_player_id[p["player_id"]], "net_vorp": p["vorp_score"]},
        }
        for p in stash
    ]
    return {
        "season": season, "week": week, "recommendations": recommendations,
        "open_roster_slots": open_roster_slots, "open_ir_slots": open_ir_slots, "ir_moves": ir_moves, **source,
        # Weeks each vorp value averages over; just [week] when only one week is valued.
        "horizon_weeks": weeks if pools is not None else [week],
        "bye_crunch_weeks": crunch_weeks,
    }


def waiver_claim_already_recorded(
    team_id: str, season, week, add_player_id: str, drop_player_id=None,
) -> bool:
    """True when bot_decisions already holds this team's claim for the same add/drop in the
    same season/week - the durable backstop for /trigger/waiver's idempotency when Redis
    doesn't know the key. Needs season/week to scope to one waiver week; without them
    there's nothing safe to match on. Fails open (False): a DB blip must not block a claim."""
    if season is None or week is None:
        return False
    try:
        with get_db_cursor() as cursor:
            cursor.execute(
                """
                SELECT 1 FROM bot_decisions
                WHERE bot_name = 'waiver_sniper' AND action = 'waiver_claim'
                  AND team_id = %s AND season = %s AND week = %s
                  AND target_player_id = %s AND drop_player_id IS NOT DISTINCT FROM %s
                LIMIT 1;
                """,
                (team_id, season, week, add_player_id, drop_player_id),
            )
            return cursor.fetchone() is not None
    except Exception as e:
        logger.warning(f"Could not check bot_decisions for a prior waiver claim; proceeding: {e}")
        return False
