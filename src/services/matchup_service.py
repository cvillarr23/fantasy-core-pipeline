"""
Weekly head-to-head matchup view. Completed ingested weeks use historical ESPN scores
and lineups in Postgres; current or un-ingested weeks use the authenticated ESPN client.
"""
import json
import logging
import os

import requests

from src.database import get_db_cursor
from src.services.espn_ownership import load_espn_ownership
from src.ingestion.sync_league import build_espn_requests, build_league
from src.ir_eligibility import lineup_ir_hints, load_lineup_slot_counts_row, open_ir_slots
from src.nfl_byes import bye_week_for, bye_weeks_or_empty, to_nflverse_team
from src.nfl_game_status import game_status_for, load_team_game_status
from src.nfl_schedule import WeekResolutionError, require_upcoming_week
from src.optimization.lineup_solver import (
    InsufficientLineupInputError,
    evaluate_lineup,
    worker_slot,
)
from src.espn_scoring import score_breakdown
from src.services.live_service import load_rule_points_or_none, player_ids_by_espn_id
from src.services.read_cache import cached, league_key
from src.services.roster_service import compute_roster_deficits
from src.services.score_history import load_completed_week, stored_scoreboard

logger = logging.getLogger("MatchupService")

# Live scores move every play, so box-score reads stay short-lived. The roster changes only
# through moves, which invalidate the league version once ESPN confirms them.
BOX_SCORE_CACHE_TTL_SECONDS = 60
ROSTER_CACHE_TTL_SECONDS = 120

ESPN_REQUEST_TIMEOUT_SECONDS = 10


class ESPNTimeoutError(Exception):
    """An ESPN call exceeded its request timeout."""


def _fetch_box_scores(season: int, week: int):
    try:
        league = build_league(season, request_timeout_seconds=ESPN_REQUEST_TIMEOUT_SECONDS)
        return league.box_scores(week)
    except requests.Timeout as exc:
        raise ESPNTimeoutError("ESPN request timed out") from exc


def _win_probabilities_from_raw(raw: dict) -> dict:
    """team_id -> ESPN's win probability (0-1) for each side of the raw schedule that has one."""
    probabilities = {}
    for game in raw.get("schedule", []):
        for side in (game.get("home") or {}, game.get("away") or {}):
            if side.get("winProbability") is not None:
                probabilities[str(side["teamId"])] = side["winProbability"]
    return probabilities


def _fetch_win_probabilities(season: int, week: int) -> dict:
    """team_id -> win probability for the week's matchup period. espn_api's BoxScore drops the
    field, so this reads the raw payload. ESPN only fills it for the matchup in progress; a
    finished or future one has none. Display-only: any failure means unknown, not a failed page."""
    try:
        raw = build_espn_requests(season, request_timeout_seconds=ESPN_REQUEST_TIMEOUT_SECONDS).league_get(
            params={"view": ["mMatchupScore", "mScoreboard"], "scoringPeriodId": week},
            headers={"x-fantasy-filter": json.dumps({"schedule": {"filterMatchupPeriodIds": {"value": [week]}}})},
        )
        return _win_probabilities_from_raw(raw)
    except Exception as exc:
        logger.warning(f"No win probability for Season {season} Week {week}: could not read ESPN's matchup scores: {exc}")
        return {}


class NoMatchupFoundError(Exception):
    """No box score found for our team this week (bye week, offseason, out-of-range week)."""


BENCH_AND_IR_SLOTS = {"BE", "IR"}


def _headshot_url(espn_id: str) -> str:
    return f"https://a.espncdn.com/i/headshots/nfl/players/full/{espn_id}.png"


def _shape_player_line(box_player, bye_weeks: dict | None = None, game_statuses: dict | None = None) -> dict:
    espn_id = str(box_player.playerId)
    pro_team = getattr(box_player, "proTeam", None)
    return {
        "player_id": espn_id,
        "espn_id": espn_id,
        "name": box_player.name,
        "position": box_player.position,
        "jersey": box_player.jersey if isinstance(box_player.jersey, str) else None,
        "slot": box_player.slot_position,
        "pro_team": to_nflverse_team(pro_team),
        "points": box_player.points,
        "projected_points": box_player.projected_points,
        "bye_week": bye_week_for(pro_team, bye_weeks or {}),
        "game_status": game_status_for(pro_team, game_statuses),
        "headshot_url": _headshot_url(espn_id),
        "deficit_flag": False,
        "suggested_swap": None,
    }


def _apply_badges(player_lines: list, deficits_by_position: dict, swap_suggestions: dict) -> None:
    """Mutates player_lines in place, overlaying our-team-only automation signals."""
    for line in player_lines:
        if deficits_by_position.get(line["position"], {}).get("deficit", 0) > 0:
            line["deficit_flag"] = True
        if line["player_id"] in swap_suggestions:
            line["suggested_swap"] = swap_suggestions[line["player_id"]]


def _load_weekly_metrics_by_espn_id(season: int, week: int, team_id: str) -> dict:
    """espn_id -> current-week VORP/projection for every rostered player, not just the candidate
    pool (which excludes bye/OUT players via INNER JOIN weekly_stats) - the lineup editor
    shows every player on the roster, so a missing row here should mean "unknown", handled
    by the caller defaulting to None rather than this function guessing a fallback."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT p.espn_id, s.vorp_score, s.projected_points
            FROM league_rosters r
            JOIN players p ON r.player_id = p.player_id
            LEFT JOIN weekly_stats s ON s.player_id = r.player_id AND s.season = %s AND s.week = %s
            WHERE r.team_id = %s AND p.espn_id IS NOT NULL;
            """,
            (season, week, team_id),
        )
        return {
            espn_id: {"net_vorp": vorp, "projected_points": projected_points}
            for espn_id, vorp, projected_points in cursor.fetchall()
        }


def _load_espn_ownership_by_espn_id(season: int, week: int, team_id: str) -> dict:
    """espn_id -> EspnOwnership for every rostered player with a market-signal row."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT p.player_id, p.espn_id
            FROM league_rosters r
            JOIN players p ON r.player_id = p.player_id
            WHERE r.team_id = %s AND p.espn_id IS NOT NULL;
            """,
            (team_id,),
        )
        espn_id_by_player = dict(cursor.fetchall())
        ownership = load_espn_ownership(cursor, list(espn_id_by_player), season, week)
    return {espn_id_by_player[pid]: espn for pid, espn in ownership.items()}


def _apply_espn_ownership(player_lines: list, ownership_by_espn_id: dict) -> None:
    """Mutates player_lines in place; `espn` stays None for a player with no market-signal row."""
    for line in player_lines:
        line["espn"] = ownership_by_espn_id.get(line["espn_id"])


def _apply_weekly_metrics(player_lines: list, metrics_by_espn_id: dict) -> None:
    """Mutates player_lines in place. Leaves metrics as None (their shaped defaults) for any
    player missing a row - e.g. dropped/added outside our own sync, not yet ingested."""
    for line in player_lines:
        if line["espn_id"] in metrics_by_espn_id:
            line.update(metrics_by_espn_id[line["espn_id"]])


def _load_rumor_signal_by_espn_id(team_id: str) -> dict:
    """espn_id -> latest rumor_signals row for every rostered player. DISTINCT ON
    (player_id) ... ORDER BY player_id, id DESC picks the highest-id row per player -
    README.md's documented contract ("the latest row per player_id is the current
    signal"), with no confidence threshold applied here since that's an
    action-evaluation-only concern (RUMOR_ACTION_CONFIDENCE_THRESHOLD in
    rumor_action_service.py), not a display one."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT ON (rs.player_id)
                p.espn_id, rs.sentiment, rs.confidence, rs.suggested_action
            FROM rumor_signals rs
            JOIN players p ON p.player_id = rs.player_id
            JOIN league_rosters r ON r.player_id = rs.player_id
            WHERE r.team_id = %s AND p.espn_id IS NOT NULL
            ORDER BY rs.player_id, rs.id DESC;
            """,
            (team_id,),
        )
        return {
            espn_id: {"sentiment": sentiment, "confidence": confidence, "suggested_action": suggested_action}
            for espn_id, sentiment, confidence, suggested_action in cursor.fetchall()
        }


def _apply_rumor_signal(player_lines: list, signal_by_espn_id: dict) -> None:
    """Mutates player_lines in place. Leaves the rumor_* fields at their shaped None
    default for any player with no rumor_signals row."""
    for line in player_lines:
        signal = signal_by_espn_id.get(line["espn_id"])
        if signal is None:
            continue
        line["rumor_sentiment"] = signal["sentiment"]
        line["rumor_confidence"] = signal["confidence"]
        line["rumor_suggested_action"] = signal["suggested_action"]


def _core_player_ids(player_lines: list) -> dict:
    """espn_id -> players.player_id for these lines. Empty when the lookup fails: the id only
    lets a client make per-player reads, so a database failure must not fail the page."""
    try:
        return player_ids_by_espn_id([line["espn_id"] for line in player_lines])
    except Exception as exc:
        logger.warning(f"No core_player_id on player lines: could not look up players: {exc}")
        return {}


def _apply_core_player_ids(player_lines: list) -> None:
    """Mutates player_lines in place. `player_id` stays ESPN's, which lineup submissions use;
    the id that the summary, news and live reads take goes beside it."""
    player_ids = _core_player_ids(player_lines)
    for line in player_lines:
        line["core_player_id"] = player_ids.get(line["espn_id"])


def _with_core_player_ids(matchup: dict) -> dict:
    """Both sides' lines identified with one lookup, for live and stored weeks alike."""
    _apply_core_player_ids([
        line
        for side in ("our_team", "opponent")
        for group in ("starters", "bench")
        for line in matchup[side][group]
    ])
    return matchup


def _split_starters_bench(
    lineup: list, bye_weeks: dict | None = None, game_statuses: dict | None = None
) -> tuple[list, list]:
    starters, bench = [], []
    for box_player in lineup:
        line = _shape_player_line(box_player, bye_weeks, game_statuses)
        (bench if box_player.slot_position in BENCH_AND_IR_SLOTS else starters).append(line)
    return starters, bench


def _shape_lineup_player_line(player, bye_weeks: dict | None = None) -> dict:
    espn_id = str(player.playerId)
    return {
        "player_id": espn_id,
        "espn_id": espn_id,
        "name": player.name,
        "position": player.position,
        "jersey": player.jersey if isinstance(player.jersey, str) else None,
        "slot": player.lineupSlot,
        "pro_team": player.proTeam,
        "injury_status": player.injuryStatus,
        "bye_week": bye_week_for(player.proTeam, bye_weeks or {}),
        "headshot_url": _headshot_url(espn_id),
        "deficit_flag": False,
        "suggested_swap": None,
        "net_vorp": None,
        "projected_points": None,
        "rumor_sentiment": None,
        "rumor_confidence": None,
        "rumor_suggested_action": None,
        "ir_suggestion": None,
    }


def _split_roster_starters_bench(players: list, bye_weeks: dict | None = None) -> tuple[list, list]:
    starters, bench = [], []
    for player in players:
        line = _shape_lineup_player_line(player, bye_weeks)
        (bench if player.lineupSlot in BENCH_AND_IR_SLOTS else starters).append(line)
    return starters, bench


def _load_lineup_slot_counts(season: int) -> dict | None:
    """Display-only, like the swap badges: a DB failure or an unsynced league means no slot
    capacity and no IR hints, not a failed page."""
    try:
        with get_db_cursor() as cursor:
            counts = load_lineup_slot_counts_row(cursor, season)
    except Exception as exc:
        logger.warning(f"No slot capacity or IR hints for Season {season}: could not read lineup slot counts: {exc}")
        return None
    if counts is None:
        return None
    return {slot: int(count or 0) for slot, count in counts.items()}


def _apply_ir_hints(player_lines: list, ir_slot_count: int) -> bool:
    """Overlays move_to_ir / activate_from_ir from ESPN's live injuryStatus and slot.
    Returns whether an active player in IR is blocking new ESPN transactions."""
    hints, blocking = lineup_ir_hints(player_lines, ir_slot_count)
    for line in player_lines:
        line["ir_suggestion"] = hints.get(line["player_id"])
    return blocking


def suggest_lineup_swaps(team_id: str, season: int, week: int) -> dict:
    """Read-only: reuses lineup_solver's pregame evaluation to see what the optimizer
    would start, but never publishes - this is a display-only badge, not an action.
    Missing lineup inputs mean no badges (logged), not a failed matchup page. Returns
    {drop_espn_id: {"bench_for": add_player_name, "add_player_id": add_espn_id,
    "slot": slot_the_add_ends_in, "moves": [{"player_id", "name", "from_slot", "to_slot"}]}}.
    The solver works on internal players.player_id, but matchup and roster lines carry ESPN's
    id, so every id here is ESPN's and never falls back to an internal one. A chain swap's
    displaced starters are in `moves` (empty for a plain swap); a swap with any player
    lacking an ESPN id, name or current slot is skipped whole, since half a chain misleads."""
    try:
        with get_db_cursor() as cursor:
            evaluation = evaluate_lineup(cursor, team_id, season, week)
            espn_ids = _espn_ids_by_player_id(cursor, _swap_player_ids(evaluation["swaps"]))
    except InsufficientLineupInputError as exc:
        logger.warning(f"No lineup swap badges for Team {team_id} Season {season} Week {week}: {exc}")
        return {}
    except Exception as exc:
        logger.warning(f"No lineup swap badges for Team {team_id} Season {season} Week {week}: could not look up players: {exc}")
        return {}

    names_by_id = {p["player_id"]: p["name"] for p in evaluation["roster_pool"]}
    current_starters = evaluation.get("current_starters", {})
    suggestions = {}
    for swap in evaluation["swaps"]:
        drop_espn_id = espn_ids.get(swap["drop_player_id"])
        add_espn_id = espn_ids.get(swap["add_player_id"])
        if drop_espn_id is None or add_espn_id is None:
            logger.warning(f"Skipping swap badge for player {swap['drop_player_id']} -> {swap['add_player_id']}: no ESPN id")
            continue
        moves = []
        for move in swap.get("moves", []):
            pid = move["player_id"]
            if pid not in espn_ids or pid not in names_by_id or pid not in current_starters:
                moves = None
                break
            moves.append({
                "player_id": espn_ids[pid],
                "name": names_by_id[pid],
                "from_slot": worker_slot(current_starters[pid]["slot_position"]),
                "to_slot": move["slot"],
            })
        if moves is None:
            logger.warning(f"Skipping swap badge for player {swap['drop_player_id']} -> {swap['add_player_id']}: a displaced starter has no ESPN id, name or slot")
            continue
        suggestions[drop_espn_id] = {
            "bench_for": names_by_id[swap["add_player_id"]],
            "add_player_id": add_espn_id,
            "slot": swap["slot"],
            "moves": moves,
        }
    return suggestions


def _swap_player_ids(swaps: list) -> list:
    return sorted({
        pid for swap in swaps
        for pid in (swap["drop_player_id"], swap["add_player_id"], *(m["player_id"] for m in swap.get("moves", [])))
    })


def _espn_ids_by_player_id(cursor, player_ids: list) -> dict:
    """players.player_id -> ESPN id (as a string, like the lines carry); players without one are absent."""
    if not player_ids:
        return {}
    cursor.execute(
        "SELECT player_id, espn_id FROM players WHERE player_id = ANY(%s) AND espn_id IS NOT NULL;",
        (player_ids,),
    )
    return {player_id: str(espn_id) for player_id, espn_id in cursor.fetchall()}


def _shape_team_side(
    team, lineup, score, projected, bye_weeks: dict | None = None, game_statuses: dict | None = None,
    win_probability: float | None = None,
) -> dict:
    starters, bench = _split_starters_bench(lineup, bye_weeks, game_statuses)
    return {
        "team": {
            "name": team.team_name,
            "logo_url": team.logo_url,
            "score": score,
            "projected": projected,
            "win_probability": win_probability,
        },
        "starters": starters,
        "bench": bench,
    }


STORED_STAT_KEYS = ("applied_stats", "applied_qty")


def _stored_rule_points(sides: list) -> dict | None:
    """The league's rule index, read only when some stored line kept ESPN's per-stat points:
    a week stored before they were kept has nothing to itemize."""
    lines = [line for side in sides for group in ("starters", "bench") for line in side[group]]
    return load_rule_points_or_none() if any("applied_stats" in line for line in lines) else None


def _stored_breakdown(line: dict, rules: dict | None) -> list | None:
    """The completed week's breakdown from the stats `sync_actual_scores` stored. ESPN's actual
    points stand in for live points, so the lines still sum to ESPN's score. None for a line
    stored without per-stat points, or when the rules can't be read."""
    if rules is None or "applied_stats" not in line:
        return None
    return score_breakdown({**line, "live_points": line.get("actual_points")}, rules)


def _stored_line(line: dict, rules: dict | None) -> dict:
    public = {key: value for key, value in line.items() if key not in STORED_STAT_KEYS}
    return {**public, "breakdown": _stored_breakdown(line, rules)}


def _stored_side(side: dict, rules: dict | None) -> dict:
    return {
        "team": side["team"],
        "starters": [_stored_line(line, rules) for line in side["starters"]],
        "bench": [_stored_line(line, rules) for line in side["bench"]],
    }


def get_current_team_matchup(season: int, week: int) -> dict:
    our_team_id = os.getenv("ESPN_TEAM_ID")
    return cached(
        ["matchup", *league_key(season), our_team_id, week],
        BOX_SCORE_CACHE_TTL_SECONDS,
        lambda: _load_current_team_matchup(season, week, our_team_id),
    )


def _load_current_team_matchup(season: int, week: int, our_team_id: str) -> dict:
    stored = load_completed_week(season, week)
    if stored:
        by_id = {side["team_id"]: side for side in stored}
        ours = by_id.get(our_team_id)
        opponent = by_id.get(ours["opponent_team_id"]) if ours else None
        if ours is None or opponent is None:
            raise NoMatchupFoundError(f"No opponent for team {our_team_id} in week {week}.")

        rules = _stored_rule_points([ours, opponent])
        return _with_core_player_ids({
            "season": season, "week": week,
            "our_team": _stored_side(ours, rules), "opponent": _stored_side(opponent, rules),
        })
    box_scores = _fetch_box_scores(season, week)

    matched_box = None
    we_are_home = False
    for box in box_scores:
        if box.home_team is not None and str(box.home_team.team_id) == our_team_id:
            matched_box, we_are_home = box, True
            break
        if box.away_team is not None and str(box.away_team.team_id) == our_team_id:
            matched_box, we_are_home = box, False
            break

    if matched_box is None:
        raise NoMatchupFoundError(f"No matchup found for team {our_team_id} in week {week}.")

    our_team = matched_box.home_team if we_are_home else matched_box.away_team
    opponent_team = matched_box.away_team if we_are_home else matched_box.home_team
    if opponent_team is None:
        raise NoMatchupFoundError(f"Team {our_team_id} has no opponent in week {week} (bye week).")

    our_lineup = matched_box.home_lineup if we_are_home else matched_box.away_lineup
    opp_lineup = matched_box.away_lineup if we_are_home else matched_box.home_lineup
    our_score = matched_box.home_score if we_are_home else matched_box.away_score
    opp_score = matched_box.away_score if we_are_home else matched_box.home_score
    our_projected = matched_box.home_projected if we_are_home else matched_box.away_projected
    opp_projected = matched_box.away_projected if we_are_home else matched_box.home_projected

    bye_weeks = bye_weeks_or_empty(season)
    game_statuses = load_team_game_status(season, week)
    win_probabilities = _fetch_win_probabilities(season, week)
    our_side = _shape_team_side(
        our_team, our_lineup, our_score, our_projected, bye_weeks, game_statuses,
        win_probabilities.get(str(our_team.team_id)),
    )
    opponent_side = _shape_team_side(
        opponent_team, opp_lineup, opp_score, opp_projected, bye_weeks, game_statuses,
        win_probabilities.get(str(opponent_team.team_id)),
    )

    deficits = compute_roster_deficits(season, week)
    deficits_by_position = deficits["teams"].get(our_team_id, {})
    swap_suggestions = suggest_lineup_swaps(our_team_id, season, week)
    _apply_badges(our_side["starters"], deficits_by_position, swap_suggestions)

    return _with_core_player_ids(
        {"season": season, "week": week, "our_team": our_side, "opponent": opponent_side}
    )


def _shape_scoreboard_team(team, score: float, projected: float, win_probability: float | None = None) -> dict | None:
    if team is None:
        return None
    return {
        "team_id": str(team.team_id),
        "name": team.team_name,
        "logo_url": team.logo_url,
        "score": score,
        "projected": projected,
        "win_probability": win_probability,
    }


def get_league_scoreboard(season: int, week: int) -> dict:
    """Every matchup in the league for a week, score-only (no rosters) - mirrors ESPN's
    own Scoreboard tab. Unlike get_current_team_matchup, doesn't filter box_scores() down
    to our team's matchup."""
    return cached(
        ["scoreboard", *league_key(season), week],
        BOX_SCORE_CACHE_TTL_SECONDS,
        lambda: _load_league_scoreboard(season, week),
    )


def _shape_scoreboard_matchup(box, win_probabilities: dict) -> dict:
    def chance(team):
        return win_probabilities.get(str(team.team_id)) if team is not None else None

    return {
        "home": _shape_scoreboard_team(box.home_team, box.home_score, box.home_projected, chance(box.home_team)),
        "away": _shape_scoreboard_team(box.away_team, box.away_score, box.away_projected, chance(box.away_team)),
    }


def _load_league_scoreboard(season: int, week: int) -> dict:
    stored = load_completed_week(season, week)
    if stored:
        return stored_scoreboard(stored, season, week)
    box_scores = _fetch_box_scores(season, week)

    win_probabilities = _fetch_win_probabilities(season, week)
    matchups = [_shape_scoreboard_matchup(box, win_probabilities) for box in box_scores]
    return {"season": season, "week": week, "matchups": matchups}


def get_current_team_roster(season: int, selected_week: int | None = None) -> dict:
    """Bye-week-safe roster view: sourced from league.teams/Team.roster, not box_scores(),
    so it works even when no opponent is scheduled (unlike get_current_team_matchup)."""
    our_team_id = os.getenv("ESPN_TEAM_ID")
    # Resolve the default week before keying, so "current week" and an explicit week share
    # one entry and a week rollover never serves the previous week's badges.
    # One attempt: a request shouldn't wait out the launcher job's retry backoff, and the route turns a
    # failure into a 503 instead of showing another week's roster.
    week = selected_week if selected_week is not None else require_upcoming_week(season, attempts=1)
    return cached(
        ["lineup-roster", *league_key(season), our_team_id, week],
        ROSTER_CACHE_TTL_SECONDS,
        lambda: _load_current_team_roster(season, week, our_team_id),
    )


def _load_current_team_roster(season: int, week: int, our_team_id: str) -> dict:
    league = build_league(season)
    our_team = next((t for t in league.teams if str(t.team_id) == our_team_id), None)
    if our_team is None:
        raise ValueError(f"Team {our_team_id} not found in league for season {season}.")

    starters, bench = _split_roster_starters_bench(our_team.roster, bye_weeks_or_empty(season))

    deficits = compute_roster_deficits(season, week)
    deficits_by_position = deficits["teams"].get(our_team_id, {})
    swap_suggestions = suggest_lineup_swaps(our_team_id, season, week)
    _apply_badges(starters, deficits_by_position, swap_suggestions)

    metrics_by_espn_id = _load_weekly_metrics_by_espn_id(season, week, our_team_id)
    _apply_weekly_metrics(starters, metrics_by_espn_id)
    _apply_weekly_metrics(bench, metrics_by_espn_id)

    ownership_by_espn_id = _load_espn_ownership_by_espn_id(season, week, our_team_id)
    _apply_espn_ownership(starters, ownership_by_espn_id)
    _apply_espn_ownership(bench, ownership_by_espn_id)

    rumor_signal_by_espn_id = _load_rumor_signal_by_espn_id(our_team_id)
    _apply_rumor_signal(starters, rumor_signal_by_espn_id)
    _apply_rumor_signal(bench, rumor_signal_by_espn_id)

    _apply_core_player_ids(starters + bench)

    slot_counts = _load_lineup_slot_counts(season)
    ir_blocking = _apply_ir_hints(starters + bench, open_ir_slots(slot_counts or {}, 0))

    return {
        "starters": starters,
        "bench": bench,
        "ir_blocking_transactions": ir_blocking,
        "lineup_slot_counts": slot_counts,
    }
