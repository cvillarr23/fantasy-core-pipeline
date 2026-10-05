"""
Weekly head-to-head matchup view (fantasy-web-ui's Matchup page). Unlike roster_service's
DB-backed deficits, this fetches live from ESPN via the same authenticated League client
sync_league.py already builds - weekly_stats.actual_points is never populated by any live
job (only the backtesting harness writes it), so real per-player scoring only exists on
ESPN's side.
"""
import logging
import os

import requests

from src.database import get_db_cursor
from src.ingestion.sync_league import build_league
from src.optimization.lineup_solver import (
    InsufficientLineupInputError,
    evaluate_lineup,
    resolve_default_week,
)
from src.services.read_cache import cached, league_key
from src.services.roster_service import compute_roster_deficits

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


class NoMatchupFoundError(Exception):
    """No box score found for our team this week (bye week, offseason, out-of-range week)."""


BENCH_AND_IR_SLOTS = {"BE", "IR"}


def _headshot_url(espn_id: str) -> str:
    return f"https://a.espncdn.com/i/headshots/nfl/players/full/{espn_id}.png"


def _shape_player_line(box_player) -> dict:
    espn_id = str(box_player.playerId)
    return {
        "player_id": espn_id,
        "espn_id": espn_id,
        "name": box_player.name,
        "position": box_player.position,
        "jersey": box_player.jersey if isinstance(box_player.jersey, str) else None,
        "slot": box_player.slot_position,
        "points": box_player.points,
        "projected_points": box_player.projected_points,
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


def _split_starters_bench(lineup: list) -> tuple[list, list]:
    starters, bench = [], []
    for box_player in lineup:
        line = _shape_player_line(box_player)
        (bench if box_player.slot_position in BENCH_AND_IR_SLOTS else starters).append(line)
    return starters, bench


def _shape_lineup_player_line(player) -> dict:
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
        "headshot_url": _headshot_url(espn_id),
        "deficit_flag": False,
        "suggested_swap": None,
        "net_vorp": None,
        "projected_points": None,
        "rumor_sentiment": None,
        "rumor_confidence": None,
        "rumor_suggested_action": None,
    }


def _split_roster_starters_bench(players: list) -> tuple[list, list]:
    starters, bench = [], []
    for player in players:
        line = _shape_lineup_player_line(player)
        (bench if player.lineupSlot in BENCH_AND_IR_SLOTS else starters).append(line)
    return starters, bench


def suggest_lineup_swaps(team_id: str, season: int, week: int) -> dict:
    """Read-only: reuses lineup_solver's pregame evaluation to see what the optimizer
    would start, but never publishes - this is a display-only badge, not an action.
    Missing lineup inputs mean no badges (logged), not a failed matchup page. Returns
    {drop_player_id: {"bench_for": add_player_name, "add_player_id": ...}}."""
    try:
        with get_db_cursor() as cursor:
            evaluation = evaluate_lineup(cursor, team_id, season, week)
    except InsufficientLineupInputError as exc:
        logger.warning(f"No lineup swap badges for Team {team_id} Season {season} Week {week}: {exc}")
        return {}

    names_by_id = {p["player_id"]: p["name"] for p in evaluation["roster_pool"]}
    return {
        swap["drop_player_id"]: {
            "bench_for": names_by_id[swap["add_player_id"]],
            "add_player_id": swap["add_player_id"],
        }
        for swap in evaluation["swaps"]
    }


def _shape_team_side(team, lineup, score, projected) -> dict:
    starters, bench = _split_starters_bench(lineup)
    return {
        "team": {
            "name": team.team_name,
            "logo_url": team.logo_url,
            "score": score,
            "projected": projected,
        },
        "starters": starters,
        "bench": bench,
    }


def get_current_team_matchup(season: int, week: int) -> dict:
    our_team_id = os.getenv("ESPN_TEAM_ID")
    return cached(
        ["matchup", *league_key(season), our_team_id, week],
        BOX_SCORE_CACHE_TTL_SECONDS,
        lambda: _load_current_team_matchup(season, week, our_team_id),
    )


def _load_current_team_matchup(season: int, week: int, our_team_id: str) -> dict:
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

    our_side = _shape_team_side(our_team, our_lineup, our_score, our_projected)
    opponent_side = _shape_team_side(opponent_team, opp_lineup, opp_score, opp_projected)

    deficits = compute_roster_deficits(season, week)
    deficits_by_position = deficits["teams"].get(our_team_id, {})
    swap_suggestions = suggest_lineup_swaps(our_team_id, season, week)
    _apply_badges(our_side["starters"], deficits_by_position, swap_suggestions)

    return {"season": season, "week": week, "our_team": our_side, "opponent": opponent_side}


def _shape_scoreboard_team(team, score: float, projected: float) -> dict | None:
    if team is None:
        return None
    return {
        "team_id": str(team.team_id),
        "name": team.team_name,
        "logo_url": team.logo_url,
        "score": score,
        "projected": projected,
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


def _load_league_scoreboard(season: int, week: int) -> dict:
    box_scores = _fetch_box_scores(season, week)

    matchups = [
        {
            "home": _shape_scoreboard_team(box.home_team, box.home_score, box.home_projected),
            "away": _shape_scoreboard_team(box.away_team, box.away_score, box.away_projected),
        }
        for box in box_scores
    ]
    return {"season": season, "week": week, "matchups": matchups}


def get_current_team_roster(season: int, selected_week: int | None = None) -> dict:
    """Bye-week-safe roster view: sourced from league.teams/Team.roster, not box_scores(),
    so it works even when no opponent is scheduled (unlike get_current_team_matchup)."""
    our_team_id = os.getenv("ESPN_TEAM_ID")
    # Resolve the default week before keying, so "current week" and an explicit week share
    # one entry and a week rollover never serves the previous week's badges.
    week = selected_week if selected_week is not None else resolve_default_week()
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

    starters, bench = _split_roster_starters_bench(our_team.roster)

    deficits = compute_roster_deficits(season, week)
    deficits_by_position = deficits["teams"].get(our_team_id, {})
    swap_suggestions = suggest_lineup_swaps(our_team_id, season, week)
    _apply_badges(starters, deficits_by_position, swap_suggestions)

    metrics_by_espn_id = _load_weekly_metrics_by_espn_id(season, week, our_team_id)
    _apply_weekly_metrics(starters, metrics_by_espn_id)
    _apply_weekly_metrics(bench, metrics_by_espn_id)

    rumor_signal_by_espn_id = _load_rumor_signal_by_espn_id(our_team_id)
    _apply_rumor_signal(starters, rumor_signal_by_espn_id)
    _apply_rumor_signal(bench, rumor_signal_by_espn_id)

    return {"starters": starters, "bench": bench}
