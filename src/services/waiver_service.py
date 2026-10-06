"""
Waiver-recommendation read endpoint backing the REST controller in src/api/waiver.py.

Reuses src/backtesting/waiver_strategy.py's pure decision functions unchanged - the same
ones season_replay.py backtests and waiver_evaluator.py publishes live from - rather than
re-deriving the VORP-delta math. margin=0.0 here, not waiver_strategy.MARGIN_THRESHOLD:
that threshold gates live auto-publish, not what's worth showing a manager to decide on.

Before the target week's projections land (draft-prep-refresher, Tuesday 05:00 CT) the
exact-week pools are empty, so the page falls back to prior same-season weeks, the same
source rules as lineup_solver.load_pregame_roster_pool (#185).
"""
import logging
import os

from src.backtesting import waiver_strategy
from src.database import get_db_cursor
from src.ingestion.metrics_extractor import normalize_team
from src.ingestion.sync_league import refresh_rosters_if_stale
from src.optimization.lineup_solver import METRIC_SOURCE, PREGAME_WINDOW_WEEKS, load_teams_playing

logger = logging.getLogger("WaiverService")


class WaiverInputsUnavailableError(Exception):
    """Neither the target week nor the prior-week fallback has usable data - distinct from
    an empty recommendation list, which means no free agent beats a bench player."""


def load_prior_week_waiver_pools(cursor, team_id: str, season: int, week: int,
                                 window: int = PREGAME_WINDOW_WEEKS) -> tuple:
    """(bench_by_position, free_agents_by_position, source) from each player's mean
    vorp_score over the up-to-`window` prior weeks of the same season, in
    waiver_strategy's shapes. Our bench and unrostered players only; players whose team
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
               (r.player_id IS NOT NULL) AS on_our_bench
        FROM weekly_stats s
        JOIN players p ON p.player_id = s.player_id
        LEFT JOIN league_rosters r ON r.player_id = p.player_id
        WHERE s.season = %s AND s.week BETWEEN %s AND %s AND s.vorp_score IS NOT NULL
          AND (r.player_id IS NULL OR (r.team_id = %s AND (r.is_starter IS NULL OR r.is_starter = FALSE)));
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
    for player_id, position, team, _, vorp_score, on_our_bench in rows:
        entry = players.setdefault(player_id, {"position": position, "team": team, "bench": on_our_bench, "vorp": []})
        entry["vorp"].append(vorp_score)

    bench_by_position: dict = {}
    free_agents_by_position: dict = {}
    for player_id, entry in players.items():
        if normalize_team(entry["team"]) not in teams_playing:
            continue
        candidate = {"player_id": player_id, "vorp_score": sum(entry["vorp"]) / len(entry["vorp"])}
        if entry["bench"]:
            bench_by_position.setdefault(entry["position"], []).append(candidate)
        else:
            best = free_agents_by_position.get(entry["position"])
            if best is None or candidate["vorp_score"] > best["vorp_score"]:
                free_agents_by_position[entry["position"]] = candidate

    missing = [label for label, pool in (("bench", bench_by_position), ("free-agent", free_agents_by_position)) if not pool]
    if missing:
        raise WaiverInputsUnavailableError(
            f"no {' or '.join(missing)} players with usable {METRIC_SOURCE} in Season {season} "
            f"Weeks {weeks} after excluding Week {week} byes"
        )
    return bench_by_position, free_agents_by_position, {"source_mode": "rolling_prior_weeks", "source_weeks": weeks}


def _load_player_display(cursor, player_ids: set) -> dict:
    """player_id -> {espn_id, name, pro_team, injury_status}, one batched query."""
    if not player_ids:
        return {}
    cursor.execute(
        """
        SELECT player_id, espn_id, name, team, injury_status
        FROM players
        WHERE player_id = ANY(%s);
        """,
        (list(player_ids),),
    )
    return {
        player_id: {"espn_id": espn_id, "name": name, "pro_team": team, "injury_status": injury_status}
        for player_id, espn_id, name, team, injury_status in cursor.fetchall()
    }


def _shape_recommendation(move: dict, display_by_player_id: dict) -> dict:
    add_display = display_by_player_id[move["add_player_id"]]
    drop_display = display_by_player_id[move["drop_player_id"]]
    return {
        "position": move["position"],
        "vorp_delta": move["add_vorp_score"] - move["drop_vorp_score"],
        "free_agent": {**add_display, "net_vorp": move["add_vorp_score"]},
        "bench_player": {**drop_display, "net_vorp": move["drop_vorp_score"]},
        # Placeholders until a future ticket wires the rumor-signal feed into this
        # endpoint, mirroring how it was added to GET /v1/matchup/roster.
        "rumor_sentiment": None,
        "rumor_confidence": None,
        "rumor_suggested_action": None,
    }


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


def compute_waiver_recommendations(season: int, week: int) -> dict:
    our_team_id = os.getenv("ESPN_TEAM_ID")
    # The free-agent pool is "not in league_rosters"; a week-old snapshot suggests
    # players other managers have since picked up.
    refresh_rosters_if_stale(season)
    with get_db_cursor() as cursor:
        bench_by_position = waiver_strategy.load_bench_by_position(cursor, our_team_id, season, week)
        free_agents_by_position = waiver_strategy.load_best_free_agent_by_position(cursor, season, week)
        if bench_by_position and free_agents_by_position:
            source = {"source_mode": "target_week", "source_weeks": [week]}
        else:
            bench_by_position, free_agents_by_position, source = load_prior_week_waiver_pools(
                cursor, our_team_id, season, week,
            )
            logger.info(
                f"No Season {season} Week {week} rows yet; waiver recommendations use "
                f"{METRIC_SOURCE} means from Weeks {source['source_weeks']}."
            )
        moves = waiver_strategy.propose_waiver_moves(bench_by_position, free_agents_by_position, margin=0.0)

        player_ids = {m["add_player_id"] for m in moves} | {m["drop_player_id"] for m in moves}
        display_by_player_id = _load_player_display(cursor, player_ids)

    recommendations = [_shape_recommendation(m, display_by_player_id) for m in moves]
    recommendations.sort(key=lambda r: r["vorp_delta"], reverse=True)
    return {"season": season, "week": week, "recommendations": recommendations, **source}


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
