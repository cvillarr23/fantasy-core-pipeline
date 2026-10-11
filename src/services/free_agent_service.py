"""Browse all claimable free agents without changing the recommendation planner."""

import os

from src.database import get_db_cursor
from src.ingestion.metrics_extractor import normalize_team
from src.ingestion.sync_league import refresh_rosters_if_stale
from src.optimization.lineup_solver import (
    METRIC_SOURCE,
    PREGAME_WINDOW_WEEKS,
    load_teams_playing,
)
from src.services.waiver_service import WaiverInputsUnavailableError


def _load_source(cursor, team_id, season, week):
    """Use the recommendations' same-season window and freshness requirements."""
    first_week = max(1, week - PREGAME_WINDOW_WEEKS)
    cursor.execute(
        """
        SELECT DISTINCT s.week
        FROM weekly_stats s
        JOIN players p ON p.player_id = s.player_id
        WHERE s.season = %s AND s.week BETWEEN %s AND %s
          AND s.vorp_score IS NOT NULL
          AND (NOT EXISTS (SELECT 1 FROM league_rosters r WHERE r.player_id = p.player_id)
               OR EXISTS (SELECT 1 FROM league_rosters r WHERE r.player_id = p.player_id
                          AND r.team_id = %s AND (r.is_starter IS NULL OR r.is_starter = FALSE)
                          AND r.slot_position <> 'IR'));
        """,
        (season, first_week, week, team_id),
    )
    weeks = sorted(row[0] for row in cursor.fetchall())
    if week in weeks:
        return {"source_mode": "target_week", "source_weeks": [week]}, None
    if not weeks or week <= 1:
        raise WaiverInputsUnavailableError(
            f"no usable {METRIC_SOURCE} for Season {season} Week {week} "
            "or prior same-season weeks"
        )
    if weeks[-1] < week - 1:
        raise WaiverInputsUnavailableError(
            f"stale {METRIC_SOURCE}: newest usable week is Season {season} Week {weeks[-1]}, "
            f"expected Week {week - 1} before evaluating Week {week}"
        )
    try:
        teams_playing = {normalize_team(t) for t in load_teams_playing(season, week)}
    except Exception as exc:
        raise WaiverInputsUnavailableError(
            f"could not load the nflverse schedule for Season {season} Week {week}: {exc}"
        ) from exc
    return {"source_mode": "rolling_prior_weeks", "source_weeks": weeks}, teams_playing


def browse_free_agents(
    season: int, week: int, position=None, q=None, sort="vorp", limit=50, offset=0
) -> dict:
    """Full pool, including negative upgrades and players without usable metrics.

    Bye players remain browsable; prior-week values cannot describe their target week,
    so their metrics are null. Missing bench metrics never become a zero-value drop.
    """
    team_id = os.getenv("ESPN_TEAM_ID")
    refresh_rosters_if_stale(season)
    with get_db_cursor() as cursor:
        source, teams_playing = _load_source(cursor, team_id, season, week)
        cursor.execute(
            """
            WITH metrics AS (
                SELECT player_id, AVG(vorp_score) AS vorp_score,
                       AVG(projected_points) AS projected_points
                FROM weekly_stats
                WHERE season = %s AND week = ANY(%s)
                  AND (vorp_score IS NOT NULL OR %s)
                GROUP BY player_id
            )
            SELECT p.player_id, p.espn_id, p.name, p.position, p.team,
                   COALESCE(r.espn_injury_status, p.injury_status),
                   m.projected_points, m.vorp_score, (r.player_id IS NOT NULL) AS on_our_bench
            FROM players p
            LEFT JOIN metrics m ON m.player_id = p.player_id
            LEFT JOIN league_rosters r ON r.player_id = p.player_id AND r.team_id = %s
                AND (r.is_starter IS NULL OR r.is_starter = FALSE) AND r.slot_position <> 'IR'
            WHERE p.espn_id IS NOT NULL AND TRIM(p.espn_id) <> ''
              AND (r.player_id IS NOT NULL
                   OR NOT EXISTS (SELECT 1 FROM league_rosters owned WHERE owned.player_id = p.player_id));
            """,
            (
                season,
                source["source_weeks"],
                source["source_mode"] == "target_week",
                team_id,
            ),
        )
        rows = cursor.fetchall()

    bench: dict = {}
    free_agents = []
    for (
        player_id,
        espn_id,
        name,
        player_position,
        team,
        injury,
        projected,
        vorp,
        on_bench,
    ) in rows:
        if teams_playing is not None and normalize_team(team) not in teams_playing:
            projected = vorp = None
        player = {
            "player_id": player_id,
            "espn_id": str(espn_id),
            "name": name,
            "position": player_position,
            "pro_team": team,
            "injury_status": injury,
            "projected_points": projected,
            "vorp_score": vorp,
        }
        if on_bench:
            if vorp is not None:
                current = bench.get(player_position)
                if current is None or (vorp, player_id) < (
                    current["vorp_score"],
                    current["player_id"],
                ):
                    bench[player_position] = player
        else:
            free_agents.append(player)

    filtered = []
    for player in free_agents:
        if position and player["position"].upper() != position.upper():
            continue
        if q and q.casefold() not in player["name"].casefold():
            continue
        drop = bench.get(player["position"])
        delta = (
            None
            if drop is None or player["vorp_score"] is None
            else player["vorp_score"] - drop["vorp_score"]
        )
        filtered.append({**player, "bench_player": drop, "vorp_delta": delta})

    metric = "vorp_score" if sort == "vorp" else "projected_points"
    filtered.sort(
        key=lambda p: (
            p[metric] is None,
            -p[metric] if p[metric] is not None else 0,
            p["name"].casefold(),
            p["player_id"],
        )
    )
    return {
        "season": season,
        "week": week,
        # limit=None returns the whole sorted pool (the chat grounding trims it per position).
        "free_agents": filtered[offset:] if limit is None else filtered[offset : offset + limit],
        "total": len(filtered),
        "limit": limit,
        "offset": offset,
        **source,
    }
