"""
`weekly_stats.projected_points` is the consensus of the per-source projections (#462): the
mean of the non-null `espn_projected_points` and `sleeper_projected_points`. Everything that
reads `projected_points` (vorp_matrix, so the lineup solver, waivers and trade scoring, and
the roster/matchup pages) reads the consensus without knowing about the sources.

Each projection writer sets only its own source column and then calls `recompute_consensus`
in the same transaction:

- `sync_projections`: Sleeper, QB/RB/WR/TE.
- `sync_espn_projections`: ESPN, QB/RB/WR/TE. It only updates rows Sleeper wrote, so row
  existence (and with it "no row = bye") stays Sleeper's call.
- `sync_kicker_dst`: ESPN, K/D-ST, the only source for those positions.

A row with neither source is never touched, so it keeps whatever `projected_points` it has
(rows from before the source columns existed, metrics-only rows, backtest rows). With only
one source the consensus is that source's value, never null or 0: while the ESPN sync hasn't
run, `projected_points` is exactly Sleeper's.
"""

SOURCE_COLUMNS = ("espn_projected_points", "sleeper_projected_points")

CONSENSUS_EXPRESSION = """
    (COALESCE(espn_projected_points, 0) + COALESCE(sleeper_projected_points, 0))
    / ((espn_projected_points IS NOT NULL)::int + (sleeper_projected_points IS NOT NULL)::int)
"""

RECOMPUTE_CONSENSUS_SQL = f"""
    UPDATE weekly_stats
    SET projected_points = {CONSENSUS_EXPRESSION}
    WHERE player_id = ANY(%s) AND season = %s AND week = %s
      AND (espn_projected_points IS NOT NULL OR sleeper_projected_points IS NOT NULL);
"""


def consensus_projection(espn: float | None, sleeper: float | None) -> float | None:
    """Pure. The mean of the sources that have a projection; None when neither does."""
    values = [float(v) for v in (espn, sleeper) if v is not None]
    return sum(values) / len(values) if values else None


def recompute_consensus(cursor, season: int, week: int, player_ids) -> int:
    """Sets projected_points to the consensus for these players' rows in season/week.
    Returns the number of rows updated."""
    player_ids = sorted(set(player_ids))
    if not player_ids:
        return 0
    cursor.execute(RECOMPUTE_CONSENSUS_SQL, (player_ids, season, week))
    return cursor.rowcount
