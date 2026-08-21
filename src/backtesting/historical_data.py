"""
Historical ground truth for the backtesting harness (CORE-15).

The real league (1248158344) has no draft/season history to replay, so this pulls real
public historical data instead: actual weekly fantasy points and a FantasyPros ECR-based
ADP proxy, both via nflreadpy, against a hardcoded standard league config rather than a
real `league_settings` row.

Implementation-time decision (documented, not a bug): production's sync_projections.py
re-fetches a live, continuously-updated Sleeper projection every week, but nflreadpy has no
proven historical per-week *point projection* source (only ordinal ECR ranks). v1 uses a
single fixed preseason `projected_points` value for the whole season, written into
weekly_stats week 1 only - matching draft_matrix.py's existing convention that week 1's row
holds the preseason projection. A documented follow-up: derive a cheap rolling-average
"projection" from prior weeks' actuals instead (respects no-lookahead trivially).
"""
import json
import logging

import nflreadpy as nfl
import polars as pl

logger = logging.getLogger("BacktestHistoricalData")

SYNTHETIC_LEAGUE_ID = "backtest"
SKILL_POSITIONS = ["QB", "RB", "WR", "TE"]

# Matches vorp_matrix.load_league_config's own no-league fallback shape exactly (10 teams,
# same slot structure), so the backtest league config is provably consistent with what the
# pipeline already assumes when nothing is synced.
HARDCODED_LEAGUE_CONFIG = {
    "league_id": SYNTHETIC_LEAGUE_ID,
    "team_count": 10,
    "lineup_slot_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 7},
    "scoring_type": "PPR",
    "scoring_settings": {},
}

# Synthetic preseason-points scale for the ADP-ranked pool: monotonically decreasing with
# rank, floored so nobody projects to a negative/zero value. Not a real point projection -
# see module docstring - just enough of a plausible scale for VORP's baseline math to be
# meaningful before any actual results are known.
_SYNTHETIC_POINTS_BASE = 30.0
_SYNTHETIC_POINTS_DECAY = 0.15
_SYNTHETIC_POINTS_FLOOR = 1.0


def rank_to_synthetic_points(rank: int) -> float:
    return max(_SYNTHETIC_POINTS_FLOOR, _SYNTHETIC_POINTS_BASE - _SYNTHETIC_POINTS_DECAY * rank)


def load_actual_weekly_stats(season: int) -> pl.DataFrame:
    """Real actual weekly fantasy points via nflreadpy, keyed on gsis_id (this function's
    own output schema; callers resolve gsis_id -> this repo's player_id via
    build_player_pool's crosswalk). Restricted to regular season - postseason weeks would
    otherwise collide with week numbers 1-17/18 and double-count for teams that made the
    playoffs."""
    df = nfl.load_player_stats(seasons=[season], summary_level="week")
    if df.is_empty():
        return pl.DataFrame(schema={"gsis_id": pl.Utf8, "week": pl.Int64, "actual_points": pl.Float64})
    return (
        df.filter(pl.col("season_type") == "REG")
        .select(["player_id", "week", "fantasy_points_ppr"])
        .rename({"player_id": "gsis_id", "fantasy_points_ppr": "actual_points"})
    )


def build_player_pool(season: int) -> pl.DataFrame:
    """The backtest's player universe and crosswalk, sourced directly from nflreadpy's
    DynastyProcess-backed ID crosswalk (the same fetch sync_id_crosswalk.py uses, loaded
    here directly rather than via that module since this needs additional columns -
    name/position/team/fantasypros_id - that sync_id_crosswalk.py doesn't select).
    Restricted to skill positions with a sleeper_id present, since `players.player_id` is
    Sleeper-keyed and a row with no sleeper_id can never be written there."""
    df = nfl.load_ff_playerids()
    return (
        df.filter(pl.col("position").is_in(SKILL_POSITIONS) & pl.col("sleeper_id").is_not_null())
        .select(["sleeper_id", "gsis_id", "fantasypros_id", "name", "position", "team"])
        .with_columns([
            pl.col("sleeper_id").cast(pl.Utf8),
            pl.col("gsis_id").cast(pl.Utf8),
            pl.col("fantasypros_id").cast(pl.Utf8),
        ])
        .unique(subset=["sleeper_id"], keep="first")
    )


def load_redraft_rankings(season: int) -> pl.DataFrame:
    """FantasyPros redraft-overall ECR nearest (but not after) that season's kickoff, as
    the ADP proxy for the draft simulator - a real, per-season, independent external
    ranking signal, not literal pick-position ADP, but a legitimate stand-in per the
    approved design. Empty if no scrape exists for/before that season (older seasons or an
    nflreadpy hiccup) - callers must treat that as the degraded-fallback trigger."""
    df = nfl.load_ff_rankings(type="all")
    if df.is_empty():
        return df
    kickoff_cutoff = f"{season}-09-15"
    candidates = (
        df.filter(
            (pl.col("page_type") == "redraft-overall")
            & (pl.col("scrape_date") <= kickoff_cutoff)
            & (pl.col("scrape_date") >= f"{season}-01-01")
        )
    )
    if candidates.is_empty():
        return candidates
    latest_date = candidates["scrape_date"].max()
    return candidates.filter(pl.col("scrape_date") == latest_date).select(["id", "player", "pos", "ecr"])


def resolve_adp(season: int, player_pool: pl.DataFrame, prior_season_actuals: pl.DataFrame | None = None) -> pl.DataFrame:
    """Returns player_id/adp_rank (rank 1 = first off the board), joined from FantasyPros
    ECR via fantasypros_id -> player_pool's crosswalk -> player_id (== sleeper_id). Falls
    back to a synthetic rank from *prior*-season actuals (never current-season - that would
    leak lookahead into the draft itself) when this season has no rankings scrape, logged
    as a degraded path."""
    rankings = load_redraft_rankings(season)
    if not rankings.is_empty():
        joined = (
            rankings.join(
                player_pool.select(["sleeper_id", "fantasypros_id"]),
                left_on="id", right_on="fantasypros_id", how="inner",
            )
            .sort("ecr")
            .with_row_index("adp_rank", offset=1)
            .select([pl.col("sleeper_id").alias("player_id"), "adp_rank"])
        )
        if not joined.is_empty():
            return joined

    logger.warning(
        f"No FantasyPros redraft-overall ranking available for season {season}; "
        f"falling back to a synthetic ADP derived from the prior season's actual points."
    )
    if prior_season_actuals is None or prior_season_actuals.is_empty():
        return pl.DataFrame(schema={"player_id": pl.Utf8, "adp_rank": pl.UInt32})
    season_totals = (
        prior_season_actuals.group_by("gsis_id")
        .agg(pl.col("actual_points").sum().alias("total_points"))
        .join(player_pool.select(["sleeper_id", "gsis_id"]), on="gsis_id", how="inner")
        .sort("total_points", descending=True)
        .with_row_index("adp_rank", offset=1)
        .select([pl.col("sleeper_id").alias("player_id"), "adp_rank"])
    )
    return season_totals


def seed_players(conn, player_pool: pl.DataFrame) -> int:
    """Upserts the backtest player universe into the (schema-pinned) players table."""
    if player_pool.is_empty():
        return 0
    rows = [
        (
            row["sleeper_id"], row["name"] or row["sleeper_id"], row["position"],
            row["team"] or "FA", "Active", None, row["gsis_id"],
        )
        for row in player_pool.iter_rows(named=True)
    ]
    with conn.cursor() as write_cursor:
        write_cursor.executemany(
            """
            INSERT INTO players (player_id, name, position, team, status, injury_status, gsis_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_id) DO UPDATE SET
                name = EXCLUDED.name, position = EXCLUDED.position, team = EXCLUDED.team,
                gsis_id = EXCLUDED.gsis_id;
            """,
            rows,
        )
    return len(rows)


def seed_preseason_projections(conn, player_pool: pl.DataFrame, adp: pl.DataFrame, season: int) -> int:
    """Writes a single synthetic preseason projected_points value into weekly_stats week 1
    only, ranked by ADP - matching draft_matrix.py's convention that week 1 holds the
    preseason projection. Players with no ADP rank (outside the ranked pool) don't get a
    week-1 row and fall back to weekly_stats' 0.0 DEFAULT via lineup_solver/vorp_matrix's
    existing COALESCE handling."""
    if adp.is_empty():
        return 0
    rows = [
        (row["player_id"], season, rank_to_synthetic_points(row["adp_rank"]))
        for row in adp.iter_rows(named=True)
    ]
    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO weekly_stats (player_id, season, week, projected_points)
            VALUES (%s, %s, 1, %s)
            ON CONFLICT (player_id, season, week) DO UPDATE SET projected_points = EXCLUDED.projected_points;
            """,
            rows,
        )
    return len(rows)


def seed_league_settings(conn, season: int) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO league_settings (league_id, season, name, team_count, scoring_type, lineup_slot_counts, scoring_settings)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (league_id) DO UPDATE SET
                season = EXCLUDED.season, team_count = EXCLUDED.team_count,
                lineup_slot_counts = EXCLUDED.lineup_slot_counts;
            """,
            (
                SYNTHETIC_LEAGUE_ID, season, "CORE-15 Backtest League",
                HARDCODED_LEAGUE_CONFIG["team_count"], HARDCODED_LEAGUE_CONFIG["scoring_type"],
                json.dumps(HARDCODED_LEAGUE_CONFIG["lineup_slot_counts"]),
                json.dumps(HARDCODED_LEAGUE_CONFIG["scoring_settings"]),
            ),
        )
