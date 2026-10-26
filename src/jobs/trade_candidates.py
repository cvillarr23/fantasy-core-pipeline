"""
League-wide trade candidate job: `python -m src.jobs.trade_candidates`.

Enumerates, prefilters, scores and classifies every 1-for-1, 2-for-1 and 1-for-2 package
between our team and each counterparty for the upcoming week (or --week), then replaces
that week's stored candidates (src/services/trade_candidate_service.py), which
GET /v1/trade/candidates serves. Exit 1 when the week can't be resolved or the trade
context is unavailable; exit 0 without writing once the regular season is over.
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime

from src.nfl_schedule import WeekResolutionError, resolve_upcoming_week
from src.services import run_log_service
from src.services.trade_candidate_service import (
    DEFAULT_MAX_CANDIDATES,
    generate_trade_candidates,
    store_run,
)
from src.services.trade_context_service import TradeContextUnavailable
from src.telemetry import configure_tracing

logger = logging.getLogger("TradeCandidatesJob")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Generate, score and store league-wide trade candidates.")
    parser.add_argument("--league-id", type=str, default=os.getenv("ESPN_LEAGUE_ID"))
    parser.add_argument("--team-id", type=str, default=os.getenv("ESPN_TEAM_ID"))
    parser.add_argument("--season", type=int, default=int(os.getenv("CURRENT_YEAR", datetime.now().year)))
    parser.add_argument("--week", type=int, default=None, help="target week; defaults to the upcoming week in the nflverse schedule")
    parser.add_argument("--horizon", type=int, default=4)
    parser.add_argument("--max-candidates", type=int, default=DEFAULT_MAX_CANDIDATES)
    args = parser.parse_args(argv)

    if not args.league_id:
        parser.error("--league-id is required (or set the ESPN_LEAGUE_ID env var)")
    if not args.team_id:
        parser.error("--team-id is required (or set the ESPN_TEAM_ID env var)")
    week = args.week
    if week is None:
        try:
            week = resolve_upcoming_week(args.season)
        except WeekResolutionError as exc:
            logger.error(f"Could not resolve the upcoming week for Season {args.season}: {exc}")
            return 1
        if week is None:
            logger.info(f"The {args.season} regular season is over; no trade candidates to generate.")
            return 0

    started = time.monotonic()
    try:
        _, rows, counts = generate_trade_candidates(
            args.league_id, args.season, week, args.horizon, args.team_id, args.max_candidates,
        )
    except TradeContextUnavailable as exc:
        logger.error(f"Trade context unavailable for {args.season} week {week}: {exc}")
        return 1
    run_id, _ = store_run(
        args.league_id, args.season, week, args.horizon, args.team_id, rows, counts,
        run_id=run_log_service.current_run_id(),
    )
    duration = round(time.monotonic() - started, 1)
    summary = {key: counts[key] for key in (
        "enumerated", "prefiltered", "scored", "proposable", "needs_drop", "one_sided",
    )}
    logger.info(
        f"Trade candidates for {args.season} week {week} (run {run_id}): "
        + ", ".join(f"{key} {value}" for key, value in summary.items())
        + f"; {duration}s"
    )
    run_log_service.add_summary(
        season=args.season, week=week, duration_seconds=duration,
        headline=f"{counts['proposable']} proposable of {counts['scored']} scored", **summary,
    )
    return 0


if __name__ == "__main__":
    configure_tracing("trade-candidates")
    sys.exit(run_log_service.run_main("trade_candidates", main))
