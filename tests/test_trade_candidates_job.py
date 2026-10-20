from unittest.mock import patch

from src.jobs import trade_candidates as job
from src.nfl_schedule import WeekResolutionError
from src.services.trade_context_service import TradeContextUnavailable

ARGS = ["--league-id", "123", "--team-id", "1", "--season", "2026"]
COUNTS = {
    "enumerated": 900, "prefiltered": 120, "scored": 120, "proposable": 7,
    "needs_drop": 30, "one_sided": 83, "teams": [],
}


def test_job_resolves_the_week_scores_stores_and_summarizes():
    with patch.object(job, "resolve_upcoming_week", return_value=7), \
         patch.object(job, "generate_trade_candidates", return_value=({}, ["row"], COUNTS)) as generate, \
         patch.object(job, "store_run", return_value=("run-1", None)) as store, \
         patch.object(job.run_log_service, "current_run_id", return_value="run-1"), \
         patch.object(job.run_log_service, "add_summary") as summary:
        assert job.main(ARGS) == 0
    generate.assert_called_once_with("123", 2026, 7, 4, "1", 400)
    store.assert_called_once_with("123", 2026, 7, 4, "1", ["row"], COUNTS, run_id="run-1")
    fields = summary.call_args.kwargs
    assert fields["enumerated"] == 900 and fields["proposable"] == 7
    assert fields["week"] == 7 and "duration_seconds" in fields


def test_job_uses_an_explicit_week_and_options():
    with patch.object(job, "resolve_upcoming_week") as resolve, \
         patch.object(job, "generate_trade_candidates", return_value=({}, [], COUNTS)) as generate, \
         patch.object(job, "store_run", return_value=("run-1", None)):
        assert job.main([*ARGS, "--week", "9", "--horizon", "2", "--max-candidates", "50"]) == 0
    resolve.assert_not_called()
    generate.assert_called_once_with("123", 2026, 9, 2, "1", 50)


def test_job_exits_zero_without_writing_once_the_season_is_over():
    with patch.object(job, "resolve_upcoming_week", return_value=None), \
         patch.object(job, "store_run") as store:
        assert job.main(ARGS) == 0
    store.assert_not_called()


def test_job_fails_when_the_week_cannot_be_resolved():
    with patch.object(job, "resolve_upcoming_week", side_effect=WeekResolutionError("down")), \
         patch.object(job, "store_run") as store:
        assert job.main(ARGS) == 1
    store.assert_not_called()


def test_job_fails_without_writing_when_the_trade_context_is_unavailable():
    with patch.object(job, "resolve_upcoming_week", return_value=7), \
         patch.object(job, "generate_trade_candidates", side_effect=TradeContextUnavailable("x")), \
         patch.object(job, "store_run") as store:
        assert job.main(ARGS) == 1
    store.assert_not_called()
