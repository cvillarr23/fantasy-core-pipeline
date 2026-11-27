import os
import runpy
import subprocess
import sys
from unittest.mock import patch

import pytest

from src.jobs import draft_prep_refresh as job
from src.nfl_schedule import WeekResolutionError
from src.services import run_log_service


def test_main_runs_all_stages_for_target_through_four_week_horizon(monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    with patch.object(job, "resolve_upcoming_week", return_value=16), \
         patch.object(job.subprocess, "run") as run:
        assert job.main() == 0

    assert [c.args[0] for c in run.call_args_list] == [
        [job.sys.executable, "-u", "-m", stage, "--season", "2026", "--week", str(week)]
        for week in (16, 17, 18)
        for stage in job.STAGES
    ]
    assert all(c.kwargs["check"] is True for c in run.call_args_list)
    assert all(c.kwargs["timeout"] <= job.WEEK_BUDGET_SECONDS for c in run.call_args_list)


def test_main_shares_a_600_second_budget_across_stages(monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    with patch.object(job, "resolve_upcoming_week", return_value=18), \
         patch.object(job.time, "monotonic", side_effect=[0, 1, 2, 10, 11, 20, 21, 22]), \
         patch.object(job.subprocess, "run") as run:
        assert job.main() == 0

    assert [c.kwargs["timeout"] for c in run.call_args_list] == [599, 590, 580]


def test_main_fails_a_week_when_its_shared_budget_is_exhausted(monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    with patch.object(job, "resolve_upcoming_week", return_value=18), \
         patch.object(job.time, "monotonic", side_effect=[0, 601, 602]), \
         patch.object(job.subprocess, "run") as run:
        assert job.main() == 1
    run.assert_not_called()


def test_main_stops_after_target_week_fails(monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    with patch.object(job, "resolve_upcoming_week", return_value=7), \
         patch.object(job.subprocess, "run", side_effect=[None, subprocess.CalledProcessError(1, "stage")]) as run:
        assert job.main() == 1

    assert run.call_count == 2


def test_main_skips_failed_future_week_stages_and_continues_horizon(monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    outcomes = [None, None, None, subprocess.CalledProcessError(1, "stage"), None, None, None, None, None, None]
    with patch.object(job, "resolve_upcoming_week", return_value=7), \
         patch.object(job.subprocess, "run", side_effect=outcomes) as run:
        assert job.main() == 1

    weeks = [int(c.args[0][-1]) for c in run.call_args_list]
    stages = [c.args[0][3] for c in run.call_args_list]
    assert weeks == [7, 7, 7, 8, 9, 9, 9, 10, 10, 10]
    assert stages == [*job.STAGES, *job.STAGES[:1], *job.STAGES, *job.STAGES]


def test_main_is_a_noop_after_regular_season(monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    with patch.object(job, "resolve_upcoming_week", return_value=None), \
         patch.object(job.subprocess, "run") as run:
        assert job.main() == 0
    run.assert_not_called()


def test_main_fails_when_season_week_cannot_be_resolved(monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    with patch.object(job, "resolve_upcoming_week", side_effect=WeekResolutionError("down")), \
         patch.object(job.subprocess, "run") as run:
        assert job.main() == 1
    run.assert_not_called()


def test_main_rejects_a_week_outside_the_regular_season(monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    with patch.object(job, "resolve_upcoming_week", return_value=19), \
         patch.object(job.subprocess, "run") as run:
        assert job.main() == 1
    run.assert_not_called()


def test_week_offset_refreshes_only_that_week(monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    with patch.object(job, "resolve_upcoming_week", return_value=7), \
         patch.object(job.subprocess, "run") as run:
        assert job.main(["--week-offset", "2"]) == 0

    assert [c.args[0] for c in run.call_args_list] == [
        [job.sys.executable, "-u", "-m", stage, "--season", "2026", "--week", "9"]
        for stage in job.STAGES
    ]


def test_week_offset_fails_when_its_week_fails(monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    with patch.object(job, "resolve_upcoming_week", return_value=7), \
         patch.object(job.subprocess, "run", side_effect=subprocess.CalledProcessError(1, "stage")) as run:
        assert job.main(["--week-offset", "0"]) == 1
    assert run.call_count == 1


@pytest.mark.parametrize("upcoming", [16, None])
def test_week_offset_past_the_regular_season_or_offseason_is_a_noop(monkeypatch, upcoming):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    with patch.object(job, "resolve_upcoming_week", return_value=upcoming), \
         patch.object(job.subprocess, "run") as run:
        assert job.main(["--week-offset", "3"]) == 0
    run.assert_not_called()


def test_week_offset_must_not_be_negative():
    with pytest.raises(SystemExit):
        job.main(["--week-offset", "-1"])


def test_entrypoint_passes_its_week_offset_through(monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    monkeypatch.delenv("RUN_ID", raising=False)
    monkeypatch.setattr(sys, "argv", ["draft_prep_refresh", "--week-offset", "1"])
    monkeypatch.delitem(sys.modules, "src.jobs.draft_prep_refresh")
    with patch("src.nfl_schedule.resolve_upcoming_week", return_value=18), \
         patch("src.telemetry.configure_tracing"), \
         patch.object(run_log_service, "start_run", return_value="run"), \
         patch.object(run_log_service, "finish_run"), \
         patch.object(job.subprocess, "run") as run, \
         pytest.raises(SystemExit) as exited:
        runpy.run_module("src.jobs.draft_prep_refresh", run_name="__main__")
    assert exited.value.code == 0
    run.assert_not_called()


def test_entrypoint_adopts_the_launcher_run_so_stages_become_children(monkeypatch):
    """A stage that inherited RUN_ID would adopt the refresh's row and end it after one stage,
    and every later stage would then fail to adopt it."""
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    monkeypatch.setenv("RUN_ID", "launcher-run")
    monkeypatch.setattr(sys, "argv", ["draft_prep_refresh"])
    monkeypatch.delenv("RUN_PARENT_ID", raising=False)
    # runpy re-executes the module as __main__; the imported copy would make it warn.
    monkeypatch.delitem(sys.modules, "src.jobs.draft_prep_refresh")
    stage_env = []

    def stage(*args, **kwargs):
        stage_env.append((os.getenv("RUN_ID"), os.getenv("RUN_PARENT_ID")))

    with patch("src.nfl_schedule.resolve_upcoming_week", return_value=18), \
         patch("src.telemetry.configure_tracing"), \
         patch.object(run_log_service, "adopt_run", return_value=True) as adopt, \
         patch.object(run_log_service, "finish_run") as finish, \
         patch.object(job.subprocess, "run", side_effect=stage), \
         pytest.raises(SystemExit) as exited:
        runpy.run_module("src.jobs.draft_prep_refresh", run_name="__main__")

    assert exited.value.code == 0
    assert stage_env == [(None, "launcher-run")] * len(job.STAGES)
    assert adopt.call_args.args[0] == "launcher-run"
    assert finish.call_args.args[:2] == ("launcher-run", "success")


def ros_main(monkeypatch, upcoming, last_playoff_week, outcomes=None, argv=("--rest-of-season",)):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    with patch.object(job, "resolve_upcoming_week", return_value=upcoming), \
         patch.object(job, "load_last_playoff_week", return_value=last_playoff_week) as last, \
         patch.object(job.subprocess, "run", side_effect=outcomes) as run:
        code = job.main(list(argv))
    weeks = [int(c.args[0][-1]) for c in run.call_args_list]
    return code, sorted(set(weeks)), run, last


def test_rest_of_season_refreshes_past_the_horizon_through_the_last_playoff_week(monkeypatch):
    code, weeks, run, last = ros_main(monkeypatch, upcoming=8, last_playoff_week=17)
    assert code == 0
    # The daily fan-out covers 8..11; this picks up 12..17.
    assert weeks == list(range(12, 18))
    assert run.call_count == len(weeks) * len(job.STAGES)
    last.assert_called_once_with(2026)


def test_rest_of_season_keeps_going_after_a_failed_week(monkeypatch):
    failure = subprocess.CalledProcessError(1, "stage")
    code, weeks, _, _ = ros_main(
        monkeypatch, upcoming=12, last_playoff_week=17,
        outcomes=[failure] + [None] * (len(job.STAGES) * 1),
    )
    assert code == 1
    assert weeks == [16, 17]


@pytest.mark.parametrize("upcoming,last_week", [(12, None), (14, 17), (None, 17)])
def test_rest_of_season_noops(monkeypatch, upcoming, last_week):
    code, weeks, run, _ = ros_main(monkeypatch, upcoming=upcoming, last_playoff_week=last_week)
    assert code == 0
    run.assert_not_called()


def test_rest_of_season_fails_when_playoff_settings_cannot_be_read(monkeypatch):
    monkeypatch.setenv("CURRENT_YEAR", "2026")
    with patch.object(job, "resolve_upcoming_week", return_value=8), \
         patch.object(job, "load_last_playoff_week", side_effect=RuntimeError("db down")), \
         patch.object(job.subprocess, "run") as run:
        assert job.main(["--rest-of-season"]) == 1
    run.assert_not_called()


def test_rest_of_season_and_week_offset_are_exclusive():
    with pytest.raises(SystemExit):
        job.main(["--rest-of-season", "--week-offset", "1"])


def test_last_playoff_week_reads_synced_playoff_settings():
    from contextlib import contextmanager
    from unittest.mock import MagicMock

    cursor = MagicMock()
    cursor.fetchone.side_effect = [({"playoff_weeks": [15, 16, 17]},), ({"playoff_weeks": []},), None]

    @contextmanager
    def db():
        yield cursor

    with patch.object(job, "get_db_cursor", db):
        assert job.load_last_playoff_week(2026) == 17
        assert job.load_last_playoff_week(2026) is None
        assert job.load_last_playoff_week(2026) is None
    assert cursor.execute.call_args.args[1] == (2026,)
