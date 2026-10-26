from __future__ import annotations

import json
import logging
import threading
import uuid
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from src.launcher.catalog import Schedule, Season, load_catalog
from src.launcher.scheduler import (
    NflSeasonCalendar,
    RunRow,
    PostgresSchedulerState,
    Scheduler,
    latest_slot,
    slot_key,
)

FIXTURE = "tests/fixtures/job-catalog.yaml"
CHICAGO = ZoneInfo("America/Chicago")
UTC = timezone.utc


def ct(*args: int) -> datetime:
    """A frozen America/Chicago wall-clock time, as UTC."""
    return datetime(*args, tzinfo=CHICAGO).astimezone(UTC)


class FakeState:
    def __init__(self):
        self.runs: dict[str, RunRow] = {}
        self.meta: dict[str, dict] = {}
        self.overrides: dict[str, datetime | None] = {}

    def insert(self, row: RunRow, **meta) -> bool:
        key = meta.get("dedupe_key")
        if key and any(m.get("dedupe_key") == key for m in self.meta.values()):
            return False
        self.runs[row.id] = row
        self.meta[row.id] = meta
        return True

    def by_key(self, key: str) -> RunRow | None:
        for identity, meta in self.meta.items():
            if meta.get("dedupe_key") == key:
                return self.runs[identity]
        return None

    def rows(self, job: str) -> list[RunRow]:
        return [row for row in self.runs.values() if row.job_name == job]

    def run_exists(self, dedupe_key):
        return self.by_key(dedupe_key) is not None

    def paused(self, name, now):
        return name in self.overrides and (
            self.overrides[name] is None or self.overrides[name] > now
        )

    def record_skipped(
        self,
        *,
        job_name,
        trigger_type,
        dedupe_key,
        scheduled_for,
        end_reason,
        chain_root_id=None,
    ):
        row = RunRow(
            str(uuid.uuid4()),
            job_name,
            "skipped",
            scheduled_for=scheduled_for,
            chain_root_id=chain_root_id,
        )
        return self.insert(
            row, dedupe_key=dedupe_key, trigger_type=trigger_type, end_reason=end_reason
        )

    def create_chain_root(
        self, *, chain, dedupe_key, scheduled_for, from_step=None, requested_by=None
    ):
        identity = str(uuid.uuid4())
        row = RunRow(
            identity,
            chain,
            "running",
            scheduled_for=scheduled_for,
            chain_root_id=identity,
            trigger={"chain": chain, "from_step": from_step},
        )
        return (
            row
            if self.insert(row, dedupe_key=dedupe_key, trigger_type="chain")
            else None
        )

    def _roots(self):
        return [r for r in self.runs.values() if r.chain_root_id == r.id]

    def active_chain_roots(self):
        return [r for r in self._roots() if r.status == "running"]

    def chain_roots_between(self, chain, start, end):
        return [
            r
            for r in self._roots()
            if r.job_name == chain and start <= r.scheduled_for < end
        ]

    def chain_steps(self, root_id):
        return {
            r.job_name: r
            for r in self.runs.values()
            if r.chain_root_id == root_id and r.id != root_id
        }

    def finish_chain_root(self, root_id, status, end_reason):
        if self.runs[root_id].status == "running":
            self.runs[root_id].status = status
            self.meta[root_id]["end_reason"] = end_reason


class AlwaysInSeason:
    def in_season(self, season, day):
        return True


@pytest.fixture
def state():
    return FakeState()


@pytest.fixture
def commands():
    return []


def fake_publish_run(state, commands):
    """Stands in for publish_run: the queued row is what chains read back."""

    def publish(catalog, job, **kwargs):
        existing = state.by_key(kwargs["dedupe_key"])
        if existing:
            return {"run_id": existing.id, "status": "duplicate"}
        command = {"run_id": str(uuid.uuid4()), "job": job, **kwargs}
        row = RunRow(
            command["run_id"], job, "queued", chain_root_id=kwargs["chain_root_id"]
        )
        state.insert(
            row, dedupe_key=kwargs["dedupe_key"], trigger_type=kwargs["trigger"]
        )
        commands.append(command)
        return {"run_id": command["run_id"], "status": "queued"}

    return publish


@pytest.fixture
def scheduler(state, commands):
    return Scheduler(
        load_catalog(FIXTURE),
        state,
        fake_publish_run(state, commands),
        seasons=AlwaysInSeason(),
    )


def jobs(commands, job=None, trigger=None):
    return [
        c
        for c in commands
        if (job is None or c["job"] == job)
        and (trigger is None or c["trigger"] == trigger)
    ]


def root(state) -> RunRow:
    (chain_root,) = [r for r in state._roots() if r.job_name == "tuesday-weekly"]
    return chain_root


def step(state, chain_root, job) -> RunRow:
    return state.chain_steps(chain_root.id)[job]


def finish(state, chain_root, job, status, attempt=1):
    row = step(state, chain_root, job)
    row.status, row.attempt = status, attempt


def end_reason(state, row) -> str | None:
    return state.meta[row.id].get("end_reason")


# Slots and catch-up


def test_catch_up_slot_inside_window_fires_once(scheduler, commands):
    # rumor-action-trigger: 21:45 daily, catch_up 1h; the launcher was down until 22:30.
    scheduler.tick(ct(2026, 10, 28, 22, 30))
    (command,) = jobs(commands, "rumor-action-trigger")
    assert command["trigger"] == "schedule"
    assert command["dedupe_key"] == "rumor-action-trigger@2026-10-29T02:45:00Z"
    assert command["scheduled_for"] == datetime(2026, 10, 29, 2, 45, tzinfo=UTC)
    assert command["expires_at"] == datetime(2026, 10, 29, 3, 45, tzinfo=UTC)

    scheduler.tick(ct(2026, 10, 28, 22, 31))
    assert len(jobs(commands, "rumor-action-trigger")) == 1


def test_catch_up_slot_outside_window_never_fires(scheduler, commands, state):
    scheduler.tick(ct(2026, 10, 28, 22, 46))
    assert jobs(commands, "rumor-action-trigger") == []
    assert state.rows("rumor-action-trigger") == []


def test_zero_catch_up_fires_on_time_slot_only(scheduler, commands):
    scheduler.tick(ct(2026, 10, 28, 10, 31))
    assert [c["dedupe_key"] for c in jobs(commands, "rumor-syncer")] == [
        "rumor-syncer@2026-10-28T15:30:00Z"
    ]
    commands.clear()
    # 29 minutes after the 10:30 slot: past the grace, so a catch_up 0 job waits for 11:00.
    scheduler.tick(ct(2026, 10, 28, 11, 29))
    assert jobs(commands, "rumor-syncer") == []


def test_slot_expires_at_the_end_of_its_window(scheduler, commands):
    scheduler.tick(ct(2026, 10, 28, 10, 30, 30))
    (command,) = jobs(commands, "rumor-syncer")
    assert command["expires_at"] == ct(2026, 10, 28, 10, 32)  # catch_up 0 -> SLOT_GRACE


def test_duplicate_publish_returns_the_existing_run(scheduler, commands):
    kwargs = dict(
        trigger="schedule",
        dedupe_key="rumor-syncer@x",
        scheduled_for=ct(2026, 10, 28, 10, 30),
        expires_at=None,
    )
    first = scheduler._publish("rumor-syncer", **kwargs)
    assert scheduler._publish("rumor-syncer", **kwargs) == first
    assert len(commands) == 1


def test_run_ticks_until_stopped_and_survives_a_failed_tick(scheduler):
    stop = threading.Event()
    calls = []

    def tick():
        calls.append(1)
        if len(calls) == 2:
            stop.set()
        raise RuntimeError("database away")

    scheduler.tick = tick
    scheduler.run(stop, poll_seconds=0)
    assert len(calls) == 2


def test_long_outage_fires_only_latest_slot(scheduler, commands):
    # trade-inbox runs every two hours from 08:00; down since 07:00, back at 12:10.
    scheduler.tick(ct(2026, 10, 28, 12, 10))
    assert [c["dedupe_key"] for c in jobs(commands, "trade-inbox")] == [
        "trade-inbox@2026-10-28T17:00:00Z"
    ]


# DST


def test_chain_slot_follows_chicago_across_fall_back(scheduler, state):
    schedule = scheduler.catalog.chains["tuesday-weekly"].schedule
    assert latest_slot(schedule, ct(2026, 10, 27, 16, 5)) == datetime(
        2026, 10, 27, 21, 0, tzinfo=UTC
    )
    assert latest_slot(schedule, ct(2026, 11, 3, 16, 5)) == datetime(
        2026, 11, 3, 22, 0, tzinfo=UTC
    )

    scheduler.tick(ct(2026, 11, 3, 16, 5))
    assert root(state).scheduled_for == datetime(2026, 11, 3, 22, 0, tzinfo=UTC)


def test_repeated_fall_back_hour_gets_distinct_keys_for_wildcard_hours():
    every_half_hour = Schedule(cron="*/30 * * * *", tz="America/Chicago")
    keys = {
        slot_key(
            "job", latest_slot(every_half_hour, datetime(2026, 11, 1, h, m, tzinfo=UTC))
        )
        for h, m in ((6, 40), (7, 10), (7, 40))
    }
    assert keys == {
        "job@2026-11-01T06:30:00Z",  # 01:30 CDT
        "job@2026-11-01T07:00:00Z",  # 01:00 CST
        "job@2026-11-01T07:30:00Z",  # 01:30 CST
    }


def test_fixed_hour_slot_fires_once_in_repeated_hour():
    daily = Schedule(cron="30 1 * * *", tz="America/Chicago")
    first_pass = datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
    assert latest_slot(daily, datetime(2026, 11, 1, 6, 40, tzinfo=UTC)) == first_pass
    assert latest_slot(daily, datetime(2026, 11, 1, 7, 40, tzinfo=UTC)) == first_pass


def test_slot_in_skipped_spring_forward_hour_fires_once():
    daily = Schedule(cron="30 2 * * *", tz="America/Chicago")
    slots = {
        latest_slot(daily, datetime(2026, 3, 8, h, m, tzinfo=UTC))
        for h, m in ((8, 1), (8, 40), (9, 30), (20, 0))
    }
    assert slots == {datetime(2026, 3, 8, 8, 0, tzinfo=UTC)}  # 03:00 CDT


# Seasons


def nfl_schedule_2026(season: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [2026, 2026, 2026, 2026],
            "game_type": ["REG", "REG", "REG", "REG"],
            "week": [1, 1, 18, 18],
            "gameday": ["2026-09-09", "2026-09-14", "2027-01-09", "2027-01-10"],
        }
    )


def test_season_window_covers_tuesday_before_week_one_and_after_end_week():
    calendar = NflSeasonCalendar(load=nfl_schedule_2026)
    regular = Season(start=1, end=18)
    assert not calendar.in_season(regular, date(2026, 9, 1))
    assert calendar.in_season(regular, date(2026, 9, 8))
    assert calendar.in_season(regular, date(2026, 12, 1))
    assert calendar.in_season(regular, date(2027, 1, 12))
    assert not calendar.in_season(regular, date(2027, 1, 13))
    assert not calendar.in_season(regular, date(2027, 3, 2))


def test_failed_nfl_schedule_read_is_cached_briefly():
    now = [datetime(2026, 10, 27, tzinfo=UTC)]
    loads = []

    def broken(season):
        loads.append(season)
        raise RuntimeError("404")

    calendar = NflSeasonCalendar(load=broken, clock=lambda: now[0])
    regular = Season(start=1, end=18)
    for minutes in (0, 1, 4):
        now[0] = datetime(2026, 10, 27, tzinfo=UTC) + timedelta(minutes=minutes)
        with pytest.raises(RuntimeError):
            calendar.in_season(regular, date(2026, 10, 27))
    assert loads == [2026]
    now[0] += timedelta(minutes=2)
    with pytest.raises(RuntimeError):
        calendar.in_season(regular, date(2026, 10, 27))
    assert loads == [2026, 2026]


def test_chain_outside_season_does_nothing(state, commands):
    scheduler = Scheduler(
        load_catalog(FIXTURE),
        state,
        fake_publish_run(state, commands),
        seasons=NflSeasonCalendar(load=nfl_schedule_2026),
    )
    scheduler.tick(ct(2027, 3, 2, 16, 1))
    assert [r for r in state.runs.values() if r.job_name == "tuesday-weekly"] == []


def test_unreadable_nfl_schedule_fails_open(state, commands, caplog):
    def broken(season):
        raise RuntimeError("404")

    scheduler = Scheduler(
        load_catalog(FIXTURE),
        state,
        fake_publish_run(state, commands),
        seasons=NflSeasonCalendar(load=broken),
    )
    with caplog.at_level(logging.WARNING, logger="LauncherScheduler"):
        scheduler.tick(ct(2026, 10, 27, 16, 1))
    assert root(state).status == "running"
    assert "firing the slot anyway" in caplog.text


# Pauses


def test_pause_suppresses_slot_with_one_audit_row(scheduler, commands, state):
    state.overrides["rumor-syncer"] = None
    scheduler.tick(ct(2026, 10, 28, 10, 30, 30))
    scheduler.tick(ct(2026, 10, 28, 10, 31))
    assert jobs(commands, "rumor-syncer") == []
    (skipped,) = state.rows("rumor-syncer")
    assert skipped.status == "skipped"
    assert end_reason(state, skipped) == "paused"


def test_expired_snooze_publishes(scheduler, commands, state):
    state.overrides["rumor-syncer"] = ct(2026, 10, 28, 10, 0)
    scheduler.tick(ct(2026, 10, 28, 10, 31))
    assert len(jobs(commands, "rumor-syncer")) == 1


def test_paused_chain_skips_its_root(scheduler, commands, state):
    state.overrides["tuesday-weekly"] = ct(2026, 10, 28, 0, 0)
    scheduler.tick(ct(2026, 10, 27, 16, 1))
    (skipped,) = state.rows("tuesday-weekly")
    assert (skipped.status, end_reason(state, skipped)) == ("skipped", "paused")
    assert jobs(commands, trigger="chain") == []


# Chains


@pytest.fixture
def tuesday(scheduler, state, commands):
    scheduler.tick(ct(2026, 10, 27, 16, 0, 30))
    chain_root = root(state)
    assert [c["job"] for c in jobs(commands, trigger="chain")] == ["sync-league"]
    command = jobs(commands, "sync-league")[0]
    assert command["dedupe_key"] == f"sync-league@{chain_root.id}"
    assert command["chain_root_id"] == command["parent_run_id"] == chain_root.id
    # No `latest`: the step is worth starting for as long as the chain may run.
    assert command["expires_at"] == chain_root.scheduled_for + timedelta(hours=8)
    return chain_root


def test_success_edge_failure_skips_only_that_branch(
    scheduler, state, commands, tuesday
):
    finish(state, tuesday, "sync-league", "failure", attempt=2)
    scheduler.tick(ct(2026, 10, 27, 16, 20))
    for job in ("metrics-extractor", "sync-actual-scores"):
        assert step(state, tuesday, job).status == "skipped"
        assert end_reason(state, step(state, tuesday, job)) == "dependency_failed"
    # projections-refresh follows scores on `finish`, so the acting half still runs.
    assert [c["job"] for c in jobs(commands, trigger="chain")] == [
        "sync-league",
        "projections-refresh",
    ]


def test_failure_with_attempts_left_is_still_pending(
    scheduler, state, commands, tuesday
):
    finish(state, tuesday, "sync-league", "failure", attempt=1)
    scheduler.tick(ct(2026, 10, 27, 16, 20))
    assert [c["job"] for c in jobs(commands, trigger="chain")] == ["sync-league"]
    assert "metrics-extractor" not in state.chain_steps(tuesday.id)


def test_finish_edge_starts_after_parent_failure(scheduler, state, commands, tuesday):
    for job in ("sync-league", "metrics-extractor", "sync-actual-scores"):
        finish(state, tuesday, job, "success")
        scheduler.tick(ct(2026, 10, 27, 16, 30))
    finish(state, tuesday, "projections-refresh", "failure")
    scheduler.tick(ct(2026, 10, 27, 17, 0))
    assert jobs(commands, trigger="chain")[-1]["job"] == "lineup-optimizer"


def test_latest_starts_step_while_parent_hangs(scheduler, state, commands, tuesday):
    for job in ("sync-league", "metrics-extractor", "sync-actual-scores"):
        finish(state, tuesday, job, "success")
        scheduler.tick(ct(2026, 10, 27, 16, 30))
    step(state, tuesday, "projections-refresh").status = "running"
    scheduler.tick(ct(2026, 10, 27, 19, 14))
    assert jobs(commands, "lineup-optimizer") == []
    scheduler.tick(ct(2026, 10, 27, 19, 15))
    (lineup,) = jobs(commands, "lineup-optimizer", trigger="chain")
    assert lineup["expires_at"] == ct(2026, 10, 27, 19, 45)


def test_max_duration_skips_remaining_steps(scheduler, state, commands, tuesday):
    for job in (
        "sync-league",
        "metrics-extractor",
        "sync-actual-scores",
        "projections-refresh",
    ):
        finish(state, tuesday, job, "success")
        scheduler.tick(ct(2026, 10, 27, 16, 30))
    step(state, tuesday, "lineup-optimizer").status = "running"
    scheduler.tick(ct(2026, 10, 28, 0, 1))
    for job in ("waiver-trigger", "rumor-action-trigger"):
        assert end_reason(state, step(state, tuesday, job)) == "max_duration"
    assert tuesday.status == "timeout"
    assert step(state, tuesday, "lineup-optimizer").status == "running"


def test_chain_root_finalizes(scheduler, state, commands, tuesday):
    for job in scheduler.catalog.chains["tuesday-weekly"].steps:
        finish(state, tuesday, job, "success")
        scheduler.tick(ct(2026, 10, 27, 16, 30))
    assert tuesday.status == "success"


def test_chain_root_with_a_failed_step_finalizes_as_failure(scheduler, state, tuesday):
    for job in scheduler.catalog.chains["tuesday-weekly"].steps:
        finish(
            state, tuesday, job, "failure" if job == "waiver-trigger" else "success", 3
        )
        scheduler.tick(ct(2026, 10, 27, 16, 30))
    assert tuesday.status == "failure"


def test_paused_step_is_skipped_and_satisfies_finish_edge(
    scheduler, state, commands, tuesday
):
    state.overrides["waiver-trigger"] = None
    for job in (
        "sync-league",
        "metrics-extractor",
        "sync-actual-scores",
        "projections-refresh",
        "lineup-optimizer",
    ):
        finish(state, tuesday, job, "success")
        scheduler.tick(ct(2026, 10, 27, 16, 30))
    assert end_reason(state, step(state, tuesday, "waiver-trigger")) == "paused"
    assert jobs(commands, trigger="chain")[-1]["job"] == "rumor-action-trigger"


# Manual runs


def test_manual_step_without_downstream_starts_nothing(scheduler, state, commands):
    state.insert(
        RunRow("manual-1", "lineup-optimizer", "success"), dedupe_key=str(uuid.uuid4())
    )
    scheduler.tick(ct(2026, 10, 28, 10, 0))
    assert jobs(commands, "waiver-trigger") == []


def test_manual_step_with_downstream_runs_its_descendants(scheduler, state, commands):
    now = ct(2026, 10, 28, 10, 0)
    root_id, run_id = scheduler.start_chain(
        "lineup-optimizer", params={"dry_run": True}, requested_by="web", now=now
    )
    (command,) = jobs(commands, "lineup-optimizer")
    assert command["run_id"] == run_id
    assert command["params"] == {"dry_run": True}
    assert command["downstream"] is True
    assert command["expires_at"] is None  # publish_run applies manual_queue_ttl
    assert command["chain_root_id"] == root_id

    state.runs[run_id].status = "success"
    scheduler.tick(now + timedelta(minutes=5))
    finish(state, state.runs[root_id], "waiver-trigger", "success")
    scheduler.tick(now + timedelta(minutes=10))
    finish(state, state.runs[root_id], "rumor-action-trigger", "success")
    scheduler.tick(now + timedelta(minutes=15))

    in_chain = [c["job"] for c in commands if c["chain_root_id"] == root_id]
    assert in_chain == ["lineup-optimizer", "waiver-trigger", "rumor-action-trigger"]
    assert state.runs[root_id].status == "success"


def test_downstream_needs_a_chain_step(scheduler):
    with pytest.raises(ValueError, match="0 chains"):
        scheduler.start_chain("rumor-syncer")


# A chain step dedupes the job's standalone slot that day


def test_chain_run_covers_standalone_slots_that_day(
    scheduler, state, commands, tuesday
):
    scheduler.tick(ct(2026, 10, 27, 18, 0, 30))
    scheduler.tick(ct(2026, 10, 27, 21, 45, 30))
    for job in ("projections-refresh", "rumor-action-trigger"):
        assert jobs(commands, job, trigger="schedule") == []
        (standalone,) = [r for r in state.rows(job) if r.chain_root_id is None]
        assert (standalone.status, end_reason(state, standalone)) == (
            "skipped",
            "chain_step",
        )

    scheduler.tick(ct(2026, 10, 28, 18, 0, 30))
    assert len(jobs(commands, "projections-refresh", trigger="schedule")) == 1


def test_skipped_chain_step_leaves_standalone_slot(scheduler, state, commands, tuesday):
    tuesday.status = "timeout"
    state.record_skipped(
        job_name="rumor-action-trigger",
        trigger_type="chain",
        dedupe_key=f"rumor-action-trigger@{tuesday.id}",
        scheduled_for=ct(2026, 10, 27, 21, 0),
        end_reason="max_duration",
        chain_root_id=tuesday.id,
    )
    scheduler.tick(ct(2026, 10, 27, 21, 45, 30))
    assert len(jobs(commands, "rumor-action-trigger", trigger="schedule")) == 1


# The tuesday-weekly chain keeps fantasy-infra's documented CronJob order
# (docs/cronjob-schedule-order.md): 16:00 metrics, 17:05 scores, 18:00 projections,
# 19:15 lineup, 20:30 waivers, 21:45 rumor actions.


def test_tuesday_weekly_edges_match_infra_schedule_order():
    chain = load_catalog(FIXTURE).chains["tuesday-weekly"]
    assert (chain.schedule.cron, chain.schedule.tz) == ("0 16 * * 2", "America/Chicago")
    edges = {
        name: [(dep.step, dep.on) for dep in step.after]
        for name, step in chain.steps.items()
    }
    assert edges == {
        "sync-league": [],
        "metrics-extractor": [("sync-league", "success")],
        "sync-actual-scores": [("metrics-extractor", "success")],
        "projections-refresh": [("sync-actual-scores", "finish")],
        "lineup-optimizer": [("projections-refresh", "finish")],
        "waiver-trigger": [("lineup-optimizer", "finish")],
        "rumor-action-trigger": [("waiver-trigger", "finish")],
    }
    assert {name: step.latest for name, step in chain.steps.items() if step.latest} == {
        "lineup-optimizer": "19:15",
        "waiver-trigger": "20:30",
        "rumor-action-trigger": "21:45",
    }


def test_tick_records_heartbeat_with_queue_depths(scheduler, state, monkeypatch):
    queue_depths = {"job_runs": 3, "job_runs.dlq": 1}
    monkeypatch.setattr("src.launcher.scheduler.read_queue_depths", lambda: queue_depths)
    state.heartbeats = []
    state.record_heartbeat = lambda depths: state.heartbeats.append(depths)

    scheduler.tick(ct(2026, 10, 27, 16, 0, 30))

    assert state.heartbeats == [queue_depths]


def test_postgres_state_upserts_launcher_heartbeat():
    class RecordingCursor:
        query = ""
        params = None

        def execute(self, query, params=None):
            self.query = query
            self.params = params

    cursor = RecordingCursor()

    @contextmanager
    def cursor_context():
        yield cursor

    PostgresSchedulerState(cursor_context).record_heartbeat(
        {"job_runs": 3, "job_runs.dlq": 1}
    )

    assert "INSERT INTO launcher_heartbeat" in cursor.query
    assert "ON CONFLICT(id) DO UPDATE" in cursor.query
    assert json.loads(cursor.params[0]) == {"job_runs": 3, "job_runs.dlq": 1}
