"""The scheduler's kickoff-driven starts (catalog ``kickoffs``), on a frozen clock with a
fake nflverse schedule."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from src.launcher.catalog import Job, load_catalog
from src.launcher.scheduler import (
    KICKOFF_CACHE_TTL,
    NflSeasonCalendar,
    Scheduler,
    slot_key,
)
from tests.test_launcher_scheduler import FIXTURE, FakeState, fake_publish_run

EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
LEAD = timedelta(minutes=20)

# (week, gameday, gametime ET or None, game_type). Times as nflverse lists them.
GAMES_2026 = [
    (1, "2026-09-09", "20:20", "REG"),  # Wednesday opener
    (1, "2026-09-10", "20:20", "REG"),
    (1, "2026-09-13", "13:00", "REG"),
    (1, "2026-09-13", "13:00", "REG"),
    (1, "2026-09-13", "13:00", "REG"),
    (1, "2026-09-13", "16:05", "REG"),
    (1, "2026-09-13", "16:25", "REG"),
    (1, "2026-09-13", "20:20", "REG"),
    (1, "2026-09-14", "20:15", "REG"),
    (5, "2026-10-11", "09:30", "REG"),  # London
    (12, "2026-11-25", "20:20", "REG"),  # Thanksgiving Eve, a Wednesday
    (12, "2026-11-26", "12:30", "REG"),
    (12, "2026-11-26", "16:30", "REG"),
    (12, "2026-11-26", "20:20", "REG"),
    (12, "2026-11-27", "15:00", "REG"),  # Black Friday
    (16, "2026-12-25", "13:00", "REG"),  # Christmas, a Friday
    (18, "2027-01-09", None, "REG"),  # time not set yet
    (18, "2027-01-10", "13:00", "REG"),
    (19, "2027-01-16", "16:30", "WC"),  # playoffs: not a regular-season kickoff
]
GAME_DAYS = sorted(
    {date.fromisoformat(day) for _, day, _, kind in GAMES_2026 if kind == "REG"}
)


def schedule_frame(games, season: int = 2026) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "season": [season] * len(games),
            "week": [g[0] for g in games],
            "gameday": [g[1] for g in games],
            "gametime": [g[2] for g in games],
            "game_type": [g[3] for g in games],
        },
        schema={
            "season": pl.Int32,
            "week": pl.Int32,
            "gameday": pl.Utf8,
            "gametime": pl.Utf8,
            "game_type": pl.Utf8,
        },
    )


def et(*args: int) -> datetime:
    """A frozen Eastern wall-clock time, as UTC."""
    return datetime(*args, tzinfo=EASTERN).astimezone(UTC)


def kickoff(day: str, clock: str) -> datetime:
    return (
        datetime.fromisoformat(f"{day}T{clock}").replace(tzinfo=EASTERN).astimezone(UTC)
    )


class Clock:
    def __init__(self, now: datetime):
        self.now = now

    def __call__(self) -> datetime:
        return self.now


class Loader:
    """A fake nflverse read: ``games`` can change and ``broken`` makes it raise."""

    def __init__(self, games=GAMES_2026):
        self.games = list(games)
        self.broken = False
        self.loads = 0

    def __call__(self, season: int) -> pl.DataFrame:
        self.loads += 1
        if self.broken:
            raise RuntimeError("404")
        return schedule_frame(self.games if season == 2026 else [], season)


def live_games_job(**kickoffs) -> Job:
    return Job.model_validate(
        {
            "class": "service",
            "profile": "core",
            "command": ["python", "-m", "src.ingestion.live_games"],
            "deadline": "8h",
            "kickoffs": {"lead": "20m", "seasons": ["regular"], **kickoffs},
        }
    )


@pytest.fixture
def state():
    return FakeState()


@pytest.fixture
def commands():
    return []


@pytest.fixture
def clock():
    return Clock(et(2026, 9, 1))


@pytest.fixture
def loader():
    return Loader()


@pytest.fixture
def scheduler(state, commands, clock, loader):
    # The fixture's profiles and seasons, with live-games as its only job.
    catalog = load_catalog(FIXTURE)
    catalog.jobs = {"live-games": live_games_job()}
    catalog.chains = {}
    return Scheduler(
        catalog,
        state,
        fake_publish_run(state, commands),
        seasons=NflSeasonCalendar(load=loader, clock=clock),
        clock=clock,
    )


def tick_at(scheduler, clock, now: datetime) -> None:
    clock.now = now
    scheduler.tick(now)


def tick_through(
    scheduler, clock, start: datetime, end: datetime, step=timedelta(minutes=1)
):
    now = start
    while now <= end:
        tick_at(scheduler, clock, now)
        now += step


def starts(commands) -> list[datetime]:
    return [c["scheduled_for"] for c in commands if c["job"] == "live-games"]


def test_every_regular_season_kickoff_gets_one_start_in_the_30_minutes_before(
    scheduler, clock, commands
):
    for day in GAME_DAYS + [date(2026, 9, 15)]:  # plus a Tuesday with no game
        start = datetime(day.year, day.month, day.day, tzinfo=EASTERN).astimezone(UTC)
        tick_through(
            scheduler, clock, start, start + timedelta(days=1) - timedelta(minutes=1)
        )

    fired = starts(commands)
    kickoffs = {
        kickoff(day, t) for _, day, t, kind in GAMES_2026 if kind == "REG" and t
    }
    for k in kickoffs:
        assert [s for s in fired if k - timedelta(minutes=30) <= s < k] == [k - LEAD], k
    timed = [s for s in fired if s.astimezone(EASTERN).date() != date(2027, 1, 9)]
    assert sorted(timed) == sorted(k - LEAD for k in kickoffs)
    assert len(fired) == len(set(fired))
    assert not [s for s in fired if s.astimezone(EASTERN).date() == date(2026, 9, 15)]
    assert all(c["trigger"] == "schedule" for c in commands)


def test_a_kickoff_start_carries_its_dedupe_key_and_expiry(scheduler, clock, commands):
    tick_at(scheduler, clock, et(2026, 9, 9, 20, 0, 5))
    tick_at(scheduler, clock, et(2026, 9, 9, 20, 0, 20))
    [command] = commands
    slot = et(2026, 9, 9, 20, 0)
    assert command["dedupe_key"] == slot_key("live-games", slot)
    assert command["scheduled_for"] == slot
    assert command["expires_at"] == slot + timedelta(hours=3)


def test_a_flexed_kickoff_is_picked_up_on_the_next_reread(
    scheduler, clock, commands, loader
):
    tick_at(scheduler, clock, et(2026, 9, 13, 8))
    loader.games = [
        (w, d, "16:25" if (d, t) == ("2026-09-13", "20:20") else t, k)
        for w, d, t, k in GAMES_2026
    ]
    clock_start = et(2026, 9, 13, 8) + KICKOFF_CACHE_TTL
    tick_through(scheduler, clock, clock_start, et(2026, 9, 13, 23, 59))
    assert et(2026, 9, 13, 20, 0) not in starts(commands)
    assert sorted(starts(commands)) == [
        et(2026, 9, 13, 12, 40),
        et(2026, 9, 13, 15, 45),
        et(2026, 9, 13, 16, 5),
    ]


def test_a_failed_reread_keeps_the_last_good_kickoffs(
    scheduler, clock, commands, loader, caplog
):
    tick_at(scheduler, clock, et(2026, 9, 13, 8))
    loader.broken = True
    with caplog.at_level(logging.WARNING, logger="LauncherScheduler"):
        tick_through(scheduler, clock, et(2026, 9, 13, 12), et(2026, 9, 13, 13))
    assert starts(commands) == [et(2026, 9, 13, 12, 40)]
    assert "using the copy from" in caplog.text


def test_an_unreadable_schedule_starts_on_the_fallback_cadence(
    scheduler, clock, commands, loader, caplog
):
    loader.broken = True
    with caplog.at_level(logging.WARNING, logger="LauncherScheduler"):
        tick_through(scheduler, clock, et(2026, 9, 13, 11, 59), et(2026, 9, 13, 13, 1))
    # The first tick catches up the latest cadence slot, as for a kickoff slot.
    assert starts(commands) == [
        et(2026, 9, 13, 11, 30),
        et(2026, 9, 13, 12),
        et(2026, 9, 13, 12, 30),
        et(2026, 9, 13, 13),
    ]
    assert caplog.text.count("Kickoff schedule unavailable") == 1

    loader.broken = False
    tick_through(
        scheduler,
        clock,
        et(2026, 9, 13, 13, 1) + timedelta(minutes=5),
        et(2026, 9, 13, 16, 30),
    )
    # Back on kickoffs, the 13:00 game's slot catches up; overlap skips it while the
    # cadence run is still up.
    assert starts(commands)[4:] == [
        et(2026, 9, 13, 12, 40),
        et(2026, 9, 13, 15, 45),
        et(2026, 9, 13, 16, 5),
    ]


def test_a_game_without_a_time_falls_back_on_its_day_only(scheduler, clock, commands):
    tick_through(
        scheduler,
        clock,
        et(2027, 1, 8, 23),
        et(2027, 1, 10, 13),
        step=timedelta(minutes=5),
    )
    by_day: dict[date, list[datetime]] = {}
    for slot in starts(commands):
        by_day.setdefault(slot.astimezone(EASTERN).date(), []).append(slot)
    assert date(2027, 1, 8) not in by_day
    assert by_day[date(2027, 1, 9)] == [
        et(2027, 1, 9, 0) + timedelta(minutes=30 * i) for i in range(48)
    ]
    # Sunday has only its timed kickoff.
    assert by_day[date(2027, 1, 10)] == [et(2027, 1, 10, 12, 40)]


def test_a_missed_start_catches_up_once_within_catch_up(scheduler, clock, commands):
    tick_at(scheduler, clock, et(2026, 9, 10, 21, 0))  # an hour into Thursday's game
    assert starts(commands) == [et(2026, 9, 10, 20, 0)]
    tick_at(scheduler, clock, et(2026, 9, 10, 21, 1))
    assert len(commands) == 1


def test_a_start_older_than_catch_up_never_fires(scheduler, clock, commands):
    tick_at(scheduler, clock, et(2026, 9, 10, 23, 1))
    assert commands == []


def test_after_an_outage_only_the_latest_slot_fires(scheduler, clock, commands):
    tick_at(scheduler, clock, et(2026, 9, 13, 16, 10))
    assert starts(commands) == [et(2026, 9, 13, 16, 5)]


def test_a_paused_job_skips_each_slot_once(scheduler, clock, commands, state):
    state.overrides["live-games"] = None
    tick_at(scheduler, clock, et(2026, 9, 9, 20, 1))
    tick_at(scheduler, clock, et(2026, 9, 9, 20, 2))
    assert commands == []
    [row] = state.rows("live-games")
    assert (row.status, row.end_reason) == ("skipped", "paused")


def test_the_offseason_starts_nothing(scheduler, clock, commands):
    tick_through(
        scheduler,
        clock,
        et(2027, 4, 5, 0),
        et(2027, 4, 5, 23),
        step=timedelta(minutes=10),
    )
    assert commands == []


def test_next_job_run_is_the_next_kickoff_slot(scheduler, clock):
    clock.now = et(2026, 9, 10, 12)
    assert scheduler.next_job_run("live-games", clock.now) == et(2026, 9, 10, 20, 0)
    assert scheduler.next_job_run("live-games", et(2027, 1, 20)) is None


def test_next_job_run_during_an_outage_is_the_next_fallback_slot(
    scheduler, clock, loader
):
    loader.broken = True
    clock.now = et(2026, 9, 10, 12, 10)
    assert scheduler.next_job_run("live-games", clock.now) == et(2026, 9, 10, 12, 30)
