"""Launcher scheduler: publishes cron slots on time, once, and advances named chains.

Every tick reads its state from ``run_log`` and ``job_overrides`` alone, so a restart loses
nothing:

- **Slots.** A schedule's due slot is croniter's latest slot at or before now, in the
  schedule's ``tz``. It fires while ``now - slot <= max(catch_up, SLOT_GRACE)``; older
  missed slots never fire, like a CronJob's ``startingDeadlineSeconds``. The dedupe key is
  ``job@<slot in UTC>``, so the repeated fall-back hour gets distinct keys; a fixed-hour
  slot still fires only on that hour's first pass, as cron does.
- **Seasons.** A slot with ``seasons`` fires only when its local date is within
  ``SEASON_LEAD`` before the first game of the window's start week and ``SEASON_TAIL``
  after the last game of its end week (nflverse game dates), so the Tuesday before week 1
  and the Tuesday after the end week both run. An unreadable NFL schedule fails open: every
  chained job already exits 0 in the offseason, while a skipped in-season slot loses a week.
- **Pauses.** A ``job_overrides`` row with ``paused_until`` NULL or in the future turns a
  due slot into a ``skipped`` run with ``end_reason='paused'``, written once per slot.
- **Chains.** A chain root is a ``run_log`` row (``job_name`` = chain, ``trigger_type`` =
  ``chain``, ``chain_root_id`` = its own id) that stays ``running`` while its steps run.
  Steps are published with ``dedupe_key=job@<root id>``. A step starts when every ``after``
  edge is met: ``success`` needs the parent's success, ``finish`` any final status. A failed
  ``success`` parent skips only that branch (``dependency_failed``); a skipped step still
  satisfies ``finish`` edges, so Tuesday's acting steps run even when ingestion fails, as
  they did under the CronJobs. A failure or timeout with attempts left (``max_attempts``)
  is still pending, since the watcher retries the same row. ``latest`` starts a step at
  that local time on the root's date even if a parent is still running. Past
  ``max_duration`` every step not yet started is skipped and the root ends ``timeout``.
- **Standalone slots.** A job's own slot is skipped (``chain_step``) on a day when a chain
  run already covers it: the step has a non-skipped run, or the chain is still running.
- **Manual runs.** ``start_chain`` runs one step with ``downstream`` semantics: a root
  scoped to that step and its descendants. A manual run without ``downstream`` has no
  ``chain_root_id``, so no edges are ever evaluated for it.

Runs are requested through ``launcher.publish.publish_run`` (injectable for tests), which
writes the queued run_log row and outbox row; a used dedupe key comes back as ``duplicate``.
Each command's ``expires_at`` is set here, the only place that knows it: a slot's catch-up
window, a chain step's ``latest`` plus ``LATEST_GRACE``, or else its chain's
``max_duration``. ``python -m src.launcher`` runs ``Scheduler.run`` on its own thread.
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from graphlib import TopologicalSorter
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo

import polars as pl
from croniter import croniter

from src.launcher.catalog import Catalog, Chain, ChainStep, Schedule, Season

logger = logging.getLogger("LauncherScheduler")

SLOT_GRACE = timedelta(minutes=2)
SEASON_LEAD = timedelta(days=7)
SEASON_TAIL = timedelta(days=2)
SEASON_CACHE_TTL = timedelta(hours=24)
# A failed nflverse read is cached briefly so ticks every few seconds don't each refetch.
SEASON_FAILURE_TTL = timedelta(minutes=5)
LATEST_GRACE = timedelta(minutes=30)
POLL_SECONDS = 15
FINAL_STATUSES = frozenset({"success", "failure", "timeout", "skipped", "cancelled"})
RETRYABLE_STATUSES = frozenset({"failure", "timeout"})

# publish_run's signature: (catalog, job, *, trigger, dedupe_key, ...) -> {run_id, status}.
Publisher = Callable[..., dict[str, str]]


@dataclass
class RunRow:
    id: str
    job_name: str
    status: str
    attempt: int = 1
    scheduled_for: datetime | None = None
    chain_root_id: str | None = None
    trigger: dict[str, Any] = field(default_factory=dict)


class SchedulerState(Protocol):
    def record_heartbeat(self, queue_depths: dict[str, int | None]) -> None: ...

    def run_exists(self, dedupe_key: str) -> bool: ...

    def paused(self, name: str, now: datetime) -> bool: ...

    def record_skipped(
        self,
        *,
        job_name: str,
        trigger_type: str,
        dedupe_key: str,
        scheduled_for: datetime,
        end_reason: str,
        chain_root_id: str | None = None,
    ) -> bool: ...

    def create_chain_root(
        self,
        *,
        chain: str,
        dedupe_key: str,
        scheduled_for: datetime,
        from_step: str | None = None,
        requested_by: str | None = None,
    ) -> RunRow | None: ...

    def active_chain_roots(self) -> list[RunRow]: ...

    def chain_roots_between(
        self, chain: str, start: datetime, end: datetime
    ) -> list[RunRow]: ...

    def chain_steps(self, root_id: str) -> dict[str, RunRow]: ...

    def finish_chain_root(
        self, root_id: str, status: str, end_reason: str | None
    ) -> None: ...


def slot_key(name: str, slot: datetime) -> str:
    return f"{name}@{slot.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"


def latest_slot(schedule: Schedule, now: datetime) -> datetime:
    """The schedule's latest slot at or before ``now``, in UTC."""
    tz = ZoneInfo(schedule.tz)
    slot = croniter(schedule.cron, now.astimezone(tz)).get_prev(datetime)
    # croniter matches a fixed-hour slot on both passes of a repeated fall-back hour.
    if "*" not in schedule.cron.split()[1]:
        while slot.astimezone(timezone.utc).astimezone(tz).fold:
            slot = croniter(schedule.cron, slot).get_prev(datetime)
    return slot.astimezone(timezone.utc)


class NflSeasonCalendar:
    """Season windows from nflverse game dates, cached per season."""

    def __init__(
        self,
        load: Callable[[int], pl.DataFrame] | None = None,
        clock: Callable[[], datetime] | None = None,
    ):
        self._load = load or _load_schedule_once
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._cache: dict[int, tuple[datetime, pl.DataFrame]] = {}
        self._failures: dict[int, tuple[datetime, Exception]] = {}

    def in_season(self, season: Season, day: date) -> bool:
        # A season runs September to early February, so January belongs to the prior year.
        year = day.year if day.month >= 3 else day.year - 1
        games = self._schedule(year).filter(
            (pl.col("season") == year) & (pl.col("game_type") == "REG")
        )
        first = games.filter(pl.col("week") == season.start)["gameday"]
        last = games.filter(pl.col("week") == season.end)["gameday"]
        if first.is_empty() or last.is_empty():
            return False
        start = date.fromisoformat(str(first.min())[:10]) - SEASON_LEAD
        end = date.fromisoformat(str(last.max())[:10]) + SEASON_TAIL
        return start <= day <= end

    def _schedule(self, year: int) -> pl.DataFrame:
        now = self._clock()
        cached = self._cache.get(year)
        if cached and now - cached[0] < SEASON_CACHE_TTL:
            return cached[1]
        failed = self._failures.get(year)
        if failed and now - failed[0] < SEASON_FAILURE_TTL:
            raise failed[1]
        try:
            schedule = self._load(year)
        except Exception as exc:
            self._failures[year] = (now, exc)
            raise
        self._failures.pop(year, None)
        self._cache[year] = (now, schedule)
        return schedule


def _publish_run(catalog: Catalog, job: str, **kwargs: Any) -> dict[str, str]:
    from src.launcher.publish import publish_run

    return publish_run(catalog, job, **kwargs)


def _load_schedule_once(season: int) -> pl.DataFrame:
    from src.nfl_schedule import load_schedule

    # One attempt: the tick must not stall, and a failure just fails open for this slot.
    return load_schedule(season, attempts=1)


class Scheduler:
    def __init__(
        self,
        catalog: Catalog,
        state: SchedulerState,
        publish: Publisher | None = None,
        seasons: NflSeasonCalendar | None = None,
        clock: Callable[[], datetime] | None = None,
    ):
        self.catalog = catalog
        self.state = state
        self.publish = publish or _publish_run
        self.seasons = seasons or NflSeasonCalendar()
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._order = {
            name: list(
                TopologicalSorter(
                    {
                        step: [dep.step for dep in s.after]
                        for step, s in chain.steps.items()
                    }
                ).static_order()
            )
            for name, chain in catalog.chains.items()
        }
        self._chains_by_job: dict[str, list[str]] = {}
        for name, chain in catalog.chains.items():
            for step in chain.steps:
                self._chains_by_job.setdefault(step, []).append(name)

    def tick(self, now: datetime | None = None) -> None:
        """Fire due chain slots, advance running chains, then fire due job slots."""
        now = now or self.clock()
        try:
            self.state.record_heartbeat(read_queue_depths())
        except Exception:
            logger.exception("Could not record launcher heartbeat")
        for name, chain in self.catalog.chains.items():
            if chain.schedule:
                self._guard(f"chain {name} slot", self._chain_slot, name, chain, now)
        for root in self.state.active_chain_roots():
            self._guard(f"chain run {root.id}", self._advance, root, now)
        for name, job in self.catalog.jobs.items():
            for schedule in job.schedules:
                self._guard(f"job {name} slot", self._job_slot, name, schedule, now)

    def run(self, stop: threading.Event, poll_seconds: float = POLL_SECONDS) -> None:
        """Ticks until ``stop`` is set; a failed tick (database away) is retried next poll."""
        while not stop.is_set():
            try:
                self.tick()
            except Exception:
                logger.exception("Scheduler tick failed")
            stop.wait(poll_seconds)

    def chain_for_step(self, job: str) -> str:
        """The one chain containing ``job``, for a manual run with ``downstream: true``."""
        chains = self._chains_by_job.get(job, [])
        if len(chains) != 1:
            raise ValueError(
                f"{job!r} is a step of {len(chains)} chains; downstream needs exactly one"
            )
        return chains[0]

    def start_chain(
        self,
        from_step: str,
        chain: str | None = None,
        params: dict[str, Any] | None = None,
        requested_by: str | None = None,
        trigger: str = "manual",
        now: datetime | None = None,
    ) -> tuple[str, str]:
        """Run ``from_step`` now, then its descendants by the chain's edges.

        Returns ``(chain_root_id, run_id)``. Steps upstream of ``from_step`` are out of
        scope, so their edges are ignored.
        """
        now = now or self.clock()
        chain = chain or self.chain_for_step(from_step)
        if from_step not in self.catalog.chains[chain].steps:
            raise ValueError(f"{from_step!r} is not a step of chain {chain!r}")
        root = self.state.create_chain_root(
            chain=chain,
            dedupe_key=str(uuid.uuid4()),
            scheduled_for=now,
            from_step=from_step,
            requested_by=requested_by,
        )
        if root is None:
            raise RuntimeError(f"chain run for {chain!r} could not be recorded")
        try:
            run_id = self._publish(
                from_step,
                trigger=trigger,
                dedupe_key=f"{from_step}@{root.id}",
                scheduled_for=now,
                chain_root_id=root.id,
                params=params,
                requested_by=requested_by,
                downstream=True,
                # A manual request: publish_run applies the catalog's manual_queue_ttl.
                expires_at=None,
            )
        except Exception:
            self.state.finish_chain_root(root.id, "failure", "publish_failed")
            raise
        return root.id, run_id

    def _guard(self, what: str, func: Callable[..., None], *args: Any) -> None:
        # One bad schedule or chain run must not stop every other slot in the tick.
        try:
            func(*args)
        except Exception:
            logger.exception("Scheduler failed on %s", what)

    def _due(
        self, name: str, schedule: Schedule, now: datetime
    ) -> tuple[datetime, str] | None:
        slot = latest_slot(schedule, now)
        if now - slot > _window(schedule):
            return None
        key = slot_key(name, slot)
        if self.state.run_exists(key) or not self._in_season(schedule, slot):
            return None
        if now - slot > SLOT_GRACE:
            logger.info("Catching up slot %s (%s late)", key, now - slot)
        return slot, key

    def _in_season(self, schedule: Schedule, slot: datetime) -> bool:
        if not schedule.seasons:
            return True
        day = slot.astimezone(ZoneInfo(schedule.tz)).date()
        try:
            return any(
                self.seasons.in_season(self.catalog.seasons[name], day)
                for name in schedule.seasons
            )
        except Exception as exc:
            logger.warning(
                "Season check failed for %s; firing the slot anyway: %s", day, exc
            )
            return True

    def _chain_slot(self, name: str, chain: Chain, now: datetime) -> None:
        assert chain.schedule is not None
        due = self._due(name, chain.schedule, now)
        if not due:
            return
        slot, key = due
        if self.state.paused(name, now):
            self._skip(name, "chain", key, slot, "paused")
            return
        root = self.state.create_chain_root(
            chain=name, dedupe_key=key, scheduled_for=slot
        )
        if root:
            logger.info("Started chain %s run %s for slot %s", name, root.id, key)

    def _job_slot(self, name: str, schedule: Schedule, now: datetime) -> None:
        due = self._due(name, schedule, now)
        if not due:
            return
        slot, key = due
        if self.state.paused(name, now):
            self._skip(name, "schedule", key, slot, "paused")
        elif self._chain_covers(name, schedule, slot):
            self._skip(name, "schedule", key, slot, "chain_step")
        else:
            self._publish(
                name,
                trigger="schedule",
                dedupe_key=key,
                scheduled_for=slot,
                expires_at=slot + _window(schedule),
            )

    def _chain_covers(self, job: str, schedule: Schedule, slot: datetime) -> bool:
        tz = ZoneInfo(schedule.tz)
        day = slot.astimezone(tz).date()
        start = datetime.combine(day, time(), tzinfo=tz)
        end = datetime.combine(day + timedelta(days=1), time(), tzinfo=tz)
        for chain in self._chains_by_job.get(job, []):
            for root in self.state.chain_roots_between(chain, start, end):
                if job not in self._scope(chain, root):
                    continue
                step = self.state.chain_steps(root.id).get(job)
                if step is None and root.status == "running":
                    return True
                if step is not None and step.status != "skipped":
                    return True
        return False

    def _scope(self, chain: str, root: RunRow) -> set[str]:
        from_step = root.trigger.get("from_step")
        if not from_step:
            return set(self.catalog.chains[chain].steps)
        scope = {from_step}
        for step in self._order[chain]:
            if any(
                dep.step in scope
                for dep in self.catalog.chains[chain].steps[step].after
            ):
                scope.add(step)
        return scope

    def _outcome(self, run: RunRow | None) -> str:
        """``pending``, ``success`` or ``finished`` (final, but not a success)."""
        if run is None or run.status not in FINAL_STATUSES:
            return "pending"
        if run.status == "success":
            return "success"
        if run.status in RETRYABLE_STATUSES and run.attempt < self._max_attempts(
            run.job_name
        ):
            return "pending"
        return "finished"

    def _max_attempts(self, job: str) -> int:
        return self.catalog.resolve(job).max_attempts

    def _advance(self, root: RunRow, now: datetime) -> None:
        name = root.trigger.get("chain") or root.job_name
        chain = self.catalog.chains.get(name)
        if chain is None:
            logger.warning(
                "Chain run %s names unknown chain %r; cancelling", root.id, name
            )
            self.state.finish_chain_root(root.id, "cancelled", "unknown_chain")
            return
        assert root.scheduled_for is not None
        scope = self._scope(name, root)
        runs = self.state.chain_steps(root.id)
        expired = now > root.scheduled_for + chain.max_duration
        from_step = root.trigger.get("from_step")
        for step_name in self._order[name]:
            if step_name not in scope or step_name in runs:
                continue
            step = chain.steps[step_name]
            key = f"{step_name}@{root.id}"
            if expired:
                runs[step_name] = self._skip_step(
                    step_name, key, root, now, "max_duration"
                )
                continue
            edges = [
                (dep.on, self._outcome(runs.get(dep.step)))
                for dep in step.after
                if dep.step in scope
            ]
            if any(on == "success" and outcome == "finished" for on, outcome in edges):
                runs[step_name] = self._skip_step(
                    step_name, key, root, now, "dependency_failed"
                )
                continue
            ready = all(
                outcome == "success" or (on == "finish" and outcome == "finished")
                for on, outcome in edges
            )
            latest = self._latest_at(chain, step, root.scheduled_for)
            if not ready:
                if latest is None or now < latest:
                    continue
                logger.info(
                    "Starting %s at its latest time %s; parents still pending",
                    key,
                    step.latest,
                )
            # A manual downstream run was asked for explicitly, so a pause doesn't stop it.
            if step_name != from_step and self.state.paused(step_name, now):
                runs[step_name] = self._skip_step(step_name, key, root, now, "paused")
                continue
            run_id = self._publish(
                step_name,
                trigger="chain",
                dedupe_key=key,
                scheduled_for=now,
                chain_root_id=root.id,
                expires_at=(
                    latest + LATEST_GRACE
                    if latest
                    else root.scheduled_for + chain.max_duration
                ),
            )
            runs[step_name] = RunRow(run_id, step_name, "queued", chain_root_id=root.id)
        if expired:
            logger.warning(
                "Chain run %s exceeded max_duration %s", root.id, chain.max_duration
            )
            self.state.finish_chain_root(root.id, "timeout", "max_duration")
        elif all(self._outcome(runs.get(step)) != "pending" for step in scope):
            succeeded = all(runs[step].status == "success" for step in scope)
            self.state.finish_chain_root(
                root.id, "success" if succeeded else "failure", None
            )

    def _latest_at(
        self, chain: Chain, step: ChainStep, root_time: datetime
    ) -> datetime | None:
        if not step.latest:
            return None
        tz = ZoneInfo(chain.schedule.tz if chain.schedule else "UTC")
        local = root_time.astimezone(tz)
        hour, minute = map(int, step.latest.split(":"))
        latest = datetime.combine(local.date(), time(hour, minute), tzinfo=tz)
        if latest < local:
            latest = datetime.combine(
                local.date() + timedelta(days=1), time(hour, minute), tzinfo=tz
            )
        return latest

    def _skip(
        self, name: str, trigger: str, key: str, slot: datetime, reason: str
    ) -> None:
        if self.state.record_skipped(
            job_name=name,
            trigger_type=trigger,
            dedupe_key=key,
            scheduled_for=slot,
            end_reason=reason,
        ):
            logger.info("Skipped %s (%s)", key, reason)

    def _skip_step(
        self, step: str, key: str, root: RunRow, now: datetime, reason: str
    ) -> RunRow:
        self.state.record_skipped(
            job_name=step,
            trigger_type="chain",
            dedupe_key=key,
            scheduled_for=now,
            end_reason=reason,
            chain_root_id=root.id,
        )
        logger.info("Skipped chain step %s (%s)", key, reason)
        return RunRow("", step, "skipped", chain_root_id=root.id)

    def _publish(
        self,
        job: str,
        *,
        trigger: str,
        dedupe_key: str,
        scheduled_for: datetime,
        chain_root_id: str | None = None,
        params: dict[str, Any] | None = None,
        requested_by: str | None = None,
        downstream: bool = False,
        expires_at: datetime | None,
    ) -> str:
        result = self.publish(
            self.catalog,
            job,
            trigger=trigger,
            params=params,
            requested_by=requested_by,
            dedupe_key=dedupe_key,
            parent_run_id=chain_root_id,
            chain_root_id=chain_root_id,
            downstream=downstream,
            scheduled_for=scheduled_for,
            expires_at=expires_at,
        )
        if result["status"] == "duplicate":
            logger.info("Run %s already exists; not published again", dedupe_key)
        else:
            logger.info("Published %s run %s (%s)", job, result["run_id"], dedupe_key)
        return result["run_id"]


def _window(schedule: Schedule) -> timedelta:
    """How long after its slot a missed run still fires, and its queue expiry."""
    return max(schedule.catch_up or timedelta(0), SLOT_GRACE)


_ROW_COLUMNS = "id,job_name,status,attempt,scheduled_for,chain_root_id,trigger"
BROKER_TIMEOUT_SECONDS = 5


def read_queue_depths() -> dict[str, int | None]:
    """Read broker queue depths without creating queues or consuming messages."""
    import pika
    from src.queue_contracts import AMQP_URL, JOB_RUNS

    connection = None
    queues = (JOB_RUNS, f"{JOB_RUNS}.dlq")
    depths: dict[str, int | None] = {queue: None for queue in queues}
    try:
        params = pika.URLParameters(AMQP_URL)
        params.socket_timeout = BROKER_TIMEOUT_SECONDS
        params.blocked_connection_timeout = BROKER_TIMEOUT_SECONDS
        connection = pika.BlockingConnection(params)
        for queue in queues:
            channel = connection.channel()
            try:
                result = channel.queue_declare(queue=queue, passive=True)
                depths[queue] = result.method.message_count
            except pika.exceptions.ChannelClosedByBroker:
                pass
            finally:
                if channel.is_open:
                    channel.close()
    except Exception:
        logger.warning("Launcher queue depths unavailable", exc_info=True)
    finally:
        if connection is not None and connection.is_open:
            try:
                connection.close()
            except Exception:
                logger.warning("Could not close RabbitMQ connection cleanly")
    return depths


def _row(values: tuple) -> RunRow:
    trigger = values[6]
    if isinstance(trigger, str):
        trigger = json.loads(trigger)
    return RunRow(
        id=values[0],
        job_name=values[1],
        status=values[2],
        attempt=values[3],
        scheduled_for=values[4],
        chain_root_id=values[5],
        trigger=trigger or {},
    )


class PostgresSchedulerState:
    """The scheduler's reads and writes against ``run_log`` and ``job_overrides``."""

    def __init__(self, cursor_factory: Callable[[], Any] | None = None):
        if cursor_factory is None:
            from src.database import get_db_cursor

            cursor_factory = get_db_cursor
        self._cursor = cursor_factory

    def record_heartbeat(self, queue_depths: dict[str, int | None]) -> None:
        with self._cursor() as c:
            c.execute(
                """INSERT INTO launcher_heartbeat(id,at,queue_depths) VALUES (1,now(),%s)
                ON CONFLICT(id) DO UPDATE SET at=now(),queue_depths=EXCLUDED.queue_depths""",
                (json.dumps(queue_depths),),
            )

    def run_exists(self, dedupe_key: str) -> bool:
        with self._cursor() as c:
            c.execute("SELECT 1 FROM run_log WHERE dedupe_key=%s", (dedupe_key,))
            return c.fetchone() is not None

    def paused(self, name: str, now: datetime) -> bool:
        with self._cursor() as c:
            c.execute(
                """SELECT 1 FROM job_overrides WHERE job_or_chain=%s
                AND (paused_until IS NULL OR paused_until > %s)""",
                (name, now),
            )
            return c.fetchone() is not None

    def record_skipped(
        self,
        *,
        job_name: str,
        trigger_type: str,
        dedupe_key: str,
        scheduled_for: datetime,
        end_reason: str,
        chain_root_id: str | None = None,
    ) -> bool:
        with self._cursor() as c:
            c.execute(
                """INSERT INTO run_log(id,job_name,trigger_type,parent_run_id,status,finished_at,
                dedupe_key,scheduled_for,chain_root_id,end_reason)
                VALUES (%s,%s,%s,%s,'skipped',now(),%s,%s,%s,%s)
                ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING RETURNING id""",
                (
                    str(uuid.uuid4()),
                    job_name,
                    trigger_type,
                    chain_root_id,
                    dedupe_key,
                    scheduled_for,
                    chain_root_id,
                    end_reason,
                ),
            )
            return c.fetchone() is not None

    def create_chain_root(
        self,
        *,
        chain: str,
        dedupe_key: str,
        scheduled_for: datetime,
        from_step: str | None = None,
        requested_by: str | None = None,
    ) -> RunRow | None:
        identity = str(uuid.uuid4())
        trigger = {"chain": chain, "from_step": from_step}
        with self._cursor() as c:
            c.execute(
                f"""INSERT INTO run_log(id,job_name,trigger_type,trigger,status,dedupe_key,
                scheduled_for,requested_by,chain_root_id)
                VALUES (%s,%s,'chain',%s,'running',%s,%s,%s,%s)
                ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
                RETURNING {_ROW_COLUMNS}""",
                (
                    identity,
                    chain,
                    json.dumps(trigger),
                    dedupe_key,
                    scheduled_for,
                    requested_by,
                    identity,
                ),
            )
            values = c.fetchone()
        return _row(values) if values else None

    def active_chain_roots(self) -> list[RunRow]:
        with self._cursor() as c:
            c.execute(
                f"""SELECT {_ROW_COLUMNS} FROM run_log WHERE chain_root_id IS NOT NULL
                AND chain_root_id=id AND status='running' ORDER BY scheduled_for"""
            )
            return [_row(values) for values in c.fetchall()]

    def chain_roots_between(
        self, chain: str, start: datetime, end: datetime
    ) -> list[RunRow]:
        with self._cursor() as c:
            c.execute(
                f"""SELECT {_ROW_COLUMNS} FROM run_log WHERE chain_root_id IS NOT NULL
                AND chain_root_id=id AND job_name=%s AND scheduled_for >= %s
                AND scheduled_for < %s""",
                (chain, start, end),
            )
            return [_row(values) for values in c.fetchall()]

    def chain_steps(self, root_id: str) -> dict[str, RunRow]:
        with self._cursor() as c:
            c.execute(
                f"""SELECT {_ROW_COLUMNS} FROM run_log WHERE chain_root_id=%s AND id<>%s
                ORDER BY attempt, started_at""",
                (root_id, root_id),
            )
            return {row.job_name: row for row in map(_row, c.fetchall())}

    def finish_chain_root(
        self, root_id: str, status: str, end_reason: str | None
    ) -> None:
        with self._cursor() as c:
            c.execute(
                """UPDATE run_log SET status=%s, end_reason=%s, finished_at=now()
                WHERE id=%s AND status='running'""",
                (status, end_reason, root_id),
            )
