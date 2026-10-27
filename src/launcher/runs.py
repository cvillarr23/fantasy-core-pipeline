"""The launcher's run_log state machine: every launcher-side status transition and its guards.

`Runs` is bound to one cursor, so a caller decides the transaction: admission holds the
advisory lock across its reads and its claim, `publish_run` inserts the row and its outbox
command together. `RunStore` opens a cursor per call for the callers that need only one.

Overlap and the pod cap are not separate locks: they are counts of `running` rows, so any
final status written by a pod's run_scope, the inline runner or the watcher frees them.

Launch details live in columns (`executor`, `k8s_job`, `expires_at`, `cancel_reconciled_at`);
`trigger` keeps only what asked for the run. A row is a launcher run once it has an executor,
which `claim` sets, so queued rows and the browser worker's rows have none.

The run's own process finishes through `run_log_service.finish_run`, which keeps its contract
with the browser worker's `db/run_log.js`; the statuses below are shared with it.
"""

import json
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, get_args

from src.database import get_db_cursor

# pg_advisory_xact_lock key serializing admission decisions, so a second launcher briefly
# alive during a Recreate rollout can't admit past the cap or a forbid overlap.
ADMISSION_LOCK_KEY = 343_001

LauncherTrigger = Literal["schedule", "chain", "event", "manual", "cli", "mcp"]
LAUNCHER_TRIGGERS = get_args(LauncherTrigger)

ACTIVE_STATUSES = ("queued", "running")
CANCELLABLE_STATUSES = ACTIVE_STATUSES
# A failure or timeout is final unless the run has attempts left (and its chain is still going).
RETRYABLE_STATUSES = ("failure", "timeout")
OUTCOME_STATUSES = ("success", *RETRYABLE_STATUSES)
# What `finish_running(force=True)` may overwrite: the launcher's verdict beats a process's.
FORCE_FINISHABLE_STATUSES = ("running", *OUTCOME_STATUSES)
REQUEUEABLE_STATUSES = ("running", *RETRYABLE_STATUSES)

# A row the launcher claimed (see module docstring).
LAUNCHER_RUN = "executor IS NOT NULL"


@dataclass(frozen=True)
class WatchedRun:
    """A run_log row the watcher reconciles, with its launch details."""

    run_id: str
    job_name: str
    status: str
    attempt: int
    started_at: datetime
    chain_root_id: str | None
    executor: str | None
    k8s_job: str | None
    # False for a chain step whose root is no longer `running` (cancelled, or past its
    # max_duration): its failure is final, since a retry must not restart an ended chain.
    chain_running: bool = True


class Runs:
    """Reads and transitions for launcher runs on one cursor."""

    def __init__(self, cursor):
        self.c = cursor

    # ---- reads ----------------------------------------------------------------------

    def status(self, run_id):
        self.c.execute("SELECT status FROM run_log WHERE id=%s", (run_id,))
        row = self.c.fetchone()
        return row[0] if row else None

    def run_state(self, run_id):
        self.c.execute("SELECT status,attempt FROM run_log WHERE id=%s", (run_id,))
        row = self.c.fetchone()
        return row if row else (None, None)

    def run_id_for_dedupe_key(self, dedupe_key):
        self.c.execute("SELECT id FROM run_log WHERE dedupe_key=%s", (dedupe_key,))
        row = self.c.fetchone()
        return row[0] if row else None

    def job_running(self, job, run_id):
        """Whether another run of this catalog job is running. Launcher runs keep their
        catalog job_name, so CronJob-era rows (entrypoint names) never match."""
        self.c.execute(
            "SELECT 1 FROM run_log WHERE job_name=%s AND status='running' AND id<>%s LIMIT 1",
            (job, run_id),
        )
        return self.c.fetchone() is not None

    def chain_running(self, chain_root_id):
        """Whether a chain root is still `running`, so its steps may start."""
        return self.status(chain_root_id) == "running"

    def running_pods(self):
        self.c.execute(
            "SELECT count(*) FROM run_log WHERE status='running' AND executor='pod'"
        )
        return self.c.fetchone()[0]

    def launcher_counts(self):
        """`(running launcher runs, queued runs)`, for the health endpoint."""
        self.c.execute(
            f"""SELECT count(*) FILTER (WHERE status='running' AND {LAUNCHER_RUN}),
            count(*) FILTER (WHERE status='queued') FROM run_log"""
        )
        return self.c.fetchone()

    def watched_runs(self, run_ids_with_k8s_job=(), max_attempts=None):
        """Return active/cancelled runs, runs with a live k8s Job, and retryable outcomes.

        Terminal outcomes are selected only while retryable (attempts left, and a chain step's
        root still running), keeping old run_log history out of every watcher tick. Pod
        failures can happen before a k8s Job is created.
        """
        self.c.execute(
            f"""SELECT r.id,r.job_name,r.status,r.attempt,r.started_at,r.chain_root_id,
            r.executor,r.k8s_job,COALESCE(r.chain_root_id IS NULL OR root.status='running',false)
            FROM run_log r LEFT JOIN run_log root ON root.id=r.chain_root_id
            WHERE r.{LAUNCHER_RUN}
            AND (r.chain_root_id IS NULL OR r.chain_root_id<>r.id)
            AND (r.status='running'
              OR (r.status='cancelled' AND r.cancel_reconciled_at IS NULL)
              OR r.id=ANY(%s)
              OR (r.executor IN ('inline','pod')
                  AND r.status IN {_in(RETRYABLE_STATUSES)}
                  AND r.attempt<COALESCE((%s::jsonb->>r.job_name)::int,r.attempt)
                  AND (r.chain_root_id IS NULL OR root.status='running')))
            ORDER BY r.started_at""",
            (list(run_ids_with_k8s_job), json.dumps(max_attempts or {})),
        )
        return [WatchedRun(*row) for row in self.c.fetchall()]

    # ---- creating rows --------------------------------------------------------------

    def insert_queued(
        self,
        *,
        run_id,
        job,
        trigger_type,
        trigger,
        parent_run_id,
        trace_id,
        dedupe_key,
        scheduled_for,
        attempt,
        requested_by,
        chain_root_id,
        event_id,
        expires_at,
    ):
        """A `queued` run. False when `dedupe_key` was already used."""
        self.c.execute(
            """INSERT INTO run_log(id,job_name,trigger_type,trigger,parent_run_id,status,trace_id,
              dedupe_key,scheduled_for,attempt,requested_by,chain_root_id,event_id,expires_at)
            VALUES (%s,%s,%s,%s,%s,'queued',%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING RETURNING id""",
            (
                run_id,
                job,
                trigger_type,
                json.dumps(trigger),
                parent_run_id,
                trace_id,
                dedupe_key,
                scheduled_for,
                attempt,
                requested_by,
                chain_root_id,
                event_id,
                expires_at,
            ),
        )
        return self.c.fetchone() is not None

    def insert_skipped(
        self,
        *,
        job,
        trigger_type,
        dedupe_key,
        scheduled_for,
        end_reason,
        chain_root_id=None,
    ):
        """A run that was never going to start. False when `dedupe_key` was already used."""
        self.c.execute(
            """INSERT INTO run_log(id,job_name,trigger_type,parent_run_id,status,finished_at,
            dedupe_key,scheduled_for,chain_root_id,end_reason)
            VALUES (%s,%s,%s,%s,'skipped',now(),%s,%s,%s,%s)
            ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING RETURNING id""",
            (
                str(uuid.uuid4()),
                job,
                trigger_type,
                chain_root_id,
                dedupe_key,
                scheduled_for,
                chain_root_id,
                end_reason,
            ),
        )
        return self.c.fetchone() is not None

    def insert_chain_root(
        self, *, chain, trigger, dedupe_key, scheduled_for, requested_by, columns
    ):
        """A `running` chain root, with the `columns` selected back. None on a reused key.

        A root has no k8s Job or process: the scheduler ends it (`finish_chain_root`).
        """
        identity = str(uuid.uuid4())
        self.c.execute(
            f"""INSERT INTO run_log(id,job_name,trigger_type,trigger,status,dedupe_key,
            scheduled_for,requested_by,chain_root_id)
            VALUES (%s,%s,'chain',%s,'running',%s,%s,%s,%s)
            ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
            RETURNING {columns}""",
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
        return self.c.fetchone()

    # ---- transitions ----------------------------------------------------------------

    def claim(self, run_id, executor, k8s_job=None):
        """queued -> running. False when the row left `queued` (cancelled) meanwhile."""
        self.c.execute(
            """UPDATE run_log SET status='running', started_at=now(), executor=%s, k8s_job=%s
            WHERE id=%s AND status='queued' RETURNING id""",
            (executor, k8s_job, run_id),
        )
        return self.c.fetchone() is not None

    def end_queued(self, run_id, status, end_reason, error=None):
        """Ends a run that never started (skipped or failed before launch)."""
        self.c.execute(
            """UPDATE run_log SET status=%s, end_reason=%s, error=%s, finished_at=now()
            WHERE id=%s AND status='queued'""",
            (status, end_reason, error, run_id),
        )

    def finish_running(
        self, run_id, status, end_reason, error=None, force=False, expected_attempt=None
    ):
        """Ends a launched run. Without `force` it only fills in a row the process itself
        left `running` (it crashed or never adopted RUN_ID), keeping what run_scope wrote.
        """
        guard, guard_params = _finish_guard(force, expected_attempt)
        self.c.execute(
            """UPDATE run_log SET status=%s, end_reason=%s,
            error=COALESCE(%s,error), finished_at=now() WHERE id=%s"""
            + guard,
            (status, end_reason, error, run_id, *guard_params),
        )

    def finish_chain_root(self, root_id, status, end_reason):
        self.c.execute(
            """UPDATE run_log SET status=%s, end_reason=%s, finished_at=now()
            WHERE id=%s AND status='running'""",
            (status, end_reason, root_id),
        )

    def cancel(self, run_id):
        """Cancels a queued or running run, and a chain root's queued and running steps in
        the same transaction (the scheduler only advances `running` roots).

        Returns True when cancelled, else the row's current status, or None for no such run.
        """
        self.c.execute(
            f"""UPDATE run_log SET status='cancelled',finished_at=now(),end_reason='cancelled'
            WHERE id=%s AND status IN {_in(CANCELLABLE_STATUSES)} RETURNING chain_root_id""",
            (run_id,),
        )
        cancelled = self.c.fetchone()
        if cancelled is None:
            return self.status(run_id)
        if cancelled[0] == run_id:
            self._cancel_chain_steps(run_id)
        return True

    def _cancel_chain_steps(self, root_id):
        self.c.execute(
            f"""UPDATE run_log SET status='cancelled',finished_at=now(),end_reason='cancelled'
            WHERE chain_root_id=%s AND id<>%s AND status IN {_in(CANCELLABLE_STATUSES)}""",
            (root_id, root_id),
        )

    def mark_cancel_reconciled(self, run_id):
        """Avoid revisiting a cancelled run after its external workload was stopped."""
        self.c.execute(
            """UPDATE run_log SET cancel_reconciled_at=now()
            WHERE id=%s AND status='cancelled'""",
            (run_id,),
        )

    def requeue_attempt(self, run_id, attempt, expected_attempt=None):
        """Queues another attempt on the same run and resets its durable outbox command.

        A chain step is requeued only while its root is `running`, so a retry can't restart
        a chain that was cancelled or ran past its max_duration.
        """
        body = self._outbox_body_for_update(run_id)
        if body is None:
            return False
        body["attempt"] = attempt
        if not self._requeue_row(run_id, attempt, expected_attempt):
            return False
        self._reset_outbox(run_id, body)
        return True

    def _outbox_body_for_update(self, run_id):
        self.c.execute(
            "SELECT body FROM job_run_outbox WHERE run_id=%s FOR UPDATE", (run_id,)
        )
        row = self.c.fetchone()
        if row is None:
            return None
        return json.loads(row[0]) if isinstance(row[0], str) else row[0]

    def _requeue_row(self, run_id, attempt, expected_attempt):
        """queued again with a clean outcome; the next claim overwrites its launch details."""
        attempt_guard, guard_params = _attempt_guard(expected_attempt)
        self.c.execute(
            f"""UPDATE run_log SET status='queued',attempt=%s,started_at=now(),
            finished_at=NULL,end_reason=NULL,error=NULL
            WHERE id=%s AND status IN {_in(REQUEUEABLE_STATUSES)}
            AND (chain_root_id IS NULL OR chain_root_id=id OR EXISTS (
              SELECT 1 FROM run_log root
              WHERE root.id=run_log.chain_root_id AND root.status='running'))"""
            + attempt_guard
            + " RETURNING id",
            (attempt, run_id, *guard_params),
        )
        return self.c.fetchone() is not None

    def _reset_outbox(self, run_id, body):
        self.c.execute(
            """UPDATE job_run_outbox SET body=%s,published_at=NULL,next_attempt_at=now(),
            last_error=NULL WHERE run_id=%s""",
            (json.dumps(body), run_id),
        )


# Admission reads and claims through the same transitions.
Admission = Runs


class RunStore:
    """`Runs` with a cursor per call, plus the admission transaction."""

    @contextmanager
    def admission(self):
        with get_db_cursor() as c:
            c.execute("SELECT pg_advisory_xact_lock(%s)", (ADMISSION_LOCK_KEY,))
            yield Runs(c)

    def status(self, run_id):
        with get_db_cursor() as c:
            return Runs(c).status(run_id)

    def run_state(self, run_id):
        with get_db_cursor() as c:
            return Runs(c).run_state(run_id)

    def queued_commands(self):
        """Run command bodies still queued, oldest first, for recovery after a restart."""
        with get_db_cursor() as c:
            c.execute(
                """SELECT o.body FROM job_run_outbox o JOIN run_log r ON r.id=o.run_id
                WHERE r.status='queued' ORDER BY r.started_at, r.id"""
            )
            return [
                json.loads(body) if isinstance(body, str) else body
                for (body,) in c.fetchall()
            ]

    def finish_running(self, *args, **kwargs):
        with get_db_cursor() as c:
            Runs(c).finish_running(*args, **kwargs)

    def watched_runs(self, run_ids_with_k8s_job=(), max_attempts=None):
        with get_db_cursor() as c:
            return Runs(c).watched_runs(run_ids_with_k8s_job, max_attempts)

    def mark_cancel_reconciled(self, run_id):
        with get_db_cursor() as c:
            Runs(c).mark_cancel_reconciled(run_id)

    def requeue_attempt(self, run_id, attempt, expected_attempt=None):
        with get_db_cursor() as c:
            return Runs(c).requeue_attempt(run_id, attempt, expected_attempt)


def _in(statuses):
    """A SQL `('a','b')` list of internal status constants."""
    return "(" + ",".join(f"'{status}'" for status in statuses) + ")"


def _finish_guard(force, expected_attempt):
    """The WHERE suffix and its params for finish_running: which statuses it may replace,
    and the attempt it must still be on."""
    statuses = (
        f" AND status IN {_in(FORCE_FINISHABLE_STATUSES)}"
        if force
        else " AND status='running'"
    )
    attempt_guard, params = _attempt_guard(expected_attempt)
    return statuses + attempt_guard, params


def _attempt_guard(expected_attempt):
    """A WHERE suffix pinning the row to the attempt the caller acted on, if given."""
    if expected_attempt is None:
        return "", ()
    return " AND attempt=%s", (expected_attempt,)
