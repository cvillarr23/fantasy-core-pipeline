"""run_log reads and writes behind the launcher's admission decisions.

Overlap and the pod cap are not separate locks: they are counts of `running` rows, so any
final status written by a pod's run_scope, the inline runner or the watcher frees them.
"""

import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime

from src.database import get_db_cursor

# pg_advisory_xact_lock key serializing admission decisions, so a second launcher briefly
# alive during a Recreate rollout can't admit past the cap or a forbid overlap.
ADMISSION_LOCK_KEY = 343_001


@dataclass(frozen=True)
class WatchedRun:
    """A run_log row the watcher reconciles, with its launch details from `trigger`."""

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


class Admission:
    """Reads and transitions for one admission decision, all in one transaction."""

    def __init__(self, cursor):
        self.c = cursor

    def status(self, run_id):
        self.c.execute("SELECT status FROM run_log WHERE id=%s", (run_id,))
        row = self.c.fetchone()
        return row[0] if row else None

    def run_state(self, run_id):
        self.c.execute("SELECT status,attempt FROM run_log WHERE id=%s", (run_id,))
        row = self.c.fetchone()
        return row if row else (None, None)

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
            "SELECT count(*) FROM run_log WHERE status='running' AND trigger->>'executor'='pod'"
        )
        return self.c.fetchone()[0]

    def claim(self, run_id, executor, k8s_job=None):
        """queued -> running. False when the row left `queued` (cancelled) meanwhile."""
        launch = {"executor": executor}
        if k8s_job:
            launch["k8s_job"] = k8s_job
        self.c.execute(
            """UPDATE run_log SET status='running', started_at=now(), trigger=trigger || %s::jsonb
            WHERE id=%s AND status='queued' RETURNING id""",
            (json.dumps(launch), run_id),
        )
        return self.c.fetchone() is not None

    def end_queued(self, run_id, status, end_reason, error=None):
        """Ends a run that never started (skipped or failed before launch)."""
        self.c.execute(
            """UPDATE run_log SET status=%s, end_reason=%s, error=%s, finished_at=now()
            WHERE id=%s AND status='queued'""",
            (status, end_reason, error, run_id),
        )


class RunStore:
    @contextmanager
    def admission(self):
        with get_db_cursor() as c:
            c.execute("SELECT pg_advisory_xact_lock(%s)", (ADMISSION_LOCK_KEY,))
            yield Admission(c)

    def status(self, run_id):
        with get_db_cursor() as c:
            return Admission(c).status(run_id)

    def run_state(self, run_id):
        with get_db_cursor() as c:
            return Admission(c).run_state(run_id)

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

    def finish_running(
        self, run_id, status, end_reason, error=None, force=False, expected_attempt=None
    ):
        """Ends a launched run. Without `force` it only fills in a row the process itself
        left `running` (it crashed or never adopted RUN_ID), keeping what run_scope wrote.
        """
        with get_db_cursor() as c:
            guard = (
                " AND status IN ('running','success','failure','timeout')"
                if force
                else " AND status='running'"
            )
            params = (status, end_reason, error, run_id)
            if expected_attempt is not None:
                guard += " AND attempt=%s"
                params += (expected_attempt,)
            c.execute(
                """UPDATE run_log SET status=%s, end_reason=%s,
                error=COALESCE(%s,error), finished_at=now() WHERE id=%s"""
                + guard,
                params,
            )

    def watched_runs(self, run_ids_with_k8s_job=(), max_attempts=None):
        """Return active/cancelled runs, runs with a live k8s Job, and retryable outcomes.

        Terminal outcomes are selected only while retryable (attempts left, and a chain step's
        root still running), keeping old run_log history out of every watcher tick. Pod
        failures can happen before a k8s Job is created.
        """
        with get_db_cursor() as c:
            c.execute(
                """SELECT r.id,r.job_name,r.status,r.attempt,r.started_at,r.chain_root_id,
                r.trigger,COALESCE(r.chain_root_id IS NULL OR root.status='running',false)
                FROM run_log r LEFT JOIN run_log root ON root.id=r.chain_root_id
                WHERE r.trigger ? 'executor'
                AND (r.chain_root_id IS NULL OR r.chain_root_id<>r.id)
                AND (r.status='running'
                  OR (r.status='cancelled' AND NOT (r.trigger ? 'cancel_reconciled'))
                  OR r.id=ANY(%s)
                  OR (r.trigger->>'executor' IN ('inline','pod')
                      AND r.status IN ('failure','timeout')
                      AND r.attempt<COALESCE((%s::jsonb->>r.job_name)::int,r.attempt)
                      AND (r.chain_root_id IS NULL OR root.status='running')))
                ORDER BY r.started_at""",
                (list(run_ids_with_k8s_job), json.dumps(max_attempts or {})),
            )
            rows = c.fetchall()
        result = []
        for (
            identity,
            job,
            status,
            attempt,
            started_at,
            chain_root_id,
            trigger,
            chain_running,
        ) in rows:
            if isinstance(trigger, str):
                trigger = json.loads(trigger)
            trigger = trigger or {}
            result.append(
                WatchedRun(
                    run_id=identity,
                    job_name=job,
                    status=status,
                    attempt=attempt,
                    started_at=started_at,
                    chain_root_id=chain_root_id,
                    executor=trigger.get("executor"),
                    k8s_job=trigger.get("k8s_job"),
                    chain_running=chain_running,
                )
            )
        return result

    def mark_cancel_reconciled(self, run_id):
        """Avoid revisiting a cancelled run after its external workload was stopped."""
        with get_db_cursor() as c:
            c.execute(
                """UPDATE run_log SET trigger=trigger || '{"cancel_reconciled":true}'::jsonb
                WHERE id=%s AND status='cancelled'""",
                (run_id,),
            )

    def requeue_attempt(self, run_id, attempt, expected_attempt=None):
        """Queues another attempt on the same run and resets its durable outbox command.

        A chain step is requeued only while its root is `running`, so a retry can't restart
        a chain that was cancelled or ran past its max_duration.
        """
        with get_db_cursor() as c:
            c.execute(
                """SELECT body FROM job_run_outbox WHERE run_id=%s FOR UPDATE""",
                (run_id,),
            )
            row = c.fetchone()
            if row is None:
                return False
            body = row[0]
            if isinstance(body, str):
                body = json.loads(body)
            body["attempt"] = attempt
            attempt_guard = " AND attempt=%s" if expected_attempt is not None else ""
            params = (attempt, run_id)
            if expected_attempt is not None:
                params += (expected_attempt,)
            c.execute(
                """UPDATE run_log SET status='queued',attempt=%s,started_at=now(),
                finished_at=NULL,end_reason=NULL,error=NULL
                WHERE id=%s AND status IN ('running','failure','timeout')
                AND (chain_root_id IS NULL OR chain_root_id=id OR EXISTS (
                  SELECT 1 FROM run_log root
                  WHERE root.id=run_log.chain_root_id AND root.status='running'))"""
                + attempt_guard
                + " RETURNING id",
                params,
            )
            if c.fetchone() is None:
                return False
            c.execute(
                """UPDATE job_run_outbox SET body=%s,published_at=NULL,next_attempt_at=now(),
                last_error=NULL WHERE run_id=%s""",
                (json.dumps(body), run_id),
            )
            return True
