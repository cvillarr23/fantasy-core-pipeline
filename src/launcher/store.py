"""run_log reads and writes behind the launcher's admission decisions.

Overlap and the pod cap are not separate locks: they are counts of `running` rows, so any
final status written by a pod's run_scope, the inline runner or the watcher frees them.
"""

import json
from contextlib import contextmanager

from src.database import get_db_cursor

# pg_advisory_xact_lock key serializing admission decisions, so a second launcher briefly
# alive during a Recreate rollout can't admit past the cap or a forbid overlap.
ADMISSION_LOCK_KEY = 343_001


class Admission:
    """Reads and transitions for one admission decision, all in one transaction."""

    def __init__(self, cursor):
        self.c = cursor

    def status(self, run_id):
        self.c.execute("SELECT status FROM run_log WHERE id=%s", (run_id,))
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

    def finish_running(self, run_id, status, end_reason, error=None, force=False):
        """Ends a launched run. Without `force` it only fills in a row the process itself
        left `running` (it crashed or never adopted RUN_ID), keeping what run_scope wrote."""
        with get_db_cursor() as c:
            guard = " AND status<>'cancelled'" if force else " AND status='running'"
            c.execute(
                """UPDATE run_log SET status=%s, end_reason=%s,
                error=COALESCE(%s,error), finished_at=now() WHERE id=%s""" + guard,
                (status, end_reason, error, run_id),
            )

    def watched_runs(self):
        """Launched rows the watcher must reconcile, including API-cancelled rows whose
        external process or Job still needs to be stopped. Chain roots have no executor.
        """
        with get_db_cursor() as c:
            c.execute(
                """SELECT id,job_name,status,attempt,started_at,chain_root_id,trigger
                FROM run_log WHERE status IN ('running','success','failure','timeout','cancelled')
                AND trigger ? 'executor'
                AND (chain_root_id IS NULL OR chain_root_id<>id) ORDER BY started_at"""
            )
            rows = c.fetchall()
        result = []
        for identity, job, status, attempt, started_at, chain_root_id, trigger in rows:
            if isinstance(trigger, str):
                trigger = json.loads(trigger)
            trigger = trigger or {}
            result.append(
                {
                    "run_id": identity,
                    "job_name": job,
                    "status": status,
                    "attempt": attempt,
                    "started_at": started_at,
                    "chain_root_id": chain_root_id,
                    "executor": trigger.get("executor"),
                    "k8s_job": trigger.get("k8s_job"),
                }
            )
        return result

    def retry_running(self, run_id, attempt):
        """Queues another attempt on the same run and resets its durable outbox command."""
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
            c.execute(
                """UPDATE run_log SET status='queued',attempt=%s,started_at=now(),
                finished_at=NULL,end_reason=NULL,error=NULL
                WHERE id=%s AND status IN ('running','failure','timeout') RETURNING id""",
                (attempt, run_id),
            )
            if c.fetchone() is None:
                return False
            c.execute(
                """UPDATE job_run_outbox SET body=%s,published_at=NULL,next_attempt_at=now(),
                last_error=NULL WHERE run_id=%s""",
                (json.dumps(body), run_id),
            )
            return True
