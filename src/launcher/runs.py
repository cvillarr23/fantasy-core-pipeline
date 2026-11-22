"""The launcher's run_log state machine: every launcher-side status transition and its guards.

`Runs` is bound to one cursor, so a caller decides the transaction: admission holds the
advisory lock across its reads and its claim, `publish_run` inserts the row and its outbox
command together. `RunStore` opens a cursor per call for the callers that need only one.

Overlap, the pod cap and concurrency groups are not separate locks: they are counts of
`running` rows, so any final status written by a pod's run_scope, the inline runner or the
watcher frees them.

Launch details live in columns (`executor`, `k8s_job`, `expires_at`, `cancel_reconciled_at`);
`trigger` keeps only what asked for the run. A row is a launcher run once it has an executor,
which `claim` sets, so queued rows and the browser worker's rows have none.

A fan-out parent is claimed with executor `fan_out` (`FAN_OUT`) and has no k8s Job or process:
its children (`parent_run_id` = the parent) do the work, and the scheduler ends it. Like a chain
root it groups runs, so a cancel reaches its children and an ended parent stops their retries.

An approval run (executor `approval`, `APPROVAL`) waits for a person. It is inserted `waiting`
and is never `running`, so it holds no pod, group or overlap slot, and it has no workload for the
watcher to stop. It ends `success` (`approved`), or `skipped` (`dismissed`, `superseded`,
`expired`); `trigger.then` names the run an approval starts.

The run's own process finishes through `run_log_service.finish_run`, which keeps its contract
with the browser worker's `db/run_log.js`; the statuses below are shared with it.
"""

import json
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime

from src.database import get_db_cursor

# pg_advisory_xact_lock key serializing admission decisions, so a second launcher briefly
# alive during a Recreate rollout can't admit past the cap or a forbid overlap.
ADMISSION_LOCK_KEY = 343_001

ACTIVE_STATUSES = ("queued", "running")
# An approval run's status until a person (or its expiry) decides it.
WAITING = "waiting"
CANCELLABLE_STATUSES = (*ACTIVE_STATUSES, WAITING)
# A failure or timeout is final unless the run has attempts left (and its chain is still going).
RETRYABLE_STATUSES = ("failure", "timeout")
OUTCOME_STATUSES = ("success", *RETRYABLE_STATUSES)
# What `finish_running(force=True)` may overwrite: the launcher's verdict beats a process's.
FORCE_FINISHABLE_STATUSES = ("running", *OUTCOME_STATUSES)
REQUEUEABLE_STATUSES = ("running", *RETRYABLE_STATUSES)

# A row the launcher claimed (see module docstring).
LAUNCHER_RUN = "executor IS NOT NULL"
# The executor of a fan-out parent, which launches nothing itself.
FAN_OUT = "fan_out"
# The executor of an approval run, which launches nothing and waits for a person.
APPROVAL = "approval"
# Executors with no k8s Job or process: nothing to watch, stop or count against a cap.
NO_WORKLOAD = (FAN_OUT, APPROVAL)
# How an approval ends: the outcome's status and end_reason.
APPROVAL_OUTCOMES = {
    "approved": "success",
    "dismissed": "skipped",
    "superseded": "skipped",
}


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
    # max_duration), or a fan-out child whose parent is not: its failure is final, since a
    # retry must not restart an ended chain or fan-out.
    group_running: bool = True
    # The run's params (`trigger.params`), which a service job's health key is formatted from.
    params: dict = field(default_factory=dict)


@dataclass(frozen=True)
class ApprovalRun:
    """An approval run, with what it asks for (`trigger`: `title`, `link`, `key`, `then`)."""

    run_id: str
    job_name: str
    status: str
    end_reason: str | None
    requested_by: str | None
    trigger: dict
    expires_at: datetime | None
    chain_root_id: str | None

    @property
    def then(self):
        """`{job, params}` to start once approved, or None (a chain step's approval)."""
        return self.trigger.get("then")


@dataclass(frozen=True)
class JobRun:
    """A run of a catalog job, as the draft endpoints look runs up by param."""

    run_id: str
    job_name: str
    status: str
    params: dict
    started_at: datetime
    end_reason: str | None = None


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

    def job_running(self, job, run_id, fan_out_parent_id=None):
        """Whether another run of this catalog job is running."""
        return bool(self.running_triggers(job, run_id, fan_out_parent_id))

    def running_triggers(self, job, run_id, fan_out_parent_id=None):
        """The trigger types of the other running runs of this catalog job. Launcher runs
        keep their catalog job_name, so CronJob-era rows (entrypoint names) never match.

        For a fan-out child, its own parent and siblings don't count: they are one run.
        """
        query = (
            "SELECT DISTINCT trigger_type FROM run_log"
            " WHERE job_name=%s AND status='running' AND id<>%s"
        )
        params = (job, run_id)
        if fan_out_parent_id:
            query += " AND id<>%s AND parent_run_id IS DISTINCT FROM %s"
            params += (fan_out_parent_id, fan_out_parent_id)
        self.c.execute(query, params)
        return {row[0] for row in self.c.fetchall()}

    def chain_running(self, chain_root_id):
        """Whether a chain root is still `running`, so its steps may start."""
        return self.status(chain_root_id) == "running"

    def fan_out_parent_status(self, run_id):
        """`run_id`'s status when it is a fan-out parent, else None."""
        self.c.execute(
            "SELECT status FROM run_log WHERE id=%s AND executor=%s", (run_id, FAN_OUT)
        )
        row = self.c.fetchone()
        return row[0] if row else None

    def running_pods(self, exclude_jobs=()):
        """Running pod rows, for the pod cap. Service jobs are passed in `exclude_jobs`: they
        run for hours and are exempt from the cap."""
        self.c.execute(
            """SELECT count(*) FROM run_log WHERE status='running' AND executor='pod'
            AND NOT job_name=ANY(%s)""",
            (list(exclude_jobs),),
        )
        return self.c.fetchone()[0]

    def active_runs(self, jobs, param=None, value=None):
        """Queued and running runs of these catalog jobs, newest first, optionally only those
        whose `param` is `value`."""
        return self._job_runs(jobs, param, value, "AND status IN ('queued','running')")

    def latest_run(self, jobs, param=None, value=None):
        """The newest run of these catalog jobs in any status, or None."""
        runs = self._job_runs(jobs, param, value, "", limit=1)
        return runs[0] if runs else None

    def _job_runs(self, jobs, param, value, status_filter, limit=None):
        query = f"""SELECT id,job_name,status,COALESCE(trigger->'params','{{}}'::jsonb),started_at,
            end_reason FROM run_log WHERE job_name=ANY(%s) {status_filter}"""
        args = [list(jobs)]
        if param is not None:
            query += " AND trigger->'params'->>%s=%s"
            args += [param, str(value)]
        query += " ORDER BY started_at DESC, id DESC"
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        self.c.execute(query, args)
        return [JobRun(*row) for row in self.c.fetchall()]

    def has_unreconciled_cancels(self):
        """Whether a cancelled launcher run still has its workload to stop."""
        self.c.execute(
            f"""SELECT EXISTS(SELECT 1 FROM run_log WHERE status='cancelled'
            AND cancel_reconciled_at IS NULL AND {LAUNCHER_RUN} AND executor NOT IN {_in(NO_WORKLOAD)})"""
        )
        return self.c.fetchone()[0]

    def running_in_jobs(self, jobs):
        """How many launcher runs of these catalog jobs are running, for a concurrency group.
        A fan-out parent holds nothing (its children each count), and rows the launcher
        didn't claim (CronJob-era, browser worker) have no executor, so they never count.
        """
        self.c.execute(
            f"""SELECT count(*) FROM run_log WHERE status='running' AND job_name=ANY(%s)
            AND {LAUNCHER_RUN} AND executor NOT IN {_in(NO_WORKLOAD)}""",
            (list(jobs),),
        )
        return self.c.fetchone()[0]

    def launcher_counts(self):
        """`(running launcher runs, queued runs)`, for the health endpoint."""
        self.c.execute(
            f"""SELECT count(*) FILTER (WHERE status='running' AND {LAUNCHER_RUN}
              AND executor NOT IN {_in(NO_WORKLOAD)}),
            count(*) FILTER (WHERE status='queued') FROM run_log"""
        )
        return self.c.fetchone()

    def watched_runs(self, run_ids_with_k8s_job=(), max_attempts=None):
        """Return active/cancelled runs, runs with a live k8s Job, and retryable outcomes.

        Terminal outcomes are selected only while retryable (attempts left, and a chain step's
        root and a fan-out child's parent still running), keeping old run_log history out of
        every watcher tick. Pod failures can happen before a k8s Job is created. Fan-out
        parents have no workload to watch, so they are left out.
        """
        self.c.execute(
            f"""SELECT r.id,r.job_name,r.status,r.attempt,r.started_at,r.chain_root_id,
            r.executor,r.k8s_job,COALESCE(
              (r.chain_root_id IS NULL OR root.status='running')
              AND (fan.id IS NULL OR fan.status='running'),false),
            COALESCE(r.trigger->'params','{{}}'::jsonb)
            FROM run_log r LEFT JOIN run_log root ON root.id=r.chain_root_id
            LEFT JOIN run_log fan ON fan.id=r.parent_run_id AND fan.executor='{FAN_OUT}'
            WHERE r.{LAUNCHER_RUN} AND r.executor NOT IN {_in(NO_WORKLOAD)}
            AND (r.chain_root_id IS NULL OR r.chain_root_id<>r.id)
            AND (r.status='running'
              OR (r.status='cancelled' AND r.cancel_reconciled_at IS NULL)
              OR r.id=ANY(%s)
              OR (r.executor IN ('inline','pod')
                  AND r.status IN {_in(RETRYABLE_STATUSES)}
                  AND r.attempt<COALESCE((%s::jsonb->>r.job_name)::int,r.attempt)
                  AND (r.chain_root_id IS NULL OR root.status='running')
                  AND (fan.id IS NULL OR fan.status='running')))
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
        event_id=None,
    ):
        """A run that was never going to start. False when `dedupe_key` was already used."""
        self.c.execute(
            """INSERT INTO run_log(id,job_name,trigger_type,parent_run_id,status,finished_at,
            dedupe_key,scheduled_for,chain_root_id,end_reason,event_id)
            VALUES (%s,%s,%s,%s,'skipped',now(),%s,%s,%s,%s,%s)
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
                event_id,
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

    def insert_approval(
        self,
        *,
        job,
        trigger_type,
        trigger,
        parent_run_id,
        dedupe_key,
        expires_at,
        chain_root_id=None,
    ):
        """A `waiting` approval run. Returns its id, or None when `dedupe_key` was used.

        Second writer: the worker's `db/run_log.js` `openApproval` inserts trade-offer
        approvals with these columns and `approval_service`'s dedupe key and trigger shape,
        and only ever moves them waiting -> superseded. Mirror any change there;
        tests/test_trade_approval_postgres.py pins the shared contract.
        """
        run_id = str(uuid.uuid4())
        self.c.execute(
            """INSERT INTO run_log(id,job_name,trigger_type,trigger,parent_run_id,status,
              executor,dedupe_key,scheduled_for,chain_root_id,expires_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now(),%s,%s)
            ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING RETURNING id""",
            (
                run_id,
                job,
                trigger_type,
                json.dumps(trigger),
                parent_run_id,
                WAITING,
                APPROVAL,
                dedupe_key,
                chain_root_id,
                expires_at,
            ),
        )
        return run_id if self.c.fetchone() is not None else None

    def approval(self, run_id):
        """The approval run with this id, or None (no such run, or not an approval)."""
        self.c.execute(
            f"SELECT {_APPROVAL_COLUMNS} FROM run_log WHERE id=%s AND executor=%s",
            (run_id, APPROVAL),
        )
        row = self.c.fetchone()
        return _approval(row) if row else None

    def approved_without_then(self, since):
        """Approvals that succeeded after `since` and name a `then` run not yet published,
        oldest first: the publish after approving failed, or the process died before it.
        """
        self.c.execute(
            f"""SELECT {_APPROVAL_COLUMNS} FROM run_log a
            WHERE a.executor=%s AND a.status='success' AND a.trigger ? 'then'
            AND a.finished_at > %s AND NOT EXISTS (
              SELECT 1 FROM run_log c WHERE c.dedupe_key={_THEN_DEDUPE_SQL})
            ORDER BY a.finished_at""",
            (APPROVAL, since),
        )
        return [_approval(row) for row in self.c.fetchall()]

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

        With `force` it may override a final status the process wrote, but keeps that row's
        `finished_at`: the process knows when it ended, and the watcher only notices up to a
        tick later, which would inflate the run's duration.
        """
        guard, guard_params = _finish_guard(force, expected_attempt)
        self.c.execute(
            """UPDATE run_log SET status=%s, end_reason=%s, error=COALESCE(%s,error),
            finished_at=CASE WHEN status='running' THEN now()
                ELSE COALESCE(finished_at, now()) END
            WHERE id=%s"""
            + guard,
            (status, end_reason, error, run_id, *guard_params),
        )

    def fail_running_inline(self, end_reason):
        """Fails every `running` inline row, returning their ids. Called by a launcher that
        just became leader, before its inline runner starts: such a row's process was a child
        of a launcher that is gone, so it would otherwise hold overlap until it orphaned.
        The watcher retries the ones with attempts left."""
        self.c.execute(
            """UPDATE run_log SET status='failure', end_reason=%s, finished_at=now()
            WHERE executor='inline' AND status='running' RETURNING id""",
            (end_reason,),
        )
        return [row[0] for row in self.c.fetchall()]

    def finish_chain_root(self, root_id, status, end_reason):
        self.c.execute(
            """UPDATE run_log SET status=%s, end_reason=%s, finished_at=now()
            WHERE id=%s AND status='running'""",
            (status, end_reason, root_id),
        )

    def finish_fan_out(self, parent_id, status, end_reason):
        """Ends a running fan-out parent once its children are done."""
        self.c.execute(
            """UPDATE run_log SET status=%s, end_reason=%s, finished_at=now()
            WHERE id=%s AND status='running' AND executor=%s""",
            (status, end_reason, parent_id, FAN_OUT),
        )

    def resolve_approval(self, run_id, outcome, decided_by):
        """Ends a `waiting` approval with `outcome` (an `APPROVAL_OUTCOMES` key), recording who
        decided as `requested_by`. An approval past its `expires_at` can still be dismissed or
        superseded but no longer approved, even before the scheduler's sweep reaches it.

        Returns True when resolved, else the row's current status, or None for no such
        approval.
        """
        self.c.execute(
            """UPDATE run_log SET status=%s, end_reason=%s, requested_by=%s, finished_at=now()
            WHERE id=%s AND executor=%s AND status=%s
            AND (%s<>'approved' OR expires_at IS NULL OR expires_at>now()) RETURNING id""",
            (
                APPROVAL_OUTCOMES[outcome],
                outcome,
                decided_by,
                run_id,
                APPROVAL,
                WAITING,
                outcome,
            ),
        )
        if self.c.fetchone() is not None:
            return True
        current = self.approval(run_id)
        return current.status if current else None

    def retarget_approval_then(self, run_id, params):
        """Replaces a `waiting` approval's `then.params`. Returns whether it changed.

        Approving a trade offer with an edited message changes its offer key, and the run
        the approval starts must send the offer that was approved.
        """
        self.c.execute(
            """UPDATE run_log SET trigger=jsonb_set(trigger,'{then,params}',%s::jsonb)
            WHERE id=%s AND executor=%s AND status=%s AND trigger ? 'then' RETURNING id""",
            (json.dumps(params), run_id, APPROVAL, WAITING),
        )
        return self.c.fetchone() is not None

    def expire_approvals(self, now):
        """Ends every approval still `waiting` at its `expires_at` as `skipped (expired)`, and
        returns their ids."""
        self.c.execute(
            """UPDATE run_log SET status='skipped', end_reason='expired', finished_at=now()
            WHERE executor=%s AND status=%s AND expires_at<=%s RETURNING id""",
            (APPROVAL, WAITING, now),
        )
        return [row[0] for row in self.c.fetchall()]

    def cancel(self, run_id):
        """Cancels a queued or running run, and a chain root's or fan-out parent's queued and
        running steps or children in the same transaction (the scheduler only advances
        `running` roots and parents).

        Returns True when cancelled, else the row's current status, or None for no such run.
        """
        self.c.execute(
            f"""UPDATE run_log SET status='cancelled',finished_at=now(),end_reason='cancelled'
            WHERE id=%s AND status IN {_in(CANCELLABLE_STATUSES)}
            RETURNING chain_root_id,executor""",
            (run_id,),
        )
        cancelled = self.c.fetchone()
        if cancelled is None:
            return self.status(run_id)
        chain_root_id, executor = cancelled
        if chain_root_id == run_id:
            self._cancel_chain_steps(run_id)
        elif executor == FAN_OUT:
            self._cancel_fan_out_children(run_id)
        return True

    def _cancel_fan_out_children(self, parent_id):
        self.c.execute(
            f"""UPDATE run_log SET status='cancelled',finished_at=now(),end_reason='cancelled'
            WHERE parent_run_id=%s AND status IN {_in(CANCELLABLE_STATUSES)}""",
            (parent_id,),
        )

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

        A chain step is requeued only while its root is `running`, and a fan-out child only
        while its parent is, so a retry can't restart a chain that was cancelled or ran past
        its max_duration, or a cancelled fan-out.
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
              WHERE root.id=run_log.chain_root_id AND root.status='running'))
            AND NOT EXISTS (
              SELECT 1 FROM run_log parent WHERE parent.id=run_log.parent_run_id
              AND parent.executor='{FAN_OUT}' AND parent.status<>'running')"""
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


class RunStore:
    """`Runs` with a cursor per call, plus the admission transaction."""

    @contextmanager
    def admission(self):
        with get_db_cursor() as c:
            c.execute("SELECT pg_advisory_xact_lock(%s)", (ADMISSION_LOCK_KEY,))
            yield Runs(c)

    def _with_runs(self, call):
        """Runs `call(runs)` on a fresh cursor, in its own transaction."""
        with get_db_cursor() as c:
            return call(Runs(c))

    def status(self, run_id):
        return self._with_runs(lambda runs: runs.status(run_id))

    def run_state(self, run_id):
        return self._with_runs(lambda runs: runs.run_state(run_id))

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
        self._with_runs(
            lambda runs: runs.finish_running(
                run_id, status, end_reason, error, force, expected_attempt
            )
        )

    def fail_running_inline(self, end_reason):
        return self._with_runs(lambda runs: runs.fail_running_inline(end_reason))

    def watched_runs(self, run_ids_with_k8s_job=(), max_attempts=None):
        return self._with_runs(
            lambda runs: runs.watched_runs(run_ids_with_k8s_job, max_attempts)
        )

    def mark_cancel_reconciled(self, run_id):
        self._with_runs(lambda runs: runs.mark_cancel_reconciled(run_id))

    def has_unreconciled_cancels(self):
        return self._with_runs(lambda runs: runs.has_unreconciled_cancels())

    def active_runs(self, jobs, param=None, value=None):
        return self._with_runs(lambda runs: runs.active_runs(jobs, param, value))

    def latest_run(self, jobs, param=None, value=None):
        return self._with_runs(lambda runs: runs.latest_run(jobs, param, value))

    def cancel(self, run_id):
        return self._with_runs(lambda runs: runs.cancel(run_id))

    def requeue_attempt(self, run_id, attempt, expected_attempt=None):
        return self._with_runs(
            lambda runs: runs.requeue_attempt(run_id, attempt, expected_attempt)
        )


_APPROVAL_COLUMNS = (
    "id,job_name,status,end_reason,requested_by,trigger,expires_at,chain_root_id"
)
# `then_dedupe_key` in SQL, for the approval aliased `a`.
_THEN_DEDUPE_SQL = "(a.trigger->'then'->>'job') || '@' || a.id"


def then_dedupe_key(job, approval_id):
    """The dedupe key of the run an approval starts, so it starts at most once."""
    return f"{job}@{approval_id}"


def _approval(row):
    trigger = row[5]
    if isinstance(trigger, str):
        trigger = json.loads(trigger)
    return ApprovalRun(*row[:5], trigger or {}, *row[6:])


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
