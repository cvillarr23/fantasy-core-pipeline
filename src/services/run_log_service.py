"""Run audit log: one row per job/bot run, plus links to what each run read and wrote.

A run is a scheduled CronJob, a queue-consumed worker bot, an HTTP-triggered evaluation or
a manual invocation. `run_links` records the rows a run consumed (`input`) and produced
(`output`) - raw_signals, rumor_signals, llm sessions, activity_actions - so the web UI can
show which article led to which LLM evaluation and which roster move.

Recording is best effort: a run_log write failure is logged and never fails the job itself.
"""

import json
import logging
import os
import re
import uuid
from collections import deque
from contextlib import contextmanager
from contextvars import ContextVar

from src.database import get_db_cursor
from src.queue_contracts import DRAFT_TARGETS, LINEUP_UPDATES, WAIVER_ACTIONS
from src.services.activity_service import league_id, redact

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS run_log (
 id TEXT PRIMARY KEY, job_name TEXT NOT NULL, trigger_type TEXT NOT NULL,
 trigger JSONB NOT NULL DEFAULT '{}', parent_run_id TEXT, status TEXT NOT NULL DEFAULT 'running',
 started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ, error TEXT,
 summary JSONB NOT NULL DEFAULT '{}', trace_id TEXT,
 dedupe_key TEXT, scheduled_for TIMESTAMPTZ, attempt INTEGER NOT NULL DEFAULT 1,
 requested_by TEXT, chain_root_id TEXT, event_id TEXT, end_reason TEXT
);
ALTER TABLE run_log ADD COLUMN IF NOT EXISTS dedupe_key TEXT;
ALTER TABLE run_log ADD COLUMN IF NOT EXISTS scheduled_for TIMESTAMPTZ;
ALTER TABLE run_log ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 1;
ALTER TABLE run_log ADD COLUMN IF NOT EXISTS requested_by TEXT;
ALTER TABLE run_log ADD COLUMN IF NOT EXISTS chain_root_id TEXT;
ALTER TABLE run_log ADD COLUMN IF NOT EXISTS event_id TEXT;
ALTER TABLE run_log ADD COLUMN IF NOT EXISTS end_reason TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS run_log_dedupe_key ON run_log(dedupe_key) WHERE dedupe_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS run_log_recent ON run_log(started_at DESC);
CREATE INDEX IF NOT EXISTS run_log_by_job ON run_log(job_name,started_at DESC);
CREATE INDEX IF NOT EXISTS run_log_by_parent ON run_log(parent_run_id) WHERE parent_run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS run_log_by_tracking ON run_log((trigger->>'tracking_id')) WHERE trigger ? 'tracking_id';
CREATE TABLE IF NOT EXISTS run_links (
 run_id TEXT NOT NULL REFERENCES run_log(id) ON DELETE CASCADE, ref_type TEXT NOT NULL,
 ref_id TEXT NOT NULL, role TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(run_id,ref_type,ref_id,role)
);
CREATE INDEX IF NOT EXISTS run_links_by_ref ON run_links(ref_type,ref_id);
CREATE TABLE IF NOT EXISTS job_overrides (
 job_or_chain TEXT PRIMARY KEY, paused_until TIMESTAMPTZ, reason TEXT,
 set_by TEXT, set_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS launcher_heartbeat (
 id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1), at TIMESTAMPTZ NOT NULL DEFAULT now(),
 queue_depths JSONB NOT NULL DEFAULT '{}'
);
"""

TRIGGER_TYPES = ("chain", "event", "cli", "mcp", "schedule", "queue", "http", "manual")
STATUSES = ("queued", "skipped", "cancelled", "timeout", "running", "success", "failure")
REF_TYPES = ("raw_signal", "rumor_signal", "llm_session", "activity_action")
ROLES = ("input", "output")
RUN_ID_HEADER = "X-Run-Id"

current_run = ContextVar("run_log_run", default=None)


class RunNotFoundError(Exception):
    """No run with that id."""


def trigger_from_env():
    """Scheduled vs manual comes from the Job name: a CronJob names its Jobs
    `<cronjob>-<minutes>`, while `kubectl create job --from=cronjob/...` picks any name.
    """
    explicit = os.getenv("RUN_TRIGGER")
    if explicit:
        return explicit, {}
    cronjob = os.getenv("RUN_CRONJOB")
    k8s_job = os.getenv("RUN_K8S_JOB") or os.getenv("HOSTNAME")
    if cronjob:
        scheduled = bool(
            k8s_job and re.fullmatch(rf"{re.escape(cronjob)}-\d+", k8s_job)
        )
        return ("schedule" if scheduled else "manual"), {
            "cronjob": cronjob,
            "k8s_job": k8s_job,
        }
    return "manual", {"host": k8s_job} if k8s_job else {}


def _trace_id():
    try:
        from opentelemetry import trace

        context = trace.get_current_span().get_span_context()
        return format(context.trace_id, "032x") if context.trace_id else None
    except Exception:
        return None


# What a run's page shows when the job itself records nothing: its own recent log lines.
RUN_LOG_TAIL_LINES = 50
RUN_LOG_LINE_CHARS = 300
# Library loggers whose lines say nothing about what the job did.
NOISY_LOGGER_PREFIXES = (
    "pika",
    "urllib3",
    "httpx",
    "httpcore",
    "opentelemetry",
    "DatabaseEngine",
)


class _RunLogCapture(logging.Handler):
    """Keeps the active run's INFO+ log lines for its summary. Records are matched to the
    run through current_run, so concurrent requests in the API process stay separate."""

    def __init__(self, run):
        super().__init__(logging.INFO)
        self.run = run
        self.lines = deque(maxlen=RUN_LOG_TAIL_LINES)
        self.warnings = 0
        self.errors = 0
        self.last_info = None

    def emit(self, record):
        if current_run.get() is not self.run or record.name.startswith(
            NOISY_LOGGER_PREFIXES
        ):
            return
        try:
            message = redact(record.getMessage())[:RUN_LOG_LINE_CHARS]
        except Exception:
            return
        self.lines.append(f"{record.levelname} {message}")
        if record.levelno >= logging.ERROR:
            self.errors += 1
        elif record.levelno >= logging.WARNING:
            self.warnings += 1
        else:
            self.last_info = message

    def summary(self):
        captured = {
            "log_tail": list(self.lines),
            "warnings": self.warnings,
            "errors": self.errors,
        }
        if self.last_info:
            captured["last_message"] = self.last_info
        return captured


class Run:
    def __init__(self, identity):
        self.id = identity
        self.summary = {}

    def add_summary(self, **fields):
        self.summary.update(fields)

    def link(self, ref_type, ids, role):
        _insert_links(self.id, ref_type, ids, role)


def _insert_links(run_id, ref_type, ids, role):
    ids = [str(i) for i in ids if i is not None]
    if not run_id or not ids:
        return
    try:
        with get_db_cursor() as c:
            c.executemany(
                "INSERT INTO run_links(run_id,ref_type,ref_id,role) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                [(run_id, ref_type, i, role) for i in ids],
            )
    except Exception:
        logger.exception("Run links could not be recorded for run %s", run_id)


def link(ref_type, ids, role, run_id=None):
    """Links rows to the given run, else the run active in this context; a no-op outside one."""
    run = current_run.get()
    _insert_links(run_id or (run.id if run else None), ref_type, ids, role)


def start_run(job_name, trigger_type=None, trigger=None, parent_run_id=None):
    if trigger_type is None:
        trigger_type, env_trigger = trigger_from_env()
        trigger = {**env_trigger, **(trigger or {})}
    identity = str(uuid.uuid4())
    try:
        with get_db_cursor() as c:
            c.execute(
                """INSERT INTO run_log(id,job_name,trigger_type,trigger,parent_run_id,trace_id)
                VALUES (%s,%s,%s,%s,%s,%s)""",
                (
                    identity,
                    job_name,
                    trigger_type,
                    json.dumps(redact(trigger or {})),
                    parent_run_id,
                    _trace_id(),
                ),
            )
    except Exception:
        logger.exception("Run %s could not be recorded", job_name)
        return None
    return identity


def adopt_run(identity, job_name, trigger_type, trigger, parent_run_id=None):
    """Attach this process to the launcher-created run row."""
    try:
        with get_db_cursor() as c:
            c.execute(
                """UPDATE run_log SET status='running', job_name=%s, trigger_type=%s,
                parent_run_id=COALESCE(parent_run_id,%s), started_at=now(), finished_at=NULL,
                error=NULL WHERE id=%s""",
                (job_name, trigger_type, parent_run_id, identity),
            )
    except Exception:
        logger.exception("Run %s could not be adopted", identity)


def finish_run(identity, status, error=None, summary=None, end_reason=None):
    if not identity:
        return
    try:
        with get_db_cursor() as c:
            c.execute(
                """UPDATE run_log SET status=%s, finished_at=now(), error=%s, end_reason=%s,
                summary=summary || %s::jsonb WHERE id=%s""",
                (
                    status,
                    redact(error)[:4000] if error else None,
                    end_reason,
                    json.dumps(redact(summary or {}), default=str),
                    identity,
                ),
            )
    except Exception:
        logger.exception("Run %s outcome could not be recorded", identity)


@contextmanager
def run_scope(job_name, trigger_type=None, trigger=None, parent_run_id=None):
    """Records one run around the block. An exception (or a non-zero SystemExit) marks it
    failed and propagates; `run.summary` is saved on exit. Nested scopes become children.
    """
    parent = current_run.get()
    previous_parent_id = os.environ.get("RUN_PARENT_ID")
    adopted_id = os.environ.pop("RUN_ID", None) if parent is None else None
    parent_run_id = (
        parent_run_id or (parent.id if parent else None) or os.getenv("RUN_PARENT_ID")
    )
    if trigger_type is None:
        trigger_type, env_trigger = trigger_from_env()
        trigger = {**env_trigger, **(trigger or {})}
    run = Run(adopted_id or start_run(job_name, trigger_type, trigger, parent_run_id))
    if adopted_id:
        adopt_run(adopted_id, job_name, trigger_type, trigger or {}, parent_run_id)
    # Only a launcher-adopted run exports its id for subprocesses: that pod runs one job, so
    # the shared process environment is safe there, unlike the API where requests overlap.
    if adopted_id:
        os.environ["RUN_PARENT_ID"] = adopted_id
    token = current_run.set(run)
    capture = _RunLogCapture(run)
    logging.getLogger().addHandler(capture)

    def finish(status, error=None):
        logging.getLogger().removeHandler(capture)
        finish_run(run.id, status, error, {**capture.summary(), **run.summary})

    try:
        yield run
    except SystemExit as exc:
        failed = exc.code not in (None, 0)
        finish(
            "failure" if failed else "success",
            f"exit code {exc.code}" if failed else None,
        )
        raise
    except BaseException as exc:
        finish("failure", f"{type(exc).__name__}: {exc}")
        raise
    else:
        finish("success")
    finally:
        logging.getLogger().removeHandler(capture)
        current_run.reset(token)
        if adopted_id:
            if previous_parent_id is None:
                os.environ.pop("RUN_PARENT_ID", None)
            else:
                os.environ["RUN_PARENT_ID"] = previous_parent_id


@contextmanager
def ensure_run(job_name, trigger_type=None, trigger=None):
    """Joins the run already active in this context (a CLI entrypoint's), else starts one -
    for services reached both from a recorded entrypoint and directly over HTTP."""
    run = current_run.get()
    if run:
        yield run
    else:
        with run_scope(job_name, trigger_type, trigger) as run:
            yield run


def run_main(job_name, main):
    """For `sys.exit(main())` entrypoints: a non-zero return code is a failed run."""
    with run_scope(job_name) as run:
        code = main()
        if code not in (None, 0):
            raise SystemExit(code)
        return code


def add_summary(**fields):
    """Adds outcome fields (counts, a `headline`) to the active run; a no-op outside one."""
    run = current_run.get()
    if run:
        run.add_summary(**fields)


def current_run_id():
    run = current_run.get()
    return run.id if run else None


def purge_older_than(days):
    with get_db_cursor() as c:
        c.execute(
            "DELETE FROM run_log WHERE started_at<now()-(%s * interval '1 day')",
            (days,),
        )


# ---- reads -----------------------------------------------------------------------------


def _rows(c):
    keys = [d[0] for d in c.description]
    return [dict(zip(keys, r)) for r in c.fetchall()]


RUN_COLUMNS = """r.id,r.job_name,r.trigger_type,r.trigger,r.parent_run_id,r.status,r.started_at,
 r.finished_at,r.error,r.summary,r.trace_id,r.dedupe_key,r.scheduled_for,r.attempt,
 r.requested_by,r.chain_root_id,r.event_id,r.end_reason,
 (SELECT count(*) FROM run_links l WHERE l.run_id=r.id AND l.role='input') AS input_count,
 (SELECT count(*) FROM run_links l WHERE l.run_id=r.id AND l.role='output' AND l.ref_type<>'llm_session') AS output_count,
 (SELECT count(*) FROM run_links l WHERE l.run_id=r.id AND l.ref_type='llm_session') AS llm_count"""


def list_runs(job=None, status=None, trigger_type=None, limit=50, offset=0):
    with get_db_cursor() as c:
        c.execute(
            f"""SELECT {RUN_COLUMNS} FROM run_log r
            WHERE (%(job)s::text IS NULL OR r.job_name=%(job)s)
              AND (%(status)s::text IS NULL OR r.status=%(status)s)
              AND (%(trigger)s::text IS NULL OR r.trigger_type=%(trigger)s)
            ORDER BY r.started_at DESC LIMIT %(limit)s OFFSET %(offset)s""",
            {
                "job": job,
                "status": status,
                "trigger": trigger_type,
                "limit": limit,
                "offset": offset,
            },
        )
        return _rows(c)


def job_names():
    with get_db_cursor() as c:
        c.execute("SELECT DISTINCT job_name FROM run_log ORDER BY job_name")
        return [r[0] for r in c.fetchall()]


def job_health():
    """Latest run per job, with how many of its last 10 runs failed."""
    with get_db_cursor() as c:
        c.execute(
            f"""SELECT DISTINCT ON (r.job_name) {RUN_COLUMNS},
              (SELECT count(*) FROM (SELECT status FROM run_log x WHERE x.job_name=r.job_name
                 ORDER BY started_at DESC LIMIT 10) recent WHERE status='failure') AS recent_failures
            FROM run_log r ORDER BY r.job_name, r.started_at DESC"""
        )
        return _rows(c)


# Each broker-owned queue dead-letters to `<queue>.dlq` (fantasy-infra's broker policy).
DEAD_LETTER_QUEUES = tuple(
    f"{queue}.dlq" for queue in (LINEUP_UPDATES, WAIVER_ACTIONS, DRAFT_TARGETS)
)
BROKER_TIMEOUT_SECONDS = 5


def dead_letter_depths():
    """Messages parked in each dead-letter queue, or None when the broker can't be read.
    Nothing consumes these queues, so any message here is a failed action someone should
    look at; the cluster doesn't scrape RabbitMQ metrics, so this is where it shows up.
    """
    import pika
    from src.api.routes import AMQP_URL

    connection = None
    depths = []
    try:
        params = pika.URLParameters(AMQP_URL)
        params.socket_timeout = BROKER_TIMEOUT_SECONDS
        params.blocked_connection_timeout = BROKER_TIMEOUT_SECONDS
        connection = pika.BlockingConnection(params)
        for queue in DEAD_LETTER_QUEUES:
            # A missing queue closes the channel, so each check gets its own.
            channel = connection.channel()
            try:
                result = channel.queue_declare(queue=queue, passive=True)
            except pika.exceptions.ChannelClosedByBroker:
                continue
            depths.append({"queue": queue, "messages": result.method.message_count})
            channel.close()
        return depths
    except Exception:
        logger.warning("Dead-letter queue depths unavailable", exc_info=True)
        return None
    finally:
        if connection is not None and connection.is_open:
            connection.close()


def _resolve(c, ref_type, ids):
    """Display rows for linked refs, keyed by id (as text)."""
    if not ids:
        return {}
    if ref_type == "raw_signal":
        c.execute(
            """SELECT s.id::text AS id,s.source,s.url,left(s.raw_text,280) AS text,s.published_at,
            s.ingested_at,s.player_id,p.name AS player_name
            FROM raw_signals s LEFT JOIN players p ON p.player_id=s.player_id
            WHERE s.id=ANY(%s::int[])""",
            (ids,),
        )
    elif ref_type == "rumor_signal":
        c.execute(
            """SELECT s.id::text AS id,s.player_id,p.name AS player_name,s.sentiment,s.confidence,
            s.suggested_action,s.source_raw_signal_ids,s.created_at
            FROM rumor_signals s LEFT JOIN players p ON p.player_id=s.player_id
            WHERE s.id=ANY(%s::int[])""",
            (ids,),
        )
    elif ref_type == "llm_session":
        c.execute(
            "SELECT id,title,status,created_at FROM activity_sessions WHERE id=ANY(%s) AND league_id=%s",
            (ids, league_id()),
        )
    elif ref_type == "activity_action":
        c.execute(
            """SELECT id,kind,title,execution,outcome,created_at FROM activity_actions
            WHERE id=ANY(%s) AND league_id=%s""",
            (ids, league_id()),
        )
    else:
        return {}
    return {row["id"]: row for row in _rows(c)}


def _numeric(ids):
    return [i for i in ids if str(i).isdigit()]


def _resolve_links(c, links):
    """Groups links as {role: {ref_type: [display row, ...]}}, keeping ids whose row is gone."""
    grouped = {}
    for role in ROLES:
        for ref_type in REF_TYPES:
            ids = [
                l["ref_id"]
                for l in links
                if l["role"] == role and l["ref_type"] == ref_type
            ]
            if not ids:
                continue
            lookup_ids = (
                _numeric(ids) if ref_type in ("raw_signal", "rumor_signal") else ids
            )
            found = _resolve(c, ref_type, lookup_ids)
            grouped.setdefault(role, {})[ref_type] = [
                found.get(i, {"id": i, "missing": True}) for i in ids
            ]
    return grouped


def run_detail(identity):
    with get_db_cursor() as c:
        c.execute(f"SELECT {RUN_COLUMNS} FROM run_log r WHERE r.id=%s", (identity,))
        rows = _rows(c)
        if not rows:
            raise RunNotFoundError(identity)
        run = rows[0]
        c.execute(
            "SELECT ref_type,ref_id,role FROM run_links WHERE run_id=%s ORDER BY ref_type,ref_id",
            (identity,),
        )
        run["links"] = _resolve_links(c, _rows(c))
        run["parent"] = None
        if run["parent_run_id"]:
            c.execute(
                "SELECT id,job_name,status,started_at FROM run_log WHERE id=%s",
                (run["parent_run_id"],),
            )
            parents = _rows(c)
            run["parent"] = parents[0] if parents else None
        # Children: nested runs, plus worker runs that executed an action this run queued.
        c.execute(
            """SELECT id,job_name,trigger_type,status,started_at,finished_at FROM run_log
            WHERE parent_run_id=%s OR (trigger->>'tracking_id') IN (
              SELECT ref_id FROM run_links WHERE run_id=%s AND ref_type='activity_action' AND role='output')
            ORDER BY started_at""",
            (identity, identity),
        )
        run["children"] = _rows(c)
        return run


def _runs_linked(c, ref_type, ids, role):
    if not ids:
        return []
    c.execute(
        """SELECT DISTINCT r.id,r.job_name,r.trigger_type,r.status,r.started_at,r.finished_at
        FROM run_log r JOIN run_links l ON l.run_id=r.id
        WHERE l.ref_type=%s AND l.ref_id=ANY(%s) AND l.role=%s ORDER BY r.started_at""",
        (ref_type, [str(i) for i in ids], role),
    )
    return _rows(c)


TRACE_ROOTS = ("raw_signal", "rumor_signal", "activity_action")


def trace(ref_type, ref_id):
    """The rumor pipeline around one article, rumor evaluation or roster move, as stages:
    ingest -> LLM evaluation -> action decision -> submission -> execution. Rumor-to-article
    edges come from rumor_signals.source_raw_signal_ids and rumor-to-action edges from
    bot_decisions.inputs.rumor_signal_id, so the chain holds even for rows written before
    run_log existed; only the run cards need run_log."""
    if ref_type not in TRACE_ROOTS:
        raise ValueError(f"Unsupported ref_type {ref_type}")
    with get_db_cursor() as c:
        raw_ids, rumor_ids, action_ids = set(), set(), set()
        if ref_type == "raw_signal":
            if not str(ref_id).isdigit():
                raise RunNotFoundError(ref_id)
            raw_ids.add(int(ref_id))
            c.execute(
                "SELECT id FROM rumor_signals WHERE source_raw_signal_ids @> ARRAY[%s::int]",
                (int(ref_id),),
            )
            rumor_ids.update(r[0] for r in c.fetchall())
        elif ref_type == "rumor_signal":
            if not str(ref_id).isdigit():
                raise RunNotFoundError(ref_id)
            rumor_ids.add(int(ref_id))
        else:
            action_ids.add(str(ref_id))
            c.execute(
                "SELECT DISTINCT (inputs->>'rumor_signal_id')::int FROM bot_decisions WHERE tracking_id=%s AND inputs ? 'rumor_signal_id'",
                (str(ref_id),),
            )
            rumor_ids.update(r[0] for r in c.fetchall() if r[0] is not None)
        if rumor_ids:
            c.execute(
                "SELECT source_raw_signal_ids FROM rumor_signals WHERE id=ANY(%s)",
                (list(rumor_ids),),
            )
            if ref_type != "raw_signal":
                for (sources,) in c.fetchall():
                    raw_ids.update(sources or [])
            if ref_type != "activity_action":
                c.execute(
                    "SELECT DISTINCT tracking_id FROM bot_decisions WHERE inputs->>'rumor_signal_id' = ANY(%s)",
                    ([str(i) for i in rumor_ids],),
                )
                action_ids.update(r[0] for r in c.fetchall())
        raw = _resolve(c, "raw_signal", sorted(raw_ids))
        rumors = _resolve(c, "rumor_signal", sorted(rumor_ids))
        actions = _resolve(c, "activity_action", sorted(action_ids))
        if not raw and not rumors and not actions:
            raise RunNotFoundError(ref_id)
        digest_runs = _runs_linked(c, "rumor_signal", rumor_ids, "output")
        sessions = []
        if digest_runs:
            c.execute(
                """SELECT s.id,s.title,s.status,s.created_at FROM activity_sessions s JOIN run_links l
                ON l.ref_type='llm_session' AND l.ref_id=s.id WHERE l.run_id=ANY(%s) ORDER BY s.created_at""",
                ([r["id"] for r in digest_runs],),
            )
            sessions = _rows(c)
        worker_runs = []
        if action_ids:
            c.execute(
                """SELECT id,job_name,trigger_type,status,started_at,finished_at,trigger->>'tracking_id' AS tracking_id
                FROM run_log WHERE trigger->>'tracking_id' = ANY(%s) ORDER BY started_at""",
                (list(action_ids),),
            )
            worker_runs = _rows(c)
        return {
            "root": {"ref_type": ref_type, "ref_id": str(ref_id)},
            "stages": [
                {
                    "key": "ingest",
                    "title": "Article ingested",
                    "runs": _runs_linked(c, "raw_signal", raw_ids, "output"),
                    "items": list(raw.values()),
                },
                {
                    "key": "evaluation",
                    "title": "LLM evaluation",
                    "runs": digest_runs,
                    "sessions": sessions,
                    "items": list(rumors.values()),
                },
                {
                    "key": "decision",
                    "title": "Action decision",
                    "runs": _runs_linked(c, "rumor_signal", rumor_ids, "input"),
                    "items": [],
                },
                {
                    "key": "submission",
                    "title": "Roster move queued",
                    "runs": _runs_linked(c, "activity_action", action_ids, "output"),
                    "items": list(actions.values()),
                },
                {
                    "key": "execution",
                    "title": "Executed by worker",
                    "runs": worker_runs,
                    "items": [],
                },
            ],
        }
