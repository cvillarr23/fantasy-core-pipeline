"""Durable user-facing history. Queue acceptance is never transaction confirmation."""

import json
import os
import re
import uuid
from src.database import get_db_cursor

SCHEMA = """
CREATE TABLE IF NOT EXISTS activity_actions (
 id TEXT PRIMARY KEY, league_id TEXT NOT NULL, team_id TEXT, season INT,
 kind TEXT NOT NULL, title TEXT NOT NULL, execution TEXT NOT NULL DEFAULT 'queued',
 outcome TEXT NOT NULL DEFAULT 'unknown', payload JSONB NOT NULL DEFAULT '{}',
 evidence JSONB NOT NULL DEFAULT '{}', idempotency_key TEXT, attempts INT NOT NULL DEFAULT 0,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 next_check_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 UNIQUE(league_id,kind,idempotency_key)
);
CREATE TABLE IF NOT EXISTS activity_events (
 id BIGSERIAL PRIMARY KEY, action_id TEXT NOT NULL REFERENCES activity_actions(id),
 stage TEXT NOT NULL, detail JSONB NOT NULL DEFAULT '{}', created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS activity_actions_recent ON activity_actions(league_id,created_at DESC);
CREATE TABLE IF NOT EXISTS activity_outbox (
 action_id TEXT PRIMARY KEY REFERENCES activity_actions(id), queue TEXT NOT NULL,
 body JSONB NOT NULL, published_at TIMESTAMPTZ, attempts INT NOT NULL DEFAULT 0
);
ALTER TABLE activity_outbox ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE activity_outbox ADD COLUMN IF NOT EXISTS last_error TEXT;
CREATE INDEX IF NOT EXISTS activity_outbox_due ON activity_outbox(next_attempt_at) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS activity_sessions (
 id TEXT PRIMARY KEY, league_id TEXT NOT NULL, action_id TEXT,
 title TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'running', content JSONB NOT NULL DEFAULT '[]',
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS activity_sessions_recent ON activity_sessions(league_id,created_at DESC);
CREATE INDEX IF NOT EXISTS activity_events_by_action ON activity_events(action_id,id);
CREATE TABLE IF NOT EXISTS activity_reads (
 user_id TEXT NOT NULL, item_id TEXT NOT NULL, read_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(user_id,item_id)
);
"""


def league_id():
    return os.environ.get("ESPN_LEAGUE_ID", "")


def redact(value):
    if isinstance(value, dict):
        return {
            k: "[REDACTED]"
            if re.search(
                r"password|secret|token|cookie|authorization|api.?key|espn_s2|swid",
                k,
                re.I,
            )
            else redact(v)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, str):
        for key, secret in os.environ.items():
            if len(secret) >= 8 and re.search(
                r"password|secret|token|api.?key|espn_s2|swid", key, re.I
            ):
                value = value.replace(secret, "[REDACTED]")
        return re.sub(r"(?i)(Bearer\s+)[\w.\-/+=]+", r"\1[REDACTED]", value)
    return value


IDEMPOTENCY_TTL_SECONDS = {"lineup_updates": 1800, "draft_targets": 900, "waiver_actions": 604800}


def create_action(queue, payload, tracking_id=None):
    identity = tracking_id or payload.get("tracking_id") or str(uuid.uuid4())
    league = str(payload.get("league_id") or league_id())
    key = payload.get("idempotency_key")
    inputs = payload.get("decision_inputs") or {}
    if key and inputs.get("season") is not None and inputs.get("week") is not None:
        key = f"{key}:season:{inputs['season']}:week:{inputs['week']}"
    body = {**payload, "tracking_id": identity, "league_id": league}
    queue_body = {k: v for k, v in body.items() if k != "decision_inputs"}
    if "optimized_swaps" in queue_body:
        queue_body["optimized_swaps"] = [
            {k: v for k, v in swap.items() if k != "decision_inputs"}
            for swap in queue_body["optimized_swaps"]
        ]
    title = f"{queue.replace('_', ' ')} · {payload.get('name') or payload.get('team_id', 'Background task')}"
    with get_db_cursor() as c:
        if key:
            # Keep pending work protected even if delivery outlives the normal retry window.
            c.execute("""UPDATE activity_actions SET idempotency_key=NULL
              WHERE league_id=%s AND kind=%s AND idempotency_key=%s
              AND execution NOT IN ('queued','delivered','running')
              AND created_at<now()-(%s * interval '1 second')""",
              (league, queue, key, IDEMPOTENCY_TTL_SECONDS.get(queue, 1800)))
        inputs = payload.get("decision_inputs") or {}
        if (
            queue == "waiver_actions"
            and key
            and inputs.get("season") is not None
            and inputs.get("week") is not None
        ):
            c.execute(
                """SELECT tracking_id FROM bot_decisions
                WHERE bot_name='waiver_sniper' AND action='waiver_claim'
                AND team_id=%s AND season=%s AND week=%s
                AND target_player_id=%s AND drop_player_id IS NOT DISTINCT FROM %s
                ORDER BY decided_at DESC LIMIT 1""",
                (
                    payload.get("team_id"),
                    inputs["season"],
                    inputs["week"],
                    payload.get("player_id"),
                    payload.get("drop_player_id"),
                ),
            )
            prior = c.fetchone()
            if prior:
                prior_id = str(prior[0])
                c.execute(
                    """INSERT INTO activity_actions(id,league_id,team_id,season,kind,title,payload,execution)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,'finished') ON CONFLICT DO NOTHING""",
                    (
                        prior_id,
                        league,
                        payload.get("team_id"),
                        inputs["season"],
                        queue,
                        title,
                        json.dumps(redact(body)),
                    ),
                )
                return prior_id
        c.execute(
            """INSERT INTO activity_actions(id,league_id,team_id,season,kind,title,payload,idempotency_key)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id""",
            (
                identity,
                league,
                payload.get("team_id"),
                payload.get("season"),
                queue,
                title,
                json.dumps(redact(body)),
                key,
            ),
        )
        row = c.fetchone()
        if not row:
            c.execute(
                "SELECT id FROM activity_actions WHERE id=%s OR (league_id=%s AND kind=%s AND idempotency_key=%s)",
                (identity, league, queue, key),
            )
            return c.fetchone()[0]
        c.execute(
            "INSERT INTO activity_outbox(action_id,queue,body) VALUES (%s,%s,%s)",
            (identity, queue, json.dumps(queue_body)),
        )
        c.execute(
            "INSERT INTO activity_events(action_id,stage) VALUES (%s,'queued')",
            (identity,),
        )
    return identity


def record_event(identity, stage, detail=None, execution=None, outcome=None):
    with get_db_cursor() as c:
        c.execute("SELECT id FROM activity_actions WHERE id=%s FOR UPDATE", (identity,))
        if not c.fetchone():
            return
        c.execute(
            "INSERT INTO activity_events(action_id,stage,detail) VALUES (%s,%s,%s)",
            (identity, stage, json.dumps(redact(detail or {}))),
        )
        c.execute(
            """UPDATE activity_actions SET updated_at=CASE WHEN %s='status_checked' THEN updated_at ELSE now() END, execution=COALESCE(%s,execution),
            outcome=COALESCE(%s,outcome), evidence=evidence || %s::jsonb WHERE id=%s""",
            (stage, execution, outcome, json.dumps(redact(detail or {})), identity),
        )


def _rows(c):
    keys = [d[0] for d in c.description]
    return [dict(zip(keys, r)) for r in c.fetchall()]


def list_actions(user, limit=50, offset=0, attention=False):
    with get_db_cursor() as c:
        c.execute(
            """SELECT a.*, (r.read_at IS NULL OR r.read_at<a.updated_at) AS unread
        FROM activity_actions a LEFT JOIN activity_reads r ON r.item_id=a.id AND r.user_id=%s
        WHERE a.league_id=%s AND (%s=false OR a.execution='failed' OR
          (a.outcome='unknown' AND a.created_at < now()-interval '5 minutes') OR a.outcome='awaiting_approval')
        ORDER BY a.created_at DESC LIMIT %s OFFSET %s""",
            (user, league_id(), attention, limit, offset),
        )
        return _rows(c)


def action_detail(identity):
    with get_db_cursor() as c:
        c.execute(
            "SELECT * FROM activity_actions WHERE id=%s AND league_id=%s",
            (identity, league_id()),
        )
        rows = _rows(c)
        if not rows:
            return None
        item = rows[0]
        c.execute(
            "SELECT * FROM activity_events WHERE action_id=%s ORDER BY id", (identity,)
        )
        item["events"] = _rows(c)
        c.execute(
            "SELECT id,title,status FROM activity_sessions WHERE action_id=%s AND league_id=%s",
            (identity, league_id()),
        )
        item["sessions"] = _rows(c)
        return item


def session_start(title, action_id=None, identity=None):
    identity = identity or str(uuid.uuid4())
    with get_db_cursor() as c:
        c.execute("SELECT set_config('statement_timeout', '1000ms', true)")
        c.execute(
            """INSERT INTO activity_sessions(id,league_id,action_id,title) VALUES (%s,%s,%s,%s)
            ON CONFLICT(id) DO NOTHING""",
            (identity, league_id(), action_id, redact(title)[:200]),
        )
    return identity


def session_event(identity, role, content, status=None):
    with get_db_cursor() as c:
        c.execute("SELECT set_config('statement_timeout', '1000ms', true)")
        c.execute(
            """UPDATE activity_sessions SET content=content || %s::jsonb,
        status=COALESCE(%s,status),updated_at=now() WHERE id=%s AND league_id=%s""",
            (
                json.dumps([{"role": role, "content": redact(content)}]),
                status,
                identity,
                league_id(),
            ),
        )


def sessions(identity=None, limit=50, offset=0):
    with get_db_cursor() as c:
        if identity:
            c.execute(
                "SELECT * FROM activity_sessions WHERE id=%s AND league_id=%s",
                (identity, league_id()),
            )
        else:
            c.execute(
                "SELECT id,title,status,action_id,created_at,updated_at FROM activity_sessions WHERE league_id=%s ORDER BY created_at DESC LIMIT %s OFFSET %s",
                (league_id(), limit, offset),
            )
        return _rows(c)


def mark_read(identity, user):
    if not action_detail(identity) and not sessions(identity):
        return False
    with get_db_cursor() as c:
        c.execute(
            "INSERT INTO activity_reads(user_id,item_id) VALUES (%s,%s) ON CONFLICT(user_id,item_id) DO UPDATE SET read_at=now()",
            (user, identity),
        )
    return True


def bot_summary():
    with get_db_cursor() as c:
        c.execute(
            """SELECT DISTINCT ON(kind) kind,title,execution,outcome,updated_at,
          (SELECT count(*) FROM activity_actions b WHERE b.kind=a.kind AND b.league_id=a.league_id AND b.execution IN ('queued','delivered','running')) AS active_count
          FROM activity_actions a WHERE league_id=%s ORDER BY kind,updated_at DESC""",
            (league_id(),),
        )
        return _rows(c)


def import_legacy(cursor):
    """Only import execution records explicitly scoped to this league; no inferred joins."""
    cursor.execute(
        """INSERT INTO activity_actions(id,league_id,team_id,season,kind,title,execution,outcome,payload,created_at,updated_at,next_check_at)
      SELECT 'legacy-execution-' || id,league_id,NULL,season,COALESCE(bot_name,transaction_type,'legacy'),
        'Historical run · ' || COALESCE(bot_name,transaction_type,'bot'),
        CASE WHEN status IN ('failure','failed') THEN 'failed' ELSE 'finished' END,'unknown',
        jsonb_build_object('historical',true,'recorded_status',status,'player_name',player_name,'note','Completion evidence was not captured for this historical record.'),
        executed_at,executed_at,'infinity'::timestamptz FROM execution_log WHERE league_id=%s
      ORDER BY executed_at DESC LIMIT 1000 ON CONFLICT DO NOTHING""",
        (league_id(),),
    )
