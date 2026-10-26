"""Optional checks of the run audit log SQL against an explicitly supplied local disposable DB."""

from contextlib import contextmanager
import json
import os
import subprocess
import sys
from unittest.mock import patch
from uuid import uuid4

import psycopg2
from psycopg2 import sql
import pytest

from src import models
from src.services import activity_service as activity, run_log_service as run_log


@pytest.fixture
def postgres(monkeypatch):
    dsn = os.getenv("ACTIVITY_TEST_DSN")
    if not dsn:
        pytest.skip("Set ACTIVITY_TEST_DSN to a disposable local Postgres")
    monkeypatch.setenv("ESPN_LEAGUE_ID", "")
    assert psycopg2.extensions.parse_dsn(dsn).get("host") in {"127.0.0.1", "localhost"}
    conn = psycopg2.connect(dsn)
    schema = "run_log_test_" + uuid4().hex
    with conn, conn.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {};").format(sql.Identifier(schema)))
        cursor.execute(sql.SQL("SET search_path TO {};").format(sql.Identifier(schema)))
        for ddl in (
            models.CREATE_PLAYERS_TABLE,
            models.CREATE_RAW_SIGNALS_TABLE,
            models.CREATE_RUMOR_SIGNALS_TABLE,
            models.CREATE_BOT_DECISIONS_TABLE,
            activity.SCHEMA,
            run_log.SCHEMA,
            run_log.SCHEMA,
        ):
            cursor.execute(ddl)

    @contextmanager
    def cursor_context():
        with conn, conn.cursor() as cursor:
            yield cursor

    with (
        patch.object(activity, "get_db_cursor", cursor_context),
        patch.object(run_log, "get_db_cursor", cursor_context),
    ):
        try:
            yield conn
        finally:
            conn.rollback()
            with conn, conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE;").format(sql.Identifier(schema))
                )
            conn.close()


def _seed(conn):
    """One article -> digest rumor -> rumor action claim, as the three jobs write them."""
    with conn, conn.cursor() as c:
        c.execute(
            "INSERT INTO players(player_id,name,position,team,status) VALUES ('4046','Player One','RB','KC','Active')"
        )
        c.execute(
            """INSERT INTO raw_signals(source,source_item_id,content_hash,raw_text,url,player_id)
            VALUES ('rotowire_rss','r1','h','Player One limited at practice','https://x/1','4046') RETURNING id"""
        )
        raw_id = c.fetchone()[0]
        c.execute(
            """INSERT INTO rumor_signals(player_id,sentiment,confidence,suggested_action,source_raw_signal_ids,max_raw_signal_id)
            VALUES ('4046','negative',0.9,'bench_watch',ARRAY[%s],%s) RETURNING id""",
            (raw_id, raw_id),
        )
        rumor_id = c.fetchone()[0]
        c.execute(
            """INSERT INTO bot_decisions(tracking_id,bot_name,action,team_id,inputs)
            VALUES ('action-1','lineup_bot','lineup_swap','8',%s)""",
            (json.dumps({"rumor_signal_id": rumor_id}),),
        )
        c.execute(
            "INSERT INTO activity_actions(id,league_id,kind,title) VALUES ('action-1','','lineup_updates','lineup updates · 8')"
        )
        c.execute(
            "INSERT INTO activity_sessions(id,league_id,title) VALUES ('session-1','','rumor_digest')"
        )
    with run_log.run_scope(
        "sync_rumors", "schedule", {"cronjob": "fantasy-rumor-syncer"}
    ):
        run_log.link("raw_signal", [raw_id], "output")
    with run_log.run_scope("rumor_digest_bot", "schedule") as digest:
        run_log.link("raw_signal", [raw_id], "input")
        run_log.link("rumor_signal", [rumor_id], "output")
        run_log.link("llm_session", ["session-1"], "output")
    with run_log.run_scope("rumor_action_trigger", "schedule") as decision:
        run_log.link("rumor_signal", [rumor_id], "input")
        run_log.link("activity_action", ["action-1"], "output")
    with run_log.run_scope(
        "lineup_bot",
        "queue",
        {"queue": "lineup_updates", "tracking_id": "action-1"},
        parent_run_id=decision.id,
    ):
        pass
    return raw_id, rumor_id, digest.id, decision.id


def test_runs_list_detail_and_health(postgres):
    raw_id, rumor_id, digest_id, decision_id = _seed(postgres)
    with pytest.raises(RuntimeError):
        with run_log.run_scope("sync_rumors", "schedule"):
            raise RuntimeError("feed down")

    runs = run_log.list_runs()
    assert [r["job_name"] for r in runs][:2] == ["sync_rumors", "lineup_bot"]
    assert [r["job_name"] for r in run_log.list_runs(status="failure")] == [
        "sync_rumors"
    ]
    assert {r["job_name"] for r in run_log.list_runs(trigger_type="queue")} == {
        "lineup_bot"
    }

    digest = run_log.run_detail(digest_id)
    assert (digest["input_count"], digest["output_count"], digest["llm_count"]) == (
        1,
        1,
        1,
    )
    assert digest["links"]["input"]["raw_signal"][0]["player_name"] == "Player One"
    assert (
        digest["links"]["output"]["rumor_signal"][0]["suggested_action"]
        == "bench_watch"
    )
    assert digest["links"]["output"]["llm_session"][0]["title"] == "rumor_digest"

    decision = run_log.run_detail(decision_id)
    assert [c["job_name"] for c in decision["children"]] == ["lineup_bot"]

    health = {row["job_name"]: row for row in run_log.job_health()}
    assert health["sync_rumors"]["status"] == "failure"
    assert health["sync_rumors"]["recent_failures"] == 1
    assert health["rumor_digest_bot"]["recent_failures"] == 0

    with pytest.raises(run_log.RunNotFoundError):
        run_log.run_detail(str(uuid4()))


@pytest.mark.parametrize("root", ["raw_signal", "rumor_signal", "activity_action"])
def test_trace_walks_article_to_execution_from_any_root(postgres, root):
    raw_id, rumor_id, digest_id, decision_id = _seed(postgres)
    ref = {
        "raw_signal": raw_id,
        "rumor_signal": rumor_id,
        "activity_action": "action-1",
    }[root]

    stages = {s["key"]: s for s in run_log.trace(root, str(ref))["stages"]}

    assert [i["id"] for i in stages["ingest"]["items"]] == [str(raw_id)]
    assert [r["job_name"] for r in stages["ingest"]["runs"]] == ["sync_rumors"]
    assert [r["id"] for r in stages["evaluation"]["runs"]] == [digest_id]
    assert [s["id"] for s in stages["evaluation"]["sessions"]] == ["session-1"]
    assert [i["id"] for i in stages["evaluation"]["items"]] == [str(rumor_id)]
    assert [r["id"] for r in stages["decision"]["runs"]] == [decision_id]
    assert [i["id"] for i in stages["submission"]["items"]] == ["action-1"]
    assert [r["job_name"] for r in stages["execution"]["runs"]] == ["lineup_bot"]


def test_trace_of_unknown_ref_is_not_found(postgres):
    with pytest.raises(run_log.RunNotFoundError):
        run_log.trace("raw_signal", "999")
    with pytest.raises(run_log.RunNotFoundError):
        run_log.trace("rumor_signal", "not-a-number")


def test_purge_cascades_links(postgres):
    raw_id, *_ = _seed(postgres)
    with postgres, postgres.cursor() as c:
        c.execute("UPDATE run_log SET started_at=now()-interval '100 days'")
    run_log.purge_older_than(90)
    with postgres, postgres.cursor() as c:
        c.execute(
            "SELECT (SELECT count(*) FROM run_log), (SELECT count(*) FROM run_links)"
        )
        assert c.fetchone() == (0, 0)


def test_launcher_columns_preserve_existing_rows_and_enforce_dedupe(postgres):
    with postgres, postgres.cursor() as c:
        c.execute(
            "INSERT INTO run_log(id,job_name,trigger_type) VALUES ('legacy','old_job','manual')"
        )
        c.execute(
            "INSERT INTO run_log(id,job_name,trigger_type,dedupe_key) VALUES ('new','new_job','chain','once')"
        )
        with pytest.raises(psycopg2.errors.UniqueViolation):
            c.execute(
                "INSERT INTO run_log(id,job_name,trigger_type,dedupe_key) VALUES ('duplicate','new_job','chain','once')"
            )
        postgres.rollback()
        c.execute(
            "SELECT dedupe_key,attempt,scheduled_for,requested_by,chain_root_id,event_id,end_reason FROM run_log WHERE id='legacy'"
        )
        assert c.fetchone() == (None, 1, None, None, None, None, None)


def test_subprocess_run_is_child_of_adopted_run(postgres, monkeypatch):
    dsn = os.environ["ACTIVITY_TEST_DSN"]
    conn_info = psycopg2.extensions.parse_dsn(dsn)
    with postgres, postgres.cursor() as c:
        c.execute("SELECT current_schema()")
        schema = c.fetchone()[0]
    monkeypatch.setenv("DB_HOST", conn_info["host"])
    monkeypatch.setenv("DB_NAME", conn_info["dbname"])
    monkeypatch.setenv("DB_USER", conn_info["user"])
    monkeypatch.setenv("DB_PASSWORD", conn_info["password"])
    monkeypatch.setenv("PGOPTIONS", f"-c search_path={schema}")
    launcher_id = str(uuid4())
    with postgres, postgres.cursor() as c:
        c.execute(
            """INSERT INTO run_log(id,job_name,trigger_type,status,dedupe_key,requested_by)
            VALUES (%s,'draft_prep_refresher','chain','queued','daily-prep','launcher')""",
            (launcher_id,),
        )
    monkeypatch.setenv("RUN_ID", launcher_id)
    with run_log.run_scope("draft_prep_refresher") as parent:
        subprocess.run(
            [
                sys.executable,
                "-c",
                "from src.services.run_log_service import run_scope; "
                "exec(\"with run_scope('draft_prep_child'): pass\")",
            ],
            check=True,
            env=os.environ.copy(),
        )
        with postgres, postgres.cursor() as c:
            c.execute("SELECT parent_run_id FROM run_log WHERE job_name='draft_prep_child'")
            assert c.fetchone() == (parent.id,)
            c.execute("SELECT count(*),status,dedupe_key,requested_by FROM run_log WHERE id=%s GROUP BY status,dedupe_key,requested_by", (launcher_id,))
            assert c.fetchone() == (1, "success", "daily-prep", "launcher")
