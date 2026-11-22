"""Optional end-to-end checks of trade-offer approval runs against a disposable local Postgres.

The worker's trade scan opens each offer's approval run and supersedes the previous scan's
with its own SQL (`db/run_log.js` `openApproval` / `supersedeApprovals`). `WORKER_OPEN_SQL`
and `WORKER_SUPERSEDE_SQL` mirror it, so these tests pin the contract both writers share:
the columns, the dedupe key, the trigger shape and which transitions the worker may make.
"""

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.launcher.runs import Runs
from src.launcher.scheduler import PostgresSchedulerState
from src.services import approval_service, jobs_service
from src.services import trade_offer_service as service
from tests.test_launcher_k8s import CATALOG
from tests.test_launcher_postgres import postgres, rows  # noqa: F401 (fixture)
from tests.test_launcher_runs_postgres import claimed, row
from tests.test_trade_offers import staged

# The worker's openApproval insert (fantasy-browser-worker src/db/run_log.js).
WORKER_OPEN_SQL = """INSERT INTO run_log(id,job_name,trigger_type,trigger,parent_run_id,status,
  executor,dedupe_key,scheduled_for,expires_at)
VALUES (gen_random_uuid()::text,'approval','approval',%s::jsonb,%s,'waiting','approval',
  'approval@' || %s || '#' || %s, now(), %s)
ON CONFLICT (dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING RETURNING id"""
# The worker's supersedeApprovals, inside its locked retire transaction.
WORKER_SUPERSEDE_SQL = """UPDATE run_log SET status='skipped', end_reason='superseded',
  requested_by='trade_scan', finished_at=now()
WHERE executor='approval' AND status='waiting' AND trigger->>'key'=%s AND started_at<%s
RETURNING id"""


def trade_catalog():
    catalog = CATALOG.model_copy(deep=True)
    catalog.jobs["trade-submit"] = catalog.jobs["trade-scan"].model_copy(
        update={
            "command": ["node", "src/bots/trade_builder.js"],
            "params": {"offer_key": "str"},
            "schedules": [],
        }
    )
    return catalog


@pytest.fixture
def db(postgres, monkeypatch):  # noqa: F811
    monkeypatch.setenv("ESPN_LEAGUE_ID", "123456789")
    monkeypatch.setenv("ESPN_TEAM_ID", "8")

    @contextmanager
    def cursor_context():
        with postgres, postgres.cursor() as cursor:
            yield cursor

    with (
        patch.object(approval_service, "get_db_cursor", cursor_context),
        patch.object(service, "get_db_cursor", cursor_context),
        patch.object(jobs_service, "_catalog", trade_catalog),
        patch("src.api.routes._record_bot_decision"),
    ):
        yield postgres, cursor_context


def stage(cursor_context, scan_run_id, message="  Fair deal?  "):
    """The worker's staging: the offer's execution_log row, then its approval run."""
    offer = staged(message)["counter_offer"]
    offer["scan_run_id"] = scan_run_id
    key = offer["offer_key"]
    with cursor_context() as c:
        c.execute(
            "INSERT INTO execution_log (transaction_type, bot_name, status, league_id, "
            "season, counter_offer, dry_run) VALUES ('trade_offer_submit', "
            "'trade_builder', 'awaiting_approval', '123456789', 2026, %s::jsonb, true)",
            (json.dumps(offer),),
        )
        trigger = {
            "key": key,
            "title": "Send trade to team 3",
            "link": f"/trades?offer={key}",
            "then": {"job": "trade-submit", "params": {"offer_key": key}},
        }
        c.execute(
            WORKER_OPEN_SQL,
            (
                json.dumps(trigger),
                scan_run_id,
                scan_run_id,
                key,
                datetime.now(timezone.utc) + timedelta(hours=72),
            ),
        )
        opened = c.fetchone()
    return key, opened[0] if opened else None


def offer_status(conn, key):
    return rows(
        conn,
        "SELECT status FROM execution_log WHERE counter_offer->>'offer_key'=%s "
        "ORDER BY id DESC LIMIT 1",
        (key,),
    )[0][0]


def test_the_worker_opened_approval_is_the_one_core_finds(db):
    conn, cursor_context = db
    scan = claimed("trade-scan")
    key, approval = stage(cursor_context, scan)
    columns = "job_name,status,executor,trigger_type,parent_run_id,dedupe_key"
    assert row(conn, approval, columns) == (
        "approval",
        "waiting",
        "approval",
        "approval",
        scan,
        approval_service.approval_dedupe_key(scan, key),
    )
    with cursor_context() as c:
        opened = Runs(c).approval(approval)
    assert opened.then == {"job": "trade-submit", "params": {"offer_key": key}}
    # Re-staging the same offer in the same scan opens nothing new.
    assert stage(cursor_context, scan)[1] is None


def test_approving_starts_trade_submit_from_the_approval(db):
    conn, cursor_context = db
    scan = claimed("trade-scan")
    key, approval = stage(cursor_context, scan)
    response = service.approve_offer(key, "conor")
    assert row(conn, approval, "status,end_reason,requested_by") == (
        "success",
        "approved",
        "conor",
    )
    assert row(
        conn, response["run_id"], "job_name,status,parent_run_id,dedupe_key,trigger"
    ) == (
        "trade-submit",
        "queued",
        approval,
        f"trade-submit@{approval}",
        {"params": {"offer_key": key}, "downstream": False},
    )
    assert offer_status(conn, key) == "approved"
    assert service.approve_offer(key, "conor")["duplicate"] is True


def test_an_edited_offer_sends_its_edited_key(db):
    conn, cursor_context = db
    key, approval = stage(cursor_context, claimed("trade-scan"))
    response = service.approve_offer(key, "conor", message="Edited note")
    assert response["offer_key"] != key
    assert row(conn, response["run_id"], "trigger")[0]["params"] == {
        "offer_key": response["offer_key"]
    }
    assert row(conn, approval, "trigger")[0]["then"]["params"] == {
        "offer_key": response["offer_key"]
    }


def test_dismissing_the_offer_dismisses_its_approval(db):
    conn, cursor_context = db
    key, approval = stage(cursor_context, claimed("trade-scan"))
    service.dismiss_offer(key, "conor")
    assert row(conn, approval, "status,end_reason,requested_by") == (
        "skipped",
        "dismissed",
        "conor",
    )
    assert offer_status(conn, key) == "dismissed"


def test_a_superseded_approval_refuses_approval_and_starts_nothing(db):
    conn, cursor_context = db
    key, approval = stage(cursor_context, claimed("trade-scan"))
    with cursor_context() as c:
        c.execute(
            WORKER_SUPERSEDE_SQL,
            (key, datetime.now(timezone.utc) + timedelta(seconds=1)),
        )
        assert c.fetchall() == [(approval,)]
    assert row(conn, approval, "status,end_reason") == ("skipped", "superseded")
    with pytest.raises(service.OfferConflict, match="skipped"):
        service.approve_offer(key, "conor")
    assert rows(conn, "SELECT id FROM run_log WHERE job_name='trade-submit'") == []


def test_the_worker_supersedes_only_older_waiting_approvals(db):
    conn, cursor_context = db
    key, approved = stage(cursor_context, claimed("trade-scan"))
    service.approve_offer(key, "conor")
    cutoff = datetime.now(timezone.utc)
    _, fresh = stage(cursor_context, claimed("trade-scan", dedupe_key="scan-2"))
    with cursor_context() as c:
        c.execute(WORKER_SUPERSEDE_SQL, (key, cutoff))
        assert c.fetchall() == []
    assert row(conn, approved, "status")[0] == "success"
    assert row(conn, fresh, "status")[0] == "waiting"


def test_expiry_dismisses_the_offer_in_the_sweep(db):
    conn, cursor_context = db
    key, approval = stage(cursor_context, claimed("trade-scan"))
    other_key, other = stage(
        cursor_context, claimed("trade-scan", dedupe_key="scan-2"), message=None
    )
    later = datetime.now(timezone.utc) + timedelta(hours=73)
    with cursor_context() as c:
        c.execute(
            "UPDATE run_log SET expires_at=%s WHERE id=%s",
            (later + timedelta(hours=1), other),
        )
    state = PostgresSchedulerState(cursor_context)
    assert state.expire_approvals(later) == [approval]
    assert row(conn, approval, "status,end_reason") == ("skipped", "expired")
    dismissal = rows(
        conn,
        "SELECT status, outcome_details->'dismissal' FROM execution_log "
        "WHERE counter_offer->>'offer_key'=%s",
        (key,),
    )
    assert [(status, d["dismissed_by"], d["reason"]) for status, d in dismissal] == [
        ("dismissed", "expired", "expired")
    ]
    assert dismissal[0][1]["approval_run_id"] == approval
    assert offer_status(conn, other_key) == "awaiting_approval"


def test_a_legacy_offer_still_starts_trade_submit_directly(db):
    conn, cursor_context = db
    scan = claimed("trade-scan")
    offer = staged()["counter_offer"]
    offer["scan_run_id"] = scan
    with cursor_context() as c:
        c.execute(
            "INSERT INTO execution_log (transaction_type, status, league_id, season, "
            "counter_offer) VALUES ('trade_offer_submit', 'awaiting_approval', "
            "'123456789', 2026, %s::jsonb)",
            (json.dumps(offer),),
        )
    response = service.approve_offer(offer["offer_key"], "conor")
    assert row(conn, response["run_id"], "job_name,parent_run_id,dedupe_key") == (
        "trade-submit",
        scan,
        f"trade-submit@{offer['offer_key']}",
    )


def test_the_generic_route_refuses_a_trade_approval(db):
    conn, cursor_context = db
    _, approval = stage(cursor_context, claimed("trade-scan"))
    for decide in (approval_service.approve, approval_service.dismiss):
        with pytest.raises(approval_service.ApprovalDecidedElsewhereError):
            decide(approval, "conor")
    assert row(conn, approval, "status")[0] == "waiting"
