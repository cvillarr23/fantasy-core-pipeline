"""Optional checks of the held-outbox SQL against an explicitly supplied local disposable DB."""

from contextlib import contextmanager
import json
import os
from unittest.mock import MagicMock, patch
from uuid import uuid4

import psycopg2
from psycopg2 import sql
import pytest

from src.services import activity_runner as runner, activity_service as activity


@pytest.fixture
def postgres(monkeypatch):
    dsn = os.getenv("ACTIVITY_TEST_DSN")
    if not dsn:
        pytest.skip("Set ACTIVITY_TEST_DSN to a disposable local Postgres")
    monkeypatch.setenv("ESPN_LEAGUE_ID", "")
    assert psycopg2.extensions.parse_dsn(dsn).get("host") in {"127.0.0.1", "localhost"}
    conn = psycopg2.connect(dsn)
    schema = "activity_test_" + uuid4().hex
    with conn, conn.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {};").format(sql.Identifier(schema)))
        cursor.execute(sql.SQL("SET search_path TO {};").format(sql.Identifier(schema)))
        # Twice, as run_migrations would on every API start.
        cursor.execute(activity.SCHEMA)
        cursor.execute(activity.SCHEMA)

    @contextmanager
    def cursor_context():
        with conn, conn.cursor() as cursor:
            yield cursor

    with patch.object(activity, "get_db_cursor", cursor_context), \
         patch.object(runner, "get_db_cursor", cursor_context), \
         patch.object(runner.pika, "BlockingConnection", return_value=MagicMock()):
        try:
            yield conn
        finally:
            conn.rollback()
            with conn, conn.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE;").format(sql.Identifier(schema)))
            conn.close()


def _parent(conn, identity, execution="delivered", outcome="unknown", evidence=None):
    with conn, conn.cursor() as cursor:
        cursor.execute(
            """INSERT INTO activity_actions(id,league_id,team_id,kind,title,execution,outcome,evidence)
            VALUES (%s,'',%s,'lineup_updates','IR move',%s,%s,%s)""",
            (identity, "8", execution, outcome, json.dumps(evidence or {})),
        )


def _child(parent_id):
    return activity.create_action(
        "waiver_actions", {"team_id": "8", "player_id": "4046"}, str(uuid4()), depends_on=parent_id,
    )


def _state(conn, identity):
    with conn, conn.cursor() as cursor:
        cursor.execute(
            """SELECT a.execution,a.outcome,o.published_at IS NOT NULL FROM activity_actions a
            JOIN activity_outbox o ON o.action_id=a.id WHERE a.id=%s""",
            (identity,),
        )
        return cursor.fetchone()


def test_a_held_child_is_released_only_after_its_parent_is_verified(postgres):
    _parent(postgres, "ir-1")
    child = _child("ir-1")

    assert runner.publish_one(child) is False
    assert runner.publish_one() is False
    assert _state(postgres, child) == ("queued", "unknown", False)

    with postgres, postgres.cursor() as cursor:
        cursor.execute("UPDATE activity_actions SET outcome='verified_complete' WHERE id='ir-1'")
    assert runner.publish_one(child) is True
    assert _state(postgres, child) == ("delivered", "unknown", True)


@pytest.mark.parametrize("parent", [
    {"execution": "failed", "evidence": {"permanent": True}},
    {"execution": "skipped"},
    {"outcome": "rejected"},
])
def test_a_child_of_a_failed_parent_is_cancelled_and_never_published(postgres, parent):
    _parent(postgres, "ir-1", **parent)
    child = _child("ir-1")

    assert runner.settle_held_actions() == [child]
    assert _state(postgres, child) == ("skipped", "cancelled", True)
    with postgres, postgres.cursor() as cursor:
        cursor.execute("UPDATE activity_actions SET outcome='verified_complete' WHERE id='ir-1'")
    assert runner.publish_one(child) is False


def test_a_child_past_its_hold_is_cancelled_but_a_pending_one_is_left(postgres):
    _parent(postgres, "ir-late")
    _parent(postgres, "ir-pending", execution="failed", evidence={"permanent": False})
    late, pending = _child("ir-late"), _child("ir-pending")
    with postgres, postgres.cursor() as cursor:
        cursor.execute("UPDATE activity_outbox SET hold_until=now()-interval '1 minute' WHERE action_id=%s", (late,))

    assert runner.settle_held_actions() == [late]
    assert _state(postgres, pending) == ("queued", "unknown", False)


def test_a_verified_parent_cannot_release_a_child_past_its_hold(postgres):
    _parent(postgres, "ir-1", execution="submitted", outcome="verified_complete")
    child = _child("ir-1")
    with postgres, postgres.cursor() as cursor:
        cursor.execute("UPDATE activity_outbox SET hold_until=now()-interval '1 minute' WHERE action_id=%s", (child,))

    assert runner.publish_one(child) is False
    assert runner.settle_held_actions() == [child]
    assert _state(postgres, child) == ("skipped", "cancelled", True)


def test_a_cancelled_claims_key_is_free_for_a_claim_held_on_a_fresh_ir_move(postgres):
    payload = {"team_id": "8", "player_id": "4046", "idempotency_key": "add-key",
               "decision_inputs": {"season": 2026, "week": 5}}
    _parent(postgres, "ir-1", execution="failed", evidence={"permanent": True})
    first = activity.create_action("waiver_actions", payload, "claim-1", depends_on="ir-1")
    assert activity.create_action("waiver_actions", payload, "claim-dup", depends_on="ir-1") == first

    runner.settle_held_actions()
    _parent(postgres, "ir-2")
    assert activity.create_action("waiver_actions", payload, "claim-2", depends_on="ir-2") == "claim-2"


def test_an_unheld_action_still_gets_a_plain_outbox_row(postgres):
    plain = activity.create_action("waiver_actions", {"team_id": "8", "player_id": "1"}, "plain")
    with postgres, postgres.cursor() as cursor:
        cursor.execute("SELECT depends_on, hold_until FROM activity_outbox WHERE action_id=%s", (plain,))
        assert cursor.fetchone() == (None, None)
    assert runner.publish_one(plain) is True


def _age(conn, identity, minutes=10):
    with conn, conn.cursor() as cursor:
        cursor.execute(
            "UPDATE activity_actions SET created_at=now()-(%s * interval '1 minute') WHERE id=%s",
            (minutes, identity),
        )


def _listed(identity, **kwargs):
    return next(item for item in activity.list_actions("operator", **kwargs) if item["id"] == identity)


@pytest.mark.parametrize("parent", [
    {},
    {"execution": "failed", "evidence": {"permanent": False}},
    {"outcome": "verified_complete"},
])
def test_old_unpublished_dependency_stays_out_of_attention(postgres, parent):
    _parent(postgres, "ir-1", **parent)
    child = _child("ir-1")
    _age(postgres, child)

    assert runner.settle_held_actions() == []
    assert child not in {item["id"] for item in activity.list_actions("operator", attention=True)}
    item = _listed(child)
    assert item["execution"] == "queued"
    assert item["outcome"] == "unknown"
    assert [event["stage"] for event in item["events"]] == ["held_for_dependency"]
    assert item["events"][0]["detail"] == {"depends_on": "ir-1"}


def test_released_unknown_claim_needs_attention_and_keeps_dependency_events(postgres):
    _parent(postgres, "ir-1", outcome="verified_complete")
    child = _child("ir-1")
    _age(postgres, child)
    assert runner.publish_one(child) is True
    activity.record_event(child, "status_checked")

    item = _listed(child, attention=True)
    assert item["outcome"] == "unknown"
    assert [event["stage"] for event in item["events"]] == ["held_for_dependency", "dependency_verified"]
    assert item["events"][-1]["detail"]["depends_on"] == "ir-1"
    assert all(event["action_id"] == child and event["created_at"] for event in item["events"])
    assert "status_checked" in {event["stage"] for event in activity.action_detail(child)["events"]}

    activity.record_event(child, "worker_failed", execution="failed")
    assert _listed(child, attention=True)["execution"] == "failed"


def test_cancelled_dependency_exposes_reason_and_preserves_attention_rules(postgres):
    _parent(postgres, "ir-1", execution="failed", evidence={"permanent": True})
    child = _child("ir-1")
    _age(postgres, child)
    assert runner.settle_held_actions() == [child]

    item = _listed(child)
    assert item["outcome"] == "cancelled"
    assert [event["stage"] for event in item["events"]] == ["held_for_dependency", "dependency_failed"]
    assert item["events"][-1]["detail"]["depends_on"] == "ir-1"
    assert item["events"][-1]["detail"]["reason"] == "parent_failed_permanently"
    assert child not in {item["id"] for item in activity.list_actions("operator", attention=True)}

    # A closed hold must no longer suppress an otherwise eligible failure.
    activity.record_event(child, "worker_failed", execution="failed")
    assert _listed(child, attention=True)["execution"] == "failed"


def test_attention_filters_holds_before_limit_and_offset(postgres):
    _parent(postgres, "ir-1", outcome="verified_complete")
    for minutes in (6, 8, 10):
        _age(postgres, _child("ir-1"), minutes)
    plain = activity.create_action("waiver_actions", {"team_id": "8"}, "plain")
    _age(postgres, plain, 7)
    # Legacy actions have no outbox row and must retain their attention eligibility.
    _parent(postgres, "legacy")
    _age(postgres, "legacy", 9)

    assert [item["id"] for item in activity.list_actions("operator", limit=1, attention=True)] == [plain]
    page = activity.list_actions("operator", limit=1, offset=1, attention=True)
    assert [item["id"] for item in page] == ["legacy"]
    assert page[0]["events"] == []
    assert activity.list_actions("operator", limit=1, offset=2, attention=True) == []
    assert _listed(plain)["events"] == []


def test_dependency_list_events_remain_league_scoped_and_preserve_unread(postgres):
    _parent(postgres, "ir-1")
    child = _child("ir-1")
    _parent(postgres, "foreign")
    with postgres, postgres.cursor() as cursor:
        cursor.execute("UPDATE activity_actions SET league_id='other' WHERE id='foreign'")
        cursor.execute(
            "INSERT INTO activity_reads(user_id,item_id,read_at) VALUES ('operator',%s,now()+interval '1 minute')",
            (child,),
        )
    assert "foreign" not in {item["id"] for item in activity.list_actions("operator")}
    assert _listed(child)["unread"] is False
    other_item = next(item for item in activity.list_actions("other-user") if item["id"] == child)
    assert other_item["unread"] is True
