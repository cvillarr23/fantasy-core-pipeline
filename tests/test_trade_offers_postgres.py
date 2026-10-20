"""Approval transactions against an explicitly supplied disposable local database."""

from contextlib import contextmanager
import json
import os
from unittest.mock import patch
from uuid import uuid4

import psycopg2
from psycopg2 import sql
import pytest

from src import migrations
from src.services import trade_offer_service as service
from tests.test_trade_offers import staged


@pytest.fixture
def postgres(monkeypatch):
    dsn = os.getenv("ACTIVITY_TEST_DSN")
    if not dsn:
        pytest.skip("Set ACTIVITY_TEST_DSN to a disposable local Postgres")
    assert psycopg2.extensions.parse_dsn(dsn).get("host") in {"127.0.0.1", "localhost"}
    monkeypatch.setenv("ESPN_LEAGUE_ID", "123456789")
    monkeypatch.setenv("ESPN_TEAM_ID", "8")
    conn = psycopg2.connect(dsn)
    schema = "trade_test_" + uuid4().hex
    with conn, conn.cursor() as cursor:
        cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        cursor.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))

    @contextmanager
    def cursor_context():
        # A separate connection lets the publisher observe only committed claims.
        transaction = psycopg2.connect(dsn, options=f"-c search_path={schema}")
        try:
            with transaction, transaction.cursor() as cursor:
                yield cursor
        finally:
            transaction.close()

    with patch.object(migrations, "get_db_cursor", cursor_context):
        migrations.run_migrations()
    with (
        patch.object(service, "get_db_cursor", cursor_context),
        patch("src.api.routes.pika.BlockingConnection") as connection,
    ):
        original = staged("  Fair deal?  ")
        with cursor_context() as cursor:
            for _ in range(2):
                cursor.execute(
                    "INSERT INTO execution_log (transaction_type, status, league_id, season, counter_offer) "
                    "VALUES ('trade_offer_submit', 'awaiting_approval', %s, %s, %s::jsonb)",
                    (
                        original["league_id"],
                        original["season"],
                        json.dumps(original["counter_offer"]),
                    ),
                )
        try:
            yield cursor_context, original["counter_offer"][
                "offer_key"
            ], connection.return_value.channel.return_value
        finally:
            conn.rollback()
            with conn, conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
                )
            conn.close()


def test_edited_claim_and_audit_committed_before_publish(postgres):
    db, key, channel = postgres

    def publish(**kwargs):
        sent = json.loads(kwargs["body"])
        with db() as cursor:
            cursor.execute(
                "SELECT status, counter_offer FROM execution_log ORDER BY id"
            )
            originals, older, edited = cursor.fetchall()
            assert originals[0] == older[0] == "superseded"
            assert originals[1]["superseded_by"] == sent["approval"]["offer_key"]
            assert edited[0] == "approval_pending"
            assert edited[1]["message"] == sent["message"] == "Edited pitch"
            assert edited[1]["drafted_message"] == "  Fair deal?  "
            cursor.execute("SELECT inputs FROM bot_decisions")
            assert cursor.fetchone()[0]["payload"]["message"] == "Edited pitch"

    channel.basic_publish.side_effect = publish
    first = service.approve_offer(key, "conor", "Edited pitch")
    assert first["offer_key"] != key
    second = service.approve_offer(key, "other", "Edited pitch")
    assert second == {**first, "duplicate": True}
    with pytest.raises(service.OfferConflict):
        service.approve_offer(key, "other", "Conflicting pitch")
    with db() as cursor:
        cursor.execute("SELECT status FROM execution_log ORDER BY id")
        assert cursor.fetchall() == [("superseded",), ("superseded",), ("approved",)]
    channel.basic_publish.assert_called_once()


def test_failed_audit_rolls_back_edit_and_supersession(postgres):
    db, key, channel = postgres
    with patch(
        "src.api.routes._record_bot_decision", side_effect=RuntimeError("audit failed")
    ):
        with pytest.raises(RuntimeError):
            service.approve_offer(key, "conor", "Edited pitch")
    with db() as cursor:
        cursor.execute("SELECT status, counter_offer->>'offer_key' FROM execution_log")
        assert cursor.fetchall() == [
            ("awaiting_approval", key),
            ("awaiting_approval", key),
        ]
        cursor.execute("SELECT count(*) FROM bot_decisions")
        assert cursor.fetchone() == (0,)
    channel.basic_publish.assert_not_called()
