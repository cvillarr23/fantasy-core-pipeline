"""Outgoing offer resolution marker and the committed-player set built from it."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from src.services import trade_offer_service as service

KEY = "a" * 64


def row(row_id=1, status="submitted", resolution=None):
    return dict(
        id=row_id,
        status=status,
        league_id="123456789",
        season=2026,
        executed_at="2026-10-21T01:00:00",
        outcome_details={"resolution": resolution} if resolution else {},
        dry_run=False,
        counter_offer=dict(offer_key=KEY, outgoing_player_ids=["4047365"]),
    )


@pytest.fixture
def db():
    cursor = MagicMock()

    @contextmanager
    def transaction():
        yield cursor

    with patch.object(service, "get_db_cursor", transaction):
        yield cursor


def saved(db):
    return [
        c.args for c in db.execute.call_args_list if "UPDATE execution_log" in c.args[0]
    ]


def test_resolve_marks_every_sent_row(db):
    with patch.object(service, "_read_offers", return_value=[row(1, "approved"), row(2)]):
        result = service.resolve_offer(KEY, "declined", espn_transaction_id="tx-9")
    assert result["offer_key"] == KEY and result["outcome"] == "declined"
    assert result["duplicate"] is False and result["espn_transaction_id"] == "tx-9"
    updates = saved(db)
    assert [params[1] for _, params in updates] == [1, 2]
    assert "COALESCE(outcome_details, '{}'::jsonb) ||" in updates[0][0]


def test_resolve_same_outcome_again_is_a_duplicate(db):
    stored = dict(outcome="declined", resolved_at="2026-10-22T00:00:00+00:00")
    with patch.object(service, "_read_offers", return_value=[row(resolution=stored)]):
        result = service.resolve_offer(KEY, "declined")
    assert result["duplicate"] is True
    assert result["resolved_at"] == stored["resolved_at"]
    assert saved(db) == []


def test_resolve_conflicting_outcome_is_rejected(db):
    stored = dict(outcome="accepted", resolved_at="2026-10-22T00:00:00+00:00")
    with patch.object(service, "_read_offers", return_value=[row(resolution=stored)]):
        with pytest.raises(service.OfferConflict):
            service.resolve_offer(KEY, "declined")
    assert saved(db) == []


def test_resolve_unknown_key(db):
    with patch.object(service, "_read_offers", return_value=[]):
        with pytest.raises(service.OfferNotFound):
            service.resolve_offer(KEY, "declined")


@pytest.mark.parametrize("status", ["awaiting_approval", "superseded", "dry_run_filled"])
def test_resolve_needs_an_offer_that_was_approved(db, status):
    with patch.object(service, "_read_offers", return_value=[row(status=status)]):
        with pytest.raises(service.OfferConflict):
            service.resolve_offer(KEY, "declined")


def test_committed_query_is_limited_to_live_unresolved_recent_offers(db):
    db.fetchall.return_value = [
        (dict(offer_key="k1", outgoing_player_ids=["10", "11"]),),
        (dict(offer_key="k2", outgoing_player_ids=["11"]),),
    ]
    result = service.committed_outgoing_players("123456789", 2026)
    sql, params = db.execute.call_args.args
    assert "transaction_type = 'trade_offer_submit'" in sql
    assert "status = ANY(%s)" in sql
    assert "'resolution'" in sql and "make_interval" in sql
    assert params[0] == ["approved", "submitting", "submitted"]
    assert params[1:3] == ("123456789", 2026)
    assert result == {"10": ["k1"], "11": ["k1", "k2"]}


def test_commit_window_is_configurable(db, monkeypatch):
    db.fetchall.return_value = []
    monkeypatch.setenv("OUTGOING_COMMIT_HOURS", "12")
    service.committed_outgoing_players("123456789", 2026)
    assert db.execute.call_args.args[1][-1] == 12


def test_resolution_route_requires_key_and_maps_errors(client):
    url = f"/v1/trade/offers/{KEY}/resolution"
    body = {"outcome": "declined"}
    with patch.object(service, "resolve_offer", side_effect=service.OfferNotFound("no")):
        assert client.post(url, json=body).status_code == 404
    with patch.object(service, "resolve_offer", side_effect=service.OfferConflict("no")):
        assert client.post(url, json=body).status_code == 409
    assert client.post(url, json={"outcome": "bogus"}).status_code == 422
    receipt = dict(
        offer_key=KEY, outcome="declined", resolved_at="2026-10-22T00:00:00+00:00",
        espn_transaction_id=None, duplicate=False,
    )
    with patch.object(service, "resolve_offer", return_value=receipt):
        assert client.post(url, json=body).status_code == 200
        assert client.post(url, json=body, headers={"X-API-Key": "wrong"}).status_code == 401


def test_list_shows_the_resolution(client):
    stored = dict(outcome="expired", resolved_at="2026-10-22T00:00:00+00:00")
    cursor = MagicMock()
    r = row(resolution=stored)
    r["counter_offer"].update(incoming_player_ids=[], outgoing_player_ids=[])
    cursor.fetchall.side_effect = [[tuple(r[k] for k in service.ROW_COLUMNS)]]
    with patch.object(service, "get_db_cursor") as cm:
        cm.return_value.__enter__.return_value = cursor
        response = client.get("/v1/trade/offers?status=submitted")
    assert response.status_code == 200, response.text
    assert response.json()["offers"][0]["resolution"]["outcome"] == "expired"
