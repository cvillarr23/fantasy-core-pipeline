"""Dismissing an unapproved outgoing proposal."""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from src.services import trade_offer_service as service

KEY = "b" * 64


def row(row_id=1, status="awaiting_approval", dismissal=None):
    return dict(
        id=row_id,
        status=status,
        league_id="123456789",
        season=2026,
        executed_at="2026-10-21T01:00:00",
        outcome_details={"dismissal": dismissal} if dismissal else {},
        dry_run=True,
        counter_offer=dict(offer_key=KEY, source="proposal"),
    )


@pytest.fixture
def db():
    cursor = MagicMock()

    @contextmanager
    def transaction():
        yield cursor

    with patch.object(service, "get_db_cursor", transaction):
        yield cursor


def dismissed_ids(db):
    return [
        c.args[1][1]
        for c in db.execute.call_args_list
        if "SET status = 'dismissed'" in c.args[0]
    ]


def test_dismiss_retires_every_awaiting_copy(db):
    rows = [row(2), row(1), row(0, "superseded")]
    with patch.object(service, "_read_offers", return_value=rows):
        result = service.dismiss_offer(KEY, "conor")
    assert dismissed_ids(db) == [2, 1]
    assert result["duplicate"] is False and result["reason"] == "manual"
    assert result["dismissed_by"] == "conor"
    assert "pg_advisory_xact_lock" in db.execute.call_args_list[0].args[0]


def test_dismiss_again_is_a_duplicate(db):
    stored = dict(reason="manual", dismissed_by="conor", dismissed_at="2026-10-22T00:00:00+00:00")
    with patch.object(service, "_read_offers", return_value=[row(status="dismissed", dismissal=stored)]):
        result = service.dismiss_offer(KEY, "someone-else")
    assert result["duplicate"] is True and result["dismissed_by"] == "conor"
    assert dismissed_ids(db) == []


@pytest.mark.parametrize("status", ["approval_pending", "approved", "submitting", "submitted"])
def test_dismiss_refuses_an_approved_offer(db, status):
    with patch.object(service, "_read_offers", return_value=[row(status=status)]):
        with pytest.raises(service.OfferConflict):
            service.dismiss_offer(KEY, "conor")
    assert dismissed_ids(db) == []


def test_dismiss_unknown_key(db):
    with patch.object(service, "_read_offers", return_value=[]):
        with pytest.raises(service.OfferNotFound):
            service.dismiss_offer(KEY, "conor")


def test_approving_a_dismissed_offer_conflicts(db):
    stored = dict(reason="manual", dismissed_by="conor", dismissed_at="2026-10-22T00:00:00+00:00")
    with patch.object(service, "_read_offers", return_value=[row(status="dismissed", dismissal=stored)]):
        with pytest.raises(service.OfferConflict):
            service.approve_offer(KEY, "conor")


@pytest.mark.parametrize(
    "error, code",
    [(service.OfferNotFound("x"), 404), (service.OfferConflict("x"), 409)],
)
def test_dismiss_route_maps_errors(client, error, code):
    with patch.object(service, "dismiss_offer", side_effect=error):
        response = client.post(f"/v1/trade/offers/{KEY}/dismiss", json={"dismissed_by": "conor"})
    assert response.status_code == code


def test_dismiss_route_rejects_a_blank_dismisser(client):
    response = client.post(f"/v1/trade/offers/{KEY}/dismiss", json={"dismissed_by": "  "})
    assert response.status_code == 422


def test_dismiss_route_returns_the_audit(client):
    result = dict(
        offer_key=KEY, reason="manual", dismissed_by="conor",
        dismissed_at="2026-10-22T00:00:00+00:00", duplicate=False,
    )
    with patch.object(service, "dismiss_offer", return_value=result) as dismiss:
        response = client.post(f"/v1/trade/offers/{KEY}/dismiss", json={"dismissed_by": "conor"})
    assert response.status_code == 200 and response.json()["dismissed_by"] == "conor"
    dismiss.assert_called_once_with(KEY, "conor")
