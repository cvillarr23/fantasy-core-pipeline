import json
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api import routes


app = FastAPI()
app.include_router(routes.router, prefix="/v1")
client = TestClient(app)

VALID_CLAIM = {
    "team_id": "7", "player_id": "4046", "name": "Some Back", "team": "KC",
    "drop_player_id": "9999",
}


def test_authorized_claim_publishes_contract_and_returns_tracking_id():
    with patch('src.services.activity_service.create_action',return_value='claim-id') as persist, patch('src.services.activity_runner.publish_one') as publish, patch('src.api.routes._reject_internal_player_id'):
        response=client.post('/v1/trigger/waiver',json=VALID_CLAIM,headers={'X-API-Key':'test-trigger-api-key'})
    assert response.status_code==202
    assert response.json()['tracking_id']=='claim-id'
    assert {k:persist.call_args.args[1][k] for k in VALID_CLAIM}==VALID_CLAIM
    publish.assert_called_once_with('claim-id')


def test_unauthorized_claim_never_publishes():
    with patch("src.api.routes.pika.BlockingConnection") as publisher:
        missing = client.post("/v1/trigger/waiver", json=VALID_CLAIM)
        wrong = client.post("/v1/trigger/waiver", json=VALID_CLAIM,
                            headers={"X-API-Key": "wrong-key"})

    assert missing.status_code == 422
    assert wrong.status_code == 401
    publisher.assert_not_called()


def test_invalid_claim_identifies_missing_field_without_publishing():
    with patch("src.api.routes.pika.BlockingConnection") as publisher:
        response = client.post("/v1/trigger/waiver", json={"team_id": "7"},
                               headers={"X-API-Key": "test-trigger-api-key"})

    assert response.status_code == 422
    fields = {issue["loc"][-1] for issue in response.json()["detail"]}
    assert {"player_id", "name", "team"} <= fields
    publisher.assert_not_called()


def _post_claim(claim, owner=None, lookup_error=None):
    lookup = patch(
        "src.services.waiver_service.rostered_team_for_espn_id",
        return_value=owner,
        side_effect=lookup_error,
    )
    with (
        lookup,
        patch("src.services.activity_service.create_action", return_value="claim-id") as persist,
        patch("src.services.activity_runner.publish_one"),
        patch("src.api.routes._reject_internal_player_id"),
    ):
        response = client.post("/v1/trigger/waiver", json=claim, headers={"X-API-Key": "test-trigger-api-key"})
    return response, persist


def test_claim_for_player_on_another_roster_is_rejected_before_queueing():
    response, persist = _post_claim(VALID_CLAIM, owner="1")

    assert response.status_code == 409
    assert "Propose a trade" in response.json()["detail"]
    persist.assert_not_called()


def test_claim_already_on_our_roster_still_reaches_dedupe():
    response, persist = _post_claim(VALID_CLAIM, owner="7")

    assert response.status_code == 202
    persist.assert_called_once()


def test_ownership_lookup_failure_fails_open():
    response, persist = _post_claim(VALID_CLAIM, lookup_error=RuntimeError("db down"))

    assert response.status_code == 202
    persist.assert_called_once()


def test_dry_run_override_is_carried_to_the_worker_payload():
    response, persist = _post_claim({**VALID_CLAIM, "dry_run": False})
    assert response.status_code == 202
    assert persist.call_args.args[1]["dry_run"] is False

    _, persist = _post_claim(VALID_CLAIM)
    assert persist.call_args.args[1]["dry_run"] is None
