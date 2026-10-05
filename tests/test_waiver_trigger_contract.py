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
