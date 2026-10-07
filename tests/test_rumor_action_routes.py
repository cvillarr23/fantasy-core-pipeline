from unittest.mock import patch

from src.nfl_schedule import RegularSeasonOverError, WeekResolutionError

RESOLVER = "src.api.rumor_action.require_upcoming_week"


def test_trigger_rumor_action_uses_env_team_id_when_no_body(client, monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "7")
    expected = {
        "considered": 0,
        "waiver_claims_published": [],
        "lineup_swaps_published": [],
        "watermark": 3,
    }

    with patch(RESOLVER, return_value=5) as mock_resolve, \
         patch("src.api.rumor_action.rumor_action_service.evaluate_and_publish_rumor_actions", return_value=expected) as mock_eval:
        response = client.post("/v1/trigger/rumor-action")

    assert response.status_code == 200
    assert response.json() == expected
    args, _ = mock_eval.call_args
    assert args[0] == "7"
    assert args[2] == 5
    # A request never waits out the CronJob retry backoff.
    assert mock_resolve.call_args.kwargs == {"attempts": 1}


def test_trigger_rumor_action_body_overrides_env_team_id(client, monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "7")
    expected = {
        "considered": 1,
        "waiver_claims_published": [],
        "lineup_swaps_published": [],
        "watermark": 5,
    }

    with patch("src.api.rumor_action.rumor_action_service.evaluate_and_publish_rumor_actions", return_value=expected) as mock_eval:
        response = client.post("/v1/trigger/rumor-action", json={"team_id": "99", "season": 2026, "week": 4})

    assert response.status_code == 200
    mock_eval.assert_called_once_with("99", 2026, 4, source="http")


def test_trigger_rumor_action_missing_team_id_returns_422(client, monkeypatch):
    monkeypatch.delenv("ESPN_TEAM_ID", raising=False)

    response = client.post("/v1/trigger/rumor-action")

    assert response.status_code == 422
    assert "team_id" in response.json()["detail"]


def test_trigger_rumor_action_service_failure_returns_500(client, monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "7")

    with patch(RESOLVER, return_value=5), patch(
        "src.api.rumor_action.rumor_action_service.evaluate_and_publish_rumor_actions",
        side_effect=Exception("db unavailable"),
    ):
        response = client.post("/v1/trigger/rumor-action")

    assert response.status_code == 500


def test_trigger_rumor_action_returns_503_when_week_unresolvable(client, monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "7")
    with patch(RESOLVER, side_effect=WeekResolutionError("download failed")), \
         patch("src.api.rumor_action.rumor_action_service.evaluate_and_publish_rumor_actions") as mock_eval:
        response = client.post("/v1/trigger/rumor-action")

    assert response.status_code == 503
    assert "week" in response.json()["detail"]
    mock_eval.assert_not_called()


def test_trigger_rumor_action_returns_409_after_the_regular_season(client, monkeypatch):
    monkeypatch.setenv("ESPN_TEAM_ID", "7")
    with patch(RESOLVER, side_effect=RegularSeasonOverError("2026 regular season is over")), \
         patch("src.api.rumor_action.rumor_action_service.evaluate_and_publish_rumor_actions") as mock_eval:
        response = client.post("/v1/trigger/rumor-action")

    assert response.status_code == 409
    mock_eval.assert_not_called()
