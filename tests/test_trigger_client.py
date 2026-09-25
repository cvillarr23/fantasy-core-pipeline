from unittest.mock import MagicMock, patch

import pytest

from src import trigger_client as tc


def test_core_api_base_url_defaults_to_core_pipeline_service():
    assert tc.CORE_API_BASE_URL == "http://fantasy-core-pipeline.core.svc.cluster.local:8000"


def test_post_trigger_posts_expected_url_and_injects_trace_context():
    with patch("src.trigger_client.requests.post") as mock_post, \
         patch("src.trigger_client.propagate.inject") as mock_inject:
        mock_post.return_value = MagicMock(status_code=202)
        tc.post_trigger("lineup", {"team_id": "7"}, timeout=10)

    args, kwargs = mock_post.call_args
    assert args[0] == f"{tc.CORE_API_BASE_URL}/v1/trigger/lineup"
    assert kwargs["json"] == {"team_id": "7"}
    assert kwargs["timeout"] == 10
    mock_inject.assert_called_once_with(kwargs["headers"])


def test_post_trigger_respects_core_api_base_url_override():
    with patch("src.trigger_client.requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=202)
        tc.post_trigger("waiver", {}, core_api_base_url="http://override:9000")

    assert mock_post.call_args[0][0] == "http://override:9000/v1/trigger/waiver"


def test_post_trigger_logs_error_response_body_then_raises(caplog):
    response = MagicMock(status_code=422, text='{"detail": "bad payload"}')
    response.raise_for_status.side_effect = tc.requests.HTTPError("422 error")

    with patch("src.trigger_client.requests.post", return_value=response):
        with caplog.at_level("ERROR", logger="TriggerClient"):
            with pytest.raises(tc.requests.HTTPError):
                tc.post_trigger("draft-pick", {"team_id": "7"})

    assert "bad payload" in caplog.text


def test_post_trigger_does_not_log_on_success():
    response = MagicMock(status_code=202)

    with patch("src.trigger_client.requests.post", return_value=response):
        result = tc.post_trigger("lineup", {})

    assert result is response
    response.raise_for_status.assert_called_once()
