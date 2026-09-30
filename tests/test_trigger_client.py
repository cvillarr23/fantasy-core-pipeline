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


# ---------------------------------------------------------------------------
# post_trigger_idempotent (#189)
# ---------------------------------------------------------------------------

def _response(status_code, body=None):
    response = MagicMock(status_code=status_code, text=str(body))
    response.json.return_value = body or {}
    if status_code >= 400:
        response.raise_for_status.side_effect = tc.requests.HTTPError(f"{status_code} error", response=response)
    return response


def test_post_trigger_idempotent_retries_a_refused_connection_with_the_same_key():
    """The 2026-09-30 failure: the API was mid-rollout, the first POST was refused, and the
    run exited nonzero instead of trying again."""
    sleep = MagicMock()
    with patch("src.trigger_client.requests.post") as mock_post:
        mock_post.side_effect = [tc.requests.ConnectionError("Connection refused"), _response(202, {"status": "accepted"})]
        response = tc.post_trigger_idempotent("lineup", {"team_id": "8"}, "key-1", sleep=sleep)

    assert response.status_code == 202
    assert mock_post.call_count == 2
    bodies = [c.kwargs["json"] for c in mock_post.call_args_list]
    assert bodies == [{"team_id": "8", "idempotency_key": "key-1"}] * 2
    sleep.assert_called_once_with(5.0)


def test_post_trigger_idempotent_retries_timeouts_and_5xx_with_capped_backoff():
    sleep = MagicMock()
    with patch("src.trigger_client.requests.post") as mock_post:
        mock_post.side_effect = [
            tc.requests.Timeout("read timed out"),
            _response(503, {"detail": "in flight"}),
            _response(500, {"detail": "broker"}),
            _response(502),
            _response(202, {"status": "duplicate"}),
        ]
        response = tc.post_trigger_idempotent("lineup", {}, "key-2", sleep=sleep)

    assert response.json() == {"status": "duplicate"}
    assert [c.args[0] for c in sleep.call_args_list] == [5.0, 10.0, 20.0, 30.0]


def test_post_trigger_idempotent_never_retries_a_4xx():
    """A 422 (e.g. #187's namespace guard) gives the same answer every time."""
    sleep = MagicMock()
    with patch("src.trigger_client.requests.post", return_value=_response(422, {"detail": "internal Sleeper id"})) as mock_post:
        with pytest.raises(tc.requests.HTTPError):
            tc.post_trigger_idempotent("lineup", {}, "key-3", sleep=sleep)

    assert mock_post.call_count == 1
    sleep.assert_not_called()


def test_post_trigger_idempotent_raises_once_attempts_are_exhausted():
    sleep = MagicMock()
    with patch("src.trigger_client.requests.post", side_effect=tc.requests.ConnectionError("down")) as mock_post:
        with pytest.raises(tc.requests.ConnectionError):
            tc.post_trigger_idempotent("lineup", {}, "key-4", attempts=3, sleep=sleep)

    assert mock_post.call_count == 3
    assert sleep.call_count == 2
