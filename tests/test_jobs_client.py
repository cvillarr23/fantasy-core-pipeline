from unittest.mock import Mock, patch

import pytest
import requests

from src import jobs_client
from src.jobs_client import JobsApiError


def _response(status=200, body=None, text=""):
    response = Mock(spec=requests.Response)
    response.status_code = status
    response.ok = status < 400
    response.json.return_value = body
    response.text = text
    return response


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("CORE_API_BASE_URL", "http://core:8000/")
    monkeypatch.setenv("TRIGGER_API_KEY", "secret")


def _call(fn, response, *args, **kwargs):
    with patch("src.jobs_client.requests.request", return_value=response) as request:
        result = fn(*args, **kwargs)
    return result, request


def test_list_jobs_is_a_keyless_get():
    result, request = _call(jobs_client.list_jobs, _response(body={"jobs": []}))
    assert result == {"jobs": []}
    method, url = request.call_args.args
    assert (method, url) == ("GET", "http://core:8000/v1/jobs")
    assert "X-API-Key" not in request.call_args.kwargs["headers"]


def test_run_job_posts_params_downstream_and_trigger_with_the_key():
    _, request = _call(
        jobs_client.run_job, _response(202, {"run_id": "r1"}),
        "sample-job", {"count": 2}, downstream=True, trigger="cli",
    )
    assert request.call_args.args == ("POST", "http://core:8000/v1/jobs/sample-job/runs")
    assert request.call_args.kwargs["json"] == {"params": {"count": 2}, "downstream": True, "trigger": "cli"}
    assert request.call_args.kwargs["headers"]["X-API-Key"] == "secret"


def test_get_run_and_cancel_run_use_the_run_identity():
    _, get_request = _call(jobs_client.get_run, _response(body={}), "abc")
    assert get_request.call_args.args == ("GET", "http://core:8000/v1/runs/abc")
    _, cancel_request = _call(jobs_client.cancel_run, _response(body={}), "abc")
    assert cancel_request.call_args.args == ("POST", "http://core:8000/v1/runs/abc/cancel")


def test_pause_and_resume():
    _, pause = _call(
        jobs_client.pause_job, _response(body={}), "sample-job", reason="maintenance",
        until="2026-12-01T00:00:00+00:00",
    )
    assert pause.call_args.args == ("PUT", "http://core:8000/v1/jobs/sample-job/pause")
    assert pause.call_args.kwargs["json"] == {"reason": "maintenance", "until": "2026-12-01T00:00:00+00:00"}
    _, indefinite = _call(jobs_client.pause_job, _response(body={}), "sample-job", reason="x")
    assert indefinite.call_args.kwargs["json"] == {"reason": "x", "until": None}
    _, resume = _call(jobs_client.unpause_job, _response(body={}), "sample-job")
    assert resume.call_args.args == ("DELETE", "http://core:8000/v1/jobs/sample-job/pause")


def test_identity_is_url_quoted():
    _, request = _call(jobs_client.get_run, _response(body={}), "a/b c")
    assert request.call_args.args[1] == "http://core:8000/v1/runs/a%2Fb%20c"


def test_error_carries_status_and_the_apis_detail():
    with patch("src.jobs_client.requests.request", return_value=_response(409, {"detail": "already running"})):
        with pytest.raises(JobsApiError) as caught:
            jobs_client.run_job("sample-job", {}, trigger="mcp")
    assert caught.value.status == 409
    assert caught.value.detail == "already running"


def test_error_falls_back_to_body_text_when_not_json():
    response = _response(502, text="bad gateway")
    response.json.side_effect = ValueError
    with patch("src.jobs_client.requests.request", return_value=response):
        with pytest.raises(JobsApiError) as caught:
            jobs_client.list_jobs()
    assert (caught.value.status, caught.value.detail) == (502, "bad gateway")


def test_mutating_call_without_a_key_fails_before_any_request(monkeypatch):
    monkeypatch.delenv("TRIGGER_API_KEY")
    with patch("src.jobs_client.requests.request") as request:
        with pytest.raises(JobsApiError, match="TRIGGER_API_KEY"):
            jobs_client.cancel_run("abc")
    request.assert_not_called()


def test_unreachable_api_is_a_jobs_api_error():
    with patch("src.jobs_client.requests.request", side_effect=requests.ConnectionError("refused")):
        with pytest.raises(JobsApiError, match="refused"):
            jobs_client.list_jobs()
