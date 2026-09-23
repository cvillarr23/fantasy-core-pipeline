from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.llm_client import LlmProviderChainExhaustedError, complete_with_fallback, parse_priority_chain


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


def _fake_response(status_code=200, json_body=None, text=""):
    response = MagicMock(status_code=status_code, text=text)
    response.json.return_value = json_body or {}
    return response


@pytest.fixture(autouse=True)
def _no_real_db(monkeypatch):
    monkeypatch.setattr("src.llm_client.get_db_cursor", lambda: _fake_db_cursor())


@pytest.fixture(autouse=True)
def _clear_module_state():
    from src import llm_client

    llm_client._request_log.clear()
    llm_client._api_key_cache.clear()
    yield
    llm_client._request_log.clear()
    llm_client._api_key_cache.clear()


def test_parse_priority_chain_splits_on_first_colon_only():
    chain = parse_priority_chain("groq:llama-3.3-70b-versatile,ollama:llama3.1:8b")
    assert chain == [("groq", "llama-3.3-70b-versatile"), ("ollama", "llama3.1:8b")]


def test_complete_with_fallback_succeeds_on_first_provider(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    response = _fake_response(json_body={"choices": [{"message": {"content": "hello"}}]})

    with patch("httpx.Client.post", return_value=response):
        result = complete_with_fallback("prompt", providers=[("groq", "llama-3.3-70b-versatile")])

    assert result.text == "hello"
    assert result.provider == "groq"
    assert result.attempt == 1


def test_complete_with_fallback_falls_back_to_second_provider_on_failure(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")

    groq_response = _fake_response(status_code=429, text="rate limit exceeded")
    gemini_response = _fake_response(json_body={"candidates": [{"content": {"parts": [{"text": "hi"}]}}]})

    with patch("httpx.Client.post", side_effect=[groq_response, gemini_response]):
        result = complete_with_fallback(
            "prompt", providers=[("groq", "llama-3.3-70b-versatile"), ("gemini", "gemini-1.5-flash")],
        )

    assert result.provider == "gemini"
    assert result.attempt == 2


def test_complete_with_fallback_raises_when_every_provider_fails(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    response = _fake_response(status_code=500, text="server error")

    with patch("httpx.Client.post", return_value=response):
        with pytest.raises(LlmProviderChainExhaustedError):
            complete_with_fallback("prompt", providers=[("groq", "llama-3.3-70b-versatile")])


def test_complete_with_fallback_skips_provider_with_no_api_key(monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")

    with patch("subprocess.run", side_effect=OSError("bao not installed")):
        gemini_response = _fake_response(json_body={"candidates": [{"content": {"parts": [{"text": "hi"}]}}]})
        with patch("httpx.Client.post", return_value=gemini_response) as mock_post:
            result = complete_with_fallback(
                "prompt", providers=[("groq", "llama-3.3-70b-versatile"), ("gemini", "gemini-1.5-flash")],
            )

    assert result.provider == "gemini"
    assert mock_post.call_count == 1  # groq was skipped entirely, never hit the network


def test_complete_with_fallback_skips_provider_over_local_rate_limit_budget(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    monkeypatch.setenv("GROQ_RPM_LIMIT", "1")
    monkeypatch.setenv("GEMINI_API_KEY", "test-gemini-key")

    groq_response = _fake_response(json_body={"choices": [{"message": {"content": "first"}}]})
    gemini_response = _fake_response(json_body={"candidates": [{"content": {"parts": [{"text": "second"}]}}]})

    with patch("httpx.Client.post", side_effect=[groq_response, gemini_response]):
        complete_with_fallback("prompt", providers=[("groq", "llama-3.3-70b-versatile")])
        result = complete_with_fallback(
            "prompt", providers=[("groq", "llama-3.3-70b-versatile"), ("gemini", "gemini-1.5-flash")],
        )

    assert result.provider == "gemini"


def test_complete_with_fallback_resolves_api_key_from_openbao(monkeypatch):
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    bao_output = MagicMock(stdout='{"data": {"data": {"groq_api_key": "bao-resolved-key"}}}')

    response = _fake_response(json_body={"choices": [{"message": {"content": "hello"}}]})
    with patch("subprocess.run", return_value=bao_output), patch("httpx.Client.post", return_value=response):
        result = complete_with_fallback("prompt", providers=[("groq", "llama-3.3-70b-versatile")])

    assert result.provider == "groq"


def test_metrics_write_failure_does_not_fail_the_llm_call(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    response = _fake_response(json_body={"choices": [{"message": {"content": "hello"}}]})

    def _broken_cursor():
        raise RuntimeError("db unavailable")

    with patch("src.llm_client.get_db_cursor", _broken_cursor), patch("httpx.Client.post", return_value=response):
        result = complete_with_fallback("prompt", providers=[("groq", "llama-3.3-70b-versatile")])

    assert result.text == "hello"


def test_timeout_is_classified_as_timeout_error_type(monkeypatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    recorded_metrics = []

    def _record(**kwargs):
        recorded_metrics.append(kwargs)

    with patch("httpx.Client.post", side_effect=httpx.TimeoutException("timed out")), \
         patch("src.llm_client._record_llm_call_metric", side_effect=_record):
        with pytest.raises(LlmProviderChainExhaustedError):
            complete_with_fallback("prompt", providers=[("groq", "llama-3.3-70b-versatile")])

    assert recorded_metrics[0]["error_type"] == "timeout"
