"""
LLM-calling infrastructure (CORE-153): a durable, provider-agnostic completion client with a
priority-chain fallback, shared by any Python call site that needs an LLM answer -- not
specific to the chat endpoint that first needed it.

Ports fantasy-browser-worker's src/lib/llm_client.js (completeWithFallback) to Python. Same
provider chain, same env-var conventions, same env-then-OpenBao key resolution, same
best-effort per-attempt metrics into the shared `llm_call_metrics` table (CORE-14) -- but
writes through this app's own connection pool (src.database.get_db_cursor) instead of a
second dedicated pool, since there's no cross-process boundary to justify one here.
"""
from src.services.llm_history import record_call, observe

import logging
import os
import subprocess
import time
from dataclasses import dataclass
from json import JSONDecodeError, loads as json_loads
from typing import Callable, Optional

import httpx
from opentelemetry import trace

from src.database import get_db_cursor

logger = logging.getLogger("LlmClient")

DEFAULT_PROVIDER_PRIORITY = "groq:openai/gpt-oss-120b,gemini:gemini-1.5-flash,ollama:llama3.1:8b"

# Conservative free-tier request-per-minute budgets, mirroring llm_client.js. No entry for
# ollama -- it's a local last-resort tier with no external quota to protect.
DEFAULT_RPM_LIMITS = {"groq": 30, "gemini": 15}

DEFAULT_TIMEOUT_SECONDS = 15
# Ollama's requests route through the KEDA HTTP Add-on interceptor, which scales the
# Deployment 0->1 and holds the request until ready -- a cold load can take well past 30s.
# src/services/chat_service.py opts out of this tier entirely rather than block on it.
DEFAULT_OLLAMA_TIMEOUT_SECONDS = 100
DEFAULT_OLLAMA_URL = "http://keda-add-ons-http-interceptor-proxy.keda.svc.cluster.local:8080"
DEFAULT_OLLAMA_HOST_HEADER = "ollama.default.svc.cluster.local"

# provider -> list of request timestamps (seconds), for the in-memory sliding-window rate
# limiter below. Process-local by design, same as llm_client.js's requestLog.
_request_log: dict[str, list[float]] = {}

# Cache resolved (or confirmed-missing) OpenBao lookups for the life of the process.
_api_key_cache: dict[str, Optional[str]] = {}


class LlmProviderChainExhaustedError(Exception):
    """Every provider in the priority chain was skipped or failed. Callers should map this
    to a "try again later" response, not retry internally -- a fresh call would walk the
    exact same exhausted chain."""


@dataclass
class LlmResult:
    text: str
    provider: str
    model: str
    attempt: int
    latency_ms: int


def _is_within_local_budget(provider: str) -> bool:
    configured = os.getenv(f"{provider.upper()}_RPM_LIMIT")
    limit = int(configured) if configured and configured.isdigit() else DEFAULT_RPM_LIMITS.get(provider)
    if not limit:
        return True

    window_start = time.time() - 60
    timestamps = [t for t in _request_log.get(provider, []) if t > window_start]
    _request_log[provider] = timestamps
    return len(timestamps) < limit


def _record_local_request(provider: str) -> None:
    _request_log.setdefault(provider, []).append(time.time())


def _fetch_api_key_from_openbao(provider: str) -> Optional[str]:
    secret_path = os.getenv(f"OPENBAO_{provider.upper()}_SECRET_PATH", f"secret/fantasy/{provider}")
    field_name = f"{provider}_api_key"

    logger.info(f'[LLM Client] "{provider}" API key not set in environment, checking OpenBao at "{secret_path}"...')
    try:
        raw = subprocess.run(
            ["bao", "kv", "get", "-format=json", secret_path],
            capture_output=True, text=True, check=True, timeout=10,
        ).stdout
    except (subprocess.SubprocessError, OSError) as e:
        logger.error(f'[LLM Client] OpenBao lookup failed for "{provider}": {e}')
        return None

    try:
        parsed = json_loads(raw)
    except JSONDecodeError as e:
        logger.error(f'[LLM Client] Could not parse OpenBao response for "{provider}": {e}')
        return None

    key = parsed.get("data", {}).get("data", {}).get(field_name)
    if not key:
        logger.error(f'[LLM Client] OpenBao secret at "{secret_path}" is missing field "{field_name}".')
        return None

    logger.info(f'[LLM Client] Loaded "{provider}" API key from OpenBao.')
    return key


def _resolve_api_key(provider: str) -> Optional[str]:
    env_key = os.getenv(f"{provider.upper()}_API_KEY")
    if env_key:
        return env_key

    if provider in _api_key_cache:
        return _api_key_cache[provider]
    key = _fetch_api_key_from_openbao(provider)
    _api_key_cache[provider] = key
    return key


def _request_timeout_seconds(provider: str) -> float:
    if provider == "ollama":
        return float(os.getenv("OLLAMA_REQUEST_TIMEOUT_SECONDS", DEFAULT_OLLAMA_TIMEOUT_SECONDS))
    return float(os.getenv("LLM_REQUEST_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS))


def _classify_http_error(status_code: int, body: str) -> "_HttpError":
    rate_limited = status_code == 429 or "quota" in body.lower() or "rate limit" in body.lower()
    return _HttpError(f"HTTP {status_code}: {body[:300]}", rate_limited=rate_limited)


class _HttpError(Exception):
    def __init__(self, message: str, rate_limited: bool):
        super().__init__(message)
        self.rate_limited = rate_limited


def _call_groq(model: str, prompt: str, system_prompt: Optional[str], max_tokens: Optional[int],
               temperature: Optional[float], api_key: str) -> str:
    messages = ([{"role": "system", "content": system_prompt}] if system_prompt else []) + [
        {"role": "user", "content": prompt}
    ]
    body = {"model": model, "messages": messages}
    if max_tokens:
        body["max_tokens"] = max_tokens
    if temperature is not None:
        body["temperature"] = temperature

    with httpx.Client(timeout=_request_timeout_seconds("groq")) as client:
        response = client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json=body,
        )
    if response.status_code != 200:
        raise _classify_http_error(response.status_code, response.text)

    text = response.json().get("choices", [{}])[0].get("message", {}).get("content")
    if not text:
        raise ValueError("Groq response missing choices[0].message.content")
    return text


def _call_gemini(model: str, prompt: str, system_prompt: Optional[str], max_tokens: Optional[int],
                  temperature: Optional[float], api_key: str) -> str:
    generation_config = {}
    if max_tokens:
        generation_config["maxOutputTokens"] = max_tokens
    if temperature is not None:
        generation_config["temperature"] = temperature

    body = {"contents": [{"role": "user", "parts": [{"text": prompt}]}]}
    if system_prompt:
        body["systemInstruction"] = {"parts": [{"text": system_prompt}]}
    if generation_config:
        body["generationConfig"] = generation_config

    # API key goes in the x-goog-api-key header, not the ?key= query string, matching
    # llm_client.js -- a query string is far more likely to land in proxy/access logs.
    with httpx.Client(timeout=_request_timeout_seconds("gemini")) as client:
        response = client.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            headers={"x-goog-api-key": api_key},
            json=body,
        )
    if response.status_code != 200:
        raise _classify_http_error(response.status_code, response.text)

    candidates = response.json().get("candidates", [{}])
    text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text")
    if not text:
        raise ValueError("Gemini response missing candidates[0].content.parts[0].text")
    return text


def _call_ollama(model: str, prompt: str, system_prompt: Optional[str], max_tokens: Optional[int],
                  temperature: Optional[float], api_key: Optional[str]) -> str:
    base_url = os.getenv("OLLAMA_URL", DEFAULT_OLLAMA_URL)
    # Only default the Host header when OLLAMA_URL is also at its default (the interceptor's
    # address) -- a manually configured OLLAMA_URL pointing at a directly reachable instance
    # shouldn't get a stale interceptor-routing Host header unless explicitly asked for.
    host_header = os.getenv("OLLAMA_HOST_HEADER") or (None if os.getenv("OLLAMA_URL") else DEFAULT_OLLAMA_HOST_HEADER)

    options = {}
    if temperature is not None:
        options["temperature"] = temperature
    if max_tokens:
        options["num_predict"] = max_tokens

    headers = {"Host": host_header} if host_header else {}
    with httpx.Client(timeout=_request_timeout_seconds("ollama")) as client:
        response = client.post(
            f"{base_url}/api/generate",
            headers=headers,
            json={
                "model": model,
                "prompt": f"{system_prompt}\n\n{prompt}" if system_prompt else prompt,
                "stream": False,
                "options": options,
            },
        )
    if response.status_code != 200:
        raise _classify_http_error(response.status_code, response.text)

    text = response.json().get("response")
    if not text:
        raise ValueError("Ollama response missing 'response' field")
    return text


# needs_api_key=False for ollama -- it's an unauthenticated in-cluster endpoint, not a cloud API.
_PROVIDERS: dict[str, tuple[bool, Callable]] = {
    "groq": (True, _call_groq),
    "gemini": (True, _call_gemini),
    "ollama": (False, _call_ollama),
}


def parse_priority_chain(raw: Optional[str] = None) -> list[tuple[str, str]]:
    """Parses a comma-separated "provider:model" chain (LLM_PROVIDER_PRIORITY by default).
    Splits on the first ":" only -- Ollama model tags contain their own ":" (e.g.
    "llama3.1:8b")."""
    raw = raw if raw is not None else os.getenv("LLM_PROVIDER_PRIORITY", DEFAULT_PROVIDER_PRIORITY)
    chain = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        provider, _, model = entry.partition(":")
        provider, model = provider.strip(), model.strip()
        if provider and model:
            chain.append((provider, model))
    return chain


def _current_trace_id() -> Optional[str]:
    span = trace.get_current_span()
    context = span.get_span_context()
    if not context.is_valid or not span.is_recording():
        return None
    return format(context.trace_id, "032x")


def _record_llm_call_metric(*, provider: str, model: str, call_site: Optional[str], attempt_number: int,
                             success: bool, latency_ms: Optional[int], error_type: Optional[str],
                             fallback_triggered: bool) -> None:
    """Best-effort write, mirroring llm_client.js's recordLlmCallMetric -- a metrics hiccup
    must never fail the actual LLM call."""
    try:
        with get_db_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO llm_call_metrics
                    (provider, model, call_site, attempt_number, success, latency_ms,
                     error_type, fallback_triggered, trace_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (provider, model, call_site, attempt_number, success, latency_ms,
                 error_type, fallback_triggered, _current_trace_id()),
            )
    except Exception as e:
        logger.error(f"[LLM Client] Failed to record LLM call metric: {e}")


@record_call
def complete_with_fallback(
    prompt: str,
    *,
    system_prompt: Optional[str] = None,
    max_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    call_site: Optional[str] = None,
    providers: Optional[list[tuple[str, str]]] = None,
) -> LlmResult:
    """
    Completes `prompt` against a provider/model priority chain, falling back to the next
    provider on a rate limit, quota exhaustion, or any other request failure. `providers`
    defaults to the parsed LLM_PROVIDER_PRIORITY chain; pass an explicit list to have a call
    site opt out of a tier (e.g. chat_service excludes "ollama" -- see its module docstring).

    Raises LlmProviderChainExhaustedError only once every provider in the chain has been
    skipped or has failed.
    """
    chain = providers if providers is not None else parse_priority_chain()

    attempt_number = 0
    last_error: Optional[Exception] = None

    for provider, model in chain:
        adapter = _PROVIDERS.get(provider)
        if not adapter:
            logger.warning(f'[LLM Client] Unknown provider "{provider}" in priority chain, skipping.')
            continue
        needs_api_key, call_fn = adapter

        api_key = None
        if needs_api_key:
            api_key = _resolve_api_key(provider)
            if not api_key:
                logger.warning(f'[LLM Client] No API key resolved for "{provider}", skipping.')
                continue

        if not _is_within_local_budget(provider):
            attempt_number += 1
            logger.warning(f'[LLM Client] Local rate-limit budget exhausted for "{provider}", skipping.')
            _record_llm_call_metric(
                provider=provider, model=model, call_site=call_site, attempt_number=attempt_number,
                success=False, latency_ms=None, error_type="rate_limited_local",
                fallback_triggered=attempt_number > 1,
            )
            continue

        attempt_number += 1
        _record_local_request(provider)
        started_at = time.monotonic()

        try:
            observe("provider_attempt", {"provider":provider,"model":model,"attempt":attempt_number})
            text = call_fn(model, prompt, system_prompt, max_tokens, temperature, api_key)
            latency_ms = int((time.monotonic() - started_at) * 1000)
            logger.info(f'[LLM Client] "{provider}:{model}" served the request successfully (attempt {attempt_number}).')
            _record_llm_call_metric(
                provider=provider, model=model, call_site=call_site, attempt_number=attempt_number,
                success=True, latency_ms=latency_ms, error_type=None,
                fallback_triggered=attempt_number > 1,
            )
            return LlmResult(text=text, provider=provider, model=model, attempt=attempt_number, latency_ms=latency_ms)
        except Exception as error:
            latency_ms = int((time.monotonic() - started_at) * 1000)
            if isinstance(error, _HttpError) and error.rate_limited:
                error_type = "rate_limited"
            elif isinstance(error, httpx.TimeoutException):
                error_type = "timeout"
            else:
                error_type = "request_failed"

            logger.error(f'[LLM Client] "{provider}:{model}" attempt {attempt_number} failed: {error}')
            _record_llm_call_metric(
                provider=provider, model=model, call_site=call_site, attempt_number=attempt_number,
                success=False, latency_ms=latency_ms, error_type=error_type,
                fallback_triggered=attempt_number > 1,
            )
            observe("provider_error", {"provider":provider,"model":model,"attempt":attempt_number,"error_type":error_type})
            last_error = error

    message = f"All configured LLM providers exhausted or unavailable ({attempt_number} attempt(s))."
    if last_error:
        message += f" Last error: {last_error}"
    raise LlmProviderChainExhaustedError(message)
