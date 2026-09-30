"""Shared client for POSTing to fastapi-deployment's /v1/trigger/* endpoints.

Every caller (lineup_solver, waiver_evaluator, draft_matrix) was independently
redeclaring CORE_API_BASE_URL/TRIGGER_API_KEY, the X-API-Key header, and trace
propagation, with inconsistent error handling (only draft_matrix logged the
response body before raising). This centralizes that plumbing; payload
construction stays with each caller since it's endpoint-specific.
"""
import logging
import os
import time

import requests
from opentelemetry import propagate

logger = logging.getLogger("TriggerClient")

CORE_API_BASE_URL = os.getenv("CORE_API_BASE_URL", "http://fantasy-core-pipeline.core.svc.cluster.local:8000")
TRIGGER_API_KEY = os.getenv("TRIGGER_API_KEY")


def post_trigger(path: str, payload: dict, core_api_base_url: str = CORE_API_BASE_URL, timeout: float = 10.0) -> requests.Response:
    """POSTs payload to f"{core_api_base_url}/v1/trigger/{path}", injecting this run's
    trace context so the FastAPI-side root span becomes a child of it instead of starting
    a disconnected one. Logs the response body on a 4xx/5xx before raising, so a rejected
    trigger is diagnosable from the caller's own logs instead of only the gateway's.
    """
    headers: dict = {"X-API-Key": TRIGGER_API_KEY}
    propagate.inject(headers)

    response = requests.post(
        f"{core_api_base_url}/v1/trigger/{path}",
        json=payload,
        headers=headers,
        timeout=timeout,
    )
    if response.status_code >= 400:
        logger.error(
            "Trigger endpoint /v1/trigger/%s rejected the request with HTTP %s: %s",
            path, response.status_code, response.text,
        )
    response.raise_for_status()
    return response


# Failures a retry can fix: the gateway was unreachable or restarting (connection refused,
# DNS, reset), too slow to answer, or failed on its own side. A 4xx - the #187 namespace
# guard's 422, a bad API key - returns the same answer to the same request, so it is final.
RETRYABLE_EXCEPTIONS = (requests.ConnectionError, requests.Timeout)


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, requests.HTTPError):
        return exc.response is not None and exc.response.status_code >= 500
    return isinstance(exc, RETRYABLE_EXCEPTIONS)


def post_trigger_idempotent(
    path: str, payload: dict, idempotency_key: str, attempts: int = 6,
    backoff_seconds: float = 5.0, max_backoff_seconds: float = 30.0,
    core_api_base_url: str = CORE_API_BASE_URL, timeout: float = 10.0, sleep=None,
) -> requests.Response:
    """post_trigger with retries, for callers whose endpoint honours an idempotency_key (#189).

    Retrying is only safe because of the key: a timeout or a reset can land after the
    gateway already enqueued the message, and the retry must then come back "duplicate"
    rather than enqueue it again. The gateway's 503 ("earlier attempt still in flight") is a
    5xx, so it is retried too. The default budget (6 attempts, 5s doubling to a 30s cap,
    ~2 minutes of waiting) outlasts a fantasy-core-pipeline rollout, which is what made the
    2026-09-30 lineup run fail."""
    sleep = sleep or time.sleep
    body = {**payload, "idempotency_key": idempotency_key}
    for attempt in range(1, attempts + 1):
        try:
            response = post_trigger(path, body, core_api_base_url=core_api_base_url, timeout=timeout)
        except Exception as exc:
            if attempt == attempts or not _is_retryable(exc):
                raise
            delay = min(backoff_seconds * 2 ** (attempt - 1), max_backoff_seconds)
            logger.warning(
                "Trigger /v1/trigger/%s attempt %s/%s failed (%s); retrying in %.0fs with idempotency key %s.",
                path, attempt, attempts, exc, delay, idempotency_key,
            )
            sleep(delay)
            continue
        if attempt > 1 or _response_status(response) == "duplicate":
            logger.info(
                "Trigger /v1/trigger/%s succeeded on attempt %s/%s (status=%s).",
                path, attempt, attempts, _response_status(response),
            )
        return response


def response_status(response: requests.Response):
    """The trigger response's "status" ("accepted", "duplicate", ...), or None if unparseable."""
    try:
        return response.json().get("status")
    except Exception:
        return None


_response_status = response_status
