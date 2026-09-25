"""Shared client for POSTing to fastapi-deployment's /v1/trigger/* endpoints.

Every caller (lineup_solver, waiver_evaluator, draft_matrix) was independently
redeclaring CORE_API_BASE_URL/TRIGGER_API_KEY, the X-API-Key header, and trace
propagation, with inconsistent error handling (only draft_matrix logged the
response body before raising). This centralizes that plumbing; payload
construction stays with each caller since it's endpoint-specific.
"""
import logging
import os

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
