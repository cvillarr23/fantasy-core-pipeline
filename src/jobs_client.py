"""HTTP client for the Jobs API (`/v1/jobs`, `/v1/runs`), shared by the CLI and the MCP tools.

Both are remote callers of the same REST controllers the web UI uses, so they go through the
API rather than into `jobs_service`: the `X-API-Key` guard, validation and status-code mapping
stay in one place. Start requests carry a `trigger` ("cli" or "mcp") so run_log records who asked.
`CORE_API_BASE_URL` and `TRIGGER_API_KEY` are read per call, not at import, so the CLI can be
configured after import and tests can set them.
"""
import os
from typing import Any
from urllib.parse import quote

import requests
from opentelemetry import propagate

DEFAULT_BASE_URL = "http://fantasy-core-pipeline.core.svc.cluster.local:8000"
TIMEOUT_SECONDS = 15.0


class JobsApiError(Exception):
    """The Jobs API refused the request or could not be reached. `status` is 0 when unreachable."""

    def __init__(self, status: int, detail: Any):
        super().__init__(f"HTTP {status}: {detail}" if status else str(detail))
        self.status = status
        self.detail = detail


def list_jobs() -> dict:
    return _request("GET", "/v1/jobs")


def run_job(name: str, params: dict | None = None, downstream: bool = False, trigger: str = "cli") -> dict:
    body = {"params": params or {}, "downstream": downstream, "trigger": trigger}
    return _request("POST", f"/v1/jobs/{_seg(name)}/runs", json=body, authenticated=True)


def get_run(identity: str) -> dict:
    return _request("GET", f"/v1/runs/{_seg(identity)}")


def cancel_run(identity: str) -> dict:
    return _request("POST", f"/v1/runs/{_seg(identity)}/cancel", authenticated=True)


def pause_job(name: str, reason: str, until: str | None = None) -> dict:
    return _request(
        "PUT", f"/v1/jobs/{_seg(name)}/pause", json={"until": until, "reason": reason}, authenticated=True
    )


def unpause_job(name: str) -> dict:
    return _request("DELETE", f"/v1/jobs/{_seg(name)}/pause", authenticated=True)


def _seg(value: str) -> str:
    return quote(value, safe="")


def _headers(authenticated: bool) -> dict:
    headers: dict = {}
    if authenticated:
        key = os.getenv("TRIGGER_API_KEY")
        if not key:
            raise JobsApiError(0, "TRIGGER_API_KEY is not set; this call needs the API key")
        headers["X-API-Key"] = key
    propagate.inject(headers)
    return headers


def _request(method: str, path: str, json: dict | None = None, authenticated: bool = False) -> dict:
    base = os.getenv("CORE_API_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    headers = _headers(authenticated)
    try:
        response = requests.request(
            method, f"{base}{path}", json=json, headers=headers, timeout=TIMEOUT_SECONDS
        )
    except requests.RequestException as exc:
        raise JobsApiError(0, f"Jobs API unreachable: {exc}") from exc
    if not response.ok:
        raise JobsApiError(response.status_code, _error_detail(response))
    return response.json()


def _error_detail(response: requests.Response) -> Any:
    try:
        return response.json()["detail"]
    except (ValueError, KeyError, TypeError):
        return response.text
