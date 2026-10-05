"""Application-visible LLM transcripts; no internal reasoning collection."""

import logging
import os
import json
import hashlib
from contextvars import ContextVar
from functools import wraps
from src.services.activity_service import session_start, session_event, redact

current_session = ContextVar("llm_session", default=None)
logger = logging.getLogger(__name__)
capture_content = ContextVar("llm_capture_content", default=True)
MAX_HISTORY_EVENT_CHARS = 16000


def history_content(content):
    content = redact(content)
    encoded = json.dumps(content, default=str)
    if not capture_content.get():
        return {"content_sha256": hashlib.sha256(encoded.encode()).hexdigest(), "characters": len(encoded), "capture": "metadata only"}
    if len(encoded) > MAX_HISTORY_EVENT_CHARS:
        return {"preview": encoded[:MAX_HISTORY_EVENT_CHARS], "truncated": True, "characters": len(encoded)}
    return content


def observe(role, content, status=None):
    identity = current_session.get()
    if identity:
        try:
            session_event(identity, role, history_content(content) if role in {"prompt", "response"} else content, status)
        except Exception:
            logger.exception("LLM history event could not be persisted")


def record_call(fn):
    @wraps(fn)
    def wrapped(prompt, *args, **kwargs):
        token = None
        capture_token = capture_content.set(bool(current_session.get()) or os.getenv("LLM_HISTORY_CAPTURE_CONTENT", "false").lower() == "true")
        if not current_session.get():
            try:
                token = current_session.set(
                    session_start(kwargs.get("call_site") or "LLM run")
                )
            except Exception:
                logger.exception("LLM history could not start")
        observe("prompt", {"system": kwargs.get("system_prompt"), "user": prompt})
        try:
            result = fn(prompt, *args, **kwargs)
            observe(
                "response",
                {
                    "text": result.text,
                    "provider": result.provider,
                    "model": result.model,
                    "latency_ms": result.latency_ms,
                },
                "completed",
            )
            return result
        except Exception as exc:
            observe("error", str(exc), "failed")
            raise
        finally:
            capture_content.reset(capture_token)
            if token is not None:
                current_session.reset(token)

    return wrapped
