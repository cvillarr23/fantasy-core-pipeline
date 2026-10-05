"""Application-visible LLM transcripts; no internal reasoning collection."""

import logging
from contextvars import ContextVar
from functools import wraps
from src.services.activity_service import session_start, session_event

current_session = ContextVar("llm_session", default=None)
logger = logging.getLogger(__name__)


def observe(role, content, status=None):
    identity = current_session.get()
    if identity:
        try:
            session_event(identity, role, content, status)
        except Exception:
            logger.exception("LLM history event could not be persisted")


def record_call(fn):
    @wraps(fn)
    def wrapped(prompt, *args, **kwargs):
        token = None
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
            if token is not None:
                current_session.reset(token)

    return wrapped
