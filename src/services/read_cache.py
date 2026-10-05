"""Successful league-scoped reads with per-key in-flight deduplication."""

import hashlib
import json
import logging
import os
import threading
from concurrent.futures import Future
from src.database import get_redis_client

logger = logging.getLogger(__name__)
_guard = threading.Lock()
_inflight = {}
MAX_INFLIGHT_KEYS = 128


def league_key(season):
    # The league id must be the second key element: cached() reads it to find the
    # invalidate_league() version counter for the entry.
    return [os.getenv("ESPN_LEAGUE_ID"), season]


def cached(key, ttl, load):
    redis = None
    try:
        redis = get_redis_client()
        key = [*key, redis.get("ux:version:" + str(key[1])) or "0"]
    except Exception:
        pass
    digest = hashlib.sha256(json.dumps(key, sort_keys=True, default=str).encode()).hexdigest()
    redis_key = "ux:read:" + digest
    try:
        result = redis.get(redis_key) if redis else None
        if result is not None:
            return json.loads(result)
    except Exception:
        logger.warning("Read cache unavailable; fetching source")
    with _guard:
        pending = _inflight.get(digest)
        owner = pending is None
        if owner:
            pending = Future()
            if len(_inflight) < MAX_INFLIGHT_KEYS:
                _inflight[digest] = pending
    if not owner:
        return pending.result()
    try:
        result = load()
        if redis is not None:
            try:
                redis.setex(redis_key, ttl, json.dumps(result, default=str))
            except Exception:
                logger.warning("Read cache write unavailable")
        pending.set_result(result)
        return result
    except BaseException as exc:
        pending.set_exception(exc)
        raise
    finally:
        with _guard:
            if _inflight.get(digest) is pending:
                del _inflight[digest]


def invalidate_league(league):
    try:
        get_redis_client().incr("ux:version:" + str(league))
    except Exception:
        logger.warning("Cache invalidation deferred to TTL")
