"""Bounded Redis cache for successful, league-scoped read models."""

import hashlib
import json
import logging
import threading
from src.database import get_redis_client

logger = logging.getLogger(__name__)
_locks = [threading.Lock() for _ in range(64)]


def cached(key, ttl, load):
    try:
        key = [*key, get_redis_client().get("ux:version:" + str(key[1])) or "0"]
    except Exception:
        pass
    digest = hashlib.sha256(
        json.dumps(key, sort_keys=True, default=str).encode()
    ).hexdigest()
    redis_key = "ux:read:" + digest
    with _locks[int(digest[:4], 16) % len(_locks)]:
        redis = None
        try:
            redis = get_redis_client()
            result = redis.get(redis_key)
            if result is not None:
                return json.loads(result)
        except Exception:
            logger.warning("Read cache unavailable; fetching source")
        result = load()
        if redis is not None:
            try:
                redis.setex(redis_key, ttl, json.dumps(result, default=str))
            except Exception:
                logger.warning("Read cache write unavailable")
        return result


def invalidate_league(league):
    try:
        get_redis_client().incr("ux:version:" + str(league))
    except Exception:
        logger.warning("Cache invalidation deferred to TTL")
