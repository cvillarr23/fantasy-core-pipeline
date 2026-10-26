"""
Stale draft-job sweeper (fantasy-infra#57).

Backstop, not a fix: cvillarr23/fantasy-browser-worker#68 and cvillarr23/fantasy-core-pipeline#103
already made practice-room teardown fast and correctly-classified, but neither guarantees teardown
is ever *called* - a session abandoned without the frontend or an operator ever hitting
DELETE /v1/draft/practice/sync-job leaves its connector/listener Jobs running indefinitely. This
CronJob (base/core-pipeline/stale-draft-job-sweeper-cronjob.yaml in fantasy-infra, every 15
minutes) catches that case.

Staleness signal: fantasy-browser-worker's own `draft:{league_id}:health` heartbeat, already
written with a 1-hour TTL and already read by src/api/draft.py (~line 720) to answer "is this room
actually still alive." No new instrumentation - if that key is gone, the room's own heartbeat has
already declared it dead. Deliberately NOT Job age: a legitimately long-running draft must never
be swept just for being old.

Only one practice run is ever active at a time (starting a new room always tears down the old one
first - see _teardown_practice_run's docstring), so "any currently-registered practice Job's
league-id has no health key" is unambiguous: retire the whole current practice run via the same
DELETE /v1/draft/practice/sync-job endpoint the UI's "End practice run" button calls, exactly
mirroring how src/optimization/draft_matrix.py's publish_draft_target talks to the gateway over
HTTP rather than importing route-handler internals directly.
"""
import logging
import os

import redis
import requests

from src.api.draft import PRACTICE_SYNC_APP_LABEL
from src.k8s_client import K8S_NAMESPACE, K8S_REQUEST_TIMEOUT_SECONDS
from src.k8s_client import batch_v1_api as _batch_v1_api
from src.database import get_redis_client
from src.services import run_log_service

logger = logging.getLogger("StaleDraftJobSweeper")

CORE_API_BASE_URL = os.getenv("CORE_API_BASE_URL", "http://fantasy-api.core.svc.cluster.local:8000")
TEARDOWN_TIMEOUT_SECONDS = 30

# The listener Job's own selector (src/api/draft.py's _delete_practice_listener_jobs /
# _wait_for_jobs_gone calls use this exact string, not a shared constant, so matching it
# literally here rather than inventing a new shared constant this sweep would be the only
# other user of).
LISTENER_LABEL_SELECTOR = "app=draft-sync,mode=practice"


def _health_key(league_id: str) -> str:
    return f"draft:{league_id}:health"


def _practice_league_ids() -> set:
    """Every league-id label currently present across the practice connector and listener
    Jobs - both, since the connector carries ttl_seconds_after_finished and can already be
    gone while the listener (which blocks on its Redis Stream forever) is still up, or vice
    versa right after a teardown that raced a new run starting."""
    batch_api = _batch_v1_api()
    league_ids = set()
    for label_selector in (f"app={PRACTICE_SYNC_APP_LABEL}", LISTENER_LABEL_SELECTOR):
        jobs = batch_api.list_namespaced_job(
            K8S_NAMESPACE, label_selector=label_selector, _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS
        )
        for job in jobs.items:
            league_id = (job.metadata.labels or {}).get("league-id")
            if league_id:
                league_ids.add(league_id)
    return league_ids


def _is_stale(r_client: redis.Redis, league_id: str) -> bool:
    return r_client.exists(_health_key(league_id)) == 0


def sweep() -> None:
    league_ids = _practice_league_ids()
    if not league_ids:
        logger.info("No practice draft-sync Jobs registered; nothing to sweep.")
        return

    r_client = get_redis_client()
    stale_league_ids = [league_id for league_id in league_ids if _is_stale(r_client, league_id)]

    if not stale_league_ids:
        logger.info(
            "Practice draft-sync Job(s) for league(s) %s still have a live heartbeat; skipping.",
            sorted(league_ids),
        )
        return

    # _teardown_practice_run has no per-league scoping - it always retires *everything*
    # currently registered. Confirmed live 2026-08-27: a leftover connector/listener Job from
    # an earlier session, not yet reaped after a room switch ("did not terminate in time" is
    # a known warning for exactly this), sat alongside a healthy, currently-active session at
    # list time. Tearing down on *any* stale league_id killed the live session as collateral.
    # Only act when EVERY currently-registered league_id is stale - i.e. nothing healthy is
    # running right now. A mix of stale-and-healthy is exactly the ambiguous "something is
    # mid-cleanup" case; leave it alone and let the next sweep tick re-evaluate once the
    # leftover has actually finished terminating.
    if len(stale_league_ids) != len(league_ids):
        logger.info(
            "Practice draft-sync Job(s) for league(s) %s have no live heartbeat, but league(s) %s "
            "do - not tearing down a mix of stale and healthy; leaving this for the next sweep tick.",
            sorted(stale_league_ids), sorted(set(league_ids) - set(stale_league_ids)),
        )
        return

    logger.warning(
        "Practice draft-sync Job(s) for league(s) %s have no live heartbeat (TTL expired); "
        "tearing down the current practice run.",
        sorted(stale_league_ids),
    )
    response = requests.delete(
        f"{CORE_API_BASE_URL}/v1/draft/practice/sync-job",
        timeout=TEARDOWN_TIMEOUT_SECONDS,
    )
    # 404 means nothing was running by the time this fired (e.g. a race with someone
    # manually ending the run between the list above and this call) - not a sweep failure.
    if response.status_code not in (200, 404):
        response.raise_for_status()
    logger.info("Stale practice run teardown request completed with status %s.", response.status_code)


if __name__ == "__main__":
    with run_log_service.run_scope("stale_draft_job_sweeper"):
        sweep()
