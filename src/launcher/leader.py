"""Leader election over a k8s Lease, so two launcher replicas never both act.

Both replicas load the catalog, then block in `acquire` until one holds the
`fantasy-job-launcher` Lease. Only the holder runs the scheduler, consumers, watcher, inline
runner and heartbeat; the other waits as a standby. `hold` renews the Lease on its own thread
and, once it can't renew within RENEW_DEADLINE_SECONDS or sees another holder, sets `lost` and
the shared stop event, so the process shuts down and restarts as a standby (client-go's
"exit on lost lease" model: no thread ever has to restart in place). A clean shutdown calls
`release`, so the standby takes over at its next retry instead of waiting out the Lease.

Timing: a standby takes an unrenewed Lease LEASE_SECONDS after it first saw that record, by
its own monotonic clock (as client-go does), so clock skew between nodes doesn't matter. A
leader stops at most RENEW_DEADLINE_SECONDS plus one retry and two timed-out calls after its
last renewal, which is still inside LEASE_SECONDS. A killed leader is replaced within about
LEASE_SECONDS + RETRY_SECONDS, well inside the scheduler's 2-minute SLOT_GRACE, so no slot is
missed. Anything that slips through that window is still guarded in Postgres: dedupe keys,
the admission advisory lock and attempt-pinned run_log transitions.

The kubernetes client's own `leaderelection` isn't used: its acquire loop can't be stopped,
it never releases, and its calls carry no request timeout.
"""

import logging
import socket
import threading
import time
import uuid
from datetime import datetime, timezone

from kubernetes import client
from kubernetes.client.rest import ApiException

from src.k8s_client import K8S_NAMESPACE, coordination_v1_api

logger = logging.getLogger(__name__)

LEASE_NAME = "fantasy-job-launcher"
LEASE_SECONDS = 15
RENEW_DEADLINE_SECONDS = 8
RETRY_SECONDS = 2
# Short, so a hung apiserver call can't carry a renewal past the Lease's expiry.
REQUEST_TIMEOUT_SECONDS = 2

HELD = "held"
TAKEN = "taken"
FAILED = "failed"


def _default_identity():
    """The pod name plus a per-process suffix: a container restarted in the same pod is a new
    candidate, not the holder it replaced."""
    return f"{socket.gethostname()}_{uuid.uuid4().hex[:8]}"


def _now():
    return datetime.now(timezone.utc)


class LeaseElector:
    def __init__(
        self,
        name=LEASE_NAME,
        namespace=K8S_NAMESPACE,
        identity=None,
        api=None,
        clock=None,
    ):
        self.name = name
        self.namespace = namespace
        self.identity = identity or _default_identity()
        self.api = api or coordination_v1_api()
        self.clock = clock or time.monotonic
        self.lost = threading.Event()
        self._observed_version = None
        self._observed_at = None

    def acquire(self, stop: threading.Event) -> bool:
        """Blocks until this process holds the Lease (True) or `stop` is set (False)."""
        logger.info("Launcher %s is waiting for the %s lease", self.identity, self.name)
        while not stop.is_set():
            if self._attempt() == HELD:
                logger.info("Launcher %s is the leader", self.identity)
                return True
            if stop.wait(RETRY_SECONDS):
                break
        return False

    def hold(self, stop: threading.Event) -> None:
        """Renews the Lease until `stop` is set. Losing it sets `lost` and `stop`."""
        last_renewed = self.clock()
        while not stop.wait(RETRY_SECONDS):
            outcome = self._attempt()
            if outcome == HELD:
                last_renewed = self.clock()
            elif outcome == TAKEN or self._past_renew_deadline(last_renewed):
                self._lose(stop, outcome)
                return

    def release(self) -> None:
        """Frees the Lease if this process still holds it; errors only log, since an
        unreleased Lease just makes the standby wait LEASE_SECONDS."""
        try:
            lease = self._read()
            if lease is None or not self._held_by_us(lease):
                return
            lease.spec.holder_identity = None
            lease.spec.lease_duration_seconds = 1
            lease.spec.renew_time = _now()
            self._replace(lease)
            logger.info("Launcher %s released the %s lease", self.identity, self.name)
        except Exception:
            logger.warning("Could not release the %s lease", self.name, exc_info=True)

    # ---- one try ----------------------------------------------------------------------

    def _attempt(self):
        """One read-then-write: HELD when this process holds the Lease afterwards, TAKEN when
        another holder's Lease is still live, FAILED on an API error or a lost write race.
        """
        try:
            lease = self._read()
            if lease is None:
                self._create()
                return HELD
            self._observe(lease)
            if not self._held_by_us(lease) and not self._claimable(lease):
                return TAKEN
            self._replace(self._claimed(lease))
            return HELD
        except ApiException as exc:
            logger.warning("Lease %s write failed (%s)", self.name, exc.status)
        except Exception:
            logger.warning("Lease %s unavailable", self.name, exc_info=True)
        return FAILED

    def _read(self):
        try:
            return self.api.read_namespaced_lease(
                self.name, self.namespace, _request_timeout=REQUEST_TIMEOUT_SECONDS
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise

    def _create(self):
        now = _now()
        body = client.V1Lease(
            metadata=client.V1ObjectMeta(name=self.name, namespace=self.namespace),
            spec=client.V1LeaseSpec(
                holder_identity=self.identity,
                lease_duration_seconds=LEASE_SECONDS,
                acquire_time=now,
                renew_time=now,
                lease_transitions=0,
            ),
        )
        self.api.create_namespaced_lease(
            self.namespace, body, _request_timeout=REQUEST_TIMEOUT_SECONDS
        )

    def _replace(self, lease):
        """Writes with the read resourceVersion, so a concurrent writer gets a 409."""
        self.api.replace_namespaced_lease(
            self.name, self.namespace, lease, _request_timeout=REQUEST_TIMEOUT_SECONDS
        )

    def _observe(self, lease):
        """Starts the expiry clock whenever the record changes, by our own clock."""
        version = lease.metadata.resource_version
        if version != self._observed_version:
            self._observed_version = version
            self._observed_at = self.clock()

    def _held_by_us(self, lease):
        return lease.spec.holder_identity == self.identity

    def _claimable(self, lease):
        """Released (no holder), or unrenewed for its whole duration since we saw it."""
        if not lease.spec.holder_identity:
            return True
        duration = lease.spec.lease_duration_seconds or LEASE_SECONDS
        return self.clock() - self._observed_at >= duration

    def _claimed(self, lease):
        """The Lease renewed by us, or taken over with a new acquireTime and transition."""
        spec = lease.spec
        now = _now()
        if not self._held_by_us(lease):
            spec.acquire_time = now
            spec.lease_transitions = (spec.lease_transitions or 0) + 1
            spec.holder_identity = self.identity
        spec.renew_time = now
        spec.lease_duration_seconds = LEASE_SECONDS
        return lease

    def _past_renew_deadline(self, last_renewed):
        return self.clock() - last_renewed >= RENEW_DEADLINE_SECONDS

    def _lose(self, stop, outcome):
        logger.error(
            "Launcher %s lost the %s lease (%s); stopping",
            self.identity,
            self.name,
            "another holder" if outcome == TAKEN else "renew deadline passed",
        )
        self.lost.set()
        stop.set()
