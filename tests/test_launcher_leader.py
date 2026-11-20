"""Lease leader election for the launcher, against an in-memory Coordination API."""

import threading
from datetime import datetime, timezone

from kubernetes import client
from kubernetes.client.rest import ApiException

from src.launcher import leader
from src.launcher.leader import LeaseElector


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class FakeLeases:
    """One namespace's Leases with resourceVersion checks, as the apiserver does them."""

    def __init__(self):
        self.leases = {}
        self.version = 0
        self.timeouts = []
        self.fail = None

    def _check(self, kwargs):
        self.timeouts.append(kwargs.get("_request_timeout"))
        if self.fail:
            raise self.fail

    def _stored(self, body):
        self.version += 1
        body.metadata.resource_version = str(self.version)
        self.leases[body.metadata.name] = body
        return body

    def read_namespaced_lease(self, name, namespace, **kwargs):
        self._check(kwargs)
        if name not in self.leases:
            raise ApiException(status=404)
        stored = self.leases[name]
        return client.V1Lease(
            metadata=client.V1ObjectMeta(
                name=name, resource_version=stored.metadata.resource_version
            ),
            spec=client.V1LeaseSpec(**stored.spec.to_dict()),
        )

    def create_namespaced_lease(self, namespace, body, **kwargs):
        self._check(kwargs)
        if body.metadata.name in self.leases:
            raise ApiException(status=409)
        return self._stored(body)

    def replace_namespaced_lease(self, name, namespace, body, **kwargs):
        self._check(kwargs)
        if (
            body.metadata.resource_version
            != self.leases[name].metadata.resource_version
        ):
            raise ApiException(status=409)
        return self._stored(body)

    def holder(self, name=leader.LEASE_NAME):
        return self.leases[name].spec.holder_identity

    def put(self, holder, duration=leader.LEASE_SECONDS, transitions=0):
        now = datetime.now(timezone.utc)
        self._stored(
            client.V1Lease(
                metadata=client.V1ObjectMeta(name=leader.LEASE_NAME),
                spec=client.V1LeaseSpec(
                    holder_identity=holder,
                    lease_duration_seconds=duration,
                    acquire_time=now,
                    renew_time=now,
                    lease_transitions=transitions,
                ),
            )
        )


def elector(api, identity="a", clock=None):
    return LeaseElector(identity=identity, api=api, clock=clock or Clock())


def stop_after(waits):
    """A stop event whose wait() returns True once it has been called `waits` times."""
    stop = threading.Event()
    calls = []

    def wait(timeout=None):
        calls.append(timeout)
        if len(calls) >= waits:
            stop.set()
        return stop.is_set()

    stop.wait = wait
    return stop, calls


def test_creates_the_lease_when_none_exists():
    api = FakeLeases()
    assert elector(api).acquire(threading.Event())
    assert api.holder() == "a"
    spec = api.leases[leader.LEASE_NAME].spec
    assert spec.lease_duration_seconds == leader.LEASE_SECONDS
    assert spec.renew_time is not None and spec.acquire_time is not None


def test_every_call_is_bounded_by_a_request_timeout():
    api = FakeLeases()
    elector(api).acquire(threading.Event())
    assert api.timeouts and set(api.timeouts) == {leader.REQUEST_TIMEOUT_SECONDS}


def test_waits_on_a_fresh_lease_someone_else_holds():
    api = FakeLeases()
    api.put("b")
    stop, waits = stop_after(3)
    assert not elector(api).acquire(stop)
    assert api.holder() == "b"
    assert waits == [leader.RETRY_SECONDS] * 3


def test_takes_a_lease_whose_holder_stopped_renewing():
    api = FakeLeases()
    api.put("b", transitions=4)
    clock = Clock()
    candidate = elector(api, clock=clock)
    stop = threading.Event()
    waits = []

    def wait(timeout=None):
        waits.append(timeout)
        clock.now += timeout
        return False

    stop.wait = wait
    assert candidate.acquire(stop)
    assert api.holder() == "a"
    # Expiry runs from when this candidate first saw the record, not from renewTime.
    assert sum(waits) >= leader.LEASE_SECONDS
    assert api.leases[leader.LEASE_NAME].spec.lease_transitions == 5


def test_takes_a_released_lease_at_once():
    api = FakeLeases()
    api.put(None, duration=1)
    assert elector(api).acquire(threading.Event())
    assert api.holder() == "a"


def test_a_conflicting_write_is_not_an_acquisition():
    api = FakeLeases()
    api.put(None, duration=1)
    original = api.replace_namespaced_lease

    def racing_replace(name, namespace, body, **kwargs):
        api.put("b")
        return original(name, namespace, body, **kwargs)

    api.replace_namespaced_lease = racing_replace
    stop, _ = stop_after(1)
    assert not elector(api).acquire(stop)
    assert api.holder() == "b"


def test_an_api_error_is_retried_instead_of_raised():
    api = FakeLeases()
    api.fail = ApiException(status=500)
    stop, waits = stop_after(2)
    assert not elector(api).acquire(stop)
    assert len(waits) == 2


def test_acquire_returns_false_once_stopped():
    api = FakeLeases()
    api.put("b")
    stop = threading.Event()
    stop.set()
    assert not elector(api).acquire(stop)


def test_hold_keeps_renewing_until_stopped():
    api = FakeLeases()
    candidate = elector(api)
    candidate.acquire(threading.Event())
    renewed = api.leases[leader.LEASE_NAME].metadata.resource_version
    stop, _ = stop_after(3)
    candidate.hold(stop)
    assert not candidate.lost.is_set()
    assert api.leases[leader.LEASE_NAME].metadata.resource_version != renewed
    assert api.holder() == "a"


def test_hold_gives_up_after_the_renew_deadline():
    api = FakeLeases()
    clock = Clock()
    candidate = elector(api, clock=clock)
    candidate.acquire(threading.Event())
    api.fail = ApiException(status=503)
    stop = threading.Event()

    def wait(timeout=None):
        clock.now += timeout
        return stop.is_set()

    stop.wait = wait
    candidate.hold(stop)
    assert candidate.lost.is_set() and stop.is_set()
    assert clock.now - 1000.0 >= leader.RENEW_DEADLINE_SECONDS
    assert clock.now - 1000.0 < leader.LEASE_SECONDS


def test_hold_stops_at_once_when_another_holder_took_the_lease():
    api = FakeLeases()
    candidate = elector(api)
    candidate.acquire(threading.Event())
    api.put("b")
    stop = threading.Event()
    stop.wait = lambda timeout=None: stop.is_set()
    candidate.hold(stop)
    assert candidate.lost.is_set() and stop.is_set()
    assert api.holder() == "b"


def test_release_frees_the_lease_for_the_standby():
    api = FakeLeases()
    elector(api).acquire(threading.Event())
    elector(api, identity="b").release()  # not the holder: leaves it alone
    assert api.holder() == "a"
    holder = elector(api)
    holder.acquire(threading.Event())
    holder.release()
    spec = api.leases[leader.LEASE_NAME].spec
    assert spec.holder_identity is None and spec.lease_duration_seconds == 1
    assert elector(api, identity="b").acquire(threading.Event())


def test_release_swallows_api_errors():
    api = FakeLeases()
    holder = elector(api)
    holder.acquire(threading.Event())
    api.fail = ApiException(status=500)
    holder.release()


def test_default_identity_is_unique_per_process(monkeypatch):
    monkeypatch.setattr(leader.socket, "gethostname", lambda: "fantasy-job-launcher-x")
    first = LeaseElector(api=FakeLeases()).identity
    second = LeaseElector(api=FakeLeases()).identity
    assert first.startswith("fantasy-job-launcher-x_") and first != second


def test_lease_timing_lets_the_leader_stop_before_the_standby_can_take_over():
    worst_renew = leader.RETRY_SECONDS + 2 * leader.REQUEST_TIMEOUT_SECONDS
    assert leader.RENEW_DEADLINE_SECONDS + worst_renew < leader.LEASE_SECONDS
