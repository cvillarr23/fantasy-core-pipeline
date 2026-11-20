"""`python -m src.launcher`: a standby does nothing until it holds the Lease."""

import threading
from contextlib import ExitStack
from unittest.mock import patch

from src.launcher import __main__ as launcher
from src.launcher.inline import KILL_GRACE_SECONDS


def start(acquired=True, on_consume=None, fail_inline=None):
    """Runs main() with every collaborator mocked; returns (exit code, mocks, call order)."""
    order = []
    mocks = {}
    with ExitStack() as stack:
        for name in ("configure_tracing", "load_catalog_from_env", "Scheduler"):
            stack.enter_context(patch.object(launcher, name))
        for name in ("PostgresSchedulerState", "EventConsumer", "Watcher"):
            stack.enter_context(patch.object(launcher, name))
        stack.enter_context(patch.object(launcher.k8s, "validate_catalog"))
        stack.enter_context(patch.object(launcher.signal, "signal"))
        stack.enter_context(patch.object(launcher.heartbeat, "run"))
        mocks["thread"] = stack.enter_context(
            patch.object(launcher.threading, "Thread")
        )
        elector = mocks["elector"] = stack.enter_context(
            patch.object(launcher, "LeaseElector")
        ).return_value
        elector.lost = threading.Event()
        elector.acquire.return_value = acquired
        elector.release.side_effect = lambda: order.append("release")
        store = mocks["store"] = stack.enter_context(
            patch.object(launcher, "RunStore")
        ).return_value
        store.fail_running_inline.side_effect = fail_inline or (
            lambda reason: order.append(("fail_inline", reason)) or []
        )
        inline = mocks["inline"] = stack.enter_context(
            patch.object(launcher, "InlineRunner")
        ).return_value
        inline.start.side_effect = lambda: order.append("inline_start")
        inline.stop.side_effect = lambda grace: order.append(("inline_stop", grace))
        consumer = mocks["consumer"] = stack.enter_context(
            patch.object(launcher, "Consumer")
        )
        consumer.return_value.run.side_effect = lambda stop: (
            order.append("consume"),
            on_consume and on_consume(elector),
        )
        code = launcher.main()
    return code, mocks, order


def thread_names(mocks):
    return [c.kwargs["name"] for c in mocks["thread"].call_args_list]


def test_a_standby_stopped_before_it_leads_starts_nothing():
    code, mocks, order = start(acquired=False)
    assert code == 0
    assert order == []
    mocks["consumer"].assert_not_called()
    assert thread_names(mocks) == []
    mocks["elector"].release.assert_not_called()


def test_the_leader_fails_a_previous_leaders_inline_runs_before_running_its_own():
    code, mocks, order = start()
    assert code == 0
    assert order == [
        ("fail_inline", "launcher_lost"),
        "inline_start",
        "consume",
        ("inline_stop", KILL_GRACE_SECONDS),
        "release",
    ]
    assert thread_names(mocks)[0] == "launcher-lease"
    assert {"launcher-scheduler", "launcher-watcher"} <= set(thread_names(mocks))


def test_an_unreachable_database_at_takeover_is_retried():
    calls = []

    def flaky(reason):
        calls.append(reason)
        if len(calls) == 1:
            raise RuntimeError("database away")
        return ["r1"]

    with patch.object(launcher, "TAKEOVER_RETRY_SECONDS", 0):
        code, _, order = start(fail_inline=flaky)
    assert code == 0 and len(calls) == 2
    assert order[0] == "inline_start"


def test_a_lost_lease_kills_inline_work_fast_and_exits_without_releasing():
    code, mocks, order = start(on_consume=lambda elector: elector.lost.set())
    assert code == 1
    assert ("inline_stop", launcher.LOST_LEASE_KILL_GRACE_SECONDS) in order
    assert "release" not in order


def test_an_unusable_catalog_refuses_to_start_even_as_a_standby():
    with (
        patch.object(launcher, "configure_tracing"),
        patch.object(launcher, "load_catalog_from_env", side_effect=ValueError),
        patch.object(launcher, "LeaseElector") as elector,
    ):
        assert launcher.main() == 1
    elector.assert_not_called()


def test_lost_lease_grace_fits_between_the_renew_deadline_and_expiry():
    from src.launcher import leader

    stop_by = (
        leader.RENEW_DEADLINE_SECONDS
        + leader.RETRY_SECONDS
        + 2 * leader.REQUEST_TIMEOUT_SECONDS
        + launcher.LOST_LEASE_KILL_GRACE_SECONDS
    )
    assert stop_by <= leader.LEASE_SECONDS
