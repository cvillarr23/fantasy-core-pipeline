from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest.mock import Mock, patch

import pytest
import requests

from src.ingestion import sync_league


def test_timed_clients_keep_overlapping_requests_and_failure_isolated():
    from src.ingestion.espn_client import TimedEspnFantasyRequests

    original_get = requests.get
    a_entered = Event()
    b_entered = Event()
    release_a = Event()
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs.get("timeout")))
        if url.endswith("/a"):
            a_entered.set()
            assert b_entered.wait(5)
            assert release_a.wait(5)
            raise requests.Timeout("A timed out")
        if url.endswith("/b"):
            b_entered.set()
            return Mock(status_code=200, json=lambda: {"client": "B"})
        return Mock(status_code=200, json=lambda: {"unrelated": True})

    a = TimedEspnFantasyRequests("nfl", 2026, 1, timeout_seconds=5)
    b = TimedEspnFantasyRequests("nfl", 2026, 1, timeout_seconds=7)
    with patch("requests.get", side_effect=fake_get) as patched_get:
        with ThreadPoolExecutor(max_workers=2) as pool:
            future_a = pool.submit(a.get, extend="/a")
            assert a_entered.wait(5)
            future_b = pool.submit(b.get, extend="/b")
            assert future_b.result(timeout=5) == {"client": "B"}
            release_a.set()
            with pytest.raises(requests.Timeout):
                future_a.result(timeout=5)

        assert requests.get is patched_get
        assert requests.get("https://example.test/unrelated").json() == {
            "unrelated": True
        }

    assert requests.get is original_get
    assert calls[0][1] == 5
    assert calls[1][1] == 7
    assert calls[2][1] is None


def test_timed_client_bounds_league_and_alternate_endpoint_requests():
    from src.ingestion.espn_client import TimedEspnFantasyRequests

    client = TimedEspnFantasyRequests(
        "nfl", 2026, 1, cookies={"espn_s2": "x", "SWID": "y"}, timeout_seconds=5
    )
    responses = [
        Mock(status_code=401),
        Mock(status_code=200, json=lambda: {"schedule": []}),
    ]
    with patch("requests.get", side_effect=responses) as get:
        assert client.league_get(params={"view": ["mMatchupScore", "mScoreboard"]}) == {
            "schedule": []
        }

    assert [call.kwargs["timeout"] for call in get.call_args_list] == [5, 5]


def test_timed_client_keeps_original_endpoint_after_alternate_timeout():
    from src.ingestion.espn_client import TimedEspnFantasyRequests

    client = TimedEspnFantasyRequests("nfl", 2026, 1, timeout_seconds=5)
    original_endpoint = client.LEAGUE_ENDPOINT
    with patch(
        "requests.get",
        side_effect=[Mock(status_code=401), requests.Timeout("alternate stalled")],
    ) as get:
        with pytest.raises(requests.Timeout):
            client.league_get()

    assert client.LEAGUE_ENDPOINT == original_endpoint
    assert [call.kwargs["timeout"] for call in get.call_args_list] == [5, 5]


def test_timed_league_installs_client_before_first_fetch(monkeypatch):
    _credentials(monkeypatch)
    league = Mock()

    def assert_client_installed():
        from src.ingestion.espn_client import TimedEspnFantasyRequests

        assert isinstance(league.espn_request, TimedEspnFantasyRequests)
        assert league.espn_request.timeout_seconds == 5

    league.fetch_league.side_effect = assert_client_installed
    with patch("src.ingestion.sync_league.League", return_value=league) as league_class:
        assert sync_league.build_league(2026, request_timeout_seconds=5) is league

    assert league_class.call_args.kwargs["fetch_league"] is False
    league.fetch_league.assert_called_once_with()


def test_regular_league_keeps_the_default_espn_client(monkeypatch):
    _credentials(monkeypatch)
    league = Mock()
    with patch("src.ingestion.sync_league.League", return_value=league) as league_class:
        assert sync_league.build_league(2026) is league

    assert league_class.call_args.kwargs == {
        "league_id": 1,
        "year": 2026,
        "espn_s2": "s2",
        "swid": "swid",
    }
    league.fetch_league.assert_not_called()


def _credentials(monkeypatch):
    monkeypatch.setenv("ESPN_LEAGUE_ID", "1")
    monkeypatch.setenv("ESPN_S2", "s2")
    monkeypatch.setenv("ESPN_SWID", "swid")
