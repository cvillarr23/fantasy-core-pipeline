import json
import os
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.launcher.runs import JobRun


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


# ---------------------------------------------------------------------------
# POST /v1/draft/config
# ---------------------------------------------------------------------------
def test_set_draft_config_upserts(client):
    mock_cursor = MagicMock()
    payload = {"league_id": "L1", "team_id": "7", "season": 2026}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.post("/v1/draft/config", json=payload)

    assert response.status_code == 200
    assert response.json() == {"status": "stored", "league_id": "L1", "team_id": "7", "season": 2026, "mode": "live", "auto_publish_enabled": True}
    query, params = mock_cursor.execute.call_args[0]
    assert "ON CONFLICT (league_id) DO UPDATE" in query
    assert params == ("L1", "7", 2026, "live", True)


def test_set_draft_config_persists_auto_publish_enabled_default_true(client):
    """#108/#109: a caller that never sends auto_publish_enabled must still get true
    persisted (and returned), so existing web-ui/API callers see no behavior change."""
    mock_cursor = MagicMock()
    payload = {"league_id": "L1", "team_id": "7", "season": 2026}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.post("/v1/draft/config", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["auto_publish_enabled"] is True
    query, params = mock_cursor.execute.call_args[0]
    assert "auto_publish_enabled" in query
    assert params[-1] is True


def test_set_draft_config_resets_team_count_only_when_the_season_changes(client):
    """The derived count is fixed for one draft, so reconfiguring a reused room for a new
    season has to clear it; if it survived, derivation would be skipped and every round and
    slot would be sized from last season's league. A same-season edit (team_id, mode) must
    leave it alone, otherwise an in-flight draft loses its count on any config touch."""
    mock_cursor = MagicMock()

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.post("/v1/draft/config", json={"league_id": "L1", "team_id": "7", "season": 2027})

    assert response.status_code == 200
    query = mock_cursor.execute.call_args[0][0]
    normalized = " ".join(query.split())
    assert (
        "team_count = CASE WHEN draft_config.season IS DISTINCT FROM EXCLUDED.season "
        "THEN NULL ELSE draft_config.team_count END"
    ) in normalized


def test_set_draft_config_validates_payload(client):
    response = client.post("/v1/draft/config", json={"league_id": "L1"})
    assert response.status_code == 422


def test_set_draft_config_practice_mode_rejects_real_league_id(client):
    mock_cursor = MagicMock()
    payload = {"league_id": "L1", "team_id": "7", "season": 2026, "mode": "practice"}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft.ESPN_LEAGUE_ID", "L1"):
        response = client.post("/v1/draft/config", json=payload)

    assert response.status_code == 400
    assert "L1" in response.json()["detail"]
    mock_cursor.execute.assert_not_called()


def test_set_draft_config_practice_mode_allows_distinct_league_id(client):
    mock_cursor = MagicMock()
    payload = {"league_id": "MOCK123", "team_id": "8", "season": 2026, "mode": "practice"}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft.ESPN_LEAGUE_ID", "L1"):
        response = client.post("/v1/draft/config", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["mode"] == "practice"
    assert body["league_id"] == "MOCK123"
    query, params = mock_cursor.execute.call_args[0]
    assert params == ("MOCK123", "8", 2026, "practice", True)


def test_set_draft_config_practice_mode_skips_check_when_espn_league_id_unset(client):
    mock_cursor = MagicMock()
    payload = {"league_id": "MOCK123", "team_id": "8", "season": 2026, "mode": "practice"}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft.ESPN_LEAGUE_ID", None):
        response = client.post("/v1/draft/config", json=payload)

    assert response.status_code == 200
    assert response.json()["mode"] == "practice"


def test_get_draft_config_returns_latest_live_row(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "live")

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/draft/config")

    assert response.status_code == 200
    assert response.json() == {"league_id": "L1", "team_id": "7", "season": 2026, "mode": "live"}
    query = mock_cursor.execute.call_args[0][0]
    assert "mode = 'live'" in query
    assert "ORDER BY updated_at DESC" in query


def test_get_draft_config_404_when_none_stored(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.get("/v1/draft/config")

    assert response.status_code == 404


def test_set_draft_config_db_failure(client):
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.api.draft.get_db_cursor", _broken_cursor):
        response = client.post("/v1/draft/config", json={"league_id": "L1", "team_id": "7", "season": 2026})

    assert response.status_code == 500


# ---------------------------------------------------------------------------
# Listener status and Redis cleanup helpers (/start, /stop and practice routes on launcher
# runs are in tests/test_draft_run_routes.py)
# ---------------------------------------------------------------------------
def test_listener_status_reports_failed_when_k8s_job_failed_even_if_redis_says_active():
    from src.api.draft import _listener_status

    listener_state, listener_last_event_at, listener_error = _listener_status(
        "Failed",
        {"listener_state": "Active", "listener_last_event_at": "1000", "listener_error": "boom"},
    )

    assert listener_state == "Failed"
    assert listener_last_event_at == "1970-01-01T00:16:40Z"
    assert listener_error == "boom"


def test_listener_status_drops_dead_runs_state_when_no_listener_job_exists():
    """The board has no TTL, so a finished run's final Failed state and error text sit in Redis
    indefinitely. With no Job left to overwrite them, they must not be reported as current."""
    from src.api.draft import _listener_status

    listener_state, listener_last_event_at, listener_error = _listener_status(
        "Unknown",
        {"listener_state": "Failed", "listener_last_event_at": "1000", "listener_error": "boom"},
        listener_job_absent=True,
    )

    assert listener_state == "Unknown"
    assert listener_error is None
    # "Last seen at" is still true even though the state is not.
    assert listener_last_event_at == "1970-01-01T00:16:40Z"


def test_listener_status_preserves_error_when_job_lookup_failed_rather_than_job_missing():
    """Absence and unreadability are different. Suppressing the error when we simply could not
    read the apiserver would hide a live run's failure exactly when something is wrong."""
    from src.api.draft import _listener_status

    listener_state, _, listener_error = _listener_status(
        "Unknown",
        {"listener_state": "Failed", "listener_last_event_at": "1000", "listener_error": "boom"},
        listener_job_absent=False,
    )

    assert listener_state == "Failed"
    assert listener_error == "boom"


def test_clear_league_draft_state_deletes_board_and_pause_flags():
    from src.api.draft import _clear_league_draft_state

    mock_redis = MagicMock()
    mock_redis.scan_iter.return_value = iter(["draft:L1:pause:t1"])
    mock_redis.delete.return_value = 2

    with patch("src.api.draft._redis_client", return_value=mock_redis):
        assert _clear_league_draft_state("L1") == 2

    assert mock_redis.delete.call_args.args == (
        "draft:L1:board", "draft:L1:suggested", "draft:L1:selecting", "draft:L1:autodraft", "draft:L1:pause:t1"
    )
    # The pick Stream and ledger are NOT touched by default - the connector starts before the
    # listener, so a pick published in that window would be lost.


def test_clear_league_draft_state_drops_the_pick_ledger_and_stream_only_when_asked():
    """The connector seeds its ledger from draft:{league}:picks and the listener replays the
    Stream from id 0, so a re-used league id would resurrect the old run's picks."""
    from src.api.draft import _clear_league_draft_state

    mock_redis = MagicMock()
    mock_redis.scan_iter.return_value = iter([])

    with patch("src.api.draft._redis_client", return_value=mock_redis):
        _clear_league_draft_state("L1", include_pick_stream=True)

    assert mock_redis.delete.call_args.args == (
        "draft:L1:board", "draft:L1:suggested", "draft:L1:selecting", "draft:L1:autodraft",
        "draft:L1:picks", "draft:L1:health", "draft_picks:L1",
    )


def test_clear_league_draft_state_fails_soft_when_redis_is_unreachable():
    """Cleanup and startup callers must not abort because leftover state could not be cleared."""
    from src.api.draft import _clear_league_draft_state

    with patch("src.api.draft._redis_client", side_effect=RuntimeError("no redis")):
        assert _clear_league_draft_state("L1") == 0


# ---------------------------------------------------------------------------
@contextmanager
def _listener(phase="Active", ok=True, status="running", end_reason=None):
    """The league's listener run as _build_status sees it: `phase` is its k8s Job phase, and
    None means no run (with `ok=False`, an unreadable run_log)."""
    run = None
    if phase is not None:
        run = JobRun("run-1", "practice-draft-listener", status, {"league_id": "L1"}, datetime(2026, 8, 21), end_reason)
    with patch("src.api.draft._listener_run", return_value=(run, ok)), \
         patch("src.api.draft.draft_run_service.run_job_state", return_value=phase or "Unknown"):
        yield run


# GET /v1/draft/status
# ---------------------------------------------------------------------------
def test_get_draft_status_aggregates_expected_shape(client):
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    def _fetchall():
        if "ORDER BY d.pick_number DESC" in mock_cursor.last_query:
            return [(2, 1, "3", "p1", "e1", "Player One", "RB", None, None, None, None, None),
                    (1, 1, "9", "p0", "e0", "Player Zero", "WR", None, None, None, None, None)]
        if "ORDER BY d.team_id ASC, d.pick_number ASC" in mock_cursor.last_query:
            return [(1, 1, "9", "p0", "e0", "Player Zero", "WR", None, None, None, None, None),
                    (2, 1, "3", "p1", "e1", "Player One", "RB", None, None, None, None, None)]
        if "draft_slots" in mock_cursor.last_query:
            return [("9", 1), ("3", 2)]
        if "SELECT transaction_type" in mock_cursor.last_query:
            return [("draft_sync", "draft_sync", "click failed", "2026-08-21T00:00:00Z")]
        return []

    def _fetchone():
        if "SELECT mode, auto_publish_enabled, auto_publish_disabled_by FROM draft_config" in mock_cursor.last_query:
            return ("practice", True, None)
        # More specific than the bare "team_count" check below: load_league_config's own
        # query also selects team_count, but as a 2-column row.
        if "lineup_slot_counts" in mock_cursor.last_query:
            return (10, {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "RB/WR/TE": 1, "BE": 6})
        if "team_count" in mock_cursor.last_query:
            return (10,)
        if "MAX(pick_number)" in mock_cursor.last_query:
            return (2,)
        if "SELECT player_name" in mock_cursor.last_query:
            return ("Breece Hall", "success", None, datetime(2026, 8, 21, 14, 32, 5))
        return None

    mock_cursor.fetchall.side_effect = _fetchall
    mock_cursor.fetchone.side_effect = _fetchone

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {
        "recommended_player_id": "p2",
        "recommended_name": "Best Back",
        "pick_number": "3",
        "last_processed_pick_at": "1000",
        "auto_publish_at": "1724257205",
        "pick_clock_expires_at": "1724257240",
    }

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["job_state"] == "Active"
    assert body["mode"] == "practice"
    assert body["auto_publish_enabled"] is True
    assert body["auto_publish_disabled_by"] is None
    assert body["last_clicker_action"] == "Drafted Breece Hall at 14:32:05"
    assert body["recommendation"]["recommended_player_id"] == "p2"
    assert body["recommendation"]["auto_publish_at"] == 1724257205
    assert body["recommendation"]["pick_clock_expires_at"] == 1724257240
    assert len(body["recent_picks"]) == 2
    assert body["recent_picks"][0]["name"] == "Player One"
    assert body["recent_picks"][0]["position"] == "RB"
    assert body["team_rosters"]["9"][0]["pick_number"] == 1
    assert body["team_rosters"]["3"][0]["pick_number"] == 2
    assert body["draft_state"] == "active"
    assert body["pipeline_health"] == "healthy"
    assert body["recent_errors"][0]["transaction_type"] == "draft_sync"
    assert body["listener_state"] == "Active"
    assert body["listener_last_event_at"] == "1970-01-01T00:16:40Z"
    assert body["listener_error"] is None


def test_draft_status_surfaces_auto_publish_enabled_and_disabled_by(client):
    """#108/#109: /status must surface the draft_config row's kill-switch state exactly,
    including a manually-disabled league's reason string."""
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute
    mock_cursor.fetchall.return_value = []

    def _fetchone():
        if "SELECT mode, auto_publish_enabled, auto_publish_disabled_by FROM draft_config" in mock_cursor.last_query:
            return ("live", False, "manual")
        return None

    mock_cursor.fetchone.side_effect = _fetchone

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["auto_publish_enabled"] is False
    assert body["auto_publish_disabled_by"] == "manual"


def test_get_draft_status_team_rosters_are_unbounded_grouped_and_keep_null_player_id(client):
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    recent_rows = [
        (12, 4, "A", "p12", "e12", "Player 12", "RB", None, None, None, None, None),
        (11, 4, "B", "p11", "e11", "Player 11", "WR", None, None, None, None, None),
        (10, 4, "A", "p10", "e10", "Player 10", "QB", None, None, None, None, None),
        (9, 3, "B", "p9", "e9", "Player 9", "TE", None, None, None, None, None),
        (8, 3, "A", "p8", "e8", "Player 8", "RB", None, None, None, None, None),
        (7, 3, "B", "p7", "e7", "Player 7", "WR", None, None, None, None, None),
        (6, 2, "A", "p6", "e6", "Player 6", "QB", None, None, None, None, None),
        (5, 2, "B", None, "e5", None, None, None, None, None, None, None),
        (4, 2, "A", "p4", "e4", "Player 4", "RB", None, None, None, None, None),
        (3, 1, "B", "p3", "e3", "Player 3", "WR", None, None, None, None, None),
    ]
    roster_rows = [
        (1, 1, "A", "p1", "e1", "Player 1", "QB", None, None, None, None, None),
        (2, 1, "B", "p2", "e2", "Player 2", "WR", None, None, None, None, None),
        (3, 1, "B", "p3", "e3", "Player 3", "WR", None, None, None, None, None),
        (4, 2, "A", "p4", "e4", "Player 4", "RB", None, None, None, None, None),
        (5, 2, "B", None, "e5", None, None, None, None, None, None, None),
        (6, 2, "A", "p6", "e6", "Player 6", "QB", None, None, None, None, None),
        (7, 3, "B", "p7", "e7", "Player 7", "WR", None, None, None, None, None),
        (8, 3, "A", "p8", "e8", "Player 8", "RB", None, None, None, None, None),
        (9, 3, "B", "p9", "e9", "Player 9", "TE", None, None, None, None, None),
        (10, 4, "A", "p10", "e10", "Player 10", "QB", None, None, None, None, None),
        (11, 4, "B", "p11", "e11", "Player 11", "WR", None, None, None, None, None),
        (12, 4, "A", "p12", "e12", "Player 12", "RB", None, None, None, None, None),
    ]

    def _fetchall():
        if "ORDER BY d.pick_number DESC" in mock_cursor.last_query:
            return recent_rows
        if "ORDER BY d.team_id ASC, d.pick_number ASC" in mock_cursor.last_query:
            return roster_rows
        if "draft_slots" in mock_cursor.last_query:
            return [("A", 1), ("B", 2), ("C", 3)]
        return []

    def _fetchone():
        if "SELECT mode, auto_publish_enabled, auto_publish_disabled_by FROM draft_config" in mock_cursor.last_query:
            return ("practice", True, None)
        if "lineup_slot_counts" in mock_cursor.last_query:
            return (3, {"QB": 1, "BE": 0})
        if "FROM league_settings" in mock_cursor.last_query:
            return (3,)
        if "MAX(pick_number)" in mock_cursor.last_query:
            return (12,)
        return None

    mock_cursor.fetchall.side_effect = _fetchall
    mock_cursor.fetchone.side_effect = _fetchone
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert len(body["recent_picks"]) == 10
    assert body["recent_picks"][0]["pick_number"] == 12
    assert body["recent_picks"][-1]["pick_number"] == 3
    assert [pick["pick_number"] for pick in body["team_rosters"]["A"]] == [1, 4, 6, 8, 10, 12]
    assert [pick["pick_number"] for pick in body["team_rosters"]["B"]] == [2, 3, 5, 7, 9, 11]
    assert body["team_rosters"].get("C", []) == []
    null_id_pick = next(pick for pick in body["team_rosters"]["B"] if pick["pick_number"] == 5)
    assert null_id_pick["player_id"] is None
    assert null_id_pick["espn_player_id"] == "e5"
    assert body["team_ids"] == ["A", "B", "C"]


def test_get_draft_status_team_ids_includes_zero_pick_teams_in_slot_order(client):
    """team_ids comes from draft_slots, independent of draft_picks - so a team that hasn't
    picked yet still appears (in its assigned slot order), unlike team_rosters/team_aggregates
    which only ever gain a key once a team's first pick lands."""
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    def _fetchall():
        if "ORDER BY d.pick_number DESC" in mock_cursor.last_query:
            return [(1, 1, "A", "p1", "e1", "Player 1", "QB", None, None, None, None, None)]
        if "ORDER BY d.team_id ASC, d.pick_number ASC" in mock_cursor.last_query:
            return [(1, 1, "A", "p1", "e1", "Player 1", "QB", None, None, None, None, None)]
        if "GROUP BY d.team_id, p.position" in mock_cursor.last_query:
            return [("A", "QB", 1, 5.0, 40.0)]
        # Slot order is deliberately not alphabetical, to prove team_ids preserves it rather
        # than re-sorting.
        if "draft_slots" in mock_cursor.last_query:
            return [("C", 1), ("A", 2), ("B", 3)]
        return []

    def _fetchone():
        if "SELECT mode, auto_publish_enabled, auto_publish_disabled_by FROM draft_config" in mock_cursor.last_query:
            return ("practice", True, None)
        if "lineup_slot_counts" in mock_cursor.last_query:
            return (3, {"QB": 1, "BE": 0})
        if "team_count" in mock_cursor.last_query:
            return (3,)
        if "MAX(pick_number)" in mock_cursor.last_query:
            return (1,)
        return None

    mock_cursor.fetchall.side_effect = _fetchall
    mock_cursor.fetchone.side_effect = _fetchone
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["team_ids"] == ["C", "A", "B"]
    # B and C have zero picks - absent from team_rosters/team_aggregates, but still present
    # in team_ids so the frontend can render an empty column for them.
    assert "B" not in body["team_rosters"]
    assert "C" not in body["team_rosters"]
    assert "B" not in body["team_aggregates"]
    assert "C" not in body["team_aggregates"]


def test_get_draft_status_team_ids_empty_when_draft_slots_unsynced(client):
    """draft_slots hasn't been synced yet (e.g. before ESPN publishes slot order) - team_ids
    degrades to an empty list rather than erroring, same as every other _build_status field's
    empty-database default."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["team_ids"] == []


def test_get_draft_status_picks_carry_current_weekly_stats_value_fields(client):
    """vorp_score/projected_points/scarcity_multiplier/matchup_coefficient ride along on
    both recent_picks and team_rosters rows via the weekly_stats LATERAL join - null when
    the joined player has no weekly_stats row at all (same case as a missing player row)."""
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    def _fetchall():
        if "ORDER BY d.pick_number DESC" in mock_cursor.last_query:
            return [(2, 1, "A", "p2", "e2", "Player Two", "WR", 12.5, 145.0, 1.1, 0.95, None),
                    (1, 1, "B", "p1", "e1", "Player One", "RB", None, None, None, None, None)]
        if "ORDER BY d.team_id ASC, d.pick_number ASC" in mock_cursor.last_query:
            return [(1, 1, "B", "p1", "e1", "Player One", "RB", None, None, None, None, None),
                    (2, 1, "A", "p2", "e2", "Player Two", "WR", 12.5, 145.0, 1.1, 0.95, None)]
        if "draft_slots" in mock_cursor.last_query:
            return [("A", 1), ("B", 2)]
        return []

    def _fetchone():
        if "SELECT mode, auto_publish_enabled, auto_publish_disabled_by FROM draft_config" in mock_cursor.last_query:
            return ("practice", True, None)
        if "team_count" in mock_cursor.last_query:
            return (2,)
        if "MAX(pick_number)" in mock_cursor.last_query:
            return (2,)
        return None

    mock_cursor.fetchall.side_effect = _fetchall
    mock_cursor.fetchone.side_effect = _fetchone
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    pick_two = next(p for p in body["recent_picks"] if p["pick_number"] == 2)
    assert pick_two["vorp_score"] == 12.5
    assert pick_two["projected_points"] == 145.0
    assert pick_two["scarcity_multiplier"] == 1.1
    assert pick_two["matchup_coefficient"] == 0.95
    pick_one = next(p for p in body["recent_picks"] if p["pick_number"] == 1)
    assert pick_one["vorp_score"] is None
    roster_pick_two = next(p for p in body["team_rosters"]["A"] if p["pick_number"] == 2)
    assert roster_pick_two["vorp_score"] == 12.5


def test_get_draft_status_picks_carry_was_autodrafted_flag(client):
    """Issue #108 (Task F, fantasy-core-pipeline#110): was_autodrafted rides along on both
    recent_picks and team_rosters rows, straight off draft_picks.was_autodrafted - True for
    an ESPN-autodrafted pick, False for one that matched our attempted target, and null for
    an opponent pick or a pick recorded before the column existed."""
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    def _fetchall():
        if "ORDER BY d.pick_number DESC" in mock_cursor.last_query:
            return [(3, 1, "A", "p3", "e3", "Player Three", "RB", None, None, None, None, True),
                    (2, 1, "A", "p2", "e2", "Player Two", "WR", None, None, None, None, False),
                    (1, 1, "B", "p1", "e1", "Player One", "QB", None, None, None, None, None)]
        if "ORDER BY d.team_id ASC, d.pick_number ASC" in mock_cursor.last_query:
            return [(1, 1, "B", "p1", "e1", "Player One", "QB", None, None, None, None, None),
                    (2, 1, "A", "p2", "e2", "Player Two", "WR", None, None, None, None, False),
                    (3, 1, "A", "p3", "e3", "Player Three", "RB", None, None, None, None, True)]
        if "draft_slots" in mock_cursor.last_query:
            return [("A", 1), ("B", 2)]
        return []

    def _fetchone():
        if "SELECT mode, auto_publish_enabled, auto_publish_disabled_by FROM draft_config" in mock_cursor.last_query:
            return ("practice", True, None)
        if "team_count" in mock_cursor.last_query:
            return (2,)
        if "MAX(pick_number)" in mock_cursor.last_query:
            return (3,)
        return None

    mock_cursor.fetchall.side_effect = _fetchall
    mock_cursor.fetchone.side_effect = _fetchone
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    pick_three = next(p for p in body["recent_picks"] if p["pick_number"] == 3)
    pick_two = next(p for p in body["recent_picks"] if p["pick_number"] == 2)
    pick_one = next(p for p in body["recent_picks"] if p["pick_number"] == 1)
    assert pick_three["was_autodrafted"] is True
    assert pick_two["was_autodrafted"] is False
    assert pick_one["was_autodrafted"] is None
    roster_pick_three = next(p for p in body["team_rosters"]["A"] if p["pick_number"] == 3)
    assert roster_pick_three["was_autodrafted"] is True


def test_get_draft_status_team_aggregates_totals_and_position_counts(client):
    """total_vorp/total_projected_points sum across a team's positions, position_counts
    tallies drafted players per position, and a player with no players-table match (the
    K/D-ST case - position is null) contributes zero to the sums without a synthetic
    position key - with no players match, p.player_id is null, so the weekly_stats LATERAL
    join (keyed on player_id = p.player_id) can never find a row for it either."""
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    def _fetchall():
        if "GROUP BY d.team_id, p.position" in mock_cursor.last_query:
            return [
                ("A", "RB", 2, 25.0, 200.0),
                ("A", "WR", 1, 10.0, 90.0),
                ("A", None, 1, 0.0, 0.0),   # K/D-ST: no players-table match, no weekly_stats
                ("B", "QB", 1, 5.0, 300.0),
            ]
        if "WITH ranked_pool" in mock_cursor.last_query:
            return []
        return []

    def _fetchone():
        if "SELECT mode, auto_publish_enabled, auto_publish_disabled_by FROM draft_config" in mock_cursor.last_query:
            return ("practice", True, None)
        # More specific than the bare "team_count" check below: load_league_config's own
        # query also selects team_count, but as a 2-column row.
        if "lineup_slot_counts" in mock_cursor.last_query:
            return (2, {"QB": 1, "BE": 0})
        if "team_count" in mock_cursor.last_query:
            return (2,)
        if "MAX(pick_number)" in mock_cursor.last_query:
            return (0,)
        return None

    mock_cursor.fetchall.side_effect = _fetchall
    mock_cursor.fetchone.side_effect = _fetchone
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    team_a = body["team_aggregates"]["A"]
    assert team_a["total_vorp"] == 35.0
    assert team_a["total_projected_points"] == 290.0
    assert team_a["position_counts"] == {"RB": 2, "WR": 1}
    team_b = body["team_aggregates"]["B"]
    assert team_b["total_vorp"] == 5.0
    assert team_b["position_counts"] == {"QB": 1}
    # A team with zero picks (not returned by GROUP BY at all) must not KeyError - it's
    # simply absent from team_aggregates, same as it's absent from team_rosters today.
    assert "C" not in body["team_aggregates"]


def test_get_draft_status_value_alert_fires_only_past_the_round_threshold(client):
    """A pick landing >= VALUE_ROUND_THRESHOLD rounds later than its overall-VORP-rank
    predicts sets most_recent_pick_value_alert; a pick roughly on-curve (or drafted ahead
    of its rank) does not."""
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    def _fetchall():
        if "GROUP BY d.team_id, p.position" in mock_cursor.last_query:
            return []
        if "WITH ranked_pool" in mock_cursor.last_query:
            # team_count=4: rank 2 -> expected_round 1; rank 5 -> expected_round 2
            return [("A", 6, 2), ("B", 1, 5)]
        return []

    def _fetchone():
        if "SELECT mode, auto_publish_enabled, auto_publish_disabled_by FROM draft_config" in mock_cursor.last_query:
            return ("practice", True, None)
        # More specific than the bare "team_count" check below: load_league_config's own
        # query also selects team_count, but as a 2-column row.
        if "lineup_slot_counts" in mock_cursor.last_query:
            return (4, {"QB": 1, "BE": 0})
        if "team_count" in mock_cursor.last_query:
            return (4,)
        if "MAX(pick_number)" in mock_cursor.last_query:
            return (6,)
        return None

    mock_cursor.fetchall.side_effect = _fetchall
    mock_cursor.fetchone.side_effect = _fetchone
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    team_a = body["team_aggregates"]["A"]
    assert team_a["most_recent_pick_vorp_rank"] == 2
    assert team_a["most_recent_pick_expected_round"] == 1
    assert team_a["most_recent_pick_value_alert"] is True  # round 6 vs expected round 1
    team_b = body["team_aggregates"]["B"]
    assert team_b["most_recent_pick_expected_round"] == 2
    assert team_b["most_recent_pick_value_alert"] is False  # round 1 vs expected round 2


def test_get_draft_status_team_aggregates_skip_value_alert_when_team_count_unknown(client):
    """team_count None (unresolvable via league_settings/draft_config) must skip the whole
    ranked_pool/most_recent_picks query rather than guess an expected_round - the aggregates
    still degrade to present-but-empty per team, same shape as an unresolvable draft size
    already produces for on_the_clock_team_id."""
    mock_cursor = MagicMock()
    executed_queries = []

    def _execute(query, params=None):
        mock_cursor.last_query = query
        executed_queries.append(query)

    mock_cursor.execute.side_effect = _execute

    def _fetchall():
        if "GROUP BY d.team_id, p.position" in mock_cursor.last_query:
            return [("A", "RB", 1, 8.0, 60.0)]
        return []

    def _fetchone():
        if "SELECT mode, auto_publish_enabled, auto_publish_disabled_by FROM draft_config" in mock_cursor.last_query:
            return ("practice", True, None)
        if "MAX(pick_number)" in mock_cursor.last_query:
            return (0,)
        return None  # no team_count from either league_settings or draft_config

    mock_cursor.fetchall.side_effect = _fetchall
    mock_cursor.fetchone.side_effect = _fetchone
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    team_a = body["team_aggregates"]["A"]
    assert team_a["most_recent_pick_value_alert"] is False
    assert team_a["most_recent_pick_vorp_rank"] is None
    assert team_a["most_recent_pick_expected_round"] is None
    assert not any("WITH ranked_pool" in q for q in executed_queries), \
        "ranked_pool query must not run when team_count is unknown"


def test_get_draft_status_reports_failed_clicker_action(client):
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    def _fetchall():
        return []

    def _fetchone():
        if "SELECT player_name" in mock_cursor.last_query:
            return ("Breece Hall", "failure", "confirmation timed out", datetime(2026, 8, 21, 14, 32, 5))
        # No team_count from either source, and no picks yet - the real "nothing synced,
        # nothing drafted" shape this test is otherwise silent about.
        if "MAX(pick_number)" in mock_cursor.last_query:
            return (0,)
        return None

    mock_cursor.fetchall.side_effect = _fetchall
    mock_cursor.fetchone.side_effect = _fetchone

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["last_clicker_action"] == "Failed to draft Breece Hall: confirmation timed out"


def test_get_draft_status_drops_a_dead_runs_recommendation_when_no_listener_exists(client):
    """Suppressing the stale error while still serving the stale top picks would only make the
    staleness harder to notice - the recommendation describes the same finished run."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {
        "recommended_player_id": "p2", "recommended_name": "Best Back", "listener_error": "boom",
    }

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener(None):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    body = response.json()
    assert body["recommendation"] == {}
    assert body["listener_error"] is None


def test_get_draft_status_keeps_the_recommendation_when_the_job_lookup_failed(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"recommended_player_id": "p2", "listener_error": "boom"}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener(None, ok=False):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    body = response.json()
    assert body["recommendation"]["recommended_player_id"] == "p2"
    assert body["listener_error"] == "boom"


def test_get_draft_status_defaults_mode_to_live_when_config_row_missing(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["mode"] == "live"


def test_get_draft_status_decodes_top_candidates_json(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None

    candidates = [{"player_id": "p2", "name": "Best Back", "position": "RB", "vorp_score": 10.0, "team": "KC"}]
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {
        "recommended_player_id": "p2",
        "recommended_name": "Best Back",
        "pick_number": "3",
        "last_processed_pick_at": "1000",
        "top_candidates": json.dumps(candidates),
    }

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["recommendation"]["top_candidates"] == [{**c, "espn": None} for c in candidates]


def test_get_draft_status_handles_malformed_top_candidates_json(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"recommended_player_id": "p2", "top_candidates": "not-json", "last_processed_pick_at": "1000"}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["recommendation"]["top_candidates"] == []


def test_get_draft_status_includes_espn_suggestion_with_vorp_match(client):
    """The common case: the worker's ESPN player id resolves against players.espn_id, so
    our own name/position/team win, and vorp_by_espn_id (published by draft_matrix.py
    alongside top_candidates) has a score for that player."""
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    def _fetchone():
        if "FROM players WHERE espn_id" in mock_cursor.last_query:
            return ("p9", "Best Back", "RB", "KC")
        return None

    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.side_effect = _fetchone

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {
        "recommended_player_id": "p2",
        "top_candidates": "[]",
        "vorp_by_espn_id": json.dumps({"4362249": 12.5}),
        "last_processed_pick_at": "1000",
    }
    mock_redis.get.side_effect = lambda key: json.dumps({
        "league_id": "L1", "player_id": 4362249, "name": "Best Back",
        "position_id": 2, "pro_team_id": 12, "timestamp": "2026-08-25T00:00:00Z",
    }) if key == "draft:L1:suggested" else None

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    suggestion = response.json()["recommendation"]["espn_suggestion"]
    assert suggestion == {
        "espn_player_id": "4362249", "player_id": "p9", "name": "Best Back",
        "position": "RB", "team": "KC", "vorp_score": 12.5,
        "suggested_at": "2026-08-25T00:00:00Z", "espn": None,
    }
    # Internal staging must never leak into the response.
    assert "_espn_suggestion_staging" not in response.json()["recommendation"]


def test_get_draft_status_espn_suggestion_falls_back_when_unmatched(client):
    """No players.espn_id match (crosswalk gap, or K/D-ST) - falls back to the worker's own
    name plus translated position_id/pro_team_id, with a null VORP score rather than being
    dropped (issue #80's explicit degrade requirement)."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None  # no players row matches

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {
        "recommended_player_id": "p2",
        "top_candidates": "[]",
        "last_processed_pick_at": "1000",
    }
    mock_redis.get.side_effect = lambda key: json.dumps({
        "league_id": "L1", "player_id": 9999999, "name": "Unmatched Guy",
        "position_id": 16, "pro_team_id": 28, "timestamp": "2026-08-25T00:00:00Z",
    }) if key == "draft:L1:suggested" else None

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    suggestion = response.json()["recommendation"]["espn_suggestion"]
    assert suggestion["player_id"] is None
    assert suggestion["name"] == "Unmatched Guy"
    assert suggestion["position"] == "D/ST"
    assert suggestion["team"] == "WAS"  # ESPN's WSH normalized to our internal WAS
    assert suggestion["vorp_score"] is None


def test_get_draft_status_omits_espn_suggestion_when_absent(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"recommended_player_id": "p2", "top_candidates": "[]", "last_processed_pick_at": "1000"}
    mock_redis.get.return_value = None

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert "espn_suggestion" not in response.json()["recommendation"]


def test_get_draft_status_drops_malformed_espn_suggestion_json(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"recommended_player_id": "p2", "top_candidates": "[]", "last_processed_pick_at": "1000"}
    mock_redis.get.return_value = "not-json"

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert "espn_suggestion" not in response.json()["recommendation"]


def test_get_draft_status_drops_non_object_espn_suggestion_json(client):
    """Reviewer-caught (PR #97): json.loads happily accepts a bare list/string/number as
    valid JSON, so a decode success alone doesn't mean _resolve_espn_suggestion can safely
    call raw.get() on the result - a non-object payload must be treated the same as
    malformed JSON, not staged and left to blow up the Postgres read block later."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"recommended_player_id": "p2", "top_candidates": "[]", "last_processed_pick_at": "1000"}
    # Only draft:L1:suggested gets the non-object payload - draft:L1:health (connector
    # health, a separate key) must stay None so it isn't collateral damage of this test.
    mock_redis.get.side_effect = lambda key: "[1, 2, 3]" if key == "draft:L1:suggested" else None

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    body = response.json()
    assert response.status_code == 200
    assert body["pipeline_health"] != "degraded"
    assert "espn_suggestion" not in body["recommendation"]


def test_get_draft_status_drops_non_object_vorp_by_espn_id_json(client):
    """Same class of bug as the suggestion payload itself - a non-object vorp_by_espn_id
    must not reach _resolve_espn_suggestion's .get() call either."""
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query
    mock_cursor.execute.side_effect = _execute

    def _fetchone():
        if "FROM players WHERE espn_id" in mock_cursor.last_query:
            return ("p9", "Best Back", "RB", "KC")
        return None

    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.side_effect = _fetchone

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {
        "recommended_player_id": "p2", "top_candidates": "[]",
        "vorp_by_espn_id": "42", "last_processed_pick_at": "1000",
    }
    mock_redis.get.side_effect = lambda key: json.dumps({
        "league_id": "L1", "player_id": 4362249, "name": "Best Back",
        "position_id": 2, "pro_team_id": 12, "timestamp": "2026-08-25T00:00:00Z",
    }) if key == "draft:L1:suggested" else None

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    body = response.json()
    assert response.status_code == 200
    assert body["pipeline_health"] != "degraded"
    assert body["recommendation"]["espn_suggestion"]["vorp_score"] is None


def test_get_draft_status_omits_espn_suggestion_once_draft_is_complete(client):
    """Reviewer-caught (PR #97): draft:{league_id}:suggested has no TTL and isn't touched by
    draft completion, only by /start or /stop - so once the draft is over, showing ESPN's
    last-seen suggestion (and VORP) as though still current would be actively misleading,
    not merely stale. Unlike top_candidates/recommended_player_id (a real record of the last
    recommendation, left as-is on completion), this must be omitted."""
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query
    mock_cursor.execute.side_effect = _execute

    mock_cursor.fetchall.return_value = []

    def _fetchone():
        query = mock_cursor.last_query
        if "SELECT mode, auto_publish_enabled, auto_publish_disabled_by FROM draft_config" in query:
            return ("practice", True, None)
        if "lineup_slot_counts" in query:
            return (10, {"QB": 1, "BE": 0})  # 1 pick/team
        if "FROM league_settings" in query:
            return (10,)
        if "MAX(pick_number)" in query:
            return (10,)  # 10 teams * 1 pick/team = 10 total picks, all seen
        if "FROM players WHERE espn_id" in query:
            return ("p9", "Best Back", "RB", "KC")
        return None
    mock_cursor.fetchone.side_effect = _fetchone

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"listener_state": "Complete", "top_candidates": "[]"}
    mock_redis.get.side_effect = lambda key: json.dumps({
        "league_id": "L1", "player_id": 4362249, "name": "Best Back",
        "position_id": 2, "pro_team_id": 12, "timestamp": "2026-08-25T00:00:00Z",
    }) if key == "draft:L1:suggested" else None

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Succeeded"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    body = response.json()
    assert response.status_code == 200
    assert body["draft_state"] == "complete"
    assert "espn_suggestion" not in body["recommendation"]


def test_get_draft_status_degraded_when_job_not_active(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener(None):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["pipeline_health"] == "degraded"
    assert response.json()["on_the_clock_team_id"] is None


def test_get_draft_status_returns_200_degraded_when_db_unavailable(client):
    """A Postgres outage must surface as pipeline_health="degraded" in a normal 200, not
    escape as a 500 - the browser needs a real response to decide on the auto-picker
    fallback, not an opaque error."""
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"recommended_player_id": "p2", "last_processed_pick_at": "1000"}

    with patch("src.api.draft.get_db_cursor", _broken_cursor), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["pipeline_health"] == "degraded"
    assert body["recent_picks"] == []
    assert body["team_rosters"] == {}
    assert body["recommendation"]["recommended_player_id"] == "p2"
    assert body["recent_errors"] == []
    assert body["last_clicker_action"] is None
    # Must NOT default to "live" on a DB outage - that could mask a practice run as live
    # in monitoring precisely when the mode lookup itself never ran.
    assert body["mode"] == "unknown"


def test_get_draft_status_returns_200_degraded_when_redis_unavailable(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", side_effect=Exception("redis unreachable")), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["pipeline_health"] == "degraded"
    assert body["recommendation"] == {}


def test_get_draft_status_degraded_when_listener_stale_while_active(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"last_processed_pick_at": "1000"}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=2000):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["pipeline_health"] == "degraded"


def test_get_draft_status_complete_is_healthy_not_degraded(client):
    """#82: a finished draft's Job is expected to no longer be Active
    (draft_matrix.run_draft_listener exits once it observes the same total_picks
    threshold) - requiring job_state == "Active" here is exactly what used to make a
    normal finished run report "degraded", and on_the_clock_team_id must go back to null
    since there is no next pick to be on the clock for."""
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query
    mock_cursor.execute.side_effect = _execute

    def _fetchall():
        return []

    def _fetchone():
        query = mock_cursor.last_query
        if "SELECT mode, auto_publish_enabled, auto_publish_disabled_by FROM draft_config" in query:
            return ("practice", True, None)
        if "lineup_slot_counts" in query:
            return (10, {"QB": 1, "BE": 0})  # 1 pick/team
        if "FROM league_settings" in query:
            return (10,)
        if "MAX(pick_number)" in query:
            return (10,)  # 10 teams * 1 pick/team = 10 total picks, all seen
        return None

    mock_cursor.fetchall.side_effect = _fetchall
    mock_cursor.fetchone.side_effect = _fetchone

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"listener_state": "Complete"}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Succeeded"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["draft_state"] == "complete"
    assert body["pipeline_health"] == "healthy"
    assert body["on_the_clock_team_id"] is None
    assert body["listener_state"] == "Complete"


def test_get_draft_status_listener_state_stays_complete_once_the_job_is_gone(client):
    """Regression for a review comment on #82's first pass: unlike a stale "Active"/
    "Failed" from an old crashed run, "Complete" describes a real, permanent fact - the
    listener Job being gone (reaped/GC'd after Succeeded) must not fall it back to
    "Unknown" the way every other explicit listener_state does."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"listener_state": "Complete"}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener(None):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["listener_state"] == "Complete"


def test_get_draft_status_waiting_before_the_first_pick(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["draft_state"] == "waiting"


def test_get_draft_status_includes_connector_health_when_present(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None

    health_snapshot = {"ws_connected": False, "pick_silence_ms": 9000, "picks_known": 42}
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}
    mock_redis.get.return_value = json.dumps(health_snapshot)

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["connector_health"] == health_snapshot
    # assert_any_call, not assert_called_with: _build_status also reads
    # draft:{league_id}:selecting/:autodraft now, so draft:L1:health is no longer necessarily
    # the last .get() call.
    mock_redis.get.assert_any_call("draft:L1:health")


def test_get_draft_status_connector_health_is_none_when_key_missing(client):
    """Never written yet, or its 1h TTL expired - a normal state, not a Redis failure, so
    pipeline_health must stay whatever the listener's own signals say."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"last_processed_pick_at": "1000"}
    mock_redis.get.return_value = None

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["connector_health"] is None
    assert body["pipeline_health"] == "healthy"


def test_get_draft_status_connector_health_is_none_when_malformed(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}
    mock_redis.get.return_value = "not-json"

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         _listener("Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["connector_health"] is None


def test_get_draft_status_connector_health_failure_does_not_affect_pipeline_health(client):
    """The connector and the listener fail independently (#81) - a broken read of the
    connector's own heartbeat must never flip pipeline_health, which is the listener's
    signal."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"last_processed_pick_at": "1000"}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft._connector_health", side_effect=Exception("boom")), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["connector_health"] is None
    assert body["pipeline_health"] == "healthy"


# ---------------------------------------------------------------------------
# POST /v1/draft/pause, /v1/draft/resume
# ---------------------------------------------------------------------------
def test_pause_draft_with_explicit_pick_number(client):
    mock_redis = MagicMock()
    with patch("src.api.draft._redis_client", return_value=mock_redis):
        response = client.post("/v1/draft/pause", json={"league_id": "L1", "pick_number": 5})

    assert response.status_code == 200
    assert response.json() == {"status": "paused", "league_id": "L1", "pick_number": 5}
    mock_redis.set.assert_called_once_with("draft:L1:pause:5", "1")


def test_pause_draft_resolves_pick_number_from_board(client):
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"pick_number": "9"}
    with patch("src.api.draft._redis_client", return_value=mock_redis):
        response = client.post("/v1/draft/pause", json={"league_id": "L1"})

    assert response.status_code == 200
    assert response.json()["pick_number"] == 9
    mock_redis.set.assert_called_once_with("draft:L1:pause:9", "1")


def test_pause_draft_no_board_and_no_pick_number(client):
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}
    with patch("src.api.draft._redis_client", return_value=mock_redis):
        response = client.post("/v1/draft/pause", json={"league_id": "L1"})

    assert response.status_code == 400


def test_resume_draft_deletes_pause_key_and_republishes(client):
    """Deleting the pause key alone does nothing - draft_matrix.py's wait_for_pause_or_timeout
    already returned False and hold_and_publish already exited in the (separate) listener
    process. Resume must recompute a fresh recommendation and republish it directly."""
    mock_redis = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("7", 2026, "practice")
    target = {"player_id": "p2", "name": "Best Back", "position": "RB", "vorp_score": 10.0, "team": "KC"}

    with patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft.compute_draft_recommendation", return_value=target) as mock_compute, \
         patch("src.api.draft.publish_draft_target") as mock_publish:
        response = client.post("/v1/draft/resume", json={"league_id": "L1", "pick_number": 5})

    assert response.status_code == 200
    body = response.json()
    assert body["published"] is True
    assert body["player_id"] == "p2"
    mock_redis.delete.assert_called_once_with("draft:L1:pause:5")
    mock_compute.assert_called_once_with(mock_cursor, "L1", 2026, 1, "7")
    # mode must be forwarded explicitly: this handler runs in the gateway process, which has
    # no DRAFT_MODE env set, so publish_draft_target's own env-var default would otherwise
    # always resolve to "live" here even for a practice league's resume.
    mock_publish.assert_called_once_with("7", target, mode="practice")


def test_resume_draft_no_feasible_recommendation(client):
    mock_redis = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("7", 2026, "practice")

    with patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft.compute_draft_recommendation", return_value=None), \
         patch("src.api.draft.publish_draft_target") as mock_publish:
        response = client.post("/v1/draft/resume", json={"league_id": "L1", "pick_number": 5})

    assert response.status_code == 200
    assert response.json()["published"] is False
    mock_publish.assert_not_called()


def test_resume_draft_requires_config_stored_first(client):
    mock_redis = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    with patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.post("/v1/draft/resume", json={"league_id": "L1", "pick_number": 5})

    assert response.status_code == 400


# ---------------------------------------------------------------------------
# POST /v1/draft/auto-publish
# ---------------------------------------------------------------------------
def test_auto_publish_toggle_endpoint_disables_and_reenables(client):
    """#108/#109: disabling records disabled_by='manual' (the only path allowed to write
    that reason); re-enabling clears it back to null."""
    with patch("src.api.draft._set_auto_publish_enabled") as mock_set:
        response = client.post("/v1/draft/auto-publish", json={"league_id": "L1", "enabled": False})

    assert response.status_code == 200
    body = response.json()
    assert body == {"status": "updated", "league_id": "L1", "auto_publish_enabled": False, "auto_publish_disabled_by": "manual"}
    mock_set.assert_called_once_with("L1", False, disabled_by="manual")

    with patch("src.api.draft._set_auto_publish_enabled") as mock_set:
        response = client.post("/v1/draft/auto-publish", json={"league_id": "L1", "enabled": True})

    assert response.status_code == 200
    body = response.json()
    assert body == {"status": "updated", "league_id": "L1", "auto_publish_enabled": True, "auto_publish_disabled_by": None}
    mock_set.assert_called_once_with("L1", True, disabled_by=None)


def test_auto_publish_toggle_endpoint_surfaces_db_failure(client):
    with patch("src.api.draft._set_auto_publish_enabled", side_effect=Exception("connection refused")):
        response = client.post("/v1/draft/auto-publish", json={"league_id": "L1", "enabled": False})

    assert response.status_code == 500


# ---------------------------------------------------------------------------
# GET /v1/draft/stream (SSE generator, driven directly via asyncio.run - not through a
# live ASGI transport, whose disconnect detection isn't reliable to exercise in a sync
# test client, and this repo has no pytest-asyncio/anyio plugin configured)
# ---------------------------------------------------------------------------
async def _collect_events(request, league_id, season, limit):
    from src.api.draft import _draft_status_event_generator
    return [event async for event in _draft_status_event_generator(request, league_id, season, limit)]


def test_stream_draft_status_emits_first_event_then_stops_on_disconnect():
    import asyncio as asyncio_module
    import json as json_module

    payload = {
        "league_id": "L1", "season": 2026, "mode": "live", "job_state": "Active", "recommendation": {},
        "last_clicker_action": None, "recent_picks": [], "team_rosters": {}, "on_the_clock_team_id": None,
        "pipeline_health": "healthy", "listener_state": "Starting", "listener_last_event_at": None, "listener_error": None,
    }
    mock_request = MagicMock()
    # Connected for the first iteration, disconnected from the second check onward.
    mock_request.is_disconnected = AsyncMock(side_effect=[False, True])

    with patch("src.api.draft._build_status", return_value=payload), \
         patch("src.api.draft.asyncio.sleep", new_callable=AsyncMock):
        events = asyncio_module.run(_collect_events(mock_request, "L1", 2026, 10))

    assert len(events) == 2
    emitted = json_module.loads(events[0][len("data: "):].strip())
    assert emitted["league_id"] == "L1"
    assert emitted["job_state"] == "Active"
    assert emitted["team_rosters"] == {}
    assert events[1].startswith("event: heartbeat\n")


def test_stream_draft_status_skips_duplicate_payloads():
    import asyncio as asyncio_module

    payload = {"league_id": "L1", "season": 2026, "job_state": "Active"}
    mock_request = MagicMock()
    mock_request.is_disconnected = AsyncMock(side_effect=[False, False, True])

    with patch("src.api.draft._build_status", return_value=payload), \
         patch("src.api.draft.asyncio.sleep", new_callable=AsyncMock):
        events = asyncio_module.run(_collect_events(mock_request, "L1", 2026, 10))

    assert len(events) == 3
    assert events[1].startswith("event: heartbeat\n")
    assert events[2].startswith("event: heartbeat\n")


def test_stream_draft_status_carries_draft_state_through(client):
    """#82: _draft_status_event_generator just re-serializes _build_status's own dict, so
    draft_state needs no separate SSE-side plumbing - this proves that's actually true."""
    import asyncio as asyncio_module
    import json as json_module

    payload = {"league_id": "L1", "season": 2026, "draft_state": "complete"}
    mock_request = MagicMock()
    mock_request.is_disconnected = AsyncMock(side_effect=[False, True])

    with patch("src.api.draft._build_status", return_value=payload), \
         patch("src.api.draft.asyncio.sleep", new_callable=AsyncMock):
        events = asyncio_module.run(_collect_events(mock_request, "L1", 2026, 10))

    emitted = json_module.loads(events[0][len("data: "):].strip())
    assert emitted["draft_state"] == "complete"


# ---------------------------------------------------------------------------
# POST/GET/DELETE /v1/draft/practice/sync-job
# ---------------------------------------------------------------------------
def test_start_practice_sync_job_rejects_non_https_url(client):
    response = client.post("/v1/draft/practice/sync-job", json={"room_url": "http://fantasy.espn.com/football/draft"})
    assert response.status_code == 422


def test_redis_client_delegates_to_the_shared_database_client():
    """Timeout-bounding itself is tested once, at the source, in test_database.py - this
    only guards that draft.py's _redis_client hasn't drifted back to its own ad hoc
    redis.Redis(...) call."""
    from src.api.draft import _redis_client

    with patch("src.api.draft.get_redis_client") as mock_get_client:
        result = _redis_client()

    mock_get_client.assert_called_once_with()
    assert result is mock_get_client.return_value


def test_practice_start_lock_renews_the_lease_while_the_start_is_running():
    from src.api.draft import _practice_start_lock

    class _FakeEvent:
        def __init__(self):
            self._set = False
            self._wait_calls = 0

        def wait(self, _timeout):
            self._wait_calls += 1
            return self._set or self._wait_calls > 1

        def set(self):
            self._set = True

    class _InlineThread:
        def __init__(self, target, daemon):
            self._target = target
            self.daemon = daemon

        def start(self):
            self._target()

        def join(self, timeout=None):
            return None

    mock_redis = MagicMock()
    mock_redis.set.return_value = True
    with patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft.threading.Event", _FakeEvent), \
         patch("src.api.draft.threading.Thread", _InlineThread):
        with _practice_start_lock():
            pass

    assert mock_redis.eval.call_count == 2
    renew_call, release_call = mock_redis.eval.call_args_list
    assert "expire" in renew_call.args[0]
    assert "del" in release_call.args[0]


def test_clear_league_draft_state_escapes_glob_metacharacters_in_the_league_id():
    """league_id is an arbitrary string from DraftConfigPayload. Unescaped, a value of `*`
    would scan draft:*:pause:* and delete every league's pause flags, a live draft's included."""
    from src.api.draft import _clear_league_draft_state

    mock_redis = MagicMock()
    mock_redis.scan_iter.return_value = iter([])

    with patch("src.api.draft._redis_client", return_value=mock_redis):
        _clear_league_draft_state("*")

    assert mock_redis.scan_iter.call_args.kwargs["match"] == "draft:\\*:pause:*"


# ---------------------------------------------------------------------------
# _on_the_clock_team_id
#
# team_count resolution itself (league_settings preferred, draft_config practice-mode
# fallback) now lives solely in _resolve_draft_size (#82), which mirrors the same order
# draft_matrix.league_team_count uses - see test_get_draft_status_aggregates_expected_shape
# and the other GET /v1/draft/status tests above for coverage of that resolution, plus
# test_league_team_count_prefers_league_settings / test_league_team_count_falls_back_to_
# draft_config in tests/test_draft_matrix.py for the equivalent draft_matrix-side logic.
# These tests cover only what _on_the_clock_team_id itself still does with an
# already-resolved team_count/total_picks/picks_seen.
# ---------------------------------------------------------------------------

def _on_the_clock_cursor(slot_rows):
    cursor = MagicMock()
    cursor.fetchall.return_value = slot_rows
    return cursor


def test_on_the_clock_team_id_computes_next_slot():
    from src.api.draft import _on_the_clock_team_id

    cursor = _on_the_clock_cursor(slot_rows=[("a", 1), ("b", 2), ("c", 3)])

    # Pick 4 of a 3-team draft wraps into round 2, which reverses: slot 3 picks again.
    assert _on_the_clock_team_id(cursor, "L1", 2026, team_count=3, total_picks=48, picks_seen=3) == "c"


def test_on_the_clock_team_id_is_none_when_team_count_unknown():
    from src.api.draft import _on_the_clock_team_id

    cursor = _on_the_clock_cursor(slot_rows=[("a", 1)])

    assert _on_the_clock_team_id(cursor, "L1", 2026, team_count=None, total_picks=None, picks_seen=0) is None
    # team_count unknown short-circuits before any slot-map query.
    cursor.execute.assert_not_called()


def test_on_the_clock_team_id_is_none_without_a_slot_map():
    """Round 2+ before every team has made a round-1 pick - see compute_on_the_clock_team."""
    from src.api.draft import _on_the_clock_team_id

    cursor = _on_the_clock_cursor(slot_rows=[])

    assert _on_the_clock_team_id(cursor, "L1", 2026, team_count=3, total_picks=48, picks_seen=0) is None


def test_on_the_clock_team_id_is_none_once_draft_is_complete():
    """#82: there is no pick after the last one - the old behavior kept computing a
    (meaningless) wrapped slot instead of recognizing the draft was over."""
    from src.api.draft import _on_the_clock_team_id

    cursor = _on_the_clock_cursor(slot_rows=[("a", 1), ("b", 2), ("c", 3)])

    assert _on_the_clock_team_id(cursor, "L1", 2026, team_count=3, total_picks=48, picks_seen=48) is None
    # Complete short-circuits before any slot-map query too.
    cursor.execute.assert_not_called()


def test_recent_picks_survive_a_null_internal_player_id(client):
    from src.api.draft import DraftPickSummary

    """K and D/ST never resolve to an internal id. The response model must allow that or
    FastAPI 500s on exactly the unnamed picks this is meant to surface."""
    summary = DraftPickSummary(pick_number=1, round=1, team_id="3", player_id=None,
                               espn_player_id="-16033", name=None, position=None)
    assert summary.player_id is None
    assert summary.espn_player_id == "-16033"


# ---------------------------------------------------------------------------
# GET /v1/draft/status: ESPN ownership
# ---------------------------------------------------------------------------
def test_get_draft_status_attaches_espn_ownership_to_picks_and_candidates(client):
    from src.schemas.espn_ownership import EspnOwnership

    espn = EspnOwnership(pos_rank=3, percent_owned=95.0, percent_started=85.0, percent_bench=10.0, percent_change=1.0)
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    candidates = [
        {"player_id": "p2", "name": "Best Back", "position": "RB", "vorp_score": 10.0, "team": "KC"},
        {"player_id": "p3", "name": "Other", "position": "WR", "vorp_score": 9.0, "team": "SF"},
    ]
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"top_candidates": json.dumps(candidates), "last_processed_pick_at": "1000"}
    seen = []

    def fake_loader(cursor, ids, season, week):
        seen.append((sorted(i for i in ids if i), season))
        return {"p2": espn}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft.load_espn_ownership", fake_loader), \
         _listener("Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    top = response.json()["recommendation"]["top_candidates"]
    assert top[0]["espn"]["pos_rank"] == 3 and top[0]["espn"]["percent_bench"] == 10.0
    assert top[1]["espn"] is None
    assert seen == [(["p2", "p3"], 2026)]
