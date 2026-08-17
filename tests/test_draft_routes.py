from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubernetes.config.config_exception import ConfigException


@contextmanager
def _fake_db_cursor(cursor=None):
    yield cursor or MagicMock()


def _k8s_patches():
    """Common patch set so no real kubeconfig/in-cluster config is ever touched."""
    return (
        patch("src.api.draft.config.load_incluster_config", side_effect=ConfigException("no cluster")),
        patch("src.api.draft.config.load_kube_config"),
    )


# ---------------------------------------------------------------------------
# POST /v1/draft/config
# ---------------------------------------------------------------------------
def test_set_draft_config_upserts(client):
    mock_cursor = MagicMock()
    payload = {"league_id": "L1", "team_id": "7", "season": 2026}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)):
        response = client.post("/v1/draft/config", json=payload)

    assert response.status_code == 200
    assert response.json() == {"status": "stored", "league_id": "L1", "team_id": "7", "season": 2026}
    query, params = mock_cursor.execute.call_args[0]
    assert "ON CONFLICT (league_id) DO UPDATE" in query
    assert params == ("L1", "7", 2026)


def test_set_draft_config_validates_payload(client):
    response = client.post("/v1/draft/config", json={"league_id": "L1"})
    assert response.status_code == 422


def test_set_draft_config_db_failure(client):
    @contextmanager
    def _broken_cursor():
        raise Exception("connection refused")
        yield  # pragma: no cover

    with patch("src.api.draft.get_db_cursor", _broken_cursor):
        response = client.post("/v1/draft/config", json={"league_id": "L1", "team_id": "7", "season": 2026})

    assert response.status_code == 500


# ---------------------------------------------------------------------------
# POST /v1/draft/start
# ---------------------------------------------------------------------------
def test_start_draft_sync_creates_job(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026)
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft.DRAFT_SYNC_IMAGE", "ghcr.io/example/fantasy-core-pipeline:test"), \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["league_id"] == "L1"
    assert body["team_id"] == "7"
    assert body["season"] == 2026

    mock_batch_api.create_namespaced_job.assert_called_once()
    namespace, job = mock_batch_api.create_namespaced_job.call_args[0]
    command = job.spec.template.spec.containers[0].command
    assert command == [
        "python", "-m", "src.optimization.draft_matrix",
        "--league-id", "L1", "--team-id", "7", "--season", "2026",
    ]


def test_start_draft_sync_requires_image_env_var(client):
    mock_cursor = MagicMock()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft.DRAFT_SYNC_IMAGE", None):
        response = client.post("/v1/draft/start")

    assert response.status_code == 500
    assert "DRAFT_SYNC_IMAGE" in response.json()["detail"]


def test_start_draft_sync_requires_config_stored_first(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft.DRAFT_SYNC_IMAGE", "ghcr.io/example/fantasy-core-pipeline:test"):
        response = client.post("/v1/draft/start")

    assert response.status_code == 400
    assert "draft config" in response.json()["detail"].lower()


def test_start_draft_sync_k8s_failure(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026)
    mock_batch_api = MagicMock()
    mock_batch_api.create_namespaced_job.side_effect = Exception("apiserver unreachable")

    incluster_patch, kubeconfig_patch = _k8s_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft.DRAFT_SYNC_IMAGE", "ghcr.io/example/fantasy-core-pipeline:test"), \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 500


# ---------------------------------------------------------------------------
# GET /v1/draft/status
# ---------------------------------------------------------------------------
def test_get_draft_status_aggregates_expected_shape(client):
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    def _fetchall():
        if "draft_picks" in mock_cursor.last_query and "SELECT pick_number" in mock_cursor.last_query:
            return [(2, 1, "3", "p1"), (1, 1, "9", "p0")]
        if "draft_slots" in mock_cursor.last_query:
            return [("9", 1), ("3", 2)]
        return []

    def _fetchone():
        if "team_count" in mock_cursor.last_query:
            return (10,)
        if "MAX(pick_number)" in mock_cursor.last_query:
            return (2,)
        return None

    mock_cursor.fetchall.side_effect = _fetchall
    mock_cursor.fetchone.side_effect = _fetchone

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"recommended_player_id": "p2", "recommended_name": "Best Back", "pick_number": "3"}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft._job_state", return_value="Active"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["job_state"] == "Active"
    assert body["last_clicker_action"] is None
    assert body["recommendation"]["recommended_player_id"] == "p2"
    assert len(body["recent_picks"]) == 2
    assert body["pipeline_health"] == "healthy"


def test_get_draft_status_degraded_when_job_not_active(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft._job_state", return_value="Unknown"):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["pipeline_health"] == "degraded"
    assert response.json()["on_the_clock_team_id"] is None


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


def test_resume_draft_deletes_pause_key(client):
    mock_redis = MagicMock()
    with patch("src.api.draft._redis_client", return_value=mock_redis):
        response = client.post("/v1/draft/resume", json={"league_id": "L1", "pick_number": 5})

    assert response.status_code == 200
    mock_redis.delete.assert_called_once_with("draft:L1:pause:5")


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
        "league_id": "L1", "season": 2026, "job_state": "Active", "recommendation": {},
        "last_clicker_action": None, "recent_picks": [], "on_the_clock_team_id": None,
        "pipeline_health": "healthy",
    }
    mock_request = MagicMock()
    # Connected for the first iteration, disconnected from the second check onward.
    mock_request.is_disconnected = AsyncMock(side_effect=[False, True])

    with patch("src.api.draft._build_status", return_value=payload), \
         patch("src.api.draft.asyncio.sleep", new_callable=AsyncMock):
        events = asyncio_module.run(_collect_events(mock_request, "L1", 2026, 10))

    assert len(events) == 1
    emitted = json_module.loads(events[0][len("data: "):].strip())
    assert emitted["league_id"] == "L1"
    assert emitted["job_state"] == "Active"


def test_stream_draft_status_skips_duplicate_payloads():
    import asyncio as asyncio_module

    payload = {"league_id": "L1", "season": 2026, "job_state": "Active"}
    mock_request = MagicMock()
    mock_request.is_disconnected = AsyncMock(side_effect=[False, False, True])

    with patch("src.api.draft._build_status", return_value=payload), \
         patch("src.api.draft.asyncio.sleep", new_callable=AsyncMock):
        events = asyncio_module.run(_collect_events(mock_request, "L1", 2026, 10))

    assert len(events) == 1
