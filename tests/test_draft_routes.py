import json
import os
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubernetes import client as k8s_client
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
    assert response.json() == {"status": "stored", "league_id": "L1", "team_id": "7", "season": 2026, "mode": "live"}
    query, params = mock_cursor.execute.call_args[0]
    assert "ON CONFLICT (league_id) DO UPDATE" in query
    assert params == ("L1", "7", 2026, "live")


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
    assert params == ("MOCK123", "8", 2026, "practice")


def test_set_draft_config_practice_mode_skips_check_when_espn_league_id_unset(client):
    mock_cursor = MagicMock()
    payload = {"league_id": "MOCK123", "team_id": "8", "season": 2026, "mode": "practice"}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft.ESPN_LEAGUE_ID", None):
        response = client.post("/v1/draft/config", json=payload)

    assert response.status_code == 200
    assert response.json()["mode"] == "practice"


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
def _env_config_patches():
    return (
        patch("src.api.draft.DRAFT_SYNC_IMAGE", "ghcr.io/example/fantasy-core-pipeline:test"),
        patch("src.api.draft.DRAFT_SYNC_ENV_SECRET", "draft-sync-secrets"),
        patch("src.api.draft.DRAFT_SYNC_ENV_CONFIGMAP", "draft-sync-config"),
    )


def test_start_draft_sync_creates_job(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "live")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listener_on_other_league", return_value=None), \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["league_id"] == "L1"
    assert body["team_id"] == "7"
    assert body["season"] == 2026
    assert body["mode"] == "live"
    assert body["already_running"] is False

    mock_batch_api.create_namespaced_job.assert_called_once()
    namespace, job = mock_batch_api.create_namespaced_job.call_args[0]
    assert job.metadata.name.startswith("draft-sync-l1-")
    assert job.metadata.labels["mode"] == "live"
    container = job.spec.template.spec.containers[0]
    assert container.command == [
        "python", "-m", "src.optimization.draft_matrix",
        "--league-id", "L1", "--team-id", "7", "--season", "2026",
    ]
    assert container.env_from[0].config_map_ref.name == "draft-sync-config"
    assert container.env_from[1].secret_ref.name == "draft-sync-secrets"


def test_start_draft_sync_job_gets_db_and_connection_env(client):
    """draft_matrix.py imports src.database, which opens a connection pool at import time -
    without DB_PASSWORD the listener Pod crashes before consuming a single pick, so the Job
    must carry an explicit secretKeyRef (envFrom cannot rename the Secret's own key) plus the
    non-secret connection settings this API process is using."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "live")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    inherited = {
        "DB_HOST": "fantasy-db.default.svc.cluster.local",
        "DB_NAME": "fantasy_football",
        "DB_USER": "bot_admin",
        "REDIS_HOST": "fantasy-cache.default.svc.cluster.local",
        "REDIS_PORT": "6379",
        "CORE_API_BASE_URL": "http://fantasy-core-pipeline.default.svc.cluster.local:8000",
    }
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listener_on_other_league", return_value=None), \
         patch.dict(os.environ, inherited, clear=False), \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 202
    _, job = mock_batch_api.create_namespaced_job.call_args[0]
    env = {e.name: e for e in job.spec.template.spec.containers[0].env}

    db_password = env["DB_PASSWORD"]
    assert db_password.value is None
    assert db_password.value_from.secret_key_ref.name == "database-credentials"
    assert db_password.value_from.secret_key_ref.key == "postgres-password"

    for name, value in inherited.items():
        assert env[name].value == value


def test_start_draft_sync_job_omits_unset_inherited_env(client):
    """An unset variable must not become an empty-string env var - that would override the
    module-level default in src.database/draft_matrix with a value that cannot connect."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "live")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listener_on_other_league", return_value=None), \
         patch.dict(os.environ, {}, clear=True), \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 202
    _, job = mock_batch_api.create_namespaced_job.call_args[0]
    env_names = {e.name for e in job.spec.template.spec.containers[0].env}
    assert env_names == {"DB_PASSWORD"}


def test_start_draft_sync_creates_practice_job_with_prefixed_name(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("MOCK123", "8", 2026, "practice")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listener_on_other_league", return_value=None), \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 202
    assert response.json()["mode"] == "practice"

    namespace, job = mock_batch_api.create_namespaced_job.call_args[0]
    # Job *names* must be lowercase RFC 1123 DNS subdomains; the league-id label and the
    # CLI arg passed to draft_matrix.py keep the original, unsanitized league_id.
    assert job.metadata.name.startswith("draft-sync-practice-mock123-")
    assert job.metadata.labels["mode"] == "practice"
    assert job.metadata.labels["league-id"] == "MOCK123"
    assert job.spec.template.metadata.labels["mode"] == "practice"
    container = job.spec.template.spec.containers[0]
    assert "MOCK123" in container.command


def test_start_draft_sync_selector_picks_named_league(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("MOCK123", "8", 2026, "practice")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listener_on_other_league", return_value=None), \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start", params={"league_id": "MOCK123"})

    assert response.status_code == 202
    query, params = mock_cursor.execute.call_args[0]
    assert "WHERE league_id = %s" in query
    assert params == ("MOCK123",)


def test_start_draft_sync_no_selector_falls_back_to_most_recent(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "live")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listener_on_other_league", return_value=None), \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 202
    query = mock_cursor.execute.call_args[0][0]
    assert "ORDER BY updated_at DESC LIMIT 1" in query


def test_start_draft_sync_requires_image_env_var(client):
    mock_cursor = MagicMock()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft.DRAFT_SYNC_IMAGE", None):
        response = client.post("/v1/draft/start")

    assert response.status_code == 500
    assert "DRAFT_SYNC_IMAGE" in response.json()["detail"]


def test_start_draft_sync_requires_env_config_and_secret(client):
    mock_cursor = MagicMock()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft.DRAFT_SYNC_IMAGE", "ghcr.io/example/fantasy-core-pipeline:test"), \
         patch("src.api.draft.DRAFT_SYNC_ENV_SECRET", None), \
         patch("src.api.draft.DRAFT_SYNC_ENV_CONFIGMAP", None):
        response = client.post("/v1/draft/start")

    assert response.status_code == 500
    assert "DRAFT_SYNC_ENV_SECRET" in response.json()["detail"]


def test_start_draft_sync_requires_config_stored_first(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None

    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         image_patch, secret_patch, configmap_patch:
        response = client.post("/v1/draft/start")

    assert response.status_code == 400
    assert "draft config" in response.json()["detail"].lower()


def test_start_draft_sync_is_idempotent_when_same_league_already_active(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "live")

    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Active"), \
         image_patch, secret_patch, configmap_patch:
        response = client.post("/v1/draft/start")

    assert response.status_code == 202
    assert response.json()["already_running"] is True
    assert response.json()["status"] == "accepted"


def test_start_draft_sync_conflicts_when_another_league_listener_is_active(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "live")

    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listener_on_other_league", return_value="L2"), \
         image_patch, secret_patch, configmap_patch:
        response = client.post("/v1/draft/start")

    assert response.status_code == 409
    assert "L2" in response.json()["detail"]


def test_start_draft_sync_k8s_failure(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "live")
    mock_batch_api = MagicMock()
    mock_batch_api.create_namespaced_job.side_effect = Exception("apiserver unreachable")

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listener_on_other_league", return_value=None), \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 500


# ---------------------------------------------------------------------------
# POST /v1/draft/stop
# ---------------------------------------------------------------------------
def test_stop_draft_deletes_job_and_clears_redis(client):
    mock_batch_api = MagicMock()
    fake_job = MagicMock()
    fake_job.metadata.name = "draft-sync-L1-1234"
    # First call (the initial listing) finds the Job; the termination-poll call after
    # deletion finds it gone.
    mock_batch_api.list_namespaced_job.side_effect = [
        MagicMock(items=[fake_job]),
        MagicMock(items=[]),
    ]

    mock_redis = MagicMock()
    mock_redis.scan_iter.return_value = ["draft:L1:pause:3"]
    mock_redis.delete.return_value = 2

    incluster_patch, kubeconfig_patch = _k8s_patches()
    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft.time.sleep"):
        response = client.post("/v1/draft/stop", params={"league_id": "L1"})

    assert response.status_code == 200
    body = response.json()
    assert body == {"status": "stopped", "league_id": "L1", "job_deleted": True, "redis_keys_cleared": 2}

    mock_batch_api.delete_namespaced_job.assert_called_once_with(
        name="draft-sync-L1-1234", namespace="default", propagation_policy="Foreground",
    )
    for call in mock_batch_api.list_namespaced_job.call_args_list:
        assert call.kwargs["label_selector"] == "app=draft-sync,league-id=L1"
    mock_redis.delete.assert_called_once_with("draft:L1:board", "draft:L1:pause:3")


def test_stop_draft_no_job_still_clears_redis_and_returns_200(client):
    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value.items = []

    mock_redis = MagicMock()
    mock_redis.scan_iter.return_value = []
    mock_redis.delete.return_value = 1

    incluster_patch, kubeconfig_patch = _k8s_patches()
    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.api.draft._redis_client", return_value=mock_redis):
        response = client.post("/v1/draft/stop", params={"league_id": "L1"})

    assert response.status_code == 200
    body = response.json()
    assert body["job_deleted"] is False
    mock_batch_api.delete_namespaced_job.assert_not_called()


def test_stop_draft_times_out_waiting_for_termination(client):
    """A Job that never disappears (wedged finalizer, apiserver hiccup) must not be treated
    as a successful stop - Redis must stay untouched so a later /stop retry can still find
    and clear it."""
    mock_batch_api = MagicMock()
    fake_job = MagicMock()
    fake_job.metadata.name = "draft-sync-L1-1234"
    mock_batch_api.list_namespaced_job.return_value = MagicMock(items=[fake_job])

    mock_redis = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft.time.sleep"):
        response = client.post("/v1/draft/stop", params={"league_id": "L1"})

    assert response.status_code == 504
    mock_redis.delete.assert_not_called()


def test_stop_draft_k8s_failure_returns_500(client):
    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.side_effect = Exception("apiserver unreachable")

    incluster_patch, kubeconfig_patch = _k8s_patches()
    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/stop", params={"league_id": "L1"})

    assert response.status_code == 500


def test_stop_draft_redis_failure_returns_500(client):
    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value.items = []

    incluster_patch, kubeconfig_patch = _k8s_patches()
    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.api.draft._redis_client", side_effect=Exception("redis unreachable")):
        response = client.post("/v1/draft/stop", params={"league_id": "L1"})

    assert response.status_code == 500


def test_stop_draft_does_not_touch_draft_config(client):
    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value.items = []
    mock_redis = MagicMock()
    mock_redis.scan_iter.return_value = []

    def _raising_db_cursor():
        raise AssertionError("stop_draft must not touch draft_config")

    incluster_patch, kubeconfig_patch = _k8s_patches()
    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft.get_db_cursor", _raising_db_cursor):
        response = client.post("/v1/draft/stop", params={"league_id": "L1"})

    assert response.status_code == 200


def test_job_state_scopes_query_to_draft_sync_app_label():
    from src.api.draft import _job_state

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value.items = []

    with patch("src.api.draft._batch_v1_api", return_value=mock_batch_api):
        state = _job_state("L1")

    assert state == "Unknown"
    assert mock_batch_api.list_namespaced_job.call_args.kwargs["label_selector"] == "app=draft-sync,league-id=L1"


def test_listener_status_reports_failed_when_k8s_job_failed_even_if_redis_says_active():
    from src.api.draft import _listener_status

    listener_state, listener_last_event_at, listener_error = _listener_status(
        "Failed",
        {"listener_state": "Active", "listener_last_event_at": "1000", "listener_error": "boom"},
    )

    assert listener_state == "Failed"
    assert listener_last_event_at == "1970-01-01T00:16:40Z"
    assert listener_error == "boom"


# ---------------------------------------------------------------------------
# GET /v1/draft/status
# ---------------------------------------------------------------------------
def test_get_draft_status_aggregates_expected_shape(client):
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    def _fetchall():
        if "draft_picks" in mock_cursor.last_query and "SELECT d.pick_number" in mock_cursor.last_query:
            return [(2, 1, "3", "p1", "Player One", "RB"), (1, 1, "9", "p0", "Player Zero", "WR")]
        if "draft_slots" in mock_cursor.last_query:
            return [("9", 1), ("3", 2)]
        if "SELECT transaction_type" in mock_cursor.last_query:
            return [("draft_sync", "draft_sync", "click failed", "2026-08-21T00:00:00Z")]
        return []

    def _fetchone():
        if "SELECT mode FROM draft_config" in mock_cursor.last_query:
            return ("practice",)
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
    }

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft._job_state", return_value="Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["job_state"] == "Active"
    assert body["mode"] == "practice"
    assert body["last_clicker_action"] == "Drafted Breece Hall at 14:32:05"
    assert body["recommendation"]["recommended_player_id"] == "p2"
    assert len(body["recent_picks"]) == 2
    assert body["recent_picks"][0]["name"] == "Player One"
    assert body["recent_picks"][0]["position"] == "RB"
    assert body["pipeline_health"] == "healthy"
    assert body["recent_errors"][0]["transaction_type"] == "draft_sync"
    assert body["listener_state"] == "Active"
    assert body["listener_last_event_at"] == "1970-01-01T00:16:40Z"
    assert body["listener_error"] is None


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
        return None

    mock_cursor.fetchall.side_effect = _fetchall
    mock_cursor.fetchone.side_effect = _fetchone

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft._job_state", return_value="Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["last_clicker_action"] == "Failed to draft Breece Hall: confirmation timed out"


def test_get_draft_status_defaults_mode_to_live_when_config_row_missing(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft._job_state", return_value="Active"):
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
         patch("src.api.draft._job_state", return_value="Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["recommendation"]["top_candidates"] == candidates


def test_get_draft_status_handles_malformed_top_candidates_json(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_cursor.fetchone.return_value = None

    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {"recommended_player_id": "p2", "top_candidates": "not-json", "last_processed_pick_at": "1000"}

    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._redis_client", return_value=mock_redis), \
         patch("src.api.draft._job_state", return_value="Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["recommendation"]["top_candidates"] == []


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
         patch("src.api.draft._job_state", return_value="Active"), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["pipeline_health"] == "degraded"
    assert body["recent_picks"] == []
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
         patch("src.api.draft._job_state", return_value="Active"):
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
         patch("src.api.draft._job_state", return_value="Active"), \
         patch("src.api.draft.time.time", return_value=2000):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    assert response.json()["pipeline_health"] == "degraded"


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
    mock_cursor.fetchone.return_value = ("7", 2026)
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
    mock_publish.assert_called_once_with("7", target)


def test_resume_draft_no_feasible_recommendation(client):
    mock_redis = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("7", 2026)

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
        "last_clicker_action": None, "recent_picks": [], "on_the_clock_team_id": None,
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


# ---------------------------------------------------------------------------
# POST/GET/DELETE /v1/draft/practice/sync-job
# ---------------------------------------------------------------------------
def test_start_practice_sync_job_creates_job(client):
    mock_batch_api = MagicMock()
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with patch("src.api.draft.BROWSER_WORKER_IMAGE", "ghcr.io/example/fantasy-browser-worker:test"), \
         patch("src.api.draft._practice_sync_job_state", return_value="Unknown"), \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/practice/sync-job", json={"room_url": "https://fantasy.espn.com/football/draft?leagueId=123"})

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "accepted"
    assert body["job_name"].startswith("practice-draft-sync-")

    mock_batch_api.create_namespaced_job.assert_called_once()
    namespace, job = mock_batch_api.create_namespaced_job.call_args[0]
    assert namespace == "default"
    assert job.metadata.labels == {"app": "practice-draft-sync", "league-id": "123"}
    assert job.spec.template.metadata.labels == {"app": "practice-draft-sync", "league-id": "123"}
    container = job.spec.template.spec.containers[0]
    assert container.image == "ghcr.io/example/fantasy-browser-worker:test"
    assert container.command == ["node", "src/bots/draft_sync.js"]
    env_by_name = {e.name: e for e in container.env}
    assert env_by_name["DRAFT_ROOM_URL"].value == "https://fantasy.espn.com/football/draft?leagueId=123"
    assert env_by_name["ESPN_S2"].value_from.secret_key_ref.name == "espn-session-secrets"
    assert env_by_name["ESPN_S2"].value_from.secret_key_ref.key == "espn_s2"
    assert env_by_name["SWID"].value_from.secret_key_ref.key == "swid"
    # Without this the kubelet 401s pulling the private GHCR image and the Pod sits in
    # ImagePullBackOff forever - job_state still reports "Active" the whole time (the Job
    # controller considers it pending, not failed), so this silently looks like it's
    # working from /status alone. Regression coverage for that exact incident.
    assert job.spec.template.spec.image_pull_secrets == [k8s_client.V1LocalObjectReference(name="ghcr-pull-secret")]


def test_start_practice_sync_job_omits_league_label_when_missing(client):
    mock_batch_api = MagicMock()
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with patch("src.api.draft.BROWSER_WORKER_IMAGE", "ghcr.io/example/fantasy-browser-worker:test"), \
         patch("src.api.draft._practice_sync_job_state", return_value="Unknown"), \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/practice/sync-job", json={"room_url": "https://fantasy.espn.com/football/draft"})

    assert response.status_code == 202
    _, job = mock_batch_api.create_namespaced_job.call_args[0]
    assert job.metadata.labels == {"app": "practice-draft-sync"}
    assert job.spec.template.metadata.labels == {"app": "practice-draft-sync"}


def test_start_practice_sync_job_rejects_non_https_url(client):
    response = client.post("/v1/draft/practice/sync-job", json={"room_url": "http://fantasy.espn.com/football/draft"})
    assert response.status_code == 422


def test_start_practice_sync_job_requires_image_env_var(client):
    with patch("src.api.draft.BROWSER_WORKER_IMAGE", None):
        response = client.post("/v1/draft/practice/sync-job", json={"room_url": "https://fantasy.espn.com/football/draft"})

    assert response.status_code == 500
    assert "BROWSER_WORKER_IMAGE" in response.json()["detail"]


def test_start_practice_sync_job_rejects_when_already_active(client):
    with patch("src.api.draft.BROWSER_WORKER_IMAGE", "ghcr.io/example/fantasy-browser-worker:test"), \
         patch("src.api.draft._practice_sync_job_state", return_value="Active"):
        response = client.post("/v1/draft/practice/sync-job", json={"room_url": "https://fantasy.espn.com/football/draft"})

    assert response.status_code == 409
    assert "already Active" in response.json()["detail"]


def test_start_practice_sync_job_k8s_failure(client):
    mock_batch_api = MagicMock()
    mock_batch_api.create_namespaced_job.side_effect = Exception("apiserver unreachable")
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with patch("src.api.draft.BROWSER_WORKER_IMAGE", "ghcr.io/example/fantasy-browser-worker:test"), \
         patch("src.api.draft._practice_sync_job_state", return_value="Unknown"), \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/practice/sync-job", json={"room_url": "https://fantasy.espn.com/football/draft"})

    assert response.status_code == 500


def _mock_practice_job(*, state: str, league_id: str = "123", room_url: str = "https://fantasy.espn.com/football/draft#room-xyz"):
    job = MagicMock()
    job.metadata.labels = {"app": "practice-draft-sync", "league-id": league_id}
    status = MagicMock()
    status.active = 1 if state == "Active" else None
    status.succeeded = 1 if state == "Succeeded" else None
    status.failed = 1 if state == "Failed" else None
    job.status = status
    env_var = MagicMock()
    env_var.name = "DRAFT_ROOM_URL"
    env_var.value = room_url
    container = MagicMock()
    container.env = [env_var]
    pod_spec = MagicMock()
    pod_spec.containers = [container]
    template = MagicMock()
    template.spec = pod_spec
    spec = MagicMock()
    spec.template = template
    job.spec = spec
    return job


def test_get_practice_sync_job_status(client):
    mock_redis = MagicMock()
    mock_redis.exists.return_value = 1
    with patch("src.api.draft._latest_practice_sync_job", return_value=_mock_practice_job(state="Active", league_id="123")), \
         patch("src.api.draft._redis_client", return_value=mock_redis):
        response = client.get("/v1/draft/practice/sync-job/status")

    assert response.status_code == 200
    assert response.json() == {
        "job_state": "Active",
        "pod_active": True,
        "room_connected": True,
        "room_handshake_complete": True,
        "active_league_id": "123",
        "room_identifier": "room-xyz",
    }


def test_get_practice_sync_job_status_room_not_connected(client):
    mock_redis = MagicMock()
    mock_redis.exists.return_value = 0
    with patch("src.api.draft._latest_practice_sync_job", return_value=_mock_practice_job(state="Active", league_id="123")), \
         patch("src.api.draft._redis_client", return_value=mock_redis):
        response = client.get("/v1/draft/practice/sync-job/status")

    assert response.status_code == 200
    assert response.json() == {
        "job_state": "Active",
        "pod_active": True,
        "room_connected": False,
        "room_handshake_complete": False,
        "active_league_id": "123",
        "room_identifier": "room-xyz",
    }


def test_get_practice_sync_job_status_not_active_skips_redis(client):
    mock_redis = MagicMock()
    with patch("src.api.draft._latest_practice_sync_job", return_value=_mock_practice_job(state="Succeeded", league_id="123")), \
         patch("src.api.draft._redis_client", return_value=mock_redis):
        response = client.get("/v1/draft/practice/sync-job/status")

    assert response.status_code == 200
    assert response.json() == {
        "job_state": "Succeeded",
        "pod_active": False,
        "room_connected": False,
        "room_handshake_complete": False,
        "active_league_id": "123",
        "room_identifier": "room-xyz",
    }
    mock_redis.exists.assert_not_called()


def test_end_practice_sync_job_deletes_latest(client):
    older = MagicMock()
    older.metadata.name = "practice-draft-sync-100"
    older.metadata.creation_timestamp = datetime(2026, 8, 21, 10, 0, 0)
    newer = MagicMock()
    newer.metadata.name = "practice-draft-sync-200"
    newer.metadata.creation_timestamp = datetime(2026, 8, 21, 11, 0, 0)

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value.items = [older, newer]
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 200
    assert response.json() == {"status": "deleted", "job_name": "practice-draft-sync-200"}
    mock_batch_api.delete_namespaced_job.assert_called_once_with("practice-draft-sync-200", "default", propagation_policy="Background")


def test_end_practice_sync_job_no_job_exists(client):
    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value.items = []
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 404


def test_end_practice_sync_job_k8s_failure(client):
    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.side_effect = Exception("apiserver unreachable")
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 500


# ---------------------------------------------------------------------------
# _on_the_clock_team_id (practice-mode team_count fallback)
# ---------------------------------------------------------------------------

def _on_the_clock_cursor(league_settings_row, draft_config_row, slot_rows, latest_pick):
    """Feeds the helper's four queries in order: league_settings, draft_config (only when the
    first came back empty), the slot map, then MAX(pick_number)."""
    cursor = MagicMock()
    fetchones = [league_settings_row]
    if league_settings_row is None or not league_settings_row[0]:
        fetchones.append(draft_config_row)
    fetchones.append((latest_pick,))
    cursor.fetchone.side_effect = fetchones
    cursor.fetchall.return_value = slot_rows
    return cursor


def test_on_the_clock_team_id_uses_draft_config_when_league_settings_is_empty():
    """Practice rooms are never synced into league_settings, so without this fallback every
    practice draft reports nobody on the clock no matter how well the listener is working."""
    from src.api.draft import _on_the_clock_team_id

    cursor = _on_the_clock_cursor(
        league_settings_row=None,
        draft_config_row=(3,),
        slot_rows=[("a", 1), ("b", 2), ("c", 3)],
        latest_pick=1,
    )

    # Pick 2 of a 3-team draft is still round 1, so slot 2 is up.
    assert _on_the_clock_team_id(cursor, "L1", 2026) == "b"
    # The draft_config lookup must be season-scoped; that row is keyed by league_id alone.
    assert cursor.execute.call_args_list[1][0][1] == ("L1", 2026)


def test_on_the_clock_team_id_prefers_league_settings_over_draft_config():
    """The synced ESPN value wins over a count derived from pick arrivals, and the derived
    fallback must not even be queried when it does."""
    from src.api.draft import _on_the_clock_team_id

    cursor = _on_the_clock_cursor(
        league_settings_row=(3,),
        draft_config_row=(99,),
        slot_rows=[("a", 1), ("b", 2), ("c", 3)],
        latest_pick=3,
    )

    # Pick 4 wraps into round 2, which reverses: slot 3 picks again.
    assert _on_the_clock_team_id(cursor, "L1", 2026) == "c"
    queried = [call[0][0] for call in cursor.execute.call_args_list]
    assert not any("draft_config" in sql for sql in queried)


def test_on_the_clock_team_id_is_none_when_no_source_has_a_team_count():
    from src.api.draft import _on_the_clock_team_id

    cursor = _on_the_clock_cursor(
        league_settings_row=None,
        draft_config_row=None,
        slot_rows=[("a", 1)],
        latest_pick=0,
    )

    assert _on_the_clock_team_id(cursor, "L1", 2026) is None
