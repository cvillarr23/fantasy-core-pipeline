import json
import os
from contextlib import contextmanager
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubernetes import client as k8s_client
from kubernetes.config.config_exception import ConfigException

from src.api.draft import K8S_REQUEST_TIMEOUT_SECONDS


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
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[]), \
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
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[]), \
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
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[]), \
         patch.dict(os.environ, {}, clear=True), \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 202
    _, job = mock_batch_api.create_namespaced_job.call_args[0]
    env_names = {e.name for e in job.spec.template.spec.containers[0].env}
    # DRAFT_PICK_CLOCK_SECONDS and AUTO_DRAFT_DELAY_SECONDS are always set, never inherited:
    # both are mode-derived, and a
    # practice listener silently falling back to the 90s live default would keep retrying
    # long after the 30s practice clock expired.
    assert env_names == {"DB_PASSWORD", "DRAFT_PICK_CLOCK_SECONDS", "AUTO_DRAFT_DELAY_SECONDS"}


def test_start_draft_sync_creates_practice_job_with_prefixed_name(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("MOCK123", "8", 2026, "practice")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[]), \
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
    # A practice room's clock is 30s, not the 90s a real draft gets. Inheriting the live
    # default would let the publish-retry loop run for a full minute after the clock it is
    # meant to protect had already expired and autodrafted.
    env = {e.name: e.value for e in container.env}
    clock = env["DRAFT_PICK_CLOCK_SECONDS"]
    assert clock == "30"
    assert env["AUTO_DRAFT_DELAY_SECONDS"] == "20"


def test_start_draft_sync_live_job_gets_the_live_pick_clock(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "live")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[]), \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 202
    _, job = mock_batch_api.create_namespaced_job.call_args[0]
    container = job.spec.template.spec.containers[0]
    env = {e.name: e.value for e in container.env}
    assert env["DRAFT_PICK_CLOCK_SECONDS"] == "90"
    assert env["AUTO_DRAFT_DELAY_SECONDS"] == "45"


def test_start_draft_sync_selector_picks_named_league(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("MOCK123", "8", 2026, "practice")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[]), \
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
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[]), \
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
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[_fake_listener_job("L2", "live")]), \
         image_patch, secret_patch, configmap_patch:
        response = client.post("/v1/draft/start")

    assert response.status_code == 409
    assert "L2" in response.json()["detail"]


def _fake_listener_job(league_id, mode, name=None):
    job = MagicMock()
    job.metadata.name = name or f"draft-sync-{league_id}"
    job.metadata.labels = {"app": "draft-sync", "league-id": league_id, "mode": mode}
    return job


def test_start_draft_sync_reclaims_stale_practice_listener(client):
    """A new practice room must not be blocked by the listener the last practice run left
    behind - that made every second practice draft unstartable."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "practice")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[_fake_listener_job("L2", "practice")]), \
         patch("src.api.draft._delete_practice_listener_jobs", return_value=["draft-sync-L2"]) as mock_delete, \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 202
    mock_delete.assert_called_once_with(except_league_id="L1")
    assert mock_batch_api.create_namespaced_job.called


def test_start_draft_sync_never_reclaims_a_live_listener(client):
    """Practice cleanup must never take down a real draft that is in progress."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "practice")

    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[_fake_listener_job("L9", "live")]), \
         patch("src.api.draft._delete_practice_listener_jobs") as mock_delete, \
         image_patch, secret_patch, configmap_patch:
        response = client.post("/v1/draft/start")

    assert response.status_code == 409
    assert "L9" in response.json()["detail"]
    mock_delete.assert_not_called()


def test_start_draft_sync_clears_the_previous_runs_stale_board(client):
    """The board outlives its run, so a fresh start over the same league would otherwise
    inherit the last run's listener error and top picks until a real pick overwrote them."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "live")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[]), \
         patch("src.api.draft._clear_league_draft_state") as mock_clear, \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 202
    # Board only: the room connector is already running and may have published picks into the
    # Stream before the listener came up.
    mock_clear.assert_called_once_with("L1")


def test_start_draft_sync_leaves_a_running_listeners_board_alone(client):
    """The idempotent path adopts a run that is already in progress. Clearing its board would
    wipe the live recommendation out from under the UI."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "live")

    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Active"), \
         patch("src.api.draft._clear_league_draft_state") as mock_clear, \
         image_patch, secret_patch, configmap_patch:
        response = client.post("/v1/draft/start")

    assert response.json()["already_running"] is True
    mock_clear.assert_not_called()


def test_start_draft_sync_k8s_failure(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "live")
    mock_batch_api = MagicMock()
    mock_batch_api.create_namespaced_job.side_effect = Exception("apiserver unreachable")

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._job_state", return_value="Unknown"), \
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[]), \
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
        _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS,
    )
    for call in mock_batch_api.list_namespaced_job.call_args_list:
        assert call.kwargs["label_selector"] == "app=draft-sync,league-id=L1"
    mock_redis.delete.assert_called_once_with("draft:L1:board", "draft:L1:suggested", "draft:L1:pause:3")


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

    assert mock_redis.delete.call_args.args == ("draft:L1:board", "draft:L1:suggested", "draft:L1:pause:t1")
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
        "draft:L1:board", "draft:L1:suggested", "draft:L1:picks", "draft:L1:health", "draft_picks:L1",
    )


def test_clear_league_draft_state_fails_soft_when_redis_is_unreachable():
    """Cleanup and startup callers must not abort because leftover state could not be cleared."""
    from src.api.draft import _clear_league_draft_state

    with patch("src.api.draft._redis_client", side_effect=RuntimeError("no redis")):
        assert _clear_league_draft_state("L1") == 0


# ---------------------------------------------------------------------------
def _fake_status_job(phase="Active"):
    """A draft-sync Job for _latest_listener_job, whose phase _build_status reads. Distinct
    from _fake_listener_job, which models the conflict-detection path."""
    job = MagicMock()
    job.status.active = 1 if phase == "Active" else 0
    job.status.succeeded = 1 if phase == "Succeeded" else 0
    job.status.failed = 1 if phase == "Failed" else 0
    return job


# GET /v1/draft/status
# ---------------------------------------------------------------------------
def test_get_draft_status_aggregates_expected_shape(client):
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    def _fetchall():
        if "ORDER BY d.pick_number DESC" in mock_cursor.last_query:
            return [(2, 1, "3", "p1", "e1", "Player One", "RB"), (1, 1, "9", "p0", "e0", "Player Zero", "WR")]
        if "ORDER BY d.team_id ASC, d.pick_number ASC" in mock_cursor.last_query:
            return [(1, 1, "9", "p0", "e0", "Player Zero", "WR"), (2, 1, "3", "p1", "e1", "Player One", "RB")]
        if "draft_slots" in mock_cursor.last_query:
            return [("9", 1), ("3", 2)]
        if "SELECT transaction_type" in mock_cursor.last_query:
            return [("draft_sync", "draft_sync", "click failed", "2026-08-21T00:00:00Z")]
        return []

    def _fetchone():
        if "SELECT mode FROM draft_config" in mock_cursor.last_query:
            return ("practice",)
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["job_state"] == "Active"
    assert body["mode"] == "practice"
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


def test_get_draft_status_team_rosters_are_unbounded_grouped_and_keep_null_player_id(client):
    mock_cursor = MagicMock()

    def _execute(query, params=None):
        mock_cursor.last_query = query

    mock_cursor.execute.side_effect = _execute

    recent_rows = [
        (12, 4, "A", "p12", "e12", "Player 12", "RB"),
        (11, 4, "B", "p11", "e11", "Player 11", "WR"),
        (10, 4, "A", "p10", "e10", "Player 10", "QB"),
        (9, 3, "B", "p9", "e9", "Player 9", "TE"),
        (8, 3, "A", "p8", "e8", "Player 8", "RB"),
        (7, 3, "B", "p7", "e7", "Player 7", "WR"),
        (6, 2, "A", "p6", "e6", "Player 6", "QB"),
        (5, 2, "B", None, "e5", None, None),
        (4, 2, "A", "p4", "e4", "Player 4", "RB"),
        (3, 1, "B", "p3", "e3", "Player 3", "WR"),
    ]
    roster_rows = [
        (1, 1, "A", "p1", "e1", "Player 1", "QB"),
        (2, 1, "B", "p2", "e2", "Player 2", "WR"),
        (3, 1, "B", "p3", "e3", "Player 3", "WR"),
        (4, 2, "A", "p4", "e4", "Player 4", "RB"),
        (5, 2, "B", None, "e5", None, None),
        (6, 2, "A", "p6", "e6", "Player 6", "QB"),
        (7, 3, "B", "p7", "e7", "Player 7", "WR"),
        (8, 3, "A", "p8", "e8", "Player 8", "RB"),
        (9, 3, "B", "p9", "e9", "Player 9", "TE"),
        (10, 4, "A", "p10", "e10", "Player 10", "QB"),
        (11, 4, "B", "p11", "e11", "Player 11", "WR"),
        (12, 4, "A", "p12", "e12", "Player 12", "RB"),
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
        if "SELECT mode FROM draft_config" in mock_cursor.last_query:
            return ("practice",)
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)):
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
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
         patch("src.api.draft._latest_listener_job", return_value=(None, True)):
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
         patch("src.api.draft._latest_listener_job", return_value=(None, False)):
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)):
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
         patch("src.api.draft.time.time", return_value=1005):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    suggestion = response.json()["recommendation"]["espn_suggestion"]
    assert suggestion == {
        "espn_player_id": "4362249", "player_id": "p9", "name": "Best Back",
        "position": "RB", "team": "KC", "vorp_score": 12.5,
        "suggested_at": "2026-08-25T00:00:00Z",
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
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
        if "SELECT mode FROM draft_config" in query:
            return ("practice",)
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Succeeded"), True)):
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
         patch("src.api.draft._latest_listener_job", return_value=(None, True)):
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)):
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
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
        if "SELECT mode FROM draft_config" in query:
            return ("practice",)
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Succeeded"), True)):
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
         patch("src.api.draft._latest_listener_job", return_value=(None, True)):
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)):
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)):
        response = client.get("/v1/draft/status", params={"league_id": "L1", "season": 2026})

    assert response.status_code == 200
    body = response.json()
    assert body["connector_health"] == health_snapshot
    mock_redis.get.assert_called_with("draft:L1:health")


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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)):
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
         patch("src.api.draft._latest_listener_job", return_value=(_fake_status_job("Active"), True)), \
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


def test_start_practice_sync_job_force_tears_down_the_existing_run_first(client):
    """The escape hatch for a wedged run. Without it the 409 tells the user to end the run
    first, and ending it is exactly what is failing - a dead end with no way out of the UI."""
    mock_batch_api = MagicMock()
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with patch("src.api.draft.BROWSER_WORKER_IMAGE", "ghcr.io/example/fantasy-browser-worker:test"), \
         patch("src.api.draft._practice_sync_job_state", return_value="Active") as mock_state, \
         patch("src.api.draft._teardown_practice_run", return_value={"job_name": "practice-draft-sync-1", "listener_jobs_deleted": ["draft-listener-1"], "connector_gone": True}) as mock_teardown, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/practice/sync-job?force=true", json={"room_url": "https://fantasy.espn.com/football/draft"})

    assert response.status_code == 202
    # strict_cleanup: a force start that proceeds after a failed Redis delete hands the new
    # connector the old run's ledger to seed from.
    mock_teardown.assert_called_once_with(strict_cleanup=True)
    # The Active guard must not even be consulted when forcing, or a run wedged Active would
    # still 409 after being torn down.
    mock_state.assert_not_called()
    mock_batch_api.create_namespaced_job.assert_called_once()


def test_start_practice_sync_job_force_surfaces_a_failed_teardown(client):
    """Silently starting a second connector against a still-live one would double-publish
    picks into the same Redis Stream."""
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with patch("src.api.draft.BROWSER_WORKER_IMAGE", "ghcr.io/example/fantasy-browser-worker:test"), \
         patch("src.api.draft._teardown_practice_run", side_effect=Exception("apiserver unreachable")), \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=MagicMock()) as mock_api:
        response = client.post("/v1/draft/practice/sync-job?force=true", json={"room_url": "https://fantasy.espn.com/football/draft"})

    assert response.status_code == 500
    assert "tear down" in response.json()["detail"]
    mock_api.return_value.create_namespaced_job.assert_not_called()


def test_start_practice_sync_job_force_refuses_when_the_old_connector_is_still_alive(client):
    """Teardown reports success once the listener is retired, even if the connector outlived
    its termination poll. Starting a replacement then would leave two connectors publishing
    into the same draft Stream - force has to be stricter than DELETE about this."""
    mock_batch_api = MagicMock()
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with patch("src.api.draft.BROWSER_WORKER_IMAGE", "ghcr.io/example/fantasy-browser-worker:test"), \
         patch("src.api.draft._teardown_practice_run", return_value={"job_name": "practice-draft-sync-1", "listener_jobs_deleted": [], "connector_gone": False}), \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/practice/sync-job?force=true", json={"room_url": "https://fantasy.espn.com/football/draft"})

    assert response.status_code == 409
    assert "has not terminated" in response.json()["detail"]
    mock_batch_api.create_namespaced_job.assert_not_called()


def test_start_practice_sync_job_force_refuses_when_redis_cleanup_fails(client):
    """Redis cleanup is best-effort everywhere else on purpose - refusing to start a draft over
    leftover state is worse than starting with it. Force is the exception: it creates a connector
    that seeds its ledger from draft:{league}:picks, so proceeding after a failed delete would
    silently resurrect the previous run's picks and drop the real ones as duplicates."""
    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value = MagicMock(items=[])
    mock_redis = MagicMock()
    mock_redis.scan_iter.return_value = []
    mock_redis.delete.side_effect = Exception("redis unreachable")
    incluster_patch, kubeconfig_patch = _k8s_patches()

    connector = _mock_practice_job(state="Active", league_id="L9")
    connector.metadata.name = "practice-draft-sync-1"
    connector.metadata.creation_timestamp = datetime(2026, 1, 1)

    with patch("src.api.draft.BROWSER_WORKER_IMAGE", "ghcr.io/example/fantasy-browser-worker:test"), \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.api.draft._delete_practice_listener_jobs", return_value=[]), \
         patch("src.api.draft._redis_client", return_value=mock_redis):
        mock_batch_api.list_namespaced_job.side_effect = [MagicMock(items=[connector]), MagicMock(items=[])]
        response = client.post("/v1/draft/practice/sync-job?force=true", json={"room_url": "https://fantasy.espn.com/football/draft?leagueId=L9"})

    assert response.status_code == 500
    assert "could not be" in response.json()["detail"]
    mock_batch_api.create_namespaced_job.assert_not_called()


def test_start_practice_sync_job_refuses_a_concurrent_start(client):
    """Teardown and create are separated by seconds of termination polling, so two concurrent
    starts could both find the room empty and then both create a connector - two publishers on
    one Stream, the thing every other guard here exists to prevent."""
    mock_redis = MagicMock()
    mock_redis.set.return_value = False  # someone else holds the lock

    with patch("src.api.draft.BROWSER_WORKER_IMAGE", "ghcr.io/example/fantasy-browser-worker:test"), \
         patch("src.api.draft._redis_client", return_value=mock_redis):
        response = client.post("/v1/draft/practice/sync-job", json={"room_url": "https://fantasy.espn.com/football/draft"})

    assert response.status_code == 409
    assert "already in progress" in response.json()["detail"]


def test_practice_start_lock_ttl_covers_the_worst_case_start(client):
    """A lease shorter than the operation expires mid-start, letting a second request acquire the
    lock and create a connector alongside the first - the overlap the lock exists to prevent,
    reintroduced by its own timeout. Two full termination waits can run (connector and listener),
    each a bounded apiserver call plus a sleep per poll."""
    from src.api.draft import (
        K8S_REQUEST_TIMEOUT_SECONDS as k8s_timeout,
        PRACTICE_START_LOCK_TTL_SECONDS,
        STOP_JOB_TERMINATION_MAX_POLLS,
        STOP_JOB_TERMINATION_POLL_INTERVAL_SECONDS,
    )

    worst_case_wait = STOP_JOB_TERMINATION_MAX_POLLS * (k8s_timeout + STOP_JOB_TERMINATION_POLL_INTERVAL_SECONDS)
    assert PRACTICE_START_LOCK_TTL_SECONDS >= 2 * worst_case_wait


def test_redis_client_bounds_its_socket_timeouts():
    """redis-py waits forever by default. These calls sit in synchronous endpoints on a bounded
    threadpool, so a Redis that accepts the connection then stops replying parks a worker
    permanently - the same failure mode as the unbounded Kubernetes calls this PR bounds, and the
    reason the start lock's fail-open behaviour is real rather than nominal."""
    from src.api.draft import _redis_client

    with patch("src.api.draft.redis.Redis") as mock_redis_cls:
        _redis_client()

    kwargs = mock_redis_cls.call_args.kwargs
    assert kwargs["socket_connect_timeout"] > 0
    assert kwargs["socket_timeout"] > 0


def test_start_practice_sync_job_proceeds_when_the_lock_is_unavailable(client):
    """An unreachable lock service must not become a hard dependency of starting a draft - the
    k8s-side Active check still catches the ordinary case."""
    mock_batch_api = MagicMock()
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with patch("src.api.draft.BROWSER_WORKER_IMAGE", "ghcr.io/example/fantasy-browser-worker:test"), \
         patch("src.api.draft._practice_sync_job_state", return_value="Unknown"), \
         patch("src.api.draft._redis_client", side_effect=Exception("redis unreachable")), \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/practice/sync-job", json={"room_url": "https://fantasy.espn.com/football/draft"})

    assert response.status_code == 202
    mock_batch_api.create_namespaced_job.assert_called_once()


def test_start_practice_sync_job_releases_the_lock_on_failure(client):
    """A lock leaked by a failed start would block every retry for its full TTL - exactly the
    wedged-and-no-way-out state this PR is about."""
    mock_redis = MagicMock()
    mock_redis.set.return_value = True

    with patch("src.api.draft.BROWSER_WORKER_IMAGE", "ghcr.io/example/fantasy-browser-worker:test"), \
         patch("src.api.draft._practice_sync_job_state", return_value="Active"), \
         patch("src.api.draft._redis_client", return_value=mock_redis):
        response = client.post("/v1/draft/practice/sync-job", json={"room_url": "https://fantasy.espn.com/football/draft"})

    assert response.status_code == 409
    # Compare-and-delete, not a plain DELETE: after a lease expiry the key belongs to a
    # DIFFERENT request, and dropping it would create the double-start the lock prevents.
    mock_redis.eval.assert_called_once()
    args = mock_redis.eval.call_args[0]
    assert args[1] == 1 and args[2] == "draft:practice:start-lock"
    # Released with the same token it acquired with.
    assert args[3] == mock_redis.set.call_args[0][1]


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


def test_teardown_practice_run_deletes_every_connector_not_just_the_newest(client):
    """Normally there is one connector, but concurrent starts or a manual kubectl apply during
    recovery can leave more. Retiring only the newest would report connector_gone=True while an
    older publisher was still writing to the same Stream - and ?force=true starts a replacement
    on the strength of that flag."""
    from src.api.draft import _teardown_practice_run

    older = _mock_practice_job(state="Active", league_id="111")
    older.metadata.name = "practice-draft-sync-100"
    older.metadata.creation_timestamp = datetime(2026, 1, 1)
    newer = _mock_practice_job(state="Active", league_id="222")
    newer.metadata.name = "practice-draft-sync-200"
    newer.metadata.creation_timestamp = datetime(2026, 1, 2)

    mock_batch_api = MagicMock()
    # Deleted Jobs are gone on the first poll.
    mock_batch_api.list_namespaced_job.side_effect = [MagicMock(items=[older, newer]), MagicMock(items=[])]
    mock_redis = MagicMock()
    mock_redis.scan_iter.return_value = []
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.api.draft._delete_practice_listener_jobs", return_value=[]), \
         patch("src.api.draft._redis_client", return_value=mock_redis):
        result = _teardown_practice_run()

    deleted = {c[0][0] for c in mock_batch_api.delete_namespaced_job.call_args_list}
    assert deleted == {"practice-draft-sync-100", "practice-draft-sync-200"}
    assert result["connector_gone"] is True
    assert result["job_name"] == "practice-draft-sync-200"
    # Both leagues get cleared, not just the newest connector's.
    cleared = {c[0][0] for c in mock_redis.delete.call_args_list if c[0]}
    assert any("111" in k for k in cleared) and any("222" in k for k in cleared)


def test_teardown_practice_run_reports_not_gone_if_an_older_connector_survives(client):
    """The whole point of the flag: an older connector outliving the poll must veto force-start."""
    from src.api.draft import _teardown_practice_run

    older = _mock_practice_job(state="Active", league_id="111")
    older.metadata.name = "practice-draft-sync-100"
    older.metadata.creation_timestamp = datetime(2026, 1, 1)
    newer = _mock_practice_job(state="Active", league_id="222")
    newer.metadata.name = "practice-draft-sync-200"
    newer.metadata.creation_timestamp = datetime(2026, 1, 2)

    mock_batch_api = MagicMock()
    # The older one never disappears from the selector.
    mock_batch_api.list_namespaced_job.return_value = MagicMock(items=[older])
    mock_redis = MagicMock()
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.STOP_JOB_TERMINATION_MAX_POLLS", 1), \
         patch("src.api.draft.STOP_JOB_TERMINATION_POLL_INTERVAL_SECONDS", 0), \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.api.draft._delete_practice_listener_jobs", return_value=[]), \
         patch("src.api.draft._redis_client", return_value=mock_redis):
        mock_batch_api.list_namespaced_job.side_effect = [MagicMock(items=[older, newer]), MagicMock(items=[older])]
        result = _teardown_practice_run()

    assert result["connector_gone"] is False
    # And nothing is cleared, since the survivor may still be publishing into it.
    mock_redis.delete.assert_not_called()


def test_teardown_practice_run_does_not_raise_when_nothing_is_running(client):
    """Only the DELETE endpoint turns "nothing running" into a 404; ?force=true on a clean
    cluster must be a no-op, not a failure."""
    from src.api.draft import _teardown_practice_run

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value = MagicMock(items=[])
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.api.draft._redis_client", return_value=MagicMock()):
        result = _teardown_practice_run()

    assert result["job_name"] is None
    assert result["listener_jobs_deleted"] == []
    # Nothing to terminate counts as gone, or ?force=true on a clean cluster would 409.
    assert result["connector_gone"] is True


def test_every_kubernetes_call_is_bounded_by_a_timeout():
    """The kubernetes client defaults to NO timeout, and every one of these calls sits in a
    synchronous FastAPI endpoint on a bounded threadpool. One hung apiserver read parks a
    worker forever; enough of them and the whole API - liveness probe included - wedges.
    That is not hypothetical: it is exactly how a degraded control plane took the app down."""
    import ast
    import inspect

    import src.api.draft as draft_module

    source = inspect.getsource(draft_module)
    tree = ast.parse(source)

    # Matched by shape, not by an allowlist of the methods that happen to exist today. An
    # allowlist would silently skip a future patch_namespaced_job or read_namespaced_pod - the
    # regression guard would keep passing while the hang it exists to prevent came back.
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and "_namespaced_" in node.func.attr
    ]
    # If this ever hits zero the matcher has drifted and the assert below is vacuously true.
    assert calls, "found no kubernetes *_namespaced_* calls - the matcher is broken, not the code"

    unbounded = [f"{c.func.attr} at line {c.lineno}" for c in calls if not any(kw.arg == "_request_timeout" for kw in c.keywords)]
    assert unbounded == [], f"unbounded kubernetes calls: {unbounded}"


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
         patch("src.api.draft._wait_for_jobs_gone", return_value=True), \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 200
    assert response.json()["job_name"] == "practice-draft-sync-200"
    # Foreground: the connector publishes the pick ledger and Stream that teardown deletes, so
    # it must actually be gone first, not merely marked for deletion.
    mock_batch_api.delete_namespaced_job.assert_any_call(
        "practice-draft-sync-200", "default", propagation_policy="Foreground",
        _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS,
    )


def test_end_practice_sync_job_clears_the_pick_stream_once_the_connector_is_gone(client):
    """The connector is the publisher for the ledger and Stream, so teardown may only drop
    them after its Pod has actually terminated."""
    connector = MagicMock()
    connector.metadata.name = "practice-draft-sync-1"
    connector.metadata.creation_timestamp = 1

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value.items = [connector]
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft._wait_for_jobs_gone", return_value=True), \
         patch("src.api.draft._delete_practice_listener_jobs", return_value=["l1"]) as mock_delete, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 200
    assert mock_delete.call_args.kwargs["clear_pick_stream"] is True


def test_end_practice_sync_job_clears_the_connectors_league_even_with_no_listener_left(client):
    """The listener carries ttl_seconds_after_finished, so it can already be GC'd while the
    connector is still up. Deriving cleanup only from listener Jobs would then clear nothing."""
    connector = MagicMock()
    connector.metadata.name = "practice-draft-sync-1"
    connector.metadata.creation_timestamp = 1
    connector.metadata.labels = {"app": "practice-draft-sync", "league-id": "L7"}

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value.items = [connector]
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft._wait_for_jobs_gone", return_value=True), \
         patch("src.api.draft._delete_practice_listener_jobs", return_value=[]), \
         patch("src.api.draft._clear_league_draft_state") as mock_clear, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 200
    mock_clear.assert_called_once_with("L7", include_pick_stream=True, raise_on_error=False)


def test_end_practice_sync_job_leaves_the_connectors_league_alone_if_it_wont_die(client):
    connector = MagicMock()
    connector.metadata.name = "practice-draft-sync-1"
    connector.metadata.creation_timestamp = 1
    connector.metadata.labels = {"app": "practice-draft-sync", "league-id": "L7"}

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value.items = [connector]
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft._wait_for_jobs_gone", return_value=False), \
         patch("src.api.draft._delete_practice_listener_jobs", return_value=["l1"]), \
         patch("src.api.draft._clear_league_draft_state") as mock_clear, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 200
    mock_clear.assert_not_called()


def test_clear_league_draft_state_escapes_glob_metacharacters_in_the_league_id():
    """league_id is an arbitrary string from DraftConfigPayload. Unescaped, a value of `*`
    would scan draft:*:pause:* and delete every league's pause flags, a live draft's included."""
    from src.api.draft import _clear_league_draft_state

    mock_redis = MagicMock()
    mock_redis.scan_iter.return_value = iter([])

    with patch("src.api.draft._redis_client", return_value=mock_redis):
        _clear_league_draft_state("*")

    assert mock_redis.scan_iter.call_args.kwargs["match"] == "draft:\\*:pause:*"


def test_end_practice_sync_job_keeps_the_pick_stream_if_the_connector_wont_die(client):
    """Still retire the listener - leaking it is the bug this endpoint exists to prevent - but
    leave the keys a possibly-still-running publisher owns."""
    connector = MagicMock()
    connector.metadata.name = "practice-draft-sync-1"
    connector.metadata.creation_timestamp = 1

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value.items = [connector]
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft._wait_for_jobs_gone", return_value=False), \
         patch("src.api.draft._delete_practice_listener_jobs", return_value=["l1"]) as mock_delete, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 200
    assert mock_delete.call_args.kwargs["clear_pick_stream"] is False


def test_end_practice_sync_job_also_deletes_the_listener(client):
    """The room connector and the listener are separate Jobs. Deleting only the connector
    leaves a Redis consumer blocking forever, which blocks the next practice run entirely."""
    connector = MagicMock()
    connector.metadata.name = "practice-draft-sync-1"
    connector.metadata.creation_timestamp = 1
    listener = _fake_listener_job("L2", "practice", name="draft-sync-practice-L2-1")

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.side_effect = [
        MagicMock(items=[connector]),
        MagicMock(items=[listener]),
    ]
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft._wait_for_jobs_gone", return_value=True), \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 200
    assert response.json()["listener_jobs_deleted"] == ["draft-sync-practice-L2-1"]
    selectors = [c.kwargs["label_selector"] for c in mock_batch_api.list_namespaced_job.call_args_list]
    assert "app=draft-sync,mode=practice" in selectors
    deleted = [c.args[0] for c in mock_batch_api.delete_namespaced_job.call_args_list]
    assert deleted == ["practice-draft-sync-1", "draft-sync-practice-L2-1"]


def test_delete_practice_listener_jobs_never_touches_live_listeners(client):
    """The label selector is the only thing standing between practice cleanup and an
    in-progress real draft."""
    from src.api.draft import _delete_practice_listener_jobs

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value = MagicMock(items=[])
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        _delete_practice_listener_jobs()

    assert mock_batch_api.list_namespaced_job.call_args.kwargs["label_selector"] == "app=draft-sync,mode=practice"


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


def test_delete_practice_listener_jobs_propagates_delete_failure(client):
    """Swallowing this made /start create a second listener alongside the one it failed to
    reclaim, and /practice/sync-job report a 200 teardown with the listener still running."""
    from src.api.draft import _delete_practice_listener_jobs

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value = MagicMock(items=[_fake_listener_job("L2", "practice")])
    mock_batch_api.delete_namespaced_job.side_effect = Exception("apiserver unreachable")
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        with pytest.raises(Exception, match="apiserver unreachable"):
            _delete_practice_listener_jobs()


def test_start_draft_sync_aborts_when_stale_listener_cannot_be_reclaimed(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "practice")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._latest_listener_job", return_value=(None, True)), \
         patch("src.api.draft._active_listeners_on_other_leagues", return_value=[_fake_listener_job("L2", "practice")]), \
         patch("src.api.draft._delete_practice_listener_jobs", side_effect=Exception("apiserver unreachable")), \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 500
    assert not mock_batch_api.create_namespaced_job.called


def test_recent_picks_survive_a_null_internal_player_id(client):
    from src.api.draft import DraftPickSummary

    """K and D/ST never resolve to an internal id. The response model must allow that or
    FastAPI 500s on exactly the unnamed picks this is meant to surface."""
    summary = DraftPickSummary(pick_number=1, round=1, team_id="3", player_id=None,
                               espn_player_id="-16033", name=None, position=None)
    assert summary.player_id is None
    assert summary.espn_player_id == "-16033"


def test_end_practice_sync_job_cleans_listener_after_connector_is_gc_d(client):
    """The connector carries ttl_seconds_after_finished; the listener blocks forever. Once
    Kubernetes collects the connector, gating cleanup on it would strand the listener with
    no way to reach it - reintroducing the exact leak this endpoint exists to prevent."""
    listener = _fake_listener_job("L2", "practice", name="draft-sync-practice-L2-1")
    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.side_effect = [
        MagicMock(items=[]),          # connector already garbage-collected
        MagicMock(items=[listener]),  # listener still running
    ]
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft._wait_for_jobs_gone", return_value=True), \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.delete("/v1/draft/practice/sync-job")

    assert response.status_code == 200
    assert response.json()["job_name"] is None
    assert response.json()["listener_jobs_deleted"] == ["draft-sync-practice-L2-1"]


def test_delete_practice_listener_jobs_waits_for_the_pod_to_actually_go():
    """Background deletion returns before the Pod dies, and /start creates the replacement
    the moment this returns - the old and new listeners would briefly share a Redis consumer
    group and split the pick stream between them."""
    from src.api.draft import _delete_practice_listener_jobs

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value = MagicMock(items=[_fake_listener_job("L2", "practice")])
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft._wait_for_jobs_gone", return_value=True) as mock_wait, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        _delete_practice_listener_jobs()

    assert mock_batch_api.delete_namespaced_job.call_args.kwargs["propagation_policy"] == "Foreground"
    mock_wait.assert_called_once()


def test_delete_practice_listener_jobs_clears_the_board_of_every_league_it_retired():
    """Deleting the Job leaves its un-TTL'd board behind, so the retired league keeps
    reporting its final listener error to anyone still watching that league's status."""
    from src.api.draft import _delete_practice_listener_jobs

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value = MagicMock(
        items=[_fake_listener_job("L2", "practice", name="j2"), _fake_listener_job("L3", "practice", name="j3")]
    )
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft._wait_for_jobs_gone", return_value=True), \
         patch("src.api.draft._clear_league_draft_state") as mock_clear, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        _delete_practice_listener_jobs()

    assert [c.args[0] for c in mock_clear.call_args_list] == ["L2", "L3"]
    # Off by default: the /start reclaim path cannot quiesce the other league's connector, so
    # only end_practice_sync_job - which waits for it - may opt in.
    assert not any(c.kwargs["include_pick_stream"] for c in mock_clear.call_args_list)


def test_delete_practice_listener_jobs_raises_if_the_pod_never_terminates():
    from src.api.draft import _delete_practice_listener_jobs

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value = MagicMock(items=[_fake_listener_job("L2", "practice")])
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft._wait_for_jobs_gone", return_value=False), \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        with pytest.raises(RuntimeError, match="did not terminate"):
            _delete_practice_listener_jobs()


def test_active_listeners_query_fails_closed_on_a_kubernetes_error():
    """An unreadable listener state is not "no conflicts": failing open lets a transient
    apiserver blip slip a second listener past the duplicate guard."""
    from src.api.draft import _active_listeners_on_other_leagues

    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.side_effect = Exception("apiserver unreachable")
    incluster_patch, kubeconfig_patch = _k8s_patches()

    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        with pytest.raises(Exception, match="apiserver unreachable"):
            _active_listeners_on_other_leagues("L1")


def test_start_draft_sync_refuses_to_start_when_listener_state_is_unreadable(client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("L1", "7", 2026, "practice")
    mock_batch_api = MagicMock()

    incluster_patch, kubeconfig_patch = _k8s_patches()
    image_patch, secret_patch, configmap_patch = _env_config_patches()
    with patch("src.api.draft.get_db_cursor", lambda: _fake_db_cursor(mock_cursor)), \
         patch("src.api.draft._latest_listener_job", return_value=(None, True)), \
         patch("src.api.draft._active_listeners_on_other_leagues", side_effect=Exception("apiserver unreachable")), \
         image_patch, secret_patch, configmap_patch, \
         incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api):
        response = client.post("/v1/draft/start")

    assert response.status_code == 500
    assert not mock_batch_api.create_namespaced_job.called


def test_wait_for_jobs_gone_ignores_jobs_it_was_not_asked_about():
    """The reclaim path deliberately spares the target league's own Job. A selector-based
    wait would keep seeing that spared Job and time out every single reclaim."""
    from src.api.draft import _wait_for_jobs_gone

    spared = _fake_listener_job("L1", "practice", name="draft-sync-practice-L1-1")
    mock_batch_api = MagicMock()
    mock_batch_api.list_namespaced_job.return_value = MagicMock(items=[spared])

    assert _wait_for_jobs_gone(mock_batch_api, "app=draft-sync,mode=practice", ["draft-sync-practice-L2-1"]) is True
    assert mock_batch_api.list_namespaced_job.call_count == 1
