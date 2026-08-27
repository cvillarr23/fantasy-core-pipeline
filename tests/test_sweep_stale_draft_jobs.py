from unittest.mock import MagicMock, patch

from kubernetes.config.config_exception import ConfigException

from src.ops.sweep_stale_draft_jobs import sweep


def _k8s_patches():
    """Same pattern as tests/test_draft_routes.py's helper - sweep_stale_draft_jobs imports
    _batch_v1_api from src.api.draft, so the config functions to patch live there too."""
    return (
        patch("src.api.draft.config.load_incluster_config", side_effect=ConfigException("no cluster")),
        patch("src.api.draft.config.load_kube_config"),
    )


def _job(name, league_id):
    job = MagicMock()
    job.metadata.name = name
    job.metadata.labels = {"league-id": league_id}
    return job


def test_no_practice_jobs_is_a_noop():
    mock_batch_api = MagicMock()
    incluster_patch, kubeconfig_patch = _k8s_patches()
    mock_batch_api.list_namespaced_job.side_effect = [
        MagicMock(items=[]),
        MagicMock(items=[]),
    ]
    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.ops.sweep_stale_draft_jobs.redis.Redis") as mock_redis_cls, \
         patch("src.ops.sweep_stale_draft_jobs.requests.delete") as mock_delete:
        sweep()

    mock_redis_cls.assert_not_called()
    mock_delete.assert_not_called()


def test_live_heartbeat_is_left_alone():
    """The whole point of this sweeper: age alone must never trigger a teardown, only a
    confirmed-dead heartbeat."""
    mock_batch_api = MagicMock()
    incluster_patch, kubeconfig_patch = _k8s_patches()
    mock_batch_api.list_namespaced_job.side_effect = [
        MagicMock(items=[_job("practice-draft-sync-1", "12345")]),
        MagicMock(items=[]),
    ]
    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.ops.sweep_stale_draft_jobs.redis.Redis") as mock_redis_cls, \
         patch("src.ops.sweep_stale_draft_jobs.requests.delete") as mock_delete:
        mock_redis_cls.return_value.exists.return_value = 1  # key present, not expired
        sweep()

    mock_delete.assert_not_called()


def test_expired_heartbeat_triggers_teardown():
    mock_batch_api = MagicMock()
    incluster_patch, kubeconfig_patch = _k8s_patches()
    mock_batch_api.list_namespaced_job.side_effect = [
        MagicMock(items=[_job("practice-draft-sync-1", "12345")]),
        MagicMock(items=[]),
    ]
    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.ops.sweep_stale_draft_jobs.redis.Redis") as mock_redis_cls, \
         patch("src.ops.sweep_stale_draft_jobs.requests.delete") as mock_delete:
        mock_redis_cls.return_value.exists.return_value = 0  # key absent - TTL expired
        mock_delete.return_value = MagicMock(status_code=200)
        sweep()

    mock_delete.assert_called_once()
    assert mock_delete.call_args.kwargs["timeout"] == 30
    assert mock_delete.call_args.args[0].endswith("/v1/draft/practice/sync-job")


def test_a_race_that_already_ended_the_run_is_not_an_error():
    """A 404 means someone else (or a previous sweep tick) already tore it down between the
    list above and this call - not a sweep failure."""
    mock_batch_api = MagicMock()
    incluster_patch, kubeconfig_patch = _k8s_patches()
    mock_batch_api.list_namespaced_job.side_effect = [
        MagicMock(items=[_job("practice-draft-sync-1", "12345")]),
        MagicMock(items=[]),
    ]
    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.ops.sweep_stale_draft_jobs.redis.Redis") as mock_redis_cls, \
         patch("src.ops.sweep_stale_draft_jobs.requests.delete") as mock_delete:
        mock_redis_cls.return_value.exists.return_value = 0
        mock_delete.return_value = MagicMock(status_code=404)
        sweep()  # must not raise

    mock_delete.assert_called_once()


def test_listener_only_job_with_expired_heartbeat_also_triggers_teardown():
    """The connector carries ttl_seconds_after_finished and can already be gone while the
    listener (which blocks on its Redis Stream forever) is still up - must not require both
    job types to be present."""
    mock_batch_api = MagicMock()
    incluster_patch, kubeconfig_patch = _k8s_patches()
    mock_batch_api.list_namespaced_job.side_effect = [
        MagicMock(items=[]),
        MagicMock(items=[_job("draft-sync-practice-L1-1", "999")]),
    ]
    with incluster_patch, kubeconfig_patch, \
         patch("src.api.draft.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.ops.sweep_stale_draft_jobs.redis.Redis") as mock_redis_cls, \
         patch("src.ops.sweep_stale_draft_jobs.requests.delete") as mock_delete:
        mock_redis_cls.return_value.exists.return_value = 0
        mock_delete.return_value = MagicMock(status_code=200)
        sweep()

    mock_delete.assert_called_once()
