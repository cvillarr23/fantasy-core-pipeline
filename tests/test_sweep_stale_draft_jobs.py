from unittest.mock import MagicMock, patch

from kubernetes.config.config_exception import ConfigException

from src.ops.sweep_stale_draft_jobs import sweep


def _k8s_patches():
    """Same pattern as tests/test_draft_routes.py's helper: both reach the cluster through
    src.k8s_client, so its config functions are the ones to patch."""
    return (
        patch("src.k8s_client.config.load_incluster_config", side_effect=ConfigException("no cluster")),
        patch("src.k8s_client.config.load_kube_config"),
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
         patch("src.k8s_client.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.ops.sweep_stale_draft_jobs.get_redis_client") as mock_redis_cls, \
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
         patch("src.k8s_client.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.ops.sweep_stale_draft_jobs.get_redis_client") as mock_redis_cls, \
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
         patch("src.k8s_client.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.ops.sweep_stale_draft_jobs.get_redis_client") as mock_redis_cls, \
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
         patch("src.k8s_client.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.ops.sweep_stale_draft_jobs.get_redis_client") as mock_redis_cls, \
         patch("src.ops.sweep_stale_draft_jobs.requests.delete") as mock_delete:
        mock_redis_cls.return_value.exists.return_value = 0
        mock_delete.return_value = MagicMock(status_code=404)
        sweep()  # must not raise

    mock_delete.assert_called_once()


def test_a_stale_leftover_alongside_a_healthy_session_is_not_torn_down():
    """Confirmed live 2026-08-27: _teardown_practice_run has no per-league scoping - it always
    retires everything currently registered. A leftover Job from an earlier session, not yet
    reaped after a room switch, sitting alongside a healthy active session killed that live
    session as collateral. Must require every listed league_id to be stale, not just one."""
    mock_batch_api = MagicMock()
    incluster_patch, kubeconfig_patch = _k8s_patches()
    mock_batch_api.list_namespaced_job.side_effect = [
        MagicMock(items=[_job("practice-draft-sync-1", "12345"), _job("practice-draft-sync-2", "999")]),
        MagicMock(items=[]),
    ]
    with incluster_patch, kubeconfig_patch, \
         patch("src.k8s_client.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.ops.sweep_stale_draft_jobs.get_redis_client") as mock_redis_cls, \
         patch("src.ops.sweep_stale_draft_jobs.requests.delete") as mock_delete:
        # league 12345 is a stale leftover (no heartbeat); 999 is the healthy, currently-active
        # session (heartbeat present) - must not be torn down as collateral.
        mock_redis_cls.return_value.exists.side_effect = lambda key: 0 if "12345" in key else 1
        sweep()

    mock_delete.assert_not_called()


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
         patch("src.k8s_client.client.BatchV1Api", return_value=mock_batch_api), \
         patch("src.ops.sweep_stale_draft_jobs.get_redis_client") as mock_redis_cls, \
         patch("src.ops.sweep_stale_draft_jobs.requests.delete") as mock_delete:
        mock_redis_cls.return_value.exists.return_value = 0
        mock_delete.return_value = MagicMock(status_code=200)
        sweep()

    mock_delete.assert_called_once()
