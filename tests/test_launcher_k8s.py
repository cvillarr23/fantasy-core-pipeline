from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from kubernetes.client import ApiClient
from kubernetes.client.rest import ApiException

from src.launcher import k8s
from src.launcher.catalog import load_catalog, params_to_args
from src.launcher.publish import RunCommand

FIXTURES = Path(__file__).parent / "fixtures"
CATALOG = load_catalog(FIXTURES / "job-catalog.yaml")

# What fantasy-infra's launcher Deployment sets, matching the CronJobs' literal values.
LAUNCHER_ENV = {
    "DB_HOST": "fantasy-db.default.svc.cluster.local",
    "DB_NAME": "fantasy_football",
    "DB_USER": "bot_admin",
    "REDIS_HOST": "fantasy-cache.default.svc.cluster.local",
    "REDIS_PORT": "6379",
    "CORE_API_BASE_URL": "http://fantasy-core-pipeline.default.svc.cluster.local:8000",
    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://tempo.default.svc.cluster.local:4318",
    "WORKER_OTEL_EXPORTER_OTLP_ENDPOINT": "http://tempo.default.svc.cluster.local:4317",
    "CORE_IMAGE": "ghcr.io/cvillarr23/fantasy-core-pipeline:abc123",
    "WORKER_IMAGE": "ghcr.io/cvillarr23/fantasy-browser-worker:def456",
}

# CronJob run-identity env the launcher replaces with RUN_ID/RUN_TRIGGER/JOB_NAME.
REPLACED_ENV = {"RUN_CRONJOB", "RUN_K8S_JOB", "JOB_NAME"}
RUN_IDENTITY_ENV = {"RUN_ID", "RUN_TRIGGER", "JOB_NAME"}

# Env and envFrom a profile adds beyond what that job's CronJob carried. Kept as exact sets,
# so a new difference in either direction fails here instead of slipping into a cutover.
SNAPSHOTS = {
    "lineup-optimizer": {
        "cronjob": "lineup-optimizer-cronjob.yaml",
        "extra_env": {"SWID", "REDIS_HOST", "REDIS_PORT"},
        "extra_env_from": set(),
    },
    "trade-scan": {
        "cronjob": "trade-scan-cronjob.yaml",
        "extra_env": {"ESPN_SWID"},
        "extra_env_from": set(),
    },
    "rumor-syncer": {
        "cronjob": "fantasy-rumor-syncer-cronjob.yaml",
        "extra_env": {
            "ESPN_S2",
            "ESPN_SWID",
            "SWID",
            "ESPN_LEAGUE_ID",
            "ESPN_TEAM_ID",
            "CORE_API_BASE_URL",
            "TRIGGER_API_KEY",
        },
        "extra_env_from": {"fantasy-league-config"},
    },
}


def command(job="lineup-optimizer", **fields):
    values = {
        "run_id": "6f1c2a9e-1b2c-4d5e-8f90-123456789abc",
        "job": job,
        "trigger": "schedule",
        "dedupe_key": f"{job}@2026-10-27T19:15:00-05:00",
        "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
        **fields,
    }
    return RunCommand(**values)


def build(job, run=None, params=None):
    resolved = CATALOG.resolve(job)
    run = run or command(job)
    argv = [*resolved.job.command, *params_to_args(params or {})]
    return ApiClient().sanitize_for_serialization(k8s.build_job(run, resolved, argv))


@pytest.fixture(autouse=True)
def launcher_env(monkeypatch):
    for name, value in LAUNCHER_ENV.items():
        monkeypatch.setenv(name, value)


def _env_from_name(entry):
    ref = entry.get("configMapRef") or entry.get("secretRef")
    return ref["name"]


@pytest.mark.parametrize("job", sorted(SNAPSHOTS))
def test_built_job_matches_cronjob_pod_spec(job):
    snapshot = SNAPSHOTS[job]
    cronjob = yaml.safe_load((FIXTURES / "cronjobs" / snapshot["cronjob"]).read_text())
    cron_pod = cronjob["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    (cron_container,) = cron_pod["containers"]
    built_pod = build(job)["spec"]["template"]["spec"]
    (built_container,) = built_pod["containers"]

    cron_env = {e["name"]: e for e in cron_container["env"] if e["name"] not in REPLACED_ENV}
    built_env = {e["name"]: e for e in built_container["env"]}
    for name, entry in cron_env.items():
        assert built_env.get(name) == entry, name
    assert set(built_env) - set(cron_env) == RUN_IDENTITY_ENV | snapshot["extra_env"]

    cron_env_from = {_env_from_name(e): e for e in cron_container.get("envFrom", [])}
    built_env_from = {_env_from_name(e): e for e in built_container.get("envFrom", [])}
    for name, entry in cron_env_from.items():
        assert built_env_from.get(name) == entry, name
    assert set(built_env_from) - set(cron_env_from) == snapshot["extra_env_from"]

    assert built_pod.get("volumes") == cron_pod.get("volumes")
    assert built_container.get("volumeMounts") == cron_container.get("volumeMounts")
    assert built_pod.get("serviceAccountName") == cron_pod.get("serviceAccountName")
    assert built_pod["imagePullSecrets"] == cron_pod["imagePullSecrets"]
    assert built_container["resources"] == cron_container["resources"]
    assert built_container["command"] == cron_container["command"]
    assert built_pod["restartPolicy"] == "Never"


def test_job_carries_run_identity_limits_and_labels():
    run = command("trade-scan", parent_run_id="parent-run", attempt=2)
    job = build("trade-scan", run)
    assert job["metadata"]["name"] == "trade-scan-6f1c2a9e1b2c-a2"
    labels = {
        "app": "fantasy-browser-worker",
        "component": "trade-scan",
        "fantasy.job": "trade-scan",
        "fantasy.run-id": run.run_id,
    }
    assert job["metadata"]["labels"] == labels
    assert job["spec"]["template"]["metadata"]["labels"] == labels
    assert job["spec"]["backoffLimit"] == 0
    assert job["spec"]["activeDeadlineSeconds"] == 960
    assert job["spec"]["ttlSecondsAfterFinished"] == 86400
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert container["image"] == LAUNCHER_ENV["WORKER_IMAGE"]
    env = {e["name"]: e.get("value") for e in container["env"]}
    assert env["RUN_ID"] == run.run_id
    assert env["RUN_TRIGGER"] == "schedule"
    assert env["RUN_PARENT_ID"] == "parent-run"
    # A schedule key already names the job, so it isn't prefixed twice.
    assert env["JOB_NAME"] == "trade-scan@2026-10-27T19:15:00-05:00"


def test_manual_run_job_name_prefixes_the_job_and_params_become_flags():
    run = command(trigger="manual", dedupe_key="0b5f", params={"week": 7, "dry_run": True})
    job = build("lineup-optimizer", run, {"week": 7, "dry_run": True})
    container = job["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e.get("value") for e in container["env"]}
    assert env["JOB_NAME"] == "lineup-optimizer@0b5f"
    assert "RUN_PARENT_ID" not in env
    assert container["command"][-3:] == ["--week", "7", "--dry-run"]


def test_job_name_stays_within_label_limit():
    run = command(job="x" * 80, attempt=12)
    assert len(k8s.job_name(run)) <= 63
    assert k8s.job_name(run).endswith("-6f1c2a9e1b2c-a12")


def test_otel_service_name_follows_the_module():
    env = {e.name: e.value for e in k8s._otel_env(["python", "-m", "src.ops.sweep_stale_draft_jobs"])}
    assert env["OTEL_SERVICE_NAME"] == "ops-sweep-stale-draft-jobs"
    assert env["OTEL_EXPORTER_OTLP_PROTOCOL"] == "http/protobuf"


def test_no_otel_env_without_an_endpoint(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    assert k8s._otel_env(["python", "-m", "src.ingestion.sync_rumors"]) == []


def test_missing_inherited_value_fails_the_build(monkeypatch):
    monkeypatch.delenv("DB_HOST")
    with pytest.raises(k8s.LaunchConfigError, match="DB_HOST"):
        build("rumor-syncer")


def test_validate_catalog_reports_missing_inherited_env(monkeypatch):
    monkeypatch.delenv("REDIS_PORT")
    with pytest.raises(k8s.LaunchConfigError, match="secrets.redis: REDIS_PORT is not set"):
        k8s.validate_catalog(CATALOG)


def test_job_exists_distinguishes_absent_from_unknown():
    api = MagicMock()
    with patch.object(k8s, "batch_v1_api", return_value=api):
        assert k8s.job_exists("j") is True
        api.read_namespaced_job.side_effect = ApiException(status=404)
        assert k8s.job_exists("j") is False
        api.read_namespaced_job.side_effect = TimeoutError()
        assert k8s.job_exists("j") is None


def test_literal_image_is_used_as_is():
    assert k8s.resolve_image("ghcr.io/x/y:1") == "ghcr.io/x/y:1"


def test_validate_catalog_accepts_fixture_and_rejects_unknown_names(monkeypatch):
    k8s.validate_catalog(CATALOG)
    broken = CATALOG.model_copy(deep=True)
    broken.profiles["core"].secrets.append("vault")
    broken.profiles["worker"].volumes.append("scratch")
    monkeypatch.delenv("WORKER_IMAGE")
    with pytest.raises(k8s.LaunchConfigError) as exc:
        k8s.validate_catalog(broken)
    message = str(exc.value)
    assert "unknown bundle 'vault'" in message
    assert "unknown volume 'scratch'" in message
    assert "WORKER_IMAGE" in message


def test_create_job_treats_already_exists_as_launched():
    api = MagicMock()
    api.create_namespaced_job.side_effect = ApiException(status=409)
    job = k8s.build_job(command(), CATALOG.resolve("lineup-optimizer"), ["python"])
    with patch.object(k8s, "batch_v1_api", return_value=api):
        k8s.create_job(job)
    assert api.create_namespaced_job.call_args.kwargs["_request_timeout"] == 10


def test_create_job_raises_other_api_errors():
    api = MagicMock()
    api.create_namespaced_job.side_effect = ApiException(status=403)
    job = k8s.build_job(command(), CATALOG.resolve("lineup-optimizer"), ["python"])
    with patch.object(k8s, "batch_v1_api", return_value=api), pytest.raises(ApiException):
        k8s.create_job(job)
