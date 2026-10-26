"""Builds the k8s Job for a pod-executor run from its catalog profile.

The catalog names env bundles abstractly (`secrets: [db, espn, ...]`); BUNDLES and VOLUMES
below turn each name into the env, envFrom and volumes the CronJobs carried. The concrete
Secret, ConfigMap and key names are fantasy-infra's: they mirror its CronJob manifests and
must change with them. Non-secret connection settings (DB_HOST, REDIS_HOST, ...) are copied
from the launcher's own environment, as `src/api/draft.py` does for draft-sync Jobs, so a
launched pod always talks to the same database and API as the launcher.
"""

import logging
import os
import re

from kubernetes import client
from kubernetes.client.rest import ApiException

from src.k8s_client import (
    K8S_NAMESPACE,
    K8S_REQUEST_TIMEOUT_SECONDS,
    batch_v1_api,
    dns1123_slug,
)

logger = logging.getLogger(__name__)

TTL_SECONDS_AFTER_FINISHED = 86400
IMAGE_PULL_SECRET = "ghcr-pull-secret"
ESPN_SESSION_SECRET = "espn-session-secrets"
ESPN_SESSION_STATE_DIR = "/var/run/espn-session"


class LaunchConfigError(RuntimeError):
    """The launcher can't build a pod spec: a bundle, volume or env value is missing."""


def _inherited(name):
    value = os.getenv(name)
    if not value:
        raise LaunchConfigError(f"{name} is not set in the launcher environment")
    return client.V1EnvVar(name=name, value=value)


def _secret(name, secret, key):
    return client.V1EnvVar(
        name=name,
        value_from=client.V1EnvVarSource(
            secret_key_ref=client.V1SecretKeySelector(name=secret, key=key)
        ),
    )


def _config_map_from(name):
    return client.V1EnvFromSource(config_map_ref=client.V1ConfigMapEnvSource(name=name))


# Each bundle returns (env, env_from). Called per build, so inherited values are read live.
BUNDLES = {
    "db": lambda: (
        [
            _inherited("DB_HOST"),
            _inherited("DB_NAME"),
            _inherited("DB_USER"),
            _secret("DB_PASSWORD", "database-credentials", "postgres-password"),
        ],
        [],
    ),
    # Core reads the SWID as ESPN_SWID and the browser worker as SWID, so both are set.
    "espn": lambda: (
        [
            _secret("ESPN_S2", ESPN_SESSION_SECRET, "espn_s2"),
            _secret("ESPN_SWID", ESPN_SESSION_SECRET, "swid"),
            _secret("SWID", ESPN_SESSION_SECRET, "swid"),
            _secret("ESPN_LEAGUE_ID", ESPN_SESSION_SECRET, "ESPN_LEAGUE_ID"),
            _secret("ESPN_TEAM_ID", ESPN_SESSION_SECRET, "ESPN_TEAM_ID"),
        ],
        [],
    ),
    "redis": lambda: ([_inherited("REDIS_HOST"), _inherited("REDIS_PORT")], []),
    "trigger-api": lambda: (
        [
            _inherited("CORE_API_BASE_URL"),
            _secret("TRIGGER_API_KEY", "trigger-api-key", "trigger-api-key"),
        ],
        [],
    ),
    "amqp": lambda: ([_secret("AMQP_URL", "rabbitmq-credentials", "amqp-url")], []),
    "llm": lambda: (
        [
            _secret("GROQ_API_KEY", "groq-api-key", "groq_api_key"),
            _secret("GEMINI_API_KEY", "gemini-api-key", "gemini_api_key"),
        ],
        [],
    ),
    # NTFY_TOKEN is optional so a pod still starts before ntfy-credentials exists.
    "notify": lambda: (
        [],
        [
            _config_map_from("fantasy-notify-config"),
            client.V1EnvFromSource(
                secret_ref=client.V1SecretEnvSource(name="ntfy-credentials", optional=True)
            ),
        ],
    ),
    "league-config": lambda: ([], [_config_map_from("fantasy-league-config")]),
}

# Each volume returns (volume, mount, env).
VOLUMES = {
    "espn-session-state": lambda: (
        client.V1Volume(
            name="espn-session-state",
            secret=client.V1SecretVolumeSource(
                secret_name="espn-session-state", optional=True
            ),
        ),
        client.V1VolumeMount(
            name="espn-session-state", mount_path=ESPN_SESSION_STATE_DIR, read_only=True
        ),
        [
            client.V1EnvVar(
                name="ESPN_SESSION_STATE_FILE", value=f"{ESPN_SESSION_STATE_DIR}/state.json"
            )
        ],
    ),
}

_IMAGE_PLACEHOLDER = re.compile(r"[A-Z][A-Z0-9_]*")


def resolve_image(image):
    """A placeholder such as CORE_IMAGE names the launcher env var holding the image ref
    (fantasy-infra pins it, and argocd-image-updater bumps it); anything else is a ref."""
    if not _IMAGE_PLACEHOLDER.fullmatch(image):
        return image
    value = os.getenv(image)
    if not value:
        raise LaunchConfigError(f"{image} is not set in the launcher environment")
    return value


def _app_label(image_ref):
    """`ghcr.io/cvillarr23/fantasy-core-pipeline:sha` -> `fantasy-core-pipeline`, the `app`
    label the CronJob pods carried and the log dashboards select on."""
    return image_ref.rsplit("/", 1)[-1].split("@")[0].split(":")[0]


def _otel_env(command):
    """Core exports OTLP over HTTP only; the Node worker exports gRPC on another port,
    so its endpoint comes from WORKER_OTEL_EXPORTER_OTLP_ENDPOINT. No endpoint, no export."""
    if command[:2] == ["python", "-m"] and len(command) > 2:
        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
        protocol = "http/protobuf"
        # src.optimization.lineup_solver -> optimization-lineup-solver, the CronJob names.
        service = command[2].removeprefix("src.").replace(".", "-").replace("_", "-")
    else:
        endpoint = os.getenv("WORKER_OTEL_EXPORTER_OTLP_ENDPOINT")
        protocol = "grpc"
        service = "browser-worker"
    if not endpoint:
        return []
    return [
        client.V1EnvVar(name="OTEL_EXPORTER_OTLP_ENDPOINT", value=endpoint),
        client.V1EnvVar(name="OTEL_EXPORTER_OTLP_PROTOCOL", value=protocol),
        client.V1EnvVar(name="OTEL_SERVICE_NAME", value=service),
    ]


def validate_catalog(catalog):
    """Fails launcher startup on a profile naming an unknown bundle or volume, a bundle
    whose inherited env var is unset, or an image placeholder with no env var, instead of
    failing every run of those jobs."""
    problems = []
    for name, profile in catalog.profiles.items():
        for kind, label, names, registry in (
            ("secrets", "bundle", profile.secrets, BUNDLES),
            ("volumes", "volume", profile.volumes, VOLUMES),
        ):
            for item in names:
                if item not in registry:
                    problems.append(f"profiles.{name}.{kind}: unknown {label} {item!r}")
                    continue
                try:
                    # Inherited values are read here too, so a missing DB_HOST fails startup.
                    registry[item]()
                except LaunchConfigError as exc:
                    problems.append(f"profiles.{name}.{kind}.{item}: {exc}")
        try:
            resolve_image(profile.image)
        except LaunchConfigError as exc:
            problems.append(f"profiles.{name}.image: {exc}")
    if problems:
        raise LaunchConfigError("; ".join(problems))


def job_name(command):
    """`<job>-<run id>-a<attempt>`: unique per attempt, and within the 63-character limit
    of the `batch.kubernetes.io/job-name` label Kubernetes derives from it."""
    suffix = f"-{command.run_id.replace('-', '')[:12]}-a{command.attempt}"
    return dns1123_slug(command.job)[: 63 - len(suffix)].rstrip("-") + suffix


def build_job(command, resolved, argv):
    """The k8s Job for one attempt of a run. `argv` is the catalog command plus params."""
    profile = resolved.profile
    image = resolve_image(profile.image)
    env = [client.V1EnvVar(name=k, value=v) for k, v in command.launch_env().items()]
    env_from = []
    for bundle in profile.secrets:
        bundle_env, bundle_env_from = BUNDLES[bundle]()
        env += bundle_env
        env_from += bundle_env_from
    volumes, mounts = [], []
    for name in profile.volumes:
        volume, mount, volume_env = VOLUMES[name]()
        volumes.append(volume)
        mounts.append(mount)
        env += volume_env
    env += _otel_env(resolved.job.command)
    labels = {
        "app": _app_label(image),
        "component": resolved.name,
        "fantasy.job": resolved.name,
        "fantasy.run-id": command.run_id,
    }
    return client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name(command), labels=labels),
        spec=client.V1JobSpec(
            backoff_limit=0,
            active_deadline_seconds=int(resolved.deadline.total_seconds()),
            ttl_seconds_after_finished=TTL_SECONDS_AFTER_FINISHED,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels=labels),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    service_account_name=profile.serviceAccount,
                    image_pull_secrets=[client.V1LocalObjectReference(name=IMAGE_PULL_SECRET)],
                    volumes=volumes or None,
                    containers=[
                        client.V1Container(
                            name=dns1123_slug(resolved.name),
                            image=image,
                            image_pull_policy="IfNotPresent",
                            command=argv,
                            env=env,
                            env_from=env_from or None,
                            volume_mounts=mounts or None,
                            resources=client.V1ResourceRequirements(
                                requests=resolved.resources.requests.model_dump(),
                                limits=resolved.resources.limits.model_dump(),
                            ),
                        )
                    ],
                ),
            ),
        ),
    )


def job_exists(name):
    """True or False when the apiserver answers, None when it can't say."""
    try:
        batch_v1_api().read_namespaced_job(
            name, K8S_NAMESPACE, _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS
        )
        return True
    except ApiException as exc:
        if exc.status == 404:
            return False
        logger.warning("Could not check k8s Job %s: %s", name, exc)
    except Exception as exc:
        logger.warning("Could not check k8s Job %s: %s", name, exc)
    return None


def create_job(job):
    """Creates the Job. AlreadyExists means a previous launcher created it for this attempt
    before crashing, so it counts as launched rather than failing the run."""
    try:
        batch_v1_api().create_namespaced_job(
            K8S_NAMESPACE, job, _request_timeout=K8S_REQUEST_TIMEOUT_SECONDS
        )
    except ApiException as exc:
        if exc.status != 409:
            raise
        logger.warning("k8s Job %s already exists; treating it as launched", job.metadata.name)
