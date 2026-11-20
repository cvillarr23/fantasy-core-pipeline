"""Kubernetes API clients shared by the draft control plane (read-only) and the job launcher."""

import os
import re

from kubernetes import client, config
from kubernetes.config.config_exception import ConfigException

K8S_NAMESPACE = os.getenv("K8S_NAMESPACE", "default")

# Kubernetes calls are made from synchronous FastAPI endpoints (a bounded threadpool) and from
# the launcher's single consumer loop. The kubernetes client applies no timeout of its own, so a
# control plane that accepts the TCP connection but never answers - the exact failure mode of an
# overloaded apiserver - parks the caller indefinitely. Enough parked API threads and every sync
# endpoint stops responding, the liveness probe starts failing, and a degraded control plane
# becomes a hard outage of the whole API. Bounded so those calls fail fast instead of hanging.
K8S_REQUEST_TIMEOUT_SECONDS = 10


def dns1123_slug(value: str) -> str:
    """Job `metadata.name` must be a lowercase RFC 1123 DNS subdomain, and a catalog job name
    or other caller-chosen string doesn't have to be one, so this sanitizes only the name."""
    slug = re.sub(r"[^a-z0-9-]", "-", value.lower()).strip("-")
    return slug or "league"


def _load_config() -> None:
    """in-cluster config in a deployed pod, falls back to local kubeconfig for dev."""
    try:
        config.load_incluster_config()
    except ConfigException:
        config.load_kube_config()


def batch_v1_api() -> client.BatchV1Api:
    _load_config()
    return client.BatchV1Api()


def core_v1_api() -> client.CoreV1Api:
    """Same in-cluster/kubeconfig fallback as batch_v1_api, for pod (not Job) reads."""
    _load_config()
    return client.CoreV1Api()


def coordination_v1_api() -> client.CoordinationV1Api:
    """For the launcher's leader-election Lease."""
    _load_config()
    return client.CoordinationV1Api()
