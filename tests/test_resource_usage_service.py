from contextlib import contextmanager
from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.api.jobs import jobs_router
from src.services import jobs_service, resource_usage_service as service
from tests.test_jobs_api import catalog  # noqa: F401 (fixture)

MIB = 2**20
NOW = datetime(2026, 11, 16, tzinfo=timezone.utc)


class Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self.rows


def patch_db(monkeypatch, rows):
    cursor = Cursor(rows)

    @contextmanager
    def db_cursor():
        yield cursor

    monkeypatch.setattr(service, "get_db_cursor", db_cursor)
    return cursor


def test_percentile_interpolates_linearly():
    assert service.percentile([5], 0.95) == 5
    assert service.percentile([1, 2, 3, 4], 0.5) == 2.5
    assert service.percentile(list(range(1, 21)), 0.95) == pytest.approx(19.05)


def test_usage_stats_derives_average_cores_and_throttled_share():
    stats = service.usage_stats([(60.0, 120.0, 100.0, 12.0), (30.0, 60.0, 300.0, None)])
    assert stats["avg_cpu_cores"] == {"p50": 0.5, "p95": 0.5, "max": 0.5}
    assert stats["peak_memory_bytes"]["max"] == 300.0
    assert stats["throttled_fraction"] == {"p50": 0.1, "p95": 0.1, "max": 0.1}


def usage(avg_p95=0.123, memory_p95=200 * MIB, memory_max=300 * MIB, throttled=0.0):
    return {
        "cpu_seconds": None,
        "avg_cpu_cores": {"p50": avg_p95, "p95": avg_p95, "max": avg_p95},
        "peak_memory_bytes": {"p50": memory_p95, "p95": memory_p95, "max": memory_max},
        "throttled_fraction": {"p50": throttled, "p95": throttled, "max": throttled},
    }


def test_suggestion_applies_headroom_and_rounding():
    assert service.suggested_resources(usage(), "500m") == {
        # 0.123 cores -> 130m; 200Mi x 1.2 = 240Mi
        "requests": {"cpu": "130m", "memory": "240Mi"},
        # 0.123 x 3 = 369m -> 400m; 300Mi x 1.5 = 450Mi -> 464Mi
        "limits": {"cpu": "400m", "memory": "464Mi"},
        "cpu_throttled": False,
    }


def test_a_throttling_cpu_limit_is_never_lowered():
    suggested = service.suggested_resources(usage(throttled=0.2), "1")
    assert suggested["limits"]["cpu"] == "1000m" and suggested["cpu_throttled"]


def test_suggestion_keeps_floors_for_idle_jobs():
    idle = usage(avg_p95=0.0001, memory_p95=MIB, memory_max=MIB)
    idle["throttled_fraction"] = None
    assert service.suggested_resources(idle, "500m") == {
        "requests": {"cpu": "10m", "memory": "16Mi"},
        "limits": {"cpu": "100m", "memory": "16Mi"},
        "cpu_throttled": False,
    }


def test_cpu_millicores_parses_catalog_quantities():
    assert [service.cpu_millicores(q) for q in ("500m", "1", "1.5")] == [500, 1000, 1500]


def test_suggest_resources_reads_successful_measured_runs_since_the_window(
    catalog, monkeypatch
):
    cursor = patch_db(monkeypatch, [("sample-job", 10.0, 100.0, 50 * MIB, 0.0)] * 3)
    report = service.suggest_resources(catalog, days=7, min_runs=3, now=NOW)
    query, (names, since) = cursor.executed[0]
    assert "status='success'" in query and "executor='pod'" in query
    assert names == ["sample-job"] and since == datetime(2026, 11, 9, tzinfo=timezone.utc)
    (job,) = report["jobs"]
    assert job["runs"] == 3 and job["reason"] is None
    assert job["current"] == catalog.resolve("sample-job").resources.model_dump()
    assert job["suggested"]["requests"] == {"cpu": "100m", "memory": "64Mi"}


def test_suggest_resources_needs_min_runs(catalog, monkeypatch):
    patch_db(monkeypatch, [("sample-job", 10.0, 100.0, 50 * MIB, 0.0)])
    (job,) = service.suggest_resources(catalog, days=30, min_runs=2)["jobs"]
    assert job["suggested"] is None and job["reason"] == "insufficient_runs"
    assert job["usage"]["cpu_seconds"]["max"] == 10.0


def test_suggest_resources_without_runs(catalog, monkeypatch):
    patch_db(monkeypatch, [])
    (job,) = service.suggest_resources(catalog, days=30, min_runs=2)["jobs"]
    assert job["usage"] is None and job["reason"] == "no_measured_runs"


@pytest.fixture
def client(catalog, monkeypatch):
    monkeypatch.setattr(jobs_service, "_catalog", lambda: catalog)
    app = FastAPI()
    app.include_router(jobs_router, prefix="/v1")
    return TestClient(app)


def test_route_returns_the_report(client, monkeypatch):
    patch_db(monkeypatch, [("sample-job", 10.0, 100.0, 50 * MIB, 0.0)] * 5)
    response = client.get("/v1/jobs/resource-suggestions?days=14")
    assert response.status_code == 200
    body = response.json()
    assert body["days"] == 14 and body["min_runs"] == 5
    assert body["jobs"][0]["suggested"]["limits"] == {"cpu": "300m", "memory": "80Mi"}


def test_route_validates_its_window(client):
    assert client.get("/v1/jobs/resource-suggestions?days=0").status_code == 422


def test_route_503s_without_a_catalog(client, monkeypatch):
    def missing():
        raise jobs_service.CatalogUnavailableError("catalog is not mounted")

    monkeypatch.setattr(jobs_service, "_catalog", missing)
    assert client.get("/v1/jobs/resource-suggestions").status_code == 503
