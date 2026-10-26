from __future__ import annotations

import subprocess
import sys

import pytest
from pydantic import ValidationError

from src.launcher.catalog import Catalog, load_catalog


FIXTURE = "tests/fixtures/job-catalog.yaml"


def catalog_data() -> dict:
    return {
        "version": 1,
        "global": {
            "max_concurrent_pods": 2,
            "max_deadline": "2h",
            "start_timeout": "10m",
            "manual_queue_ttl": "30m",
        },
        "profiles": {"core": {"image": "core"}},
        "jobs": {"one": {"profile": "core", "command": ["python", "-m", "main"]}},
    }


def test_current_catalog_fixture_loads_with_sixteen_jobs():
    catalog = load_catalog(FIXTURE)
    assert len(catalog.jobs) == 16


@pytest.mark.parametrize(
    "field,value",
    [("cron", "61 * * * *"), ("cron", "* * * *"), ("tz", "Mars/Olympus_Mons")],
)
def test_schedules_reject_invalid_cron_and_timezone(field, value):
    raw = catalog_data()
    raw["jobs"]["one"]["schedules"] = [{"cron": "* * * * *", "tz": "UTC", field: value}]
    with pytest.raises(ValidationError):
        Catalog.model_validate(raw)


def test_job_deadline_cannot_exceed_global_maximum():
    raw = catalog_data()
    raw["jobs"]["one"]["deadline"] = "3h"
    with pytest.raises(ValidationError, match="deadline exceeds global.max_deadline"):
        Catalog.model_validate(raw)


def test_unknown_profile_and_season_are_rejected():
    raw = catalog_data()
    raw["jobs"]["one"]["profile"] = "missing"
    with pytest.raises(ValidationError, match="unknown profile"):
        Catalog.model_validate(raw)

    raw = catalog_data()
    raw["jobs"]["one"]["schedules"] = [
        {"cron": "0 0 * * *", "tz": "UTC", "seasons": ["missing"]}
    ]
    with pytest.raises(ValidationError, match="unknown season"):
        Catalog.model_validate(raw)


def test_chain_rejects_unknown_job_and_dependency():
    raw = catalog_data()
    raw["chains"] = {
        "chain": {
            "max_duration": "1h",
            "steps": {"one": {"after": [{"step": "missing"}]}},
        }
    }
    with pytest.raises(ValidationError, match="unknown step"):
        Catalog.model_validate(raw)
    raw["chains"]["chain"]["steps"] = {"missing-job": {}}
    with pytest.raises(ValidationError, match="unknown job"):
        Catalog.model_validate(raw)


def test_chain_rejects_dependency_cycles():
    raw = catalog_data()
    raw["jobs"]["two"] = {"profile": "core", "command": ["python", "two"]}
    raw["chains"] = {
        "chain": {
            "max_duration": "1h",
            "steps": {
                "one": {"after": [{"step": "two"}]},
                "two": {"after": [{"step": "one"}]},
            },
        }
    }
    with pytest.raises(ValidationError, match="contains a cycle"):
        Catalog.model_validate(raw)


@pytest.mark.parametrize("type_name", ["list", "int??", "object"])
def test_params_reject_unknown_types(type_name):
    raw = catalog_data()
    raw["jobs"]["one"]["params"] = {"value": type_name}
    with pytest.raises(ValidationError, match="unsupported params type"):
        Catalog.model_validate(raw)


@pytest.mark.parametrize("field", ["class", "group", "fan_out", "approval"])
def test_v2_fields_fail_with_not_implemented_error(field):
    raw = catalog_data()
    raw["jobs"]["one"][field] = True
    with pytest.raises(ValueError, match="not implemented"):
        Catalog.model_validate(raw)


def test_json_schema_and_check_cli():
    schema = subprocess.run(
        [sys.executable, "-m", "src.launcher.catalog", "--json-schema"],
        capture_output=True,
        text=True,
    )
    assert schema.returncode == 0
    assert '"global"' in schema.stdout
    checked = subprocess.run(
        [sys.executable, "-m", "src.launcher.catalog", "--check", FIXTURE],
        capture_output=True,
        text=True,
    )
    assert checked.returncode == 0, checked.stderr
    assert "catalog valid" in checked.stdout
