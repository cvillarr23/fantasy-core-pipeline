from __future__ import annotations

import subprocess
import sys
from datetime import timedelta

import pytest
from pydantic import ValidationError

from src.launcher.catalog import (
    Catalog,
    UnknownJobError,
    load_catalog,
    load_catalog_from_env,
    params_to_args,
    validate_run_params,
)


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


# Every production CronJob schedule (fantasy-infra base/) and the job that now owns it.
CRONJOB_SCHEDULES = {
    ("rumor-syncer", "*/30 * * * *"),
    ("rapid-status-syncer", "*/10 * * * 4,0,1"),
    ("stale-draft-job-sweeper", "*/15 * * * *"),
    ("projections-refresh", "0 18 * * *"),
    ("rumor-action-trigger", "45 21 * * *"),
    ("sync-actual-scores", "0 10 * * 5"),
    ("trade-candidates", "0 7 * * *"),
    ("trade-candidates", "30 12 * * 3,5"),
    ("espn-session-healthcheck", "0 6 * * 1"),
    ("rumor-digest-bot", "0 11 * * *"),
    ("trade-inbox", "0 8-22/2 * * *"),
    ("trade-scan", "0 13 * * 3,5"),
}
# weekly-ingestor, sync-actual-scores (Tuesday), lineup-optimizer and weekly-waiver-trigger
# ran at clock offsets; the chain now orders them.
TUESDAY_STEPS = [
    "sync-league",
    "metrics-extractor",
    "sync-actual-scores",
    "projections-refresh",
    "lineup-optimizer",
    "waiver-trigger",
    "rumor-action-trigger",
]


def test_fixture_mirrors_every_production_cronjob():
    catalog = load_catalog(FIXTURE)
    scheduled = {
        (name, schedule.cron)
        for name, job in catalog.jobs.items()
        for schedule in job.schedules
    }
    assert scheduled == CRONJOB_SCHEDULES
    tuesday = catalog.chains["tuesday-weekly"]
    assert tuesday.schedule.cron == "0 16 * * 2"
    assert list(tuesday.steps) == TUESDAY_STEPS
    assert catalog.jobs["trade-scan"].max_attempts is None  # defaults to 1: never auto-retry
    assert catalog.jobs["trade-inbox"].resources.limits.memory == "1200Mi"


def test_catch_up_zero_means_never_fire_a_missed_slot():
    catalog = load_catalog(FIXTURE)
    assert catalog.jobs["rumor-syncer"].schedules[0].catch_up == timedelta(0)
    raw = catalog_data()
    raw["jobs"]["one"]["schedules"] = [
        {"cron": "* * * * *", "tz": "UTC", "catch_up": "-1m"}
    ]
    with pytest.raises(ValidationError):
        Catalog.model_validate(raw)


def test_resources_default_to_the_core_cronjob_pod_spec():
    defaults = Catalog.model_validate(catalog_data()).defaults.resources
    assert (defaults.requests.cpu, defaults.requests.memory) == ("100m", "256Mi")
    assert (defaults.limits.cpu, defaults.limits.memory) == ("500m", "512Mi")


@pytest.mark.parametrize("latest", ["7:15", "24:00", "19:60", "evening"])
def test_chain_latest_must_be_hh_mm(latest):
    raw = catalog_data()
    raw["chains"] = {
        "c": {"max_duration": "1h", "steps": {"one": {"latest": latest}}}
    }
    with pytest.raises(ValidationError, match="HH:MM"):
        Catalog.model_validate(raw)


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


def test_resolve_applies_defaults_and_attaches_the_profile():
    catalog = load_catalog(FIXTURE)
    lineup = catalog.resolve("lineup-optimizer")
    assert (lineup.executor, lineup.overlap, lineup.max_attempts) == ("pod", "forbid", 3)
    assert lineup.deadline == timedelta(hours=1)
    assert lineup.alert == "critical"
    assert lineup.resources.limits.memory == "512Mi"
    assert lineup.profile.image == "CORE_IMAGE"
    scan = catalog.resolve("trade-scan")
    assert scan.deadline == timedelta(minutes=16)
    assert scan.resources.limits.memory == "1200Mi"
    assert scan.profile.serviceAccount == "fantasy-browser-worker"
    with pytest.raises(UnknownJobError):
        catalog.resolve("missing")


def test_validate_run_params_types_and_optionality():
    raw = catalog_data()
    raw["jobs"]["one"]["params"] = {"week": "int?", "ratio": "float", "dry_run": "bool?", "team": "str?"}
    job = Catalog.model_validate(raw).jobs["one"]
    assert validate_run_params(job, {"ratio": 1, "week": None, "dry_run": False}) == {
        "ratio": 1.0,
        "dry_run": False,
    }
    with pytest.raises(ValueError, match="'ratio' is required"):
        validate_run_params(job, {})
    with pytest.raises(ValueError, match="unknown params: season"):
        validate_run_params(job, {"ratio": 1, "season": 2026})
    with pytest.raises(ValueError, match="'week' must be int"):
        validate_run_params(job, {"ratio": 1, "week": True})
    with pytest.raises(ValueError, match="'team' must be str"):
        validate_run_params(job, {"ratio": 1, "team": 7})


def test_params_cannot_expand_environment_references():
    assert params_to_args({"team": "$(DB_PASSWORD)"}) == ["--team", "$$(DB_PASSWORD)"]


def test_params_to_args():
    assert params_to_args({"week": 7, "dry_run": True, "verbose": False, "team_id": "3"}) == [
        "--week", "7", "--dry-run", "--team-id", "3",
    ]


def test_load_catalog_from_env(monkeypatch):
    monkeypatch.setenv("JOB_CATALOG_PATH", FIXTURE)
    assert "lineup-optimizer" in load_catalog_from_env().jobs


def test_season_window_must_be_ordered_weeks():
    raw = catalog_data()
    raw["seasons"] = {"regular": {"start": 18, "end": 1}}
    with pytest.raises(ValidationError, match="start must not be after its end"):
        Catalog.model_validate(raw)

    raw["seasons"] = {"regular": {"start": 1, "end": 18, "extra": True}}
    with pytest.raises(ValidationError, match="extra"):
        Catalog.model_validate(raw)
