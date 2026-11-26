from __future__ import annotations

import subprocess
import sys
from datetime import timedelta

import pytest
from pydantic import ValidationError

from src.launcher.catalog import (
    Catalog,
    UnknownJobError,
    fans_out,
    health_key,
    load_catalog,
    load_catalog_from_env,
    params_to_args,
    split_params,
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


def test_a_job_level_approval_key_is_rejected():
    raw = catalog_data()
    raw["jobs"]["one"]["approval"] = True
    with pytest.raises(ValidationError, match="approval"):
        Catalog.model_validate(raw)


def approval_chain(step="confirm") -> dict:
    raw = catalog_data()
    raw["jobs"]["two"] = dict(raw["jobs"]["one"])
    raw["jobs"]["three"] = dict(raw["jobs"]["one"])
    raw["chains"] = {
        "daily": {
            "max_duration": "6h",
            "steps": {
                "one": {},
                step: {
                    "approval": {"timeout": "2h", "title": "Go ahead?"},
                    "after": [{"step": "one"}],
                },
                "two": {"after": [{"step": step}]},
            },
        }
    }
    return raw


def test_an_approval_step_parses():
    catalog = Catalog.model_validate(approval_chain())
    approval = catalog.chains["daily"].steps["confirm"].approval
    assert approval.timeout == timedelta(hours=2)
    assert approval.title == "Go ahead?"
    assert catalog.chains["daily"].steps["one"].approval is None


def test_an_approval_step_cannot_share_a_job_name():
    with pytest.raises(ValidationError, match="can't share a job's name"):
        Catalog.model_validate(approval_chain(step="three"))


def test_a_step_without_approval_must_still_name_a_job():
    raw = approval_chain()
    del raw["chains"]["daily"]["steps"]["confirm"]["approval"]
    with pytest.raises(ValidationError, match="unknown job 'confirm'"):
        Catalog.model_validate(raw)


def grouped_catalog(limit=1, group="espn-session") -> dict:
    raw = catalog_data()
    raw["groups"] = {"espn-session": {"limit": limit}}
    raw["jobs"]["two"] = {**raw["jobs"]["one"], "group": group}
    raw["jobs"]["three"] = {**raw["jobs"]["one"], "group": group}
    return raw


def test_jobs_in_a_group_resolve_with_it():
    catalog = Catalog.model_validate(grouped_catalog())
    assert catalog.groups["espn-session"].limit == 1
    assert catalog.resolve("two").group == "espn-session"
    assert catalog.resolve("one").group is None
    assert catalog.group_jobs("espn-session") == ["two", "three"]


def test_job_group_must_be_defined():
    with pytest.raises(ValidationError, match="unknown group 'other'"):
        Catalog.model_validate(grouped_catalog(group="other"))


def test_group_limit_must_be_positive():
    with pytest.raises(ValidationError, match="limit"):
        Catalog.model_validate(grouped_catalog(limit=0))


def fan_out_catalog(fan_out: dict, params: dict | None = None) -> dict:
    raw = catalog_data()
    raw["jobs"]["one"]["params"] = {"week_offset": "int?"} if params is None else params
    raw["jobs"]["one"]["fan_out"] = fan_out
    return raw


def test_fan_out_runs_without_its_param_fan_out():
    catalog = Catalog.model_validate(
        fan_out_catalog({"param": "week_offset", "values": [0, 1, 2, 3]})
    )
    job = catalog.jobs["one"]
    assert job.fan_out.values == [0, 1, 2, 3]
    assert fans_out(job, {})
    assert not fans_out(job, {"week_offset": 2})
    assert not fans_out(catalog_data_job(), {})


def catalog_data_job():
    return Catalog.model_validate(catalog_data()).jobs["one"]


@pytest.mark.parametrize(
    ("fan_out", "params", "message"),
    [
        ({"param": "week", "values": [1]}, None, "not one of the job's params"),
        ({"param": "week_offset", "values": [1]}, {"week_offset": "int"}, "optional"),
        ({"param": "week_offset", "values": []}, None, "at least 1"),
        ({"param": "week_offset", "values": [1, 1]}, None, "unique"),
        ({"param": "week_offset", "values": [1, "two"]}, None, "must be int"),
    ],
)
def test_fan_out_rejects_bad_param_or_values(fan_out, params, message):
    with pytest.raises(ValidationError, match=message):
        Catalog.model_validate(fan_out_catalog(fan_out, params))


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


@pytest.mark.parametrize("key", ["rumor.*", "rumor.#", "rumor", "Rumor.Created", ""])
def test_on_event_key_must_be_an_exact_routing_key(key):
    raw = catalog_data()
    raw["jobs"]["one"]["on_event"] = [{"key": key, "debounce": "30m"}]
    with pytest.raises(ValidationError, match="key"):
        Catalog.model_validate(raw)


def test_on_event_job_cannot_require_params():
    raw = catalog_data()
    raw["jobs"]["one"]["on_event"] = [{"key": "rumor.created", "debounce": "30m"}]
    raw["jobs"]["one"]["params"] = {"week": "int"}
    with pytest.raises(ValidationError, match="jobs.one.on_event"):
        Catalog.model_validate(raw)

    raw["jobs"]["one"]["params"] = {"week": "int?"}
    assert Catalog.model_validate(raw).jobs["one"].on_event[0].key == "rumor.created"


def test_counted_k8s_jobs_defaults_empty_and_accepts_label_selectors():
    assert Catalog.model_validate(catalog_data()).global_settings.counted_k8s_jobs == []
    data = catalog_data()
    data["global"]["counted_k8s_jobs"] = ["scaledjob.keda.sh/name=worker"]
    catalog = Catalog.model_validate(data)
    assert catalog.global_settings.counted_k8s_jobs == ["scaledjob.keda.sh/name=worker"]


def service_catalog(**job) -> dict:
    raw = catalog_data()
    raw["global"]["max_service_deadline"] = "8h"
    raw["jobs"]["draft"] = {
        "profile": "core",
        "command": ["python", "-m", "listener"],
        "class": "service",
        "deadline": "6h",
        **job,
    }
    return raw


def test_service_jobs_resolve_with_their_class_and_own_deadline_cap():
    catalog = Catalog.model_validate(service_catalog())
    assert catalog.resolve("draft").job_class == "service"
    assert catalog.resolve("draft").deadline == timedelta(hours=6)
    assert catalog.resolve("one").job_class == "batch"
    assert catalog.service_jobs() == ["draft"]


def test_service_deadline_is_capped_by_max_service_deadline():
    with pytest.raises(ValidationError, match="max_service_deadline"):
        Catalog.model_validate(service_catalog(deadline="9h"))


def test_service_deadline_cap_defaults_to_max_deadline():
    raw = service_catalog()
    raw["global"].pop("max_service_deadline")
    with pytest.raises(ValidationError, match="max_service_deadline"):
        Catalog.model_validate(raw)


def test_batch_deadline_is_still_capped_by_max_deadline():
    raw = service_catalog()
    raw["jobs"]["one"]["deadline"] = "3h"
    with pytest.raises(ValidationError, match="exceeds global.max_deadline"):
        Catalog.model_validate(raw)


def test_unknown_class_is_rejected():
    with pytest.raises(ValidationError, match="class"):
        Catalog.model_validate(service_catalog(**{"class": "daemon"}))


@pytest.mark.parametrize(
    ("job", "message"),
    [
        ({"executor": "inline"}, "pod executor"),
        (
            {"params": {"week": "int?"}, "fan_out": {"param": "week", "values": [1]}},
            "cannot fan out",
        ),
    ],
)
def test_service_jobs_run_as_single_pods(job, message):
    with pytest.raises(ValidationError, match=message):
        Catalog.model_validate(service_catalog(**job))


def test_param_env_moves_params_out_of_argv():
    catalog = Catalog.model_validate(
        service_catalog(
            params={"league_id": "str", "room_url": "str", "clock": "int?"},
            param_env={"room_url": "DRAFT_ROOM_URL", "clock": "DRAFT_PICK_CLOCK_SECONDS"},
        )
    )
    job = catalog.jobs["draft"]
    argv_params, env = split_params(
        job, {"league_id": "42", "room_url": "https://x/y", "clock": 30}
    )
    assert argv_params == {"league_id": "42"}
    assert env == {"DRAFT_ROOM_URL": "https://x/y", "DRAFT_PICK_CLOCK_SECONDS": "30"}
    assert params_to_args(argv_params) == ["--league-id", "42"]
    assert split_params(job, {"league_id": "42", "room_url": "u"})[1] == {
        "DRAFT_ROOM_URL": "u"
    }


@pytest.mark.parametrize(
    ("param_env", "message"),
    [
        ({"missing": "X"}, "not one of the job's params"),
        ({"room_url": "lower"}, "env var name"),
    ],
)
def test_param_env_must_name_params_and_valid_env_vars(param_env, message):
    with pytest.raises(ValidationError, match=message):
        Catalog.model_validate(
            service_catalog(params={"room_url": "str"}, param_env=param_env)
        )


def test_health_key_formats_from_params_and_skips_runs_without_them():
    catalog = Catalog.model_validate(
        service_catalog(
            params={"league_id": "str?"},
            health={"key": "draft:{league_id}:health", "grace": "10m"},
        )
    )
    job = catalog.jobs["draft"]
    assert job.health.grace == timedelta(minutes=10)
    assert health_key(job, {"league_id": "42"}) == "draft:42:health"
    assert health_key(job, {}) is None
    assert health_key(catalog.jobs["one"], {"league_id": "42"}) is None


@pytest.mark.parametrize(
    ("job", "message"),
    [
        (
            {"params": {"league_id": "str"}, "health": {"key": "draft:{team}:x", "grace": "1m"}},
            "not one of the job's params",
        ),
        (
            {
                "class": "batch",
                "deadline": "1h",
                "params": {"league_id": "str"},
                "health": {"key": "draft:{league_id}", "grace": "1m"},
            },
            "only service jobs",
        ),
    ],
)
def test_health_must_use_job_params_on_a_service_job(job, message):
    with pytest.raises(ValidationError, match=message):
        Catalog.model_validate(service_catalog(**job))


def kickoff_catalog(kickoffs: dict, params: dict | None = None) -> dict:
    raw = catalog_data()
    raw["seasons"] = {"regular": {"start": 1, "end": 18}}
    raw["jobs"]["one"]["kickoffs"] = kickoffs
    if params:
        raw["jobs"]["one"]["params"] = params
    return raw


def test_a_kickoffs_block_parses_with_defaults():
    catalog = Catalog.model_validate(
        kickoff_catalog({"lead": "20m", "seasons": ["regular"]})
    )
    kickoffs = catalog.jobs["one"].kickoffs
    assert kickoffs.lead == timedelta(minutes=20)
    assert kickoffs.catch_up == timedelta(hours=3)
    assert kickoffs.fallback_every == timedelta(minutes=30)


def test_kickoff_lead_matches_the_live_pollers_lookahead():
    from src.ingestion.live_games import KICKOFF_LOOKAHEAD
    from src.launcher.catalog import MAX_KICKOFF_LEAD

    assert MAX_KICKOFF_LEAD == KICKOFF_LOOKAHEAD


@pytest.mark.parametrize(
    "kickoffs, params, message",
    [
        ({"seasons": ["regular"]}, None, "kickoffs.lead\n  Field required"),
        ({"lead": "31m", "seasons": ["regular"]}, None, "lead must be at most"),
        ({"lead": "20m", "seasons": []}, None, "at least 1"),
        ({"lead": "20m", "seasons": ["missing"]}, None, "unknown season"),
        ({"lead": "20m", "seasons": ["regular"]}, {"week": "int"}, "requires week"),
        ({"lead": "0m", "seasons": ["regular"]}, None, "positive"),
    ],
)
def test_kickoffs_reject_bad_settings(kickoffs, params, message):
    with pytest.raises(ValidationError, match=message):
        Catalog.model_validate(kickoff_catalog(kickoffs, params))
