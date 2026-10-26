"""Validated job catalog schema and command-line validation tools."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)


class _CatalogYamlLoader(yaml.SafeLoader):
    """Use YAML 1.2 boolean rules so v1 fields such as ``on`` remain strings."""


_CatalogYamlLoader.yaml_implicit_resolvers = {
    key: [
        (tag, pattern) for tag, pattern in resolvers if tag != "tag:yaml.org,2002:bool"
    ]
    for key, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
_CatalogYamlLoader.add_implicit_resolver(
    "tag:yaml.org,2002:bool",
    re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"),
    list("tTfF"),
)


class CatalogModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _duration(value: str | int | float, allow_zero: bool = False) -> timedelta:
    if isinstance(value, (int, float)):
        if value < 0 or (value == 0 and not allow_zero):
            raise ValueError("duration must be positive")
        return timedelta(seconds=value)
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(s|m|h|d)\s*", value)
    if not match:
        raise ValueError(
            "duration must use seconds, minutes, hours, or days (for example '30m')"
        )
    amount = float(match.group(1))
    if amount == 0 and not allow_zero:
        raise ValueError("duration must be positive")
    return timedelta(
        seconds=amount * {"s": 1, "m": 60, "h": 3600, "d": 86400}[match.group(2)]
    )


def _cron_field_valid(field: str, minimum: int, maximum: int) -> bool:
    if field == "*":
        return True
    for item in field.split(","):
        base, slash, step = item.partition("/")
        if slash and (not step.isdigit() or int(step) < 1):
            return False
        if base == "*":
            continue
        if "-" in base:
            bounds = base.split("-")
            if len(bounds) != 2 or not all(part.isdigit() for part in bounds):
                return False
            start, end = map(int, bounds)
            if start > end or start < minimum or end > maximum:
                return False
        elif base.isdigit():
            if not minimum <= int(base) <= maximum:
                return False
        else:
            return False
    return True


def _validate_cron(value: str) -> str:
    fields = value.split()
    limits = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))
    if len(fields) != 5 or not all(
        _cron_field_valid(field, *bounds) for field, bounds in zip(fields, limits)
    ):
        raise ValueError(f"invalid 5-field cron expression: {value!r}")
    return value


class GlobalSettings(CatalogModel):
    max_concurrent_pods: int = Field(ge=1)
    max_deadline: timedelta
    start_timeout: timedelta
    manual_queue_ttl: timedelta

    @field_validator("max_deadline", "start_timeout", "manual_queue_ttl", mode="before")
    @classmethod
    def parse_durations(cls, value: Any) -> timedelta:
        return _duration(value)


class Season(CatalogModel):
    """A window of NFL regular-season weeks; the scheduler maps it to game dates."""

    start: int = Field(ge=1, le=22)
    end: int = Field(ge=1, le=22)

    @model_validator(mode="after")
    def validate_order(self) -> "Season":
        if self.start > self.end:
            raise ValueError("season start must not be after its end")
        return self


class Profile(CatalogModel):
    image: str
    secrets: list[str] = Field(default_factory=list)
    serviceAccount: str | None = None
    volumes: list[str] = Field(default_factory=list)


class ResourceSpec(CatalogModel):
    cpu: str
    memory: str


class Resources(CatalogModel):
    """Kubernetes container requests and limits for a job's pod."""

    requests: ResourceSpec
    limits: ResourceSpec


class Defaults(CatalogModel):
    executor: Literal["pod", "inline"] = "pod"
    overlap: Literal["forbid", "queue"] = "forbid"
    max_attempts: int = Field(default=1, ge=1)
    deadline: timedelta = timedelta(hours=1)
    alert: Literal["none", "warning", "critical"] = "warning"
    resources: Resources = Resources(
        requests=ResourceSpec(cpu="100m", memory="256Mi"),
        limits=ResourceSpec(cpu="500m", memory="512Mi"),
    )

    @field_validator("deadline", mode="before")
    @classmethod
    def parse_deadline(cls, value: Any) -> timedelta:
        return _duration(value)


class Schedule(CatalogModel):
    cron: str
    tz: str
    catch_up: timedelta | None = None
    seasons: list[str] = Field(default_factory=list)

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, value: str) -> str:
        return _validate_cron(value)

    @field_validator("tz")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(f"unknown timezone: {value}") from exc
        return value

    @field_validator("catch_up", mode="before")
    @classmethod
    def parse_catch_up(cls, value: Any) -> timedelta | None:
        # 0 means "never fire a missed slot", which the frequent syncers use.
        return None if value is None else _duration(value, allow_zero=True)


class EventSubscription(CatalogModel):
    key: str
    debounce: timedelta

    @field_validator("debounce", mode="before")
    @classmethod
    def parse_debounce(cls, value: Any) -> timedelta:
        return _duration(value)


_PARAM_TYPES = {"int", "integer", "float", "number", "str", "string", "bool", "boolean"}


def _validate_params(value: dict[str, str]) -> dict[str, str]:
    for name, type_name in value.items():
        base = type_name[:-1] if type_name.endswith("?") else type_name
        if base not in _PARAM_TYPES:
            raise ValueError(f"unsupported params type for {name!r}: {type_name!r}")
    return value


class Job(CatalogModel):
    profile: str
    command: list[str] = Field(min_length=1)
    params: dict[str, str] = Field(default_factory=dict)
    executor: Literal["pod", "inline"] | None = None
    overlap: Literal["forbid", "queue"] | None = None
    max_attempts: int | None = Field(default=None, ge=1)
    deadline: timedelta | None = None
    alert: Literal["none", "warning", "critical"] | None = None
    resources: Resources | None = None
    acts: bool = False
    schedules: list[Schedule] = Field(default_factory=list)
    on_event: list[EventSubscription] = Field(default_factory=list)

    @field_validator("params")
    @classmethod
    def validate_params(cls, value: dict[str, str]) -> dict[str, str]:
        return _validate_params(value)

    @field_validator("deadline", mode="before")
    @classmethod
    def parse_job_deadline(cls, value: Any) -> timedelta | None:
        return None if value is None else _duration(value)


class Dependency(CatalogModel):
    step: str
    on: Literal["success", "finish"] = "success"


class ChainStep(CatalogModel):
    after: list[Dependency] = Field(default_factory=list)
    latest: str | None = None

    @field_validator("latest")
    @classmethod
    def validate_latest(cls, value: str | None) -> str | None:
        if value is not None and not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", value):
            raise ValueError(f"latest must be HH:MM in the chain's timezone: {value!r}")
        return value


class Chain(CatalogModel):
    schedule: Schedule | None = None
    max_duration: timedelta
    steps: dict[str, ChainStep]

    @field_validator("max_duration", mode="before")
    @classmethod
    def parse_max_duration(cls, value: Any) -> timedelta:
        return _duration(value)


_RESERVED = {"class", "group", "fan_out", "approval"}


def _reject_reserved(value: Any, path: str = "catalog") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key in _RESERVED:
                raise ValueError(f"{path}.{key}: not implemented (reserved for v2)")
            _reject_reserved(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_reserved(child, f"{path}[{index}]")


class UnknownJobError(ValueError):
    """The catalog has no job by that name."""


@dataclass(frozen=True)
class ResolvedJob:
    """A catalog job with `defaults` applied and its profile attached."""

    name: str
    job: Job
    profile: Profile
    executor: str
    overlap: str
    max_attempts: int
    deadline: timedelta
    alert: str
    resources: Resources


class Catalog(CatalogModel):
    version: Literal[1]
    global_settings: GlobalSettings = Field(alias="global")
    seasons: dict[str, Season] = Field(default_factory=dict)
    profiles: dict[str, Profile]
    defaults: Defaults = Field(default_factory=Defaults)
    jobs: dict[str, Job]
    chains: dict[str, Chain] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def reject_unimplemented_fields(cls, value: Any) -> Any:
        _reject_reserved(value)
        return value

    @model_validator(mode="after")
    def validate_references(self) -> "Catalog":
        max_deadline = self.global_settings.max_deadline
        for name, job in self.jobs.items():
            if job.profile not in self.profiles:
                raise ValueError(
                    f"jobs.{name}.profile references unknown profile {job.profile!r}"
                )
            deadline = job.deadline or self.defaults.deadline
            if deadline > max_deadline:
                raise ValueError(f"jobs.{name}.deadline exceeds global.max_deadline")
            for schedule in job.schedules:
                self._validate_schedule_seasons(schedule, f"jobs.{name}.schedules")
        for name, chain in self.chains.items():
            if chain.schedule:
                self._validate_schedule_seasons(
                    chain.schedule, f"chains.{name}.schedule"
                )
            step_names = set(chain.steps)
            graph: dict[str, list[str]] = {step: [] for step in step_names}
            for step_name, step in chain.steps.items():
                if step_name not in self.jobs:
                    raise ValueError(
                        f"chains.{name}.steps references unknown job {step_name!r}"
                    )
                for dependency in step.after:
                    if dependency.step not in step_names:
                        raise ValueError(
                            f"chains.{name}.steps.{step_name}.after references unknown step {dependency.step!r}"
                        )
                    graph[step_name].append(dependency.step)
            visiting: set[str] = set()
            visited: set[str] = set()

            def visit(step: str) -> None:
                if step in visiting:
                    raise ValueError(f"chains.{name} dependency graph contains a cycle")
                if step in visited:
                    return
                visiting.add(step)
                for dependency in graph[step]:
                    visit(dependency)
                visiting.remove(step)
                visited.add(step)

            for step_name in graph:
                visit(step_name)
        return self

    def resolve(self, name: str) -> ResolvedJob:
        job = self.jobs.get(name)
        if job is None:
            raise UnknownJobError(f"unknown job {name!r}")
        defaults = self.defaults
        return ResolvedJob(
            name=name,
            job=job,
            profile=self.profiles[job.profile],
            executor=job.executor or defaults.executor,
            overlap=job.overlap or defaults.overlap,
            max_attempts=job.max_attempts or defaults.max_attempts,
            deadline=job.deadline or defaults.deadline,
            alert=job.alert or defaults.alert,
            resources=job.resources or defaults.resources,
        )

    def _validate_schedule_seasons(self, schedule: Schedule, path: str) -> None:
        for season in schedule.seasons:
            if season not in self.seasons:
                raise ValueError(f"{path}.seasons references unknown season {season!r}")


def _coerce_param(name: str, base: str, value: Any) -> Any:
    if base in ("bool", "boolean"):
        if isinstance(value, bool):
            return value
    elif base in ("int", "integer"):
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    elif base in ("float", "number"):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
    elif isinstance(value, str):
        return value
    raise ValueError(f"param {name!r} must be {base}, got {value!r}")


def validate_run_params(job: Job, params: dict[str, Any] | None) -> dict[str, Any]:
    """Checks on-demand run params against the job's allowlist and returns them typed, with
    omitted or null optional params dropped. Raises ValueError for anything else."""
    params = params or {}
    unknown = sorted(set(params) - set(job.params))
    if unknown:
        raise ValueError(f"unknown params: {', '.join(unknown)}")
    checked = {}
    for name, type_name in job.params.items():
        optional = type_name.endswith("?")
        base = type_name.rstrip("?")
        value = params.get(name)
        if value is None:
            if not optional:
                raise ValueError(f"param {name!r} is required")
            continue
        checked[name] = _coerce_param(name, base, value)
    return checked


def params_to_args(params: dict[str, Any]) -> list[str]:
    """`{"week": 7, "dry_run": True}` -> `["--week", "7", "--dry-run"]`; a false flag is omitted.

    `$` is escaped as `$$` because Kubernetes (and the inline runner) expands `$(VAR)` in a
    container command: a caller's `$(DB_PASSWORD)` must stay literal text, unlike the
    catalog's own `$(CURRENT_YEAR)`."""
    args = []
    for name, value in params.items():
        flag = "--" + name.replace("_", "-")
        if value is True:
            args.append(flag)
        elif value is not False and value is not None:
            args.extend([flag, str(value).replace("$", "$$")])
    return args


# Where fantasy-infra mounts the `fantasy-job-catalog` ConfigMap in the launcher pod.
DEFAULT_CATALOG_PATH = "/etc/fantasy-job-catalog/catalog.yaml"


def load_catalog_from_env() -> Catalog:
    return load_catalog(os.getenv("JOB_CATALOG_PATH", DEFAULT_CATALOG_PATH))


def load_catalog(path: str | Path) -> Catalog:
    """Load and validate a YAML catalog from ``path``."""
    with Path(path).open(encoding="utf-8") as source:
        raw = yaml.load(source, Loader=_CatalogYamlLoader)
    if not isinstance(raw, dict):
        raise ValueError("catalog must be a YAML mapping")
    _reject_reserved(raw)
    return Catalog.model_validate(raw)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", metavar="FILE", help="validate a YAML job catalog")
    mode.add_argument(
        "--json-schema", action="store_true", help="print the catalog JSON schema"
    )
    args = parser.parse_args(argv)
    if args.json_schema:
        print(json.dumps(Catalog.model_json_schema(by_alias=True), indent=2))
        return 0
    try:
        load_catalog(args.check)
    except (OSError, yaml.YAMLError, ValidationError, ValueError) as exc:
        print(f"catalog invalid: {exc}", file=sys.stderr)
        return 1
    print(f"catalog valid: {args.check}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
