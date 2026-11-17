"""Request and response models for the unified jobs API."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class StartRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    params: dict[str, Any] = Field(default_factory=dict)
    downstream: bool = False
    # Who is asking. Scheduler-owned triggers (schedule, chain, event) are not claimable.
    trigger: Literal["manual", "cli", "mcp"] = "manual"


class RunStartResponse(BaseModel):
    run_id: str
    status: str
    chain_root_id: str | None = None


class CancelRunResponse(BaseModel):
    run_id: str
    status: str


class PauseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    until: datetime | None = None
    reason: str = Field(min_length=1, max_length=500)


class PauseResponse(BaseModel):
    name: str
    paused_until: datetime | None
    reason: str


class UnpauseResponse(BaseModel):
    name: str
    status: str


class LastRun(BaseModel):
    job_name: str
    id: str
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    trigger_type: str


class PauseInfo(BaseModel):
    name: str
    paused_until: datetime | None
    reason: str | None = None
    set_by: str | None = None
    set_at: datetime


class JobInfo(BaseModel):
    name: str
    executor: str
    overlap: str
    max_attempts: int
    deadline_seconds: int
    alert: str
    acts: bool
    params: dict[str, str]
    profile: str
    command: list[str]
    schedules: list[dict[str, Any]]
    on_event: list[dict[str, Any]]
    last_run: LastRun | None = None
    next_run_at: datetime | None = None
    pause: PauseInfo | None = None


class ChainStepInfo(BaseModel):
    after: list[dict[str, Any]]
    latest: str | None = None
    last_run: LastRun | None = None


class ChainInfo(BaseModel):
    name: str
    schedule: dict[str, Any] | None = None
    max_duration_seconds: int
    steps: dict[str, ChainStepInfo]
    next_run_at: datetime | None = None
    pause: PauseInfo | None = None


class JobsResponse(BaseModel):
    jobs: list[JobInfo]
    chains: list[ChainInfo]


class ResourceQuantities(BaseModel):
    cpu: str
    memory: str


class JobResources(BaseModel):
    requests: ResourceQuantities
    limits: ResourceQuantities


class UsageStats(BaseModel):
    p50: float
    p95: float
    max: float


class JobUsage(BaseModel):
    """Percentiles over a job's measured successful runs (`run_log.summary.resources`)."""

    cpu_seconds: UsageStats | None
    avg_cpu_cores: UsageStats | None
    peak_memory_bytes: UsageStats | None
    # Share of each run's wall time held back by its CPU limit.
    throttled_fraction: UsageStats | None


class SuggestedResources(JobResources):
    # The current CPU limit is throttling the job, so the suggestion keeps at least it.
    cpu_throttled: bool


class JobResourceSuggestion(BaseModel):
    name: str
    runs: int
    current: JobResources
    usage: JobUsage | None
    # Null with a `reason` (`no_measured_runs`, `insufficient_runs`) until enough runs exist.
    suggested: SuggestedResources | None
    reason: str | None


class ResourceSuggestionsResponse(BaseModel):
    days: int
    min_runs: int
    jobs: list[JobResourceSuggestion]


class LauncherHealthResponse(BaseModel):
    heartbeat_at: datetime
    heartbeat_age_seconds: float
    queue_depths: dict[str, int | None]
    running_count: int
    queued_count: int
