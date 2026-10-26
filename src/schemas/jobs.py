"""Request and response models for the unified jobs API."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class StartRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    params: dict[str, Any] = Field(default_factory=dict)
    downstream: bool = False


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
    pause: PauseInfo | None = None


class JobsResponse(BaseModel):
    jobs: list[JobInfo]
    chains: list[ChainInfo]
