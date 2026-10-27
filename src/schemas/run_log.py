"""Pydantic response models for the run audit log endpoints (src/api/activity.py)."""

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


class RunSummary(BaseModel):
    id: str
    job_name: str
    trigger_type: str
    status: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    dedupe_key: Optional[str] = None
    scheduled_for: Optional[datetime] = None
    attempt: int = 1
    requested_by: Optional[str] = None
    chain_root_id: Optional[str] = None
    event_id: Optional[str] = None
    end_reason: Optional[str] = None
    executor: Optional[str] = None
    k8s_job: Optional[str] = None
    expires_at: Optional[datetime] = None


class Run(RunSummary):
    trigger: dict = {}
    parent_run_id: Optional[str] = None
    error: Optional[str] = None
    summary: dict = {}
    trace_id: Optional[str] = None
    input_count: int = 0
    output_count: int = 0
    llm_count: int = 0


class RunList(BaseModel):
    items: list[Run]


class JobHealth(Run):
    recent_failures: int = 0


class DeadLetterQueue(BaseModel):
    queue: str
    messages: int


class JobHealthList(BaseModel):
    items: list[JobHealth]
    # None when the broker couldn't be read.
    dead_letters: Optional[list[DeadLetterQueue]] = None


class RunDetail(Run):
    # {role: {ref_type: [display row]}}; rows vary by ref_type (article, rumor, session, action).
    links: dict[str, dict[str, list[dict[str, Any]]]] = {}
    parent: Optional[dict[str, Any]] = None
    children: list[RunSummary] = []


class TraceStage(BaseModel):
    key: str
    title: str
    runs: list[RunSummary] = []
    sessions: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []


class Trace(BaseModel):
    root: dict[str, str]
    stages: list[TraceStage]
