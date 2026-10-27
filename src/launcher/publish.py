"""Run commands, and the publish helper the Jobs API and the scheduler use to request runs.

`publish_run` writes the `queued` run_log row and its job_run_outbox row in one transaction,
then tries an immediate delivery to `job_runs`. A failed delivery is retried by
`activity_runner.drain_job_runs` (the API's background tick and the launcher loop), so the
queue and the table can't drift apart. The unique `run_log.dedupe_key` index makes a second
request with the same key return the first run instead of creating another.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from opentelemetry import propagate
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from src.database import get_db_cursor
from src.launcher.catalog import Catalog, validate_run_params
from src.launcher.runs import Runs
from src.services import run_log_service
from src.services.run_log_service import LauncherTrigger

logger = logging.getLogger(__name__)


class RunCommand(BaseModel):
    """The `job_runs` message body: ADR 0001's run command, plus when it stops being worth
    starting (`expires_at`): manual queue TTL, a schedule's catch_up, or a chain's latest.
    """

    model_config = ConfigDict(extra="ignore")

    run_id: str
    job: str
    params: dict[str, Any] = Field(default_factory=dict)
    trigger: LauncherTrigger
    requested_by: str | None = None
    dedupe_key: str
    parent_run_id: str | None = None
    chain_root_id: str | None = None
    event_id: str | None = None
    downstream: bool = False
    scheduled_for: datetime | None = None
    attempt: int = Field(default=1, ge=1)
    traceparent: str | None = None
    expires_at: AwareDatetime

    def job_name_env(self) -> str:
        """`<job>@<dedupe>` for JOB_NAME, which scopes the lineup/rumor idempotency keys to this
        run. Schedule and chain keys already start with `<job>@`, so they're used as is.
        """
        if self.dedupe_key.startswith(f"{self.job}@"):
            return self.dedupe_key
        return f"{self.job}@{self.dedupe_key}"

    def launch_env(self) -> dict[str, str]:
        """The run identity a launched process adopts through run_log_service.run_scope."""
        env = {
            "RUN_ID": self.run_id,
            "RUN_ATTEMPT": str(self.attempt),
            "RUN_TRIGGER": self.trigger,
            "JOB_NAME": self.job_name_env(),
        }
        if self.parent_run_id:
            env["RUN_PARENT_ID"] = self.parent_run_id
        return env


def publish_run(
    catalog: Catalog,
    job: str,
    *,
    trigger: LauncherTrigger,
    params: dict[str, Any] | None = None,
    requested_by: str | None = None,
    dedupe_key: str | None = None,
    parent_run_id: str | None = None,
    chain_root_id: str | None = None,
    event_id: str | None = None,
    downstream: bool = False,
    scheduled_for: datetime | None = None,
    expires_at: datetime | None = None,
) -> dict[str, str]:
    """Queues one run of `job`. Returns `{"run_id", "status"}`, where status is `queued`, or
    `duplicate` with the existing run's id when `dedupe_key` was already used.

    Raises UnknownJobError for a job the catalog doesn't have and ValueError for params
    outside its allowlist. `dedupe_key` defaults to a fresh UUID (manual, cli and mcp runs);
    `expires_at` defaults to now plus the catalog's `manual_queue_ttl`.
    """
    resolved = catalog.resolve(job)
    checked_params = validate_run_params(resolved.job, params)
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    command = RunCommand(
        run_id=run_id,
        job=job,
        params=checked_params,
        trigger=trigger,
        requested_by=requested_by,
        dedupe_key=dedupe_key or run_id,
        parent_run_id=parent_run_id,
        chain_root_id=chain_root_id,
        event_id=event_id,
        downstream=downstream,
        scheduled_for=scheduled_for,
        traceparent=carrier.get("traceparent"),
        expires_at=expires_at or now + catalog.global_settings.manual_queue_ttl,
    )
    body = command.model_dump(mode="json")
    with get_db_cursor() as c:
        runs = Runs(c)
        inserted = runs.insert_queued(
            run_id=run_id,
            job=job,
            trigger_type=trigger,
            trigger={"params": body["params"], "downstream": downstream},
            parent_run_id=parent_run_id,
            trace_id=run_log_service._trace_id(),
            dedupe_key=command.dedupe_key,
            scheduled_for=scheduled_for,
            attempt=command.attempt,
            requested_by=requested_by,
            chain_root_id=chain_root_id,
            event_id=event_id,
            expires_at=command.expires_at,
        )
        if not inserted:
            existing = runs.run_id_for_dedupe_key(command.dedupe_key)
            return {"run_id": existing, "status": "duplicate"}
        c.execute(
            "INSERT INTO job_run_outbox(run_id,body) VALUES (%s,%s)",
            (run_id, json.dumps(body)),
        )
    from src.services.activity_runner import publish_job_run

    try:
        publish_job_run(run_id)
    except Exception:
        logger.exception("Run %s queued; background outbox will retry delivery", run_id)
    return {"run_id": run_id, "status": "queued"}
