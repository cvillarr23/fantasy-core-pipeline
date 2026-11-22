"""Starting the run an approval gates (`trigger.then`), shared by the approve path and the
scheduler, which retries a start that didn't happen after its approval committed.

The run is a manual run by the approver, parented to the approval, with dedupe key
`<job>@<approval id>` (`runs.then_dedupe_key`), so however often both callers try, it starts
once. It carries no chain root: the run that asked for the approval may be a chain step whose
chain has long ended.
"""

from typing import Any, Callable

from src.launcher.catalog import Catalog
from src.launcher.runs import ApprovalRun, then_dedupe_key

Publisher = Callable[..., dict[str, str]]


def start_then(
    catalog: Catalog, approval: ApprovalRun, publish: Publisher
) -> dict[str, Any] | None:
    """Publishes the approved approval's `then` run, returning publish's result, or None when
    the approval gates nothing (a chain step's approval)."""
    then = approval.then
    if not then:
        return None
    return publish(
        catalog,
        then["job"],
        trigger="manual",
        params=then.get("params") or {},
        requested_by=approval.requested_by,
        dedupe_key=then_dedupe_key(then["job"], approval.run_id),
        parent_run_id=approval.run_id,
    )
