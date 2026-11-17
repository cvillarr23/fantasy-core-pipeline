"""Catalog `resources` suggestions from launcher runs' recorded usage.

Each run records its own usage as `run_log.summary.resources` (`src/run_usage.py`, and the
browser worker's `db/run_log.js` for node jobs). Only successful pod runs count. A run
killed at its memory limit or deadline records nothing, and an inline run's usage says
nothing about a pod's. A job needs `min_runs` measured runs before it gets a suggestion.

Rules (memory rounded up to `MEMORY_STEP_MI`, CPU to `CPU_REQUEST_STEP_M`/`CPU_LIMIT_STEP_M`):
- memory request: p95 peak (cgroup `memory.peak`) x `MEMORY_REQUEST_HEADROOM`.
- memory limit: the largest peak x `MEMORY_LIMIT_HEADROOM`, never below the request. Going
  over it is an OOM kill, so it follows the worst run, not a percentile.
- CPU request: p95 average cores (cpu_seconds / wall_seconds), at least `MIN_CPU_REQUEST_M`.
  The scheduler reserves requests, so the typical sustained rate is enough.
- CPU limit: p95 average cores x `CPU_LIMIT_HEADROOM`, at least `MIN_CPU_LIMIT_M`, leaving
  room for bursts an average hides. Going over it only throttles. When the p95 run spent more
  than `THROTTLED_FRACTION` of its wall time throttled, the limit is binding, so the
  suggestion never goes below the current limit (`cpu_throttled: true`).
"""

import math
import re
from datetime import datetime, timedelta, timezone

from src.database import get_db_cursor

MEMORY_REQUEST_HEADROOM = 1.2
MEMORY_LIMIT_HEADROOM = 1.5
CPU_LIMIT_HEADROOM = 3.0
THROTTLED_FRACTION = 0.05
MEMORY_STEP_MI = 16
CPU_REQUEST_STEP_M = 10
CPU_LIMIT_STEP_M = 50
MIN_CPU_REQUEST_M = 10
MIN_CPU_LIMIT_M = 100
MIB = 1024 * 1024


def suggest_resources(catalog, days, min_runs, now=None):
    """One entry per catalog job: its usage percentiles, current and suggested resources."""
    since = (now or datetime.now(timezone.utc)) - timedelta(days=days)
    usage = _measured_runs(list(catalog.jobs), since)
    return {
        "days": days,
        "min_runs": min_runs,
        "jobs": [
            _job_suggestion(catalog.resolve(name), usage.get(name, []), min_runs)
            for name in catalog.jobs
        ],
    }


def _measured_runs(names, since):
    """Each job's measured successful pod runs since `since`, as
    `(cpu_seconds, wall_seconds, peak_memory_bytes, cpu_throttled_seconds)`."""
    if not names:
        return {}
    with get_db_cursor() as cursor:
        cursor.execute(
            """SELECT job_name,(summary->'resources'->>'cpu_seconds')::float8,
            (summary->'resources'->>'wall_seconds')::float8,
            (summary->'resources'->>'peak_memory_bytes')::float8,
            (summary->'resources'->>'cpu_throttled_seconds')::float8
            FROM run_log WHERE job_name=ANY(%s) AND executor='pod' AND status='success'
            AND finished_at>=%s AND summary->'resources' ? 'cpu_seconds'""",
            (names, since),
        )
        runs = {}
        for name, *values in cursor.fetchall():
            runs.setdefault(name, []).append(tuple(values))
        return runs


def _job_suggestion(resolved, runs, min_runs):
    current = resolved.resources.model_dump()
    entry = {"name": resolved.name, "runs": len(runs), "current": current}
    if not runs:
        return {**entry, "usage": None, "suggested": None, "reason": "no_measured_runs"}
    usage = usage_stats(runs)
    if len(runs) < min_runs:
        return {**entry, "usage": usage, "suggested": None, "reason": "insufficient_runs"}
    suggested = suggested_resources(usage, current["limits"]["cpu"])
    return {**entry, "usage": usage, "suggested": suggested, "reason": None}


def usage_stats(runs):
    """p50/p95/max of each usage figure over `(cpu_seconds, wall_seconds,
    peak_memory_bytes, cpu_throttled_seconds)` rows. A figure no run has is None."""
    timed = [run for run in runs if run[0] is not None and run[1]]
    return {
        "cpu_seconds": _stats([run[0] for run in runs if run[0] is not None]),
        "avg_cpu_cores": _stats([run[0] / run[1] for run in timed]),
        "peak_memory_bytes": _stats([run[2] for run in runs if run[2] is not None]),
        "throttled_fraction": _stats(
            [run[3] / run[1] for run in timed if run[3] is not None]
        ),
    }


def suggested_resources(usage, current_cpu_limit):
    """Catalog `resources` from `usage_stats` output, as Kubernetes quantity strings, plus
    whether the current CPU limit is throttling the job."""
    memory_request = _round_up(
        _p(usage, "peak_memory_bytes", "p95") * MEMORY_REQUEST_HEADROOM / MIB,
        MEMORY_STEP_MI,
    )
    memory_limit = max(
        _round_up(
            _p(usage, "peak_memory_bytes", "max") * MEMORY_LIMIT_HEADROOM / MIB,
            MEMORY_STEP_MI,
        ),
        memory_request,
    )
    avg_cores = _p(usage, "avg_cpu_cores", "p95")
    cpu_request = max(_round_up(avg_cores * 1000, CPU_REQUEST_STEP_M), MIN_CPU_REQUEST_M)
    throttled = _p(usage, "throttled_fraction", "p95") > THROTTLED_FRACTION
    cpu_limit = max(
        _round_up(avg_cores * CPU_LIMIT_HEADROOM * 1000, CPU_LIMIT_STEP_M),
        MIN_CPU_LIMIT_M,
        cpu_request,
        cpu_millicores(current_cpu_limit) if throttled else 0,
    )
    return {
        "requests": {"cpu": f"{cpu_request}m", "memory": f"{memory_request}Mi"},
        "limits": {"cpu": f"{cpu_limit}m", "memory": f"{memory_limit}Mi"},
        "cpu_throttled": throttled,
    }


def cpu_millicores(quantity):
    """A Kubernetes CPU quantity (`500m`, `1`, `1.5`) in millicores."""
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(m?)", quantity.strip())
    if not match:
        raise ValueError(f"Unsupported CPU quantity {quantity!r}")
    value = float(match.group(1))
    return math.ceil(value if match.group(2) else value * 1000)


def percentile(values, fraction):
    """Linear-interpolated percentile of a non-empty list (numpy's default method)."""
    ordered = sorted(values)
    rank = (len(ordered) - 1) * fraction
    low = math.floor(rank)
    high = math.ceil(rank)
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def _stats(values):
    if not values:
        return None
    return {
        "p50": percentile(values, 0.5),
        "p95": percentile(values, 0.95),
        "max": max(values),
    }


def _p(usage, figure, stat):
    """One statistic, 0 when no run recorded that figure."""
    stats = usage[figure]
    return stats[stat] if stats else 0.0


def _round_up(value, step):
    """`value` rounded up to a whole `step`, at least one. Rounds off float noise first, so
    300.00000000000006m stays 300m."""
    return max(math.ceil(round(value / step, 6)), 1) * step
