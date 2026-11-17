"""A launcher run's own CPU and memory usage, recorded as `run_log.summary.resources`.

The run measures itself as it finishes because most launcher pods live 2-10 seconds, shorter
than one cAdvisor scrape, so Prometheus would only ever see a container's first moment.

A pod runs one job, so its container's cgroup (v2, `CGROUP_ROOT`) is the run's usage,
child processes included, against the same counters the limits apply to:
- `cpu_seconds`: `cpu.stat` `usage_usec`.
- `cpu_throttled_seconds`: `cpu.stat` `throttled_usec`, time held back by the CPU limit.
- `peak_memory_bytes`: `memory.peak`, the most the container held (page cache included),
  which is what `memory.max` and the OOM killer compare against.
- `limits`: `cpu.max` as cores and `memory.max` as bytes, each null when unlimited.

An inline run (`RUN_EXECUTOR=inline`) shares the launcher's cgroup, and a pod without the
cgroup v2 files has nothing better, so both fall back to `getrusage` for this process and its
waited-for children (`source: "rusage"`, no throttling or limits).

`wall_seconds` is this process's lifetime (`/proc/self/stat`), the same span the CPU counters
cover, interpreter start-up and imports included: a short job can spend most of its CPU before
the run scope opens. Where `/proc` can't say, the run scope's own duration stands in. A run
killed by its limit or deadline records nothing.
"""

import logging
import os
import resource
from pathlib import Path

logger = logging.getLogger(__name__)

CGROUP_ROOT = Path("/sys/fs/cgroup")
PROC_ROOT = Path("/proc")


def measure_run_usage(attempt, scope_seconds, cgroup_root=None, proc_root=None):
    """`summary.resources` for this run, or None when nothing could be read."""
    try:
        usage = (
            None
            if os.getenv("RUN_EXECUTOR") == "inline"
            else _cgroup_usage(cgroup_root or CGROUP_ROOT)
        )
        usage = usage or _rusage()
    except Exception:
        logger.warning("Could not measure this run's resource usage", exc_info=True)
        return None
    wall_seconds = _process_lifetime(proc_root or PROC_ROOT) or scope_seconds
    return {**usage, "attempt": attempt, "wall_seconds": round(wall_seconds, 3)}


def _process_lifetime(proc):
    """Seconds since this process started, or None when `/proc` can't say. `stat`'s 22nd
    field is the start time in clock ticks since boot; the command name (field 2) can hold
    spaces, so fields are counted after its closing parenthesis."""
    try:
        uptime = float((proc / "uptime").read_text().split()[0])
        fields = (proc / "self" / "stat").read_text().rsplit(")", 1)[1].split()
        started = int(fields[19]) / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError):
        return None
    return max(uptime - started, 0.0)


def _cgroup_usage(root):
    """Usage from the container's cgroup v2 files, or None when they aren't there."""
    cpu_stat = root / "cpu.stat"
    memory_peak = root / "memory.peak"
    if not (cpu_stat.is_file() and memory_peak.is_file()):
        return None
    stats = _cpu_stat(cpu_stat.read_text())
    return {
        "source": "cgroup",
        "cpu_seconds": stats.get("usage_usec", 0) / 1e6,
        "cpu_throttled_seconds": stats.get("throttled_usec", 0) / 1e6,
        "peak_memory_bytes": int(memory_peak.read_text().strip()),
        "limits": {
            "cpu_cores": _cpu_limit(root / "cpu.max"),
            "memory_bytes": _memory_limit(root / "memory.max"),
        },
    }


def _cpu_stat(text):
    """`cpu.stat`'s `key value` lines as integers."""
    pairs = (line.split() for line in text.splitlines())
    return {pair[0]: int(pair[1]) for pair in pairs if len(pair) == 2}


def _cpu_limit(path):
    """`cpu.max` (`<quota> <period>` or `max <period>`) as cores, None when unlimited."""
    if not path.is_file():
        return None
    quota, period = path.read_text().split()
    return None if quota == "max" else int(quota) / int(period)


def _memory_limit(path):
    if not path.is_file():
        return None
    value = path.read_text().strip()
    return None if value == "max" else int(value)


def _rusage():
    """This process plus its waited-for children. `ru_maxrss` is KiB on Linux, and each
    side's peak is its single largest process, so the larger of the two is the peak."""
    own = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return {
        "source": "rusage",
        "cpu_seconds": sum(
            (own.ru_utime, own.ru_stime, children.ru_utime, children.ru_stime)
        ),
        "peak_memory_bytes": max(own.ru_maxrss, children.ru_maxrss) * 1024,
    }
