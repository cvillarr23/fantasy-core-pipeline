import os

from src import run_usage

TICKS = os.sysconf("SC_CLK_TCK")


def proc(tmp_path, uptime="1000.50", started_ticks=None, name="python -m job"):
    """A fake /proc whose process started 12.25 seconds before `uptime`."""
    root = tmp_path / "proc"
    (root / "self").mkdir(parents=True)
    (root / "uptime").write_text(f"{uptime} 4000.00\n")
    ticks = started_ticks if started_ticks is not None else int((1000.50 - 12.25) * TICKS)
    rest = " ".join(["S", "1", "1", "1", "0", "-1"] + ["0"] * 13 + [str(ticks), "0"])
    (root / "self" / "stat").write_text(f"1 ({name}) {rest}\n")
    return root


def cgroup(tmp_path, cpu_max="50000 100000", memory_max="536870912"):
    (tmp_path / "cpu.stat").write_text(
        "usage_usec 2500000\nuser_usec 2000000\nsystem_usec 500000\n"
        "nr_periods 40\nnr_throttled 3\nthrottled_usec 125000\n"
    )
    (tmp_path / "memory.peak").write_text("104857600\n")
    (tmp_path / "cpu.max").write_text(f"{cpu_max}\n")
    (tmp_path / "memory.max").write_text(f"{memory_max}\n")
    return tmp_path


def test_pod_run_reads_its_containers_cgroup(tmp_path, monkeypatch):
    monkeypatch.delenv("RUN_EXECUTOR", raising=False)
    usage = run_usage.measure_run_usage(
        2, 0.5, cgroup_root=cgroup(tmp_path), proc_root=proc(tmp_path)
    )
    assert usage == {
        "source": "cgroup",
        "cpu_seconds": 2.5,
        "cpu_throttled_seconds": 0.125,
        "peak_memory_bytes": 104857600,
        "limits": {"cpu_cores": 0.5, "memory_bytes": 536870912},
        "attempt": 2,
        "wall_seconds": 12.25,
    }


def test_unlimited_cgroup_has_null_limits(tmp_path, monkeypatch):
    monkeypatch.delenv("RUN_EXECUTOR", raising=False)
    root = cgroup(tmp_path, cpu_max="max 100000", memory_max="max")
    usage = run_usage.measure_run_usage(1, 1.0, cgroup_root=root)
    assert usage["limits"] == {"cpu_cores": None, "memory_bytes": None}


def test_without_cgroup_v2_files_it_falls_back_to_rusage(tmp_path, monkeypatch):
    monkeypatch.delenv("RUN_EXECUTOR", raising=False)
    usage = run_usage.measure_run_usage(1, 1.0, cgroup_root=tmp_path)
    assert usage["source"] == "rusage"
    assert usage["cpu_seconds"] > 0 and usage["peak_memory_bytes"] > 1024 * 1024
    assert "limits" not in usage


def test_inline_run_ignores_the_launchers_cgroup(tmp_path, monkeypatch):
    monkeypatch.setenv("RUN_EXECUTOR", "inline")
    usage = run_usage.measure_run_usage(1, 1.0, cgroup_root=cgroup(tmp_path))
    assert usage["source"] == "rusage"


def test_unreadable_usage_is_none(tmp_path, monkeypatch):
    monkeypatch.delenv("RUN_EXECUTOR", raising=False)
    root = cgroup(tmp_path)
    (root / "memory.peak").write_text("garbage")
    assert run_usage.measure_run_usage(1, 1.0, cgroup_root=root) is None


def test_wall_time_is_the_process_lifetime_not_the_scope(tmp_path, monkeypatch):
    """The CPU counters include start-up and imports, so the wall time must too."""
    monkeypatch.setenv("RUN_EXECUTOR", "inline")
    usage = run_usage.measure_run_usage(1, 0.02, proc_root=proc(tmp_path, name="odd) name"))
    assert usage["wall_seconds"] == 12.25


def test_wall_time_falls_back_to_the_scope_without_proc(tmp_path, monkeypatch):
    monkeypatch.setenv("RUN_EXECUTOR", "inline")
    usage = run_usage.measure_run_usage(1, 0.02, proc_root=tmp_path / "missing")
    assert usage["wall_seconds"] == 0.02


def test_real_process_lifetime_is_positive():
    assert run_usage._process_lifetime(run_usage.PROC_ROOT) > 0
