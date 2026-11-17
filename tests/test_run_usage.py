from src import run_usage


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
    usage = run_usage.measure_run_usage(2, 4.56789, cgroup_root=cgroup(tmp_path))
    assert usage == {
        "source": "cgroup",
        "cpu_seconds": 2.5,
        "cpu_throttled_seconds": 0.125,
        "peak_memory_bytes": 104857600,
        "limits": {"cpu_cores": 0.5, "memory_bytes": 536870912},
        "attempt": 2,
        "wall_seconds": 4.568,
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
