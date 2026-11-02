import json
from unittest.mock import patch

import pytest

from src import cli
from src.jobs_client import JobsApiError


def _run(argv, capsys, **client):
    with patch.multiple("src.cli.jobs_client", **client) as mocks:
        code = cli.main(argv)
    out = capsys.readouterr()
    return code, out.out, out.err, mocks


def test_list_prints_the_api_response(capsys):
    code, out, _, mocks = _run(["jobs", "list"], capsys, list_jobs=lambda: {"jobs": [{"name": "a"}]})
    assert code == 0
    assert json.loads(out) == {"jobs": [{"name": "a"}]}


def test_run_decodes_params_and_sends_the_cli_trigger(capsys):
    seen = {}

    def run_job(name, params, downstream, trigger):
        seen.update(name=name, params=params, downstream=downstream, trigger=trigger)
        return {"run_id": "r1", "status": "queued"}

    code, out, _, _ = _run(
        ["jobs", "run", "sample-job", "--param", "count=4", "--param", "flag=true",
         "--param", "label=hello", "--param", "expr=a=b", "--downstream"],
        capsys, run_job=run_job,
    )
    assert code == 0
    assert seen == {
        "name": "sample-job",
        "params": {"count": 4, "flag": True, "label": "hello", "expr": "a=b"},
        "downstream": True,
        "trigger": "cli",
    }
    assert json.loads(out)["run_id"] == "r1"


def test_run_defaults_to_no_params_and_no_downstream(capsys):
    seen = {}

    def run_job(name, params, downstream, trigger):
        seen.update(params=params, downstream=downstream)
        return {}

    _run(["jobs", "run", "sample-job"], capsys, run_job=run_job)
    assert seen == {"params": {}, "downstream": False}


def test_param_without_equals_is_a_usage_error(capsys):
    with pytest.raises(SystemExit) as caught:
        cli.main(["jobs", "run", "sample-job", "--param", "oops"])
    assert caught.value.code == 2
    assert "KEY=VALUE" in capsys.readouterr().err


def test_status_cancel_pause_resume(capsys):
    calls = []
    client = dict(
        get_run=lambda identity: calls.append(("get", identity)) or {},
        cancel_run=lambda identity: calls.append(("cancel", identity)) or {},
        pause_job=lambda name, reason, until=None: calls.append(("pause", name, reason, until)) or {},
        unpause_job=lambda name: calls.append(("resume", name)) or {},
    )
    for argv in (
        ["jobs", "status", "r1"],
        ["jobs", "cancel", "r1"],
        ["jobs", "pause", "sample-job", "--reason", "maint", "--until", "2026-12-01T00:00:00+00:00"],
        ["jobs", "pause", "sample-job", "--reason", "maint"],
        ["jobs", "resume", "sample-job"],
    ):
        assert _run(argv, capsys, **client)[0] == 0
    assert calls == [
        ("get", "r1"), ("cancel", "r1"),
        ("pause", "sample-job", "maint", "2026-12-01T00:00:00+00:00"),
        ("pause", "sample-job", "maint", None),
        ("resume", "sample-job"),
    ]


def test_pause_requires_a_reason(capsys):
    with pytest.raises(SystemExit) as caught:
        cli.main(["jobs", "pause", "sample-job"])
    assert caught.value.code == 2


def test_api_error_exits_1_with_the_detail_on_stderr(capsys):
    def refuse(*_args, **_kwargs):
        raise JobsApiError(409, "already running")

    code, out, err, _ = _run(["jobs", "run", "sample-job"], capsys, run_job=refuse)
    assert code == 1
    assert out == ""
    assert "already running" in err and "409" in err
