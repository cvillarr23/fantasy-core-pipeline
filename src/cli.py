"""Operator CLI for launcher jobs and runs: `python -m src.cli jobs <command>`.

Every command calls the Jobs API through `jobs_client` (so `CORE_API_BASE_URL` and, for the
mutating commands, `TRIGGER_API_KEY` must be set) and prints the JSON response. Runs started
here carry `trigger="cli"`. Exit status: 0 on success, 1 when the API refuses or is unreachable,
2 on a usage error.
"""
import argparse
import json
import sys
from typing import Any

from src import jobs_client
from src.jobs_client import JobsApiError


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = args.handler(args)
    except JobsApiError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, default=str))
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.cli", description=__doc__.splitlines()[0])
    jobs = parser.add_subparsers(dest="group", required=True).add_parser("jobs", help="launcher jobs and runs")
    commands = jobs.add_subparsers(dest="command", required=True)
    _add_list(commands)
    _add_run(commands)
    _add_status(commands)
    _add_cancel(commands)
    _add_pause(commands)
    _add_resume(commands)
    return parser


def _add_list(commands) -> None:
    command = commands.add_parser("list", help="catalog jobs, chains and their last runs")
    command.set_defaults(handler=lambda _args: jobs_client.list_jobs())


def _add_run(commands) -> None:
    command = commands.add_parser("run", help="start a run now")
    command.add_argument("name", help="job name")
    command.add_argument(
        "--param", action="append", default=[], type=_key_value, metavar="KEY=VALUE",
        help="job param; VALUE is read as JSON when it parses (4, true), else as text. Repeatable",
    )
    command.add_argument("--downstream", action="store_true", help="also run the job's downstream chain steps")
    command.set_defaults(handler=_run_job)


def _add_status(commands) -> None:
    command = commands.add_parser("status", help="show one run")
    command.add_argument("run", help="run id")
    command.set_defaults(handler=lambda args: jobs_client.get_run(args.run))


def _add_cancel(commands) -> None:
    command = commands.add_parser("cancel", help="cancel a queued or running run")
    command.add_argument("run", help="run id")
    command.set_defaults(handler=lambda args: jobs_client.cancel_run(args.run))


def _add_pause(commands) -> None:
    command = commands.add_parser("pause", help="stop scheduling a job or chain")
    command.add_argument("name", help="job or chain name")
    command.add_argument("--reason", required=True, help="why it is paused")
    command.add_argument("--until", help="ISO 8601 time with offset to resume at; omit to pause indefinitely")
    command.set_defaults(handler=lambda args: jobs_client.pause_job(args.name, args.reason, args.until))


def _add_resume(commands) -> None:
    command = commands.add_parser("resume", help="lift a pause")
    command.add_argument("name", help="job or chain name")
    command.set_defaults(handler=lambda args: jobs_client.unpause_job(args.name))


def _run_job(args: argparse.Namespace) -> dict:
    return jobs_client.run_job(args.name, dict(args.param), args.downstream, "cli")


def _key_value(text: str) -> tuple[str, Any]:
    key, separator, raw = text.partition("=")
    if not separator or not key:
        raise argparse.ArgumentTypeError(f"{text!r} is not KEY=VALUE")
    return key, _decoded(raw)


def _decoded(raw: str) -> Any:
    try:
        return json.loads(raw)
    except ValueError:
        return raw


if __name__ == "__main__":
    sys.exit(main())
