"""
Shared OpenTelemetry bootstrap for both the FastAPI gateway (src/api/main.py) and the
CronJob/listener pipeline entrypoints (vorp_matrix, lineup_solver, draft_matrix), so
tracer-provider setup isn't duplicated 4x.

configure_tracing() does no eager work at import time - the TracerProvider and any
exporter/processor are only constructed inside the function body - so importing this
module never requires patching in tests/conftest.py the way src.database's eager
ThreadedConnectionPool does.
"""
import json
import logging
import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

_configured = False


def configure_tracing(service_name: str) -> trace.Tracer:
    """
    Idempotent: safe to call more than once per process (e.g. across a session-scoped
    test client that only imports src.api.main once anyway). Behavior is controlled by
    OTEL_EXPORTER_OTLP_ENDPOINT:

    - Unset: true no-op - no span processor is attached at all, so local dev/test runs
      never print span JSON or require a live collector. Set OTEL_CONSOLE_EXPORT=1 to
      opt into a ConsoleSpanExporter for local debugging without a live Tempo instance.
    - Set: exports via OTLP over HTTP/protobuf (not gRPC - this repo has no grpcio
      dependency elsewhere, and Tempo's HTTP receiver is a drop-in per
      homelab-deployment's README). The HTTP exporter needs the full /v1/traces path
      appended, unlike the gRPC exporter which just wants the bare host:port.
    """
    global _configured
    if _configured:
        return trace.get_tracer(service_name)

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if endpoint:
        exporter = OTLPSpanExporter(endpoint=f"{endpoint.rstrip('/')}/v1/traces")
        provider.add_span_processor(BatchSpanProcessor(exporter))
    elif os.getenv("OTEL_CONSOLE_EXPORT") == "1":
        provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)
    _configured = True
    return trace.get_tracer(service_name)


class TraceIdLogFilter(logging.Filter):
    """Stamps record.trace_id from the current OTel span so log lines can be correlated
    to a trace in Grafana (Tempo -> Loki tracesToLogsV2 wiring, per homelab-deployment)."""

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = trace.get_current_span().get_span_context()
        record.trace_id = format(ctx.trace_id, "032x") if ctx.is_valid else "-"
        return True


class LogfmtFormatter(logging.Formatter):
    """One logfmt line per record, with no timestamp: the container runtime and Loki
    already stamp every line, so a second in-band timestamp is just noise. key=value
    pairs let Grafana detect the level and `| logfmt` pull trace_id without a regex.
    The message is JSON-quoted so spaces, quotes and multi-line tracebacks stay inside
    a single msg field and a single Loki entry."""

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)
        if record.stack_info:
            message += "\n" + self.formatStack(record.stack_info)
        trace_id = getattr(record, "trace_id", "-")
        return (
            f"level={record.levelname} logger={record.name} "
            f"trace_id={trace_id} msg={json.dumps(message)}"
        )


class HealthCheckAccessFilter(logging.Filter):
    """Drops uvicorn access lines for the kubelet's /v1/health probe, which otherwise
    dominate the gateway's log volume without carrying any signal."""

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args if isinstance(record.args, tuple) else ()
        return not (len(args) >= 3 and args[2] == "/v1/health")


_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")
_logging_configured = False


def configure_trace_logging() -> None:
    """Attaches TraceIdLogFilter and LogfmtFormatter to the root logger's handlers.

    Idempotent, same as configure_tracing() above: several modules in this repo call this
    at their own import time (e.g. draft_matrix.py imports vorp_matrix.py, which calls
    this itself, then draft_matrix.py's own module body calls it again) - without a guard,
    every such call would re-run setFormatter()/addFilter() on the same handler(s), piling
    up duplicate TraceIdLogFilter instances that each redundantly recompute the same
    current-span lookup per log record.

    Every entrypoint in this repo imports src.database at some point, which itself calls
    logging.basicConfig() with no format at import time - since basicConfig() is a no-op
    once the root logger already has a handler, whichever module imports src.database
    first silently wins the format string, regardless of what a later basicConfig() call
    elsewhere tries to set. Rather than relying on import-order basicConfig races, this
    explicitly (re)configures whatever handler(s) the root logger already has - or adds
    one if none exist - so the trace_id field reliably shows up no matter which module
    happens to trigger logging setup first.

    The filter is attached per-handler, not via root.addFilter(): a Logger-level filter
    only runs for records that logger itself originates, not for records propagated up
    from a child logger (e.g. "GatewayEngine" -> root) - since every module logger in
    this repo is a child of root, a root-Logger-level filter would silently never fire
    and every line would carry trace_id=- even inside an active span.
    """
    global _logging_configured
    if _logging_configured:
        return

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not root.handlers:
        root.addHandler(logging.StreamHandler())
    for handler in root.handlers:
        handler.setFormatter(LogfmtFormatter())
        handler.addFilter(TraceIdLogFilter())
    # uvicorn installs its own handlers and formatters before importing the app, so its
    # access/error lines would bypass the format above; hand them to root instead.
    for name in _UVICORN_LOGGERS:
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers = []
        uvicorn_logger.propagate = True
    logging.getLogger("uvicorn.access").addFilter(HealthCheckAccessFilter())
    _logging_configured = True
