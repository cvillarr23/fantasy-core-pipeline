import logging
import json
import subprocess
import sys
from unittest.mock import patch

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.util._once import Once

import src.telemetry as telemetry


def test_importing_src_configures_logfmt_logging():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import logging; import src; from src.telemetry import LogfmtFormatter, TraceIdLogFilter; "
            "root = logging.getLogger(); assert root.handlers; "
            "assert all(isinstance(handler.formatter, LogfmtFormatter) for handler in root.handlers); "
            "assert all(any(isinstance(f, TraceIdLogFilter) for f in handler.filters) for handler in root.handlers)",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_logfmt_formatter_includes_extra_fields_and_quotes_values():
    record = logging.LogRecord("test.logger", logging.INFO, "", 0, "event happened", (), None)
    record.trace_id = "abc123"
    record.status = "partial failure: \"retry\""
    record.duration_ms = 42
    record.details = {"attempt": 2}

    formatted = telemetry.LogfmtFormatter().format(record)

    assert formatted.startswith(
        'level=INFO logger=test.logger trace_id=abc123 msg="event happened" '
        'status="partial failure: \\"retry\\"" duration_ms=42 '
    )
    encoded_details = json.dumps(json.dumps({"attempt": 2}))
    assert formatted.endswith(f"details={encoded_details}")


@pytest.fixture(autouse=True)
def reset_otel_state(monkeypatch):
    """Every test gets a clean slate. Two separate pieces of global state need resetting:

    1. src.telemetry's own `_configured` guard - without resetting it, only the first
       test to call configure_tracing() would ever actually build a provider.
    2. OTel's own global TracerProvider: trace.set_tracer_provider() is itself a
       call-once guard (see opentelemetry.trace._TRACER_PROVIDER_SET_ONCE) that silently
       no-ops every call after the first *within the whole pytest process*, independent
       of our own guard - so it has to be reset too, or every test after the first would
       observe the first test's provider instead of its own.
    """
    monkeypatch.setattr(telemetry, "_configured", False)
    trace._TRACER_PROVIDER = None
    trace._TRACER_PROVIDER_SET_ONCE = Once()
    yield


def _processor_types(provider):
    return [type(p) for p in provider._active_span_processor._span_processors]


def test_configure_tracing_is_a_true_noop_by_default(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_CONSOLE_EXPORT", raising=False)

    telemetry.configure_tracing("test-service")

    provider = trace.get_tracer_provider()
    assert _processor_types(provider) == []


def test_configure_tracing_console_export_opt_in(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.setenv("OTEL_CONSOLE_EXPORT", "1")

    telemetry.configure_tracing("test-service")

    provider = trace.get_tracer_provider()
    processors = provider._active_span_processor._span_processors
    assert len(processors) == 1
    assert isinstance(processors[0], SimpleSpanProcessor)
    assert isinstance(processors[0].span_exporter, ConsoleSpanExporter)


def test_configure_tracing_uses_otlp_http_when_endpoint_set(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://tempo.default.svc.cluster.local:4318")
    monkeypatch.delenv("OTEL_CONSOLE_EXPORT", raising=False)

    with patch("src.telemetry.OTLPSpanExporter") as mock_exporter_cls:
        telemetry.configure_tracing("test-service")

    # The HTTP exporter needs the full /v1/traces path, unlike the gRPC exporter which
    # just wants the bare host:port.
    mock_exporter_cls.assert_called_once_with(endpoint="http://tempo.default.svc.cluster.local:4318/v1/traces")

    provider = trace.get_tracer_provider()
    processors = provider._active_span_processor._span_processors
    assert len(processors) == 1
    assert isinstance(processors[0], BatchSpanProcessor)


def test_configure_tracing_endpoint_takes_precedence_over_console_export(monkeypatch):
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://tempo.default.svc.cluster.local:4318")
    monkeypatch.setenv("OTEL_CONSOLE_EXPORT", "1")

    with patch("src.telemetry.OTLPSpanExporter"):
        telemetry.configure_tracing("test-service")

    provider = trace.get_tracer_provider()
    assert _processor_types(provider) == [BatchSpanProcessor]


def test_configure_tracing_is_idempotent(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.setenv("OTEL_CONSOLE_EXPORT", "1")

    telemetry.configure_tracing("test-service")
    provider_after_first_call = trace.get_tracer_provider()

    # A second call - even with a since-changed environment - must not rebuild or
    # re-register a provider: this is what protects the FastAPI process (which imports
    # src.api.main, itself calling configure_tracing() once) from silently duplicating
    # span processors if some other import path ever calls it again.
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://tempo.default.svc.cluster.local:4318")
    telemetry.configure_tracing("test-service")

    assert trace.get_tracer_provider() is provider_after_first_call
    assert _processor_types(provider_after_first_call) == [SimpleSpanProcessor]


def test_configure_trace_logging_is_idempotent():
    root = logging.getLogger()
    handler = logging.StreamHandler()
    root.handlers = [handler]
    telemetry._logging_configured = False

    telemetry.configure_trace_logging()
    telemetry.configure_trace_logging()
    telemetry.configure_trace_logging()

    trace_id_filters = [f for f in handler.filters if isinstance(f, telemetry.TraceIdLogFilter)]
    assert len(trace_id_filters) == 1


def test_trace_id_log_filter_stamps_valid_trace_id_when_span_active(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_CONSOLE_EXPORT", raising=False)
    telemetry.configure_tracing("test-service")
    tracer = trace.get_tracer("test")
    log_filter = telemetry.TraceIdLogFilter()
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "msg", None, None)

    with tracer.start_as_current_span("some-span") as span:
        log_filter.filter(record)
        expected_trace_id = format(span.get_span_context().trace_id, "032x")

    assert record.trace_id == expected_trace_id


def test_trace_id_log_filter_stamps_placeholder_without_active_span():
    log_filter = telemetry.TraceIdLogFilter()
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "msg", None, None)

    log_filter.filter(record)

    assert record.trace_id == "-"


def _record(msg, *args, name="x", level=logging.INFO, exc_info=None):
    record = logging.LogRecord(name, level, __file__, 1, msg, args, exc_info)
    record.trace_id = "-"
    return record


def test_logfmt_formatter_omits_timestamp_and_quotes_message():
    line = telemetry.LogfmtFormatter().format(_record('said "hi" to %s', "bob", name="GatewayEngine"))

    assert line == 'level=INFO logger=GatewayEngine trace_id=- msg="said \\"hi\\" to bob"'


def test_logfmt_formatter_keeps_traceback_on_one_line():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        line = telemetry.LogfmtFormatter().format(_record("failed", level=logging.ERROR, exc_info=sys.exc_info()))

    assert "\n" not in line
    assert line.startswith("level=ERROR ") and "ValueError: boom" in line


def test_configure_trace_logging_routes_uvicorn_through_root_and_drops_health_checks():
    root = logging.getLogger()
    root.handlers = [logging.StreamHandler()]
    access = logging.getLogger("uvicorn.access")
    access.handlers = [logging.StreamHandler()]
    access.propagate = False
    access.filters = []
    telemetry._logging_configured = False

    telemetry.configure_trace_logging()

    assert access.handlers == [] and access.propagate
    health = _record('%s - "%s %s HTTP/%s" %d', "10.1.0.40:5598", "GET", "/v1/health", "1.1", 200)
    other = _record('%s - "%s %s HTTP/%s" %d', "10.1.0.40:5598", "POST", "/v1/roster/move-preview", "1.1", 500)
    assert not access.filter(health)
    livez = _record('%s - "%s %s HTTP/%s" %d', "10.1.0.40:5598", "GET", "/v1/livez", "1.1", 200)
    assert not access.filter(livez)
    assert access.filter(other)
