import logging
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
