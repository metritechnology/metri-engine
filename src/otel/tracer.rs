// otel/tracer.rs — wrapper OTel para Metri Engine.
// FASE 1: usa tracing puro para structured logging.
// FASE 2: integrar opentelemetry-otlp completo con exportador a X-Ray/Grafana.

use opentelemetry::trace::TraceContextExt;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Inicializa el tracer con JSON structured logging (CloudWatch compatible).
pub fn init(_service_name: &str, _otlp_endpoint: Option<&str>) {
    // FASE 2 TODO: conectar opentelemetry-otlp exporter
    // Por ahora: tracing-subscriber ya inicializado en main.rs con .json()
    tracing::info!("OTel: modo structured-JSON (CloudWatch) — OTLP pendiente FASE 2");
}

/// Extrae el trace-id del span activo como string hex.
pub fn current_trace_id() -> String {
    let span = Span::current();
    let context = span.context();
    let span_ref = context.span();
    let span_context = span_ref.span_context();
    if span_context.is_valid() {
        span_context.trace_id().to_string()
    } else {
        format!("{:032x}", 0u128)
    }
}

/// Extrae el span-id del span activo como string hex.
pub fn current_span_id() -> String {
    let span = Span::current();
    let context = span.context();
    let span_ref = context.span();
    let span_context = span_ref.span_context();
    if span_context.is_valid() {
        span_context.span_id().to_string()
    } else {
        format!("{:016x}", 0u64)
    }
}

/// Añade atributos al span activo.
pub fn set_attributes(attrs: &[(&str, &str)]) {
    let span = Span::current();
    for (key, val) in attrs {
        span.record(*key, *val);
    }
}

/// Macro de conveniencia para crear spans nombrados.
#[macro_export]
macro_rules! otel_span {
    ($name:expr) => {
        tracing::info_span!($name)
    };
}
