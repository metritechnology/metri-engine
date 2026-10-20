//! Subsistema de observabilidad y trazabilidad distribuida OpenTelemetry (`otel`).
//!
//! Expone utilidades para correlación de trazas (`trace_id`, `span_id`), inyección de
//! atributos en el span activo y estructuración de logs en formato JSON compatible con
//! AWS CloudWatch y colectores OTLP.
pub mod tracer;
