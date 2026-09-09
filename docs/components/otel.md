# OTel — Tracing

> **English summary:** OpenTelemetry tracing wrapper with OTLP export: structured JSON logging for CloudWatch, trace-id propagation into forensic error DTOs, and a development-friendly fallback (pure `tracing` filterable via `RUST_LOG`).

## Purpose

Que toda operación sea trazable en producción (X-Ray/Grafana vía OTLP) sin obligar a un colector para desarrollar en local.

## Responsibilities / non-responsibilities

**Hace:** inicialización del tracing subscriber (JSON para CloudWatch, filtro por `RUST_LOG`); wrapper OTel y `current_trace_id` para el DTO forense de `iop::error_response`.
**No hace:** logs de negocio — eso es `tracing` en cada módulo.

## Internal flow

```text
RUST_LOG (default info) ─▶ tracing_subscriber (JSON) ─▶ CloudWatch
OTLP exporter ─▶ X-Ray / Grafana
trace_id actual ─▶ iop::error_response (errores forenses correlacionables)
```

## Invariants

1. **Logs JSON estructurados** — requisito de CloudWatch Insights.
2. **El trace_id cruza la frontera de errores** — toda respuesta de error forense lo lleva.

## Entry points

- [`otel::tracer`] — init y `current_trace_id`.

## Errors

Fallos de exportación OTel degradan a logging local — nunca bloquean el request.

## Decisions

- Fase 1 del port usó `tracing` puro; la integración OTLP completa llega sin cambiar los call-sites.

## Known risks / TODO

- Fijar sampling antes de escalar tráfico (hoy traza todo lo que llega).
