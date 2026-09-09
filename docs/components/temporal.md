# Temporal — Time Primitives

> **English summary:** Canonical, timezone-aware time primitives: epoch-second invariant, named time-frame resolution, calendar-safe comparison shifts (leap-year/DST via chrono-tz) and adapters that translate `TimeRange` into engine-specific clauses. Pure functions, no I/O.

## Purpose

Ser la SSOT de toda aritmética de fechas del motor: un solo sitio que sabe resolver ventanas ("este mes", "últimos 30 días", shifts por calendario) con zonas horarias reales.

## Responsibilities / non-responsibilities

**Hace:** primitivas canónicas (`core`); resolución de `TimeFrameContext` a rangos epoch (`time_frame`); períodos de comparación analítica con shifts calendar-safe (`comparison`); traducción de rangos a cláusulas por motor (`adapters`).
**No hace:** persistir ni interpretar timestamps de negocio — eso es de los modelos Codice.

## Internal flow

```text
TimeFrameContext (proto/FBS) ─▶ time_frame ─▶ {start_ts, end_ts} epoch-s
AnalyticalComparison ─▶ comparison (shift_by_calendar, bisiesto/DST-safe) ─▶ períodos
{start_ts, end_ts} ─▶ adapters ─▶ cláusulas Athena/EAV
```

## Invariants

1. **Epoch-segundos (i64) por dentro** — salvo campos con sufijo `_ms`; la frontera convierte explícitamente.
2. **Shifts por calendario, no por segundos** — "hace un mes" cruza DST y bisiestos correctamente (`chrono-tz`).
3. **Totalidad** — las funciones clave son totales (clamp calendario) tras la migración al patrón Result; sin panics por fechas extremas.

## Entry points

- [`temporal::core`] — primitivas y aritmética.
- [`temporal::time_frame`] — ventanas nombradas.
- [`temporal::comparison::resolve_comparison_period`] — comparaciones analíticas.
- [`temporal::adapters`] — cláusulas por motor.

## Errors

Errores de zona horaria desconocida o rango imposible, tipados tras la migración Result.

## Decisions

- `chrono` + `chrono-tz` como base (reemplazo de `java.time.ZonedDateTime` del stack anterior).

## Known risks / TODO

- Cubrir más casos DST en doctests (los shortcuts de `comparison` ya tienen tests de calendario).
