// temporal/mod.rs — Primitivas temporales canónicas del Metri Engine.
//
// SSOT para toda aritmética de fechas: sin I/O, sin estado, sin dependencias cruzadas.
//
// Módulos:
//   core       → constantes, conversiones ms↔s, shift_by_calendar, truncate_to_unit,
//                parse_athena_ts, shift_period, duration_seconds
//   time_frame → resolve_time_frame (29 tipos del contrato TimeFrameContext)
//   comparison → resolve_comparison_period, resolve_shortcut, smart_history_window
//   adapters   → to_datalog_clauses (Datahike), to_honey_clause (Athena),
//                to_bucket_fn (TIMESERIES in-memory)

pub mod adapters;
pub mod comparison;
pub mod core;
pub mod time_frame;
