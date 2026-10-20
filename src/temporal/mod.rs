//! Primitivas temporales canónicas de `metri-engine`.
//!
//! Actúa como fuente única de verdad (SSOT) para toda aritmética de fechas y ventanas
//! de tiempo: opera como un núcleo puro sin I/O, sin estado global mutable y sin
//! dependencias cruzadas hacia infraestructura externa.
//!
//! Submódulos:
//! - `core`: Constantes de escala, conversiones milisegundos/segundos, desplazamientos de
//!   calendario (`shift_by_calendar`), truncamiento con zona horaria (`truncate_to_unit`),
//!   desplazamiento de períodos (`shift_period`) y cálculo de duración (`duration_seconds`).
//! - `time_frame`: Resolución determinista de las 29 ventanas nombradas del contrato
//!   `TimeFrameContext` (`resolve_time_frame`).
//! - `comparison`: Resolución de períodos comparativos analíticos (`resolve_comparison_period`,
//!   `resolve_shortcut`, `smart_history_window`).
//! - `adapters`: Adaptadores de rangos temporales hacia predicados Datalog, árboles de filtros
//!   JSON (`WhereNode`) y bucketing de series temporales en memoria (`apply_bucket`).

pub mod adapters;
pub mod comparison;
pub mod core;
pub mod time_frame;
