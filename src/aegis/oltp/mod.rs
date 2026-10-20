//! Compilador y ejecutor transaccional/analítico en memoria sobre el motor EAV / `.mbf`.
//!
//! Pipeline de lectura y escritura:
//! - `compiler`: Traduce el AST IR (`Value` o FlatBuffers `AnalyticsRequestT`) a un `NativeQueryPlan`.
//! - `executor`: Orquesta la compilación, ejecución de índices, hidratación con caché (`hydrator`),
//!   filtrado en streaming (`filter`), agregación (`aggregation`), comparaciones (`comparison`),
//!   jerarquías (`hierarchy`) y proyección de salida (`caster`).
//! - `channel`: Implementa el puerto `IWriteChannel` para transacciones de escritura ACID sobre EAV / `.mbf`.
pub mod aggregation; // Agregación completa: 14 funciones + FilterNode + derive_columns
pub(crate) mod caster;
pub mod channel;
pub mod comparison; // 5 tipos AnalyticalComparison
pub mod compiler;
pub mod executor;
pub(crate) mod filter;
pub mod fuzzy; // Levenshtein + omnisearch
pub mod hierarchy; // inject_has_children (Modo 2 EAV)
pub(crate) mod hydrator;
