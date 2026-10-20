//! Lectores del motor EAV — reconstrucción selectiva de estado (`pull`) y ejecución física de planes de consulta (`query`) sobre `.mbf`.
/// Reconstrucción selectiva del estado vigente o histórico (`as-of` / `history`) de una entidad.
pub mod pull;
/// Ejecutor físico de los planes de consulta sobre los índices `EAVT`, `AEVT`, `AVET` y `FTS` de `.mbf`.
pub mod query;
pub use pull::{EavReader, EntityMap, HistoryEntry};
