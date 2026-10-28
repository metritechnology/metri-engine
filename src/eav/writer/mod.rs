//! Subsistema de escritura del motor EAV — transacción ACID sobre `.mbf` y sus módulos satélites.
//!
//! `transact` orquesta el flujo de extremo a extremo; cada satélite encapsula una decisión pura y verificable:
//!
//! - `datom_plan` — planificador puro que traduce el payload a listas de datoms (`retract`/`assert`), delta y solicitudes FTS.
//! - `enricher` y `system_attrs` — inyección determinista de atributos de sistema reservados.
//! - `constraints` — verificación de estado, referencias e invariantes de unicidad dentro de la misma transacción.
//! - `outbox` — materialización atómica del evento de dominio en la misma transacción (patrón Transactional Outbox).
//! - `history_mirror` — datoms espejo `_refs/<hija>` en la partición del dueño, en la misma transacción.
//! - `cache_policy` — invalidación inmediata o diferida de las cachés de lectura tras confirmar el commit.

/// Políticas de invalidación de las cachés de lectura (`EAV_CACHE` y `AEVT_SCAN_CACHE`) post-commit.
pub mod cache_policy;
/// Planificadores y validadores de restricciones de integridad e invariantes de unicidad en la transacción.
pub mod constraints;
/// Planificador puro del camino de escritura (`TransactPayload` -> `DatomPlan`).
pub mod datom_plan;
/// Enriquecimiento automático de datoms con atributos de sistema y generación monotónica de `tx_id`.
pub mod enricher;
/// Espejo de relaciones (`_refs/<hija>`) en la partición del dueño para historial agregado.
pub mod history_mirror;
/// Generación pura de datoms para el patrón Transactional Outbox y serialización JSON del contrato.
pub mod outbox;
/// Fuente única de verdad para los identificadores (`u16`) y nombres de atributos de sistema reservados.
pub mod system_attrs;
/// Orquestador transaccional ACID (`EavWriter`) sobre el almacenamiento `.mbf`.
pub mod transact;

/// Utilidades de inicialización del Códice para pruebas unitarias puras del escritor.
#[cfg(test)]
pub mod test_support;

pub use enricher::{enrich_datoms, generate_entity_id, generate_tx_id};
pub use transact::{EavWriter, TransactOp, TransactPayload, TransactResult};
