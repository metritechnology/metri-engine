//! EAV writer — the ACID transaction and its satellites.
//!
//! [`transact`] orquesta; cada satélite es una decisión aislada y testeable:
//!
//! - [`datom_plan`] — planificador puro: payload → lista de datoms (retracts/asserts).
//! - [`chunker`] — divide lotes sobre el límite de 100 items por transacción.
//! - [`enricher`] + [`system_attrs`] — atributos de sistema auto-generados.
//! - [`constraints`] — unicidad por claim item DENTRO de la transacción.
//! - [`outbox`] — el evento nace en la misma transacción (patrón outbox).
//! - [`fts_dispatch`] — el índice FTS se despacha DESPUÉS del commit.
//! - [`cache_policy`] — invalidación de caches de lectura tras el commit.
pub mod cache_policy;
pub mod chunker;
pub mod constraints;
pub mod datom_plan;
pub mod enricher;
pub mod fts_dispatch;
pub mod outbox;
pub mod system_attrs;
pub mod transact;

#[cfg(test)]
pub mod test_support;

pub use chunker::{plan_chunks, ChunkStrategy};
pub use enricher::{enrich_datoms, generate_entity_id, generate_tx_id};
pub use transact::{EavWriter, TransactOp, TransactPayload, TransactResult};
