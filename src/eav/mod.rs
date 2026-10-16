//! EAV storage engine — immutable datoms over the unified Metri Vortex (.mbf) engine.
//!
//! # Guarantees
//!
//! - Inmutabilidad: un datom persistido jamás se actualiza ni borra
//!   (retract + assert). Sin UPDATE statements en todo el motor.
//! - ACID dentro de una transacción (`TransactWriteItems` sobre [`crate::mbf::storage::MbfStorageClient`]),
//!   con claims de unicidad optimistas para resolver colisiones concurrentes.
//! - Aislamiento por tenant: `tenant_id` es parte de cada PK.
//!
//! # Submodules
//!
//! - [`types`] — datom, `DatomValue`, value types y el encoding de claves.
//! - [`writer`] — transacción ACID y satélites (outbox, FTS, caches, secuencias).
//! - [`reader`] — pull (estado de entidad) y query (planes de ejecución sobre `.mbf`).
//! - [`fts`] — búsqueda full-text por trigramas.
pub mod fts;
pub mod reader;
pub mod types;
pub mod writer;
