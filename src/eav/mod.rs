//! EAV storage engine — immutable datoms over a DynamoDB single table.
//!
//! # Guarantees
//!
//! - Inmutabilidad: un datom persistido jamás se actualiza ni borra
//!   (retract + assert). Sin UPDATE statements en todo el motor.
//! - ACID dentro de una transacción (`TransactWriteItems`), con claims
//!   de unicidad optimistas para resolver colisiones concurrentes.
//! - Aislamiento por tenant: `tenant_id` es parte de cada PK.
//!
//! # Submodules
//!
//! - [`types`] — datom, `DatomValue`, los 12 value types y el encoding binario de SK.
//! - [`index`] — los cuatro índices: EAVT (tabla principal), AEVT, AVET y VAET.
//! - [`writer`] — transacción ACID y satélites (outbox, FTS, caches, planes).
//! - [`reader`] — pull (estado de entidad) y query (5 planes físicos).
//! - [`cursor`] — paginación estable multi-índice.
//! - [`fts`] — búsqueda full-text por trigramas.
//! - [`hierarchy`] — materialized paths.
//! - [`registry`] — catálogo de atributos y migración de esquema.
//! - [`sharding`] — scatter determinista de hot partitions.
//! - [`telemetry`] — bucketing de lecturas IoT + LZ4.
//!
//! # Origin
//!
//! Port del motor Datahike/Datomic-style del stack Clojure anterior;
//! los comentarios `Blueprint` citan las secciones del diseño original.
pub mod cursor;
pub mod fts;
pub mod hierarchy;
pub mod index;
pub mod reader;
pub mod registry;
pub mod sharding;
pub mod telemetry;
pub mod types;
pub mod writer;
