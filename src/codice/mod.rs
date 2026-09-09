//! Codice — SSOT registry of the JSON models in `config/models/`.
//!
//! Carga y compila los ~60 modelos al arranque (fail-fast, ADR-004) y
//! provee coerción, validación y generación de ids (secuencias ACID y
//! códigos Base36 aleatorios).
//!
//! # Guarantees
//!
//! - El registry es inmutable post-bootstrap (`OnceLock`), lookup O(1).
//! - Un modelo inválido aborta el arranque: nunca se sirve con esquema a medias.
//!
//! # Submodules
//!
//! - [`registry`] — `CodeRegistry`, descriptors de atributos y engine channels.
//! - [`coercion`] — conversiones seguras de tipos en payloads.
//! - [`validator`] — validación estructural/semántica hacia `DatomValue`.
//! - [`generator`] — inyector de atributos auto-generados (secuencia/Base36).
//! - [`sequence`] — contadores ACID con scope resolution.
//! - [`base36`] — códigos aleatorios no predecibles (zero-drop).
//!
//! # Origin
//!
//! Port del Integrant `:codice/registry` del stack Clojure anterior.
pub mod base36;
pub mod coercion;
pub mod generator;
pub mod registry;
pub mod sequence;

pub use registry::{global, init_global};
pub use registry::{AttrType, AttributeDescriptor, CodeRegistry, EngineChannel, EntityModel};
pub mod validator;
