//! Contratos del dominio — errores canónicos, catálogo TOML, eventos de dominio y puertos (DIP).
//!
//! Núcleo puro libre de dependencias de infraestructura: todo el crate se comunica
//! mediante sus tipos canónicos (`DomainError`, `pipeline::Railway`, sobres de eventos y puertos).
//!
//! # Submódulos
//!
//! - `errors` — `DomainError` y `ErrorCode` (catálogo canónico 1:1).
//! - `error_catalog` — cargador TOML fail-fast en el arranque.
//! - `events` — contrato de eventos `metri-contracts` v1.0 (lado productor).
//! - `protocols` — puertos del dominio (traits de inversión de dependencias).
//! - `pipeline` — utilidades para el patrón Railway (`Result<T, DomainError>`).
//! - `config` — configuración inmutable del motor cargada en el arranque.
//! - `audit` — contratos e interceptores para el registro de auditoría.

/// Contratos de tipificación de acciones e interceptor de auditoría.
pub mod audit;
/// Configuración global inmutable del motor cargada en el arranque (`EngineConfig`).
pub mod config;
/// Cargador y validador en memoria del catálogo canónico de errores TOML (`ErrorCatalog`).
pub mod error_catalog;
/// Mónada de error `DomainError` y enumeración exhaustiva `ErrorCode`.
pub mod errors;
/// Contratos de eventos de dominio y cálculo de deltas (`metri-contracts` v1.0).
pub mod events;
/// Tipos y macros de conveniencia para composición Railway-Oriented Programming.
pub mod pipeline;
/// Puertos estáticos del dominio (traits DIP para sesión, colas, autorización, caché y `.mbf`).
pub mod protocols;

pub use error_catalog::{
    global as error_catalog, init_global as init_error_catalog, ErrorCatalog, ErrorEntry,
};
pub use errors::{DomainError, ErrorCode};
pub use protocols::DomainResult;
