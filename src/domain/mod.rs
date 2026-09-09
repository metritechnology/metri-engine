//! Domain contracts — canonical errors, TOML catalog, domain events, ports.
//!
//! Núcleo sin dependencias de infraestructura: todo el crate habla en sus
//! tipos ([`DomainError`], [`pipeline::Railway`], sobres de eventos).
//!
//! # Submodules
//!
//! - [`errors`] — `DomainError` / `ErrorCode` (catálogo canónico 1:1).
//! - `error_catalog` — loader TOML fail-fast del arranque.
//! - [`events`] — contrato de eventos metri-contracts (lado productor).
//! - [`protocols`] — puertos del dominio (traits DIP).
//! - [`pipeline`] — helpers Railway.
//! - [`config`] — configuración del engine leída una sola vez.
//! - [`audit`] — contrato del audit log OLAP.
pub mod audit;
pub mod config;
pub mod error_catalog;
pub mod errors;
pub mod events;
pub mod pipeline;
pub mod protocols;

pub use error_catalog::{
    global as error_catalog, init_global as init_error_catalog, ErrorCatalog, ErrorEntry,
};
pub use errors::{DomainError, ErrorCode};
pub use protocols::DomainResult;
