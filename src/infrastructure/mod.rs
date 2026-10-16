//! Infrastructure — outbound adapters: AWS SDK clients and local emulations.
//!
//! Cada adaptador implementa un puerto de [`crate::application`] o de
//! [`crate::domain`]; los interruptores `*_MODE` permiten levantar el
//! engine completo sin AWS (modo stub) — ver `docs/guides/desarrollo-local.md`.
pub mod audit;
pub mod domain_event_bus;
pub use crate::mbf::storage as dynamodb;
pub mod eventbridge;
pub mod s3_export;
pub mod session_store;
pub mod sqs;
pub mod tenant_guard;
