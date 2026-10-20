//! Implementación de los manejadores RPC de `MetriGrpcService`, organizados por dominio funcional.
//!
//! `grpc/service.rs` conserva la estructura `ServiceDeps`, `MetriGrpcService` y la delegación
//! del trait `MetriService`; en este submódulo reside la lógica de negocio de cada operación.

pub(crate) mod authz;
pub(crate) mod bulk;
pub(crate) mod composite;
pub(crate) mod explore;
pub(crate) mod list_support;
pub(crate) mod query;
pub(crate) mod query_support;
pub(crate) mod routing;
pub(crate) mod sequence_config;
pub(crate) mod transact;
pub(crate) mod validations;
