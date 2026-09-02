//! Cuerpos de los RPC del `MetriGrpcService`, separados por dominio (fase 2).
//!
//! `grpc/service.rs` conserva la raíz de dependencias (`ServiceDeps`), el
//! struct y el `impl` del trait gRPC como delegación pura; aquí vive el
//! cuerpo de cada operación.

pub(crate) mod authz;
pub(crate) mod bulk;
pub(crate) mod explore;
pub(crate) mod list_support;
pub(crate) mod query;
pub(crate) mod query_support;
pub(crate) mod routing;
pub(crate) mod transact;
pub(crate) mod validations;
