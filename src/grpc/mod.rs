//! Superficie de transporte gRPC (`grpc`): servidor Tonic, interceptores WAF/Auth, traductores y handlers.
//!
//! El submódulo `pb` aloja el código generado por `tonic-build` (`include_proto!`) y
//! permanece exento de lints de documentación y del patrón `Result` por diseño.
#![allow(clippy::result_large_err)]

/// Módulo autogenerado por `tonic-build` a partir de `proto/metri.proto` y `proto/eda.proto`.
pub mod pb {
    // Código generado por tonic (include_proto!) — usa unwrap()/expect() por
    // diseño y queda exento de missing_docs y de los lints del patrón Result.
    #![allow(
        missing_docs,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::doc_lazy_continuation,
        clippy::doc_overindented_list_items
    )]

    tonic::include_proto!("metri");
    /// Descriptor binario compilado para el servicio de reflexión gRPC (`tonic-reflection`).
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("metri_descriptor");

    /// Contratos Protobuf del subsistema de Arquitectura Orientada a Eventos (`metri.eda.v1`).
    pub mod eda {
        /// Versión 1 del protocolo de ruteo y evaluación de reglas de eventos (`MoiraRoutingService`).
        pub mod v1 {
            tonic::include_proto!("metri.eda.v1");
        }
    }
}

pub mod agent_config_service;
pub mod bootstrap;
pub mod eda_service;
pub mod error_status;
pub(crate) mod handlers;
pub mod interceptors;
pub mod quota_service;
pub mod server;
pub mod service;
pub mod translator;
