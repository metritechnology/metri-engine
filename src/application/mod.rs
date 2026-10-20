//! Casos de uso y puertos de salida de la capa de aplicación (DIP).
//!
//! Siguiendo el Principio de Inversión de Dependencias (DIP), este módulo define
//! traits delimitados por la necesidad del consumidor de dominio/aplicación.
//! Las implementaciones concretas (EventBridge, outbox transaccional, almacenamiento `.mbf`)
//! residen en `infrastructure/` y se inyectan en las raíces de composición
//! (`grpc/bootstrap.rs`, `grpc/server.rs`). Las pruebas unitarias componen dobles en memoria sin red.

/// Puertos de salida para publicación de eventos de dominio con contrato de reintentos.
pub mod ports;
