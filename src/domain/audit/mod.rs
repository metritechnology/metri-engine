//! Contratos de auditoría — clasificación de acciones y puerto interceptor para la bitácora de auditoría.
//!
//! `action_type` deriva la acción de forma pura y `protocol` define el
//! interceptor asíncrono fire-and-forget que nunca bloquea ni altera el resultado de negocio.

/// Derivación pura del tipo de acción (`ActionType`) a partir del resultado de una operación.
pub mod action_type;
/// Protocolo `IAuditInterceptor` de registro de auditoría desacoplado.
pub mod protocol;
