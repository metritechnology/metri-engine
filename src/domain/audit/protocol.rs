//! Protocolo del interceptor de auditoría — ejecución fire-and-forget con absorción de fallos propios.
//!
//! Se invoca tanto en caminos de éxito (`Ok`) como de fallo (`Err`) sin bloquear
//! ni alterar el resultado devuelto al llamador.

use async_trait::async_trait;
use serde_json::Value;

/// IAuditInterceptor — Fire-and-forget. El resultado es ignorado por el IOP.
#[async_trait]
pub trait IAuditInterceptor: Send + Sync {
    /// Registra un evento de auditoría.
    /// request = payload gRPC original.
    /// result  = Ok(ctx) | Err(DomainError)
    ///
    /// CONTRATO: nunca lanza ni propaga errores — los absorbe y loguea.
    async fn audit(&self, request: &Value, succeeded: bool, error_stage: Option<&str>);
}
