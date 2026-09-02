// [PORTED_FROM: src/metri/domain/audit/protocol.clj]
// domain/audit/protocol.rs — protocolo del AuditInterceptor.
// Se invoca SIEMPRE ([:ok] y [:error]) — nunca bloquea ni altera el result.

use async_trait::async_trait;
use serde_json::Value;

/// IAuditInterceptor — Fire-and-forget. El resultado es ignorado por el IOP.
/// [PORTED_FROM: (defprotocol IAuditInterceptor)]
#[async_trait]
pub trait IAuditInterceptor: Send + Sync {
    /// Registra un evento de auditoría.
    /// request = payload gRPC original.
    /// result  = Ok(ctx) | Err(DomainError)
    ///
    /// CONTRATO: nunca lanza ni propaga errores — los absorbe y loguea.
    /// [PORTED_FROM: (audit! [this request result])]
    async fn audit(&self, request: &Value, succeeded: bool, error_stage: Option<&str>);
}
