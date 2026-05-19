// [STUB — FASE 4: conectar con Redis/DynamoDB real]
// iop/quota_step.rs — IopStep wrapper para QuotaGuard.
// En Clojure: Paso 2 del pipeline — :iop/quota-guard
//
// Stub policy: QuotaOK para todos los tenants.
// FASE 4: conteo real en DynamoDB (atomic counter por tenant+operation).

use tracing::{info, warn};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::iop::core::{IopContext, IopStep};

/// Wrapper IopStep para el control de cuotas por tenant (Paso 2).
/// [PORTED_FROM: Paso 2 del pipeline en ig/init-key :iop/orchestrator]
pub struct QuotaGuardStep;

impl QuotaGuardStep {
    pub fn new() -> Self {
        info!("[QuotaStep] Inicializado (STUB QuotaOK — FASE 4: DynamoDB counter real)");
        Self
    }
}

#[async_trait::async_trait]
impl IopStep for QuotaGuardStep {
    /// Verifica cuota del tenant para la operación.
    /// STUB: permite todo excepto tenant "quota-exceeded-tenant".
    /// FASE 4: reemplazar con conteo atómico en DynamoDB/Redis.
    #[tracing::instrument(name = "iop.step2.quota.start", skip(self, ctx), fields(tenant_id = %ctx.tenant_id))]
    async fn execute(&self, mut ctx: IopContext) -> Result<IopContext, DomainError> {
        let op = ctx.operation.to_uppercase();

        // Pass-through O(1) para UPDATE, DELETE y UPSERT
        // [PORTED_FROM: (if (#{:update :delete :upsert} operation) [:ok ctx] ...)]
        if matches!(op.as_str(), "UPDATE" | "DELETE" | "UPSERT") {
            info!(
                tenant    = %ctx.tenant_id,
                operation = %op,
                "[QuotaStep] Pass-through O(1) — no debita cuota"
            );
            return Ok(ctx);
        }

        if ctx.tenant_id == "quota-exceeded-tenant" {
            warn!(
                tenant    = %ctx.tenant_id,
                operation = %ctx.operation,
                "[QuotaStep] STUB: Quota excedida"
            );
            return Err(DomainError::new(
                ErrorCode::Quota001,
                format!(
                    "STUB Quota: límite excedido para tenant '{}' en operación '{}'",
                    ctx.tenant_id, ctx.operation
                ),
            ));
        }

        info!(
            tenant    = %ctx.tenant_id,
            operation = %ctx.operation,
            "[QuotaStep] STUB: QuotaOK"
        );
        
        ctx.quota_reservation = Some(serde_json::json!({
            "status": "pending",
            "debit": 1
        }));
        
        Ok(ctx)
    }
}
