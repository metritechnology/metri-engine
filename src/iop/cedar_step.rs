// [STUB — FASE 4: conectar con cedar_policy real]
// iop/cedar_step.rs — IopStep wrapper para CedarAuthorizer.
// En Clojure: Paso 1 del pipeline — :iop/cedar-authorizer
//
// Stub policy: AlwaysAllow (salvo tenant == "evil-tenant").
// El día que se active Cedar real, solo hay que cambiar la impl de execute().

use tracing::{info, warn};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::iop::core::{IopContext, IopStep};

/// Wrapper IopStep para el autorizador Cedar (Zero-Trust — Paso 1).
/// [PORTED_FROM: Paso 1 del pipeline en ig/init-key :iop/orchestrator]
pub struct CedarAuthorizerStep;

impl CedarAuthorizerStep {
    pub fn new() -> Self {
        info!("[CedarStep] Inicializado (STUB AlwaysAllow — FASE 4: cedar-policy real)");
        Self
    }
}

#[async_trait::async_trait]
impl IopStep for CedarAuthorizerStep {
    /// Evalúa autorización Zero-Trust.
    /// STUB: permite todo excepto tenant "evil-tenant".
    /// FASE 4: reemplazar con cedar::authorizer::CedarAuthorizer real.
    #[tracing::instrument(name = "iop.step1.cedar.start", skip(self, ctx), fields(tenant_id = %ctx.tenant_id))]
    async fn execute(&self, mut ctx: IopContext) -> Result<IopContext, DomainError> {
        if ctx.tenant_id == "evil-tenant" {
            warn!("[CedarStep] STUB: Access Denied para evil-tenant");
            return Err(DomainError::new(
                ErrorCode::Janus403, // FASE 2: crear ABAC_403
                "STUB Cedar: acceso denegado",
            ).with_stage("cedar"));
        }

        info!(
            tenant = %ctx.tenant_id,
            "[CedarStep] STUB: AlwaysAllow — autorización OK"
        );
        
        // FASE 4: atributos computados reales desde Cedar
        ctx.is_super_master = ctx.tenant_id == "tnt_master";
        if ctx.is_super_master {
            ctx.cross_tenant_scope = "FULL".to_string();
            ctx.granted_action_keys.clear(); // P-SUPER-MASTER es el único permit
        } else {
            ctx.cross_tenant_scope = "NONE".to_string();
            ctx.granted_action_keys.insert(format!("{}:VIEW", ctx.entity_type));
            ctx.granted_action_keys.insert(format!("{}:CREATE", ctx.entity_type));
            ctx.granted_action_keys.insert(format!("{}:UPDATE", ctx.entity_type));
        }
        ctx.roles.push("admin".to_string());
        
        Ok(ctx)
    }
}
