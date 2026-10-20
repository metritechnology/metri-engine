//! Paso 3 del pipeline IOP (`JanusRouterStep`) — validación de esquema y ruteo al canal de escritura.

use std::sync::Arc;
use tracing::info;

use crate::domain::errors::DomainError;
use crate::iop::core::{IopContext, IopStep};
use crate::janus_router::router::JanusRouter;

/// Wrapper IopStep para el JanusRouter (Paso 3 del pipeline IOP).
pub struct JanusRouterStep {
    router: Arc<JanusRouter>,
}

impl JanusRouterStep {
    /// Construye un nuevo `JanusRouterStep` envolviendo la instancia compartida de `JanusRouter`.
    pub fn new(router: Arc<JanusRouter>) -> Self {
        info!("[JanusStep] Inicializado — Write Path activo");
        Self { router }
    }
}

#[async_trait::async_trait]
impl IopStep for JanusRouterStep {
    /// Ejecuta el pipeline Write Path del JanusRouter.
    /// Pasos internos:
    ///   1. Cargar schema Códice
    ///   2. Validar payload
    ///   3. Pre-checks (write_path_locked, is_system_seeded)
    ///   4. Resolver engine (oltp | olap)
    ///   5. Enriquecer ctx (tenant_id inyectado)
    ///   6. Despachar al canal
    ///
    #[tracing::instrument(
        name = "iop.step3.janus.start",
        skip(self, ctx),
        fields(
            tenant_id = %ctx.tenant_id,
            error.code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty
        )
    )]
    async fn execute(&self, ctx: IopContext) -> Result<IopContext, DomainError> {
        crate::iop::core::record_step_span_error(self.execute_inner(ctx).await)
    }
}

impl JanusRouterStep {
    async fn execute_inner(&self, mut ctx: IopContext) -> Result<IopContext, DomainError> {
        let result = self.router.route(ctx.clone()).await?;

        // El resultado del canal (entity_id, tx_id, ingested_count, etc.)
        // se almacena en ctx.request bajo "result" para que el normalizer lo recoja.
        if let Some(obj) = ctx.request.get_mut("result") {
            // Ya existe — merge
            if let (Some(r_obj), Some(res_obj)) = (obj.as_object_mut(), result.as_object()) {
                for (k, v) in res_obj {
                    r_obj.insert(k.clone(), v.clone());
                }
            }
        } else {
            ctx.request.insert("result".to_string(), result);
        }

        Ok(ctx)
    }
}

#[cfg(test)]
#[path = "tests/janus_step_tests.rs"]
mod tests;
