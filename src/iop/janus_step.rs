// [PORTED_FROM: src/metri/janus_router/core.clj — ig/init-key :iop/janus-router]
// iop/janus_step.rs — IopStep wrapper para JanusRouter.
// Paso 3 (y último) del pipeline IOP: validación + ruteo al canal de escritura.
//
// En Clojure: (fn [ctx] (route ctx {:channel-registry channel-registry}))
// En Rust:    JanusRouterStep implementa IopStep, invoca JanusRouter::route()

use std::sync::Arc;
use tracing::info;

use crate::domain::errors::DomainError;
use crate::iop::core::{IopContext, IopStep};
use crate::janus_router::router::JanusRouter;

/// Wrapper IopStep para el JanusRouter (Paso 3 del pipeline IOP).
/// [PORTED_FROM: ig/init-key :iop/janus-router → (fn [ctx] (route ctx deps))]
pub struct JanusRouterStep {
    router: Arc<JanusRouter>,
}

impl JanusRouterStep {
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
    /// [PORTED_FROM: (.route channel safe-ctx) en janus_router/core.clj]
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
        match self.execute_inner(ctx).await {
            Ok(c) => Ok(c),
            Err(e) => {
                let span = tracing::Span::current();
                span.record("error.code", e.code.canonical_code());
                span.record("otel.status_code", "ERROR");
                Err(e)
            }
        }
    }
}

impl JanusRouterStep {
    async fn execute_inner(&self, mut ctx: IopContext) -> Result<IopContext, DomainError> {
        let result = self.router.route(ctx.clone()).await?;

        // El resultado del canal (entity_id, tx_id, ingested_count, etc.)
        // se almacena en ctx.request bajo "result" para que el normalizer lo recoja.
        // [PORTED_FROM: [:ok {:entity-id ... :channel :oltp}] → normalizer]
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
