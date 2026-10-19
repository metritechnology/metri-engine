//! CedarAuthorizer as an IOP step.
//!
//! IopStep wrapper para CedarAuthorizer.
//!
//! # Origin
//! En el stack anterior: Paso 1 del pipeline — :iop/cedar-authorizer
//!
//! Conecta con cedar-policy real y realiza evaluación Zero-Trust.

use std::collections::HashMap;
use std::sync::Arc;

use cedar_policy::PolicySet;
use tracing::info;

use crate::cedar::{
    intercept_auth, is_master_tenant, AuthRequest, CedarAuthorizer, PrincipalCache,
};
use crate::domain::errors::DomainError;
use crate::domain::protocols::ISessionStore;
use crate::eav::reader::pull::EavReader;
use crate::iop::core::{IopContext, IopStep};

/// Wrapper IopStep para el autorizador Cedar (Zero-Trust — Paso 1).
pub struct CedarAuthorizerStep {
    valkey_store: Arc<dyn ISessionStore>,
    eav_reader: EavReader,
    cache: Arc<dyn PrincipalCache>,
    cedar_engine: CedarAuthorizer,
    policy_cache: HashMap<String, PolicySet>,
}

impl CedarAuthorizerStep {
    pub fn new(
        valkey_store: Arc<dyn ISessionStore>,
        eav_reader: EavReader,
        cache: Arc<dyn PrincipalCache>,
    ) -> Self {
        info!("[CedarStep] Inicializando con motor real Cedar ABAC (Zero-Trust)");

        let policy_cache = crate::cedar::engine::default_policy_cache();

        Self {
            valkey_store,
            eav_reader,
            cache,
            cedar_engine: CedarAuthorizer::new(),
            policy_cache,
        }
    }
}

#[async_trait::async_trait]
impl IopStep for CedarAuthorizerStep {
    /// Evalúa autorización Zero-Trust usando cedar::pipeline::intercept_auth real.
    #[tracing::instrument(
        name = "iop.step1.cedar.start",
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

impl CedarAuthorizerStep {
    async fn execute_inner(&self, mut ctx: IopContext) -> Result<IopContext, DomainError> {
        info!(tenant = %ctx.tenant_id, "[CedarStep] Evaluando autorización Zero-Trust...");

        let req_str = |key: &str| ctx.request.get(key).and_then(|v| v.as_str());
        let action_fallback = (!ctx.operation.is_empty()).then_some(ctx.operation.as_str());
        let entity_type_fallback =
            (!ctx.entity_type.is_empty()).then_some(ctx.entity_type.as_str());

        let auth = AuthRequest {
            authorization: req_str("authorization"),
            sid: req_str("sid"),
            action: req_str("x-metri-action").or(action_fallback),
            entity_type: req_str("x-metri-entity-type").or(entity_type_fallback),
            entity_id: req_str("x-metri-entity-id"),
            domains: req_str("x-metri-domains"),
            dev_test_tenant: req_str("test-tenant"),
            dev_test_user: req_str("test-user"),
            dev_test_roles: req_str("test-roles"),
        };

        let cedar_ctx = intercept_auth(
            &auth,
            self.valkey_store.as_ref(),
            &self.eav_reader,
            self.cache.as_ref(),
            &self.cedar_engine,
            &self.policy_cache,
        )
        .await?;

        if ctx.tenant_id.is_empty() || !is_master_tenant(&cedar_ctx.tenant_id) {
            ctx.tenant_id = cedar_ctx.tenant_id.clone();
        }
        ctx.user_id = cedar_ctx.user_id.clone();
        ctx.roles = cedar_ctx.roles.into_iter().collect();
        ctx.domain_boundaries = cedar_ctx.domain_boundaries;
        ctx.is_super_master = is_master_tenant(&cedar_ctx.tenant_id);

        if ctx.is_super_master {
            ctx.cross_tenant_scope = "FULL".to_string();
            ctx.granted_action_keys.clear();
        } else {
            ctx.cross_tenant_scope = "NONE".to_string();
            for verb in ["VIEW", "CREATE", "UPDATE"] {
                ctx.granted_action_keys
                    .insert(format!("{}:{verb}", ctx.entity_type));
            }
        }

        info!(
            tenant = %ctx.tenant_id,
            user = %ctx.user_id,
            "[CedarStep] Autorización COMPLETADA exitosamente"
        );

        Ok(ctx)
    }
}

#[cfg(test)]
#[path = "tests/cedar_step_tests.rs"]
mod tests;
