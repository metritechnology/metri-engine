// cedar_step.rs — IopStep wrapper para CedarAuthorizer.
// En el stack anterior: Paso 1 del pipeline — :iop/cedar-authorizer
//
// Conecta con cedar-policy real y realiza evaluación Zero-Trust.

use cedar_policy::PolicySet;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;

use crate::cedar::{intercept, is_master_tenant, CedarAuthorizer, PrincipalCache};
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

        let policies_src = include_str!("../../config/policies/metri.cedar");
        let policies = PolicySet::from_str(policies_src).expect("Failed to parse metri.cedar");

        let mut policy_cache = HashMap::new();
        // Registrar para roles conocidos y soportados
        policy_cache.insert("admin".to_string(), policies.clone());
        policy_cache.insert("tenant-admin".to_string(), policies.clone());
        policy_cache.insert("contractor".to_string(), policies.clone());
        policy_cache.insert("user".to_string(), policies.clone());
        policy_cache.insert("system-bff".to_string(), policies.clone());
        policy_cache.insert("system-admin".to_string(), policies.clone());
        policy_cache.insert("role_super_master".to_string(), policies.clone());

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
    /// Evalúa autorización Zero-Trust usando cedar::authorizer::intercept real.
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

impl CedarAuthorizerStep {
    async fn execute_inner(&self, mut ctx: IopContext) -> Result<IopContext, DomainError> {
        info!(tenant = %ctx.tenant_id, "[CedarStep] Evaluando autorización Zero-Trust...");

        // Construir un dummy request para poder invocar la frontera de transporte del interceptor
        let mut dummy_req = tonic::Request::new(());

        // Copiar metadatos / cabeceras desde la request guardada en IopContext
        if let Some(auth_val) = ctx.request.get("authorization").and_then(|v| v.as_str()) {
            if let Ok(m_val) = auth_val.parse() {
                dummy_req.metadata_mut().insert("authorization", m_val);
            }
        }
        if let Some(act_val) = ctx.request.get("x-metri-action").and_then(|v| v.as_str()) {
            if let Ok(m_val) = act_val.parse() {
                dummy_req.metadata_mut().insert("x-metri-action", m_val);
            }
        }
        if let Some(et_val) = ctx
            .request
            .get("x-metri-entity-type")
            .and_then(|v| v.as_str())
        {
            if let Ok(m_val) = et_val.parse() {
                dummy_req
                    .metadata_mut()
                    .insert("x-metri-entity-type", m_val);
            }
        }
        if let Some(eid_val) = ctx
            .request
            .get("x-metri-entity-id")
            .and_then(|v| v.as_str())
        {
            if let Ok(m_val) = eid_val.parse() {
                dummy_req.metadata_mut().insert("x-metri-entity-id", m_val);
            }
        }
        if let Some(dom_val) = ctx.request.get("x-metri-domains").and_then(|v| v.as_str()) {
            if let Ok(m_val) = dom_val.parse() {
                dummy_req.metadata_mut().insert("x-metri-domains", m_val);
            }
        }

        // Si faltan cabeceras específicas en la request pero están en el contexto IOP, poblarlas
        if dummy_req.metadata().get("x-metri-action").is_none() {
            if let Ok(m_val) = ctx.operation.parse() {
                dummy_req.metadata_mut().insert("x-metri-action", m_val);
            }
        }
        if dummy_req.metadata().get("x-metri-entity-type").is_none() {
            if let Ok(m_val) = ctx.entity_type.parse() {
                dummy_req
                    .metadata_mut()
                    .insert("x-metri-entity-type", m_val);
            }
        }

        // Ejecutar interceptor real
        let cedar_ctx = intercept(
            &dummy_req,
            self.valkey_store.as_ref(),
            &self.eav_reader,
            self.cache.as_ref(),
            &self.cedar_engine,
            &self.policy_cache,
        )
        .await?;

        // Enriquecer el IopContext con el output verificado e inyectado por Cedar
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
            ctx.granted_action_keys
                .insert(format!("{}:VIEW", ctx.entity_type));
            ctx.granted_action_keys
                .insert(format!("{}:CREATE", ctx.entity_type));
            ctx.granted_action_keys
                .insert(format!("{}:UPDATE", ctx.entity_type));
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
