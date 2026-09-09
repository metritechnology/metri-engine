//! Zero-Trust authorization pipeline — steps 1 to 3b.
//!
//! Pipeline de autorización Zero-Trust.
//!
//! UNA composición de los pasos step1→step2→step3→step3b que antes estaba
//! copiada en `intercept` y `get_principal_data`. Las fachadas públicas
//! (`intercept`, `get_principal_data` en authorizer.rs) delegan aquí.

use std::time::Instant;

use chrono::Utc;
use tracing::Instrument;

use crate::cedar::authn::step1_extract_token;
use crate::cedar::ports::{EntityReader, PolicyStore, PrincipalCache};
use crate::cedar::principal_graph::{step2_query_oltp, step3_consolidate};
use crate::cedar::request::AuthRequest;
use crate::cedar::rules::step3b_validate_time_window;
use crate::cedar::types::{CedarContext, PrincipalData};
use crate::domain::errors::DomainError;
use crate::domain::protocols::ISessionStore;

/// Resuelve y consolida el principal para la sesión de la petición:
/// autenticación (step1), grafo de roles/grupos (step2), intersección de
/// perímetros (step3) y ventana horaria (step3b).
pub async fn resolve_principal(
    auth: &AuthRequest<'_>,
    valkey_store: &dyn ISessionStore,
    eav_reader: &dyn EntityReader,
    cache: &dyn PrincipalCache,
) -> Result<PrincipalData, DomainError> {
    // Observabilidad por paso (5C): cada etapa emite su latencia como evento
    // tracing estructurado — agregable por el colector de logs sin necesitar
    // un SDK de métricas propio.
    let started = Instant::now();

    let session = step1_extract_token(auth, valkey_store)
        .instrument(tracing::info_span!("cedar", step = "1_authn"))
        .await?;
    tracing::debug!(target: "cedar", step = "1_authn", elapsed_ms = started.elapsed().as_millis() as u64);

    let raw_principal = step2_query_oltp(eav_reader, &session.tenant_id, &session.user_id, cache)
        .instrument(tracing::info_span!("cedar", step = "2_principal"))
        .await?;

    let principal = step3_consolidate(eav_reader, &session.tenant_id, raw_principal, true)
        .instrument(tracing::info_span!("cedar", step = "3_consolidate"))
        .await?;

    // step3b es síncrona — el span se entra y se suelta en la misma línea.
    {
        let _step = tracing::info_span!("cedar", step = "3b_time_window").entered();
        step3b_validate_time_window(&principal, Utc::now())?;
    }

    tracing::info!(
        target: "cedar",
        metric = "pipeline_latency",
        step = "principal_resuelto",
        elapsed_ms = started.elapsed().as_millis() as u64,
        "principal resuelto y consolidado"
    );
    Ok(principal)
}

/// Principal sintetizado del bypass de desarrollo — la decisión de activarlo
/// la toma la raíz de composición (`resolve_dev_auth_bypass`, fail-closed);
/// en producción esta rama es inalcanzable.
pub(crate) fn dev_bypass_principal(auth: &AuthRequest<'_>) -> PrincipalData {
    let test_tenant = auth.dev_test_tenant.unwrap_or("system").to_string();
    let test_user = auth.dev_test_user.unwrap_or("usr_system_bff").to_string();
    let test_roles: std::collections::HashSet<String> = auth
        .dev_test_roles
        .map(|s| s.split(',').map(|r| r.to_string()).collect())
        .unwrap_or_else(|| ["system-admin".to_string()].into_iter().collect());

    PrincipalData {
        user_id: test_user,
        tenant_id: test_tenant,
        status: "ACTIVE".to_string(),
        user_type: "SYSTEM".to_string(),
        company_id: String::new(),
        roles: test_roles,
        roles_boundaries: vec![],
        time_restrictions: vec![],
        group_allowed_locations: vec![],
        group_allowed_assets: vec![],
        groups: std::collections::HashSet::new(),
    }
}

/// Contexto Cedar para los handlers: quién pide y con qué boundaries por dominio.
pub(crate) fn cedar_context(
    session_tenant_id: String,
    session_user_id: String,
    principal_roles: std::collections::HashSet<String>,
    domain_boundaries: serde_json::Value,
) -> CedarContext {
    CedarContext {
        tenant_id: session_tenant_id,
        user_id: session_user_id,
        roles: principal_roles,
        domain_boundaries,
    }
}

// ── Fachadas públicas de la pipeline ────────────────────────────────────────

use crate::cedar::engine::CedarAuthorizer;
use crate::cedar::evaluator::step4_evaluate_cedar;

pub async fn intercept<T>(
    req: &tonic::Request<T>,
    valkey_store: &dyn ISessionStore,
    eav_reader: &dyn EntityReader,
    cache: &dyn PrincipalCache,
    cedar_engine: &CedarAuthorizer,
    policy_cache: &dyn PolicyStore,
) -> Result<CedarContext, DomainError> {
    let auth = AuthRequest::from_tonic(req);

    let started = Instant::now();
    let principal = resolve_principal(&auth, valkey_store, eav_reader, cache).await?;

    let action = auth.cedar_action();
    let mut resource = auth.cedar_resource();
    crate::cedar::resource_hydrator::hydrate_company_assignment(
        eav_reader,
        &principal.tenant_id,
        &mut resource,
    )
    .await;

    let domain_dict =
        match step4_evaluate_cedar(cedar_engine, policy_cache, &principal, &action, &resource) {
            Ok(dict) => {
                tracing::info!(
                    target: "cedar",
                    metric = "decision",
                    decision = "allow",
                    action = %action,
                    tenant = %principal.tenant_id,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "Cedar ALLOW"
                );
                dict
            }
            Err(e) => {
                tracing::warn!(
                    target: "cedar",
                    metric = "decision",
                    decision = "deny",
                    action = %action,
                    tenant = %principal.tenant_id,
                    stage = %e.stage,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "Cedar DENY"
                );
                return Err(e);
            }
        };

    Ok(cedar_context(
        principal.tenant_id,
        principal.user_id,
        principal.roles,
        domain_dict,
    ))
}

pub async fn get_principal_data<T>(
    req: &tonic::Request<T>,
    valkey_store: &dyn ISessionStore,
    eav_reader: &dyn EntityReader,
    cache: &dyn PrincipalCache,
    policy: crate::cedar::rules::AuthenticationPolicy,
) -> Result<PrincipalData, DomainError> {
    // Costura explícita de desarrollo: la política la decide la raíz de
    // composición vía `resolve_dev_auth_bypass` (fail-closed) — nunca el
    // entorno por petición. En producción es siempre `Production`.
    let auth = AuthRequest::from_tonic(req);
    if policy.is_bypass() {
        return Ok(dev_bypass_principal(&auth));
    }
    resolve_principal(&auth, valkey_store, eav_reader, cache).await
}
