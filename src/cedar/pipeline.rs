// cedar/pipeline.rs — Pipeline de autorización Zero-Trust.
//
// UNA composición de los pasos step1→step2→step3→step3b que antes estaba
// copiada en `intercept` y `get_principal_data`. Las fachadas públicas
// (`intercept`, `get_principal_data` en authorizer.rs) delegan aquí.

use chrono::Utc;

use crate::cedar::ports::{EntityReader, PrincipalCache};
use crate::cedar::request::AuthRequest;
use crate::cedar::types::{CedarContext, PrincipalData};
use crate::cedar::authorizer::step3b_validate_time_window;
use crate::domain::errors::DomainError;
use crate::domain::protocols::ISessionStore;

/// Resuelve y consolida el principal para la sesión de la petición:
/// autenticación (step1), grafo de roles/grupos (step2), intersección de
/// perímetros (step3) y ventana horaria (step3b).
pub(crate) async fn resolve_principal(
    auth: &AuthRequest<'_>,
    valkey_store: &dyn ISessionStore,
    eav_reader: &dyn EntityReader,
    cache: &dyn PrincipalCache,
) -> Result<PrincipalData, DomainError> {
    let session = crate::cedar::authorizer::step1_extract_token(auth, valkey_store).await?;
    let raw_principal = crate::cedar::authorizer::step2_query_oltp(
        eav_reader,
        &session.tenant_id,
        &session.user_id,
        cache,
    )
    .await?;
    let principal = crate::cedar::authorizer::step3_consolidate(
        eav_reader,
        &session.tenant_id,
        raw_principal,
        true,
    )
    .await?;
    step3b_validate_time_window(&principal, Utc::now())?;
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
