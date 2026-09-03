// cedar/engine.rs — Fachada sobre el motor cedar-policy.
//
// El PDP de Cedar puro: políticas compiladas + entidades + request →
// decisión. Pierde los diagnósticos al colapsar a bool — aceptado: los
// handlers reportan el DENY canónico; si algún día se necesita el motivo
// exacto, devolver Decision aquí y propagar.

use tracing::{info, warn};

use cedar_policy::{Authorizer, Decision, Entities, PolicySet, Request};

use crate::domain::errors::DomainError;

pub struct CedarAuthorizer {
    authorizer: Authorizer,
}

impl Default for CedarAuthorizer {
    fn default() -> Self {
        Self::new()
    }
}

impl CedarAuthorizer {
    pub fn new() -> Self {
        Self {
            authorizer: Authorizer::new(),
        }
    }

    pub fn is_authorized(
        &self,
        policies: &PolicySet,
        entities: &Entities,
        request: &Request,
    ) -> Result<bool, DomainError> {
        let response = self.authorizer.is_authorized(request, policies, entities);

        match response.decision() {
            Decision::Allow => {
                info!("[Cedar ABAC] Request ALLOWED");
                Ok(true)
            }
            Decision::Deny => {
                warn!(
                    "[Cedar ABAC] Request DENIED by policies: {:?}",
                    response.diagnostics().reason().collect::<Vec<_>>()
                );
                Ok(false)
            }
        }
    }
}

/// Roles para los que se compila la política base al arranque.
pub const POLICY_ROLES: &[&str] = &[
    "admin",
    "tenant-admin",
    "contractor",
    "user",
    "system-bff",
    "system-admin",
    "role_super_master",
];

/// Cache de políticas compiladas para los roles conocidos — el bootstrap
/// estaba copiado en grpc/service.rs e iop/cedar_step.rs (D2). Una sola
/// fuente: mismo include, mismo parse, mismos roles.
pub fn default_policy_cache() -> std::collections::HashMap<String, PolicySet> {
    use std::str::FromStr;

    let policies_src = include_str!("../../config/policies/metri.cedar");
    let policies = PolicySet::from_str(policies_src).expect("Failed to parse metri.cedar");

    POLICY_ROLES
        .iter()
        .map(|role| (role.to_string(), policies.clone()))
        .collect()
}
