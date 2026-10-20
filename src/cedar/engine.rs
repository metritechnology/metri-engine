//! Fachada sobre el motor oficial `cedar-policy` y caché de políticas compiladas al arranque.
//!
//! Evalúa políticas compiladas (`PolicySet`), entidades (`Entities`) y solicitud (`Request`)
//! para emitir una decisión booleana (`Allow` / `Deny`) registrando diagnósticos en trazas.

use tracing::{info, warn};

use cedar_policy::{Authorizer, Decision, Entities, PolicySet, Request};

use crate::domain::errors::DomainError;

/// Envoltorio sobre `cedar_policy::Authorizer` para evaluación determinista de políticas ABAC.
pub struct CedarAuthorizer {
    authorizer: Authorizer,
}

impl Default for CedarAuthorizer {
    fn default() -> Self {
        Self::new()
    }
}

impl CedarAuthorizer {
    /// Construye una nueva instancia sin estado del evaluador `CedarAuthorizer`.
    pub fn new() -> Self {
        Self {
            authorizer: Authorizer::new(),
        }
    }

    /// Evalúa si la solicitud `request` está autorizada bajo el conjunto de políticas y entidades dado.
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
#[allow(clippy::expect_used)] // invariante allowlisted (PLAN_PATRON_RESULT.md R7)
pub fn default_policy_cache() -> std::collections::HashMap<String, PolicySet> {
    use std::str::FromStr;

    let policies_src = include_str!("../../config/policies/metri.cedar");
    let policies = PolicySet::from_str(policies_src).expect("Failed to parse metri.cedar");

    POLICY_ROLES
        .iter()
        .map(|role| (role.to_string(), policies.clone()))
        .collect()
}
