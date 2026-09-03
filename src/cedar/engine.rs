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

