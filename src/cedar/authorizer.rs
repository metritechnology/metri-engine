// cedar/authorizer.rs — Integración con cedar-policy 3.x para ABAC.
// SRP: Evalúa peticiones gRPC/REST contra políticas de Cedar.

use cedar_policy::{Authorizer, Context, Entities, PolicySet, Request, Decision};
use tracing::{info, warn, error};

use crate::domain::errors::{DomainError, ErrorCode};

pub struct CedarAuthorizer {
    authorizer: Authorizer,
}

impl CedarAuthorizer {
    pub fn new() -> Self {
        Self {
            authorizer: Authorizer::new(),
        }
    }

    /// Evalúa una solicitud de autorización.
    /// Retorna Ok(true) si es Allowed, Ok(false) si Deny, o DomainError.
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
                warn!("[Cedar ABAC] Request DENIED by policies: {:?}", response.diagnostics().reason().collect::<Vec<_>>());
                Ok(false)
            }
        }
    }
}

// Implementación de STUB temporal (hasta conectar con la base de datos de políticas)
// [PORTED_FROM: (defmethod ig/init-key :iop/cedar-authorizer)]
pub fn always_allow_stub(tenant_id: &str) -> Result<bool, DomainError> {
    if tenant_id == "evil-tenant" {
        warn!("[Cedar STUB] Access Denied para evil-tenant");
        Err(DomainError::janus(ErrorCode::Janus403, "access-denied"))
    } else {
        info!("[Cedar STUB] AlwaysAllow activo");
        Ok(true)
    }
}
