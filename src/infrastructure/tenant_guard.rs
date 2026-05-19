// [PORTED_FROM: src/metri/infrastructure/tenant_guard.clj]
// infrastructure/tenant_guard.rs — Aislamiento multitenant en la capa EAV.
// En Clojure: Pool Model gate para Datahike + schema verification.
// En Rust:    TenantGuard valida que todo I/O tenga tenant_id en el PK.
//
// Zero-Drop Policy: replicas query_with_tenant, transact_with_tenant!,
// y el chequeo de schema (adapt: en EAV, el "schema" es el CodeRegistry).

use crate::codice::CodeRegistry;
use crate::domain::errors::{DomainError, ErrorCode};

/// ID de sistema para operaciones de bootstrap (seeds, migraciones).
/// [PORTED_FROM: (def SYSTEM_TENANT_ID "SYSTEM")]
pub const SYSTEM_TENANT_ID: &str = "SYSTEM";

/// TenantGuard — garantiza que todo I/O del EAV engine tenga tenant_id válido.
/// [PORTED_FROM: (defmethod ig/init-key :infra/tenant-guard ...)]
pub struct TenantGuard {
    /// Prefijo usado en el PK de DynamoDB: "T#<tenant_id>#..."
    /// Verifica que el tenant_id no sea vacío ni "SYSTEM" en operaciones de usuario.
    pub allowed_system_ops: bool,
}

impl TenantGuard {
    pub fn new() -> Self {
        TenantGuard { allowed_system_ops: false }
    }

    pub fn with_system_ops() -> Self {
        TenantGuard { allowed_system_ops: true }
    }

    /// Valida que el tenant_id sea un valor no-vacío y bien formado.
    /// [PORTED_FROM: la validación implícita de transact-with-tenant!]
    pub fn validate_tenant(&self, tenant_id: &str) -> Result<(), DomainError> {
        if tenant_id.is_empty() {
            return Err(DomainError::auth(
                ErrorCode::Auth403,
                "tenant_id no puede ser vacío",
            ));
        }
        if tenant_id == SYSTEM_TENANT_ID && !self.allowed_system_ops {
            return Err(DomainError::auth(
                ErrorCode::Auth403,
                "Operaciones del sistema SYSTEM_TENANT_ID no permitidas en este contexto",
            ));
        }
        Ok(())
    }

    /// Genera el Partition Key canónico para la tabla EAV.
    /// [PORTED_FROM: lógica implícita de isolation por :tenant/id en Datahike]
    /// Formato: "T#<tenant_id>#E#<entity_id>"
    pub fn eavt_pk(&self, tenant_id: &str, entity_id: &str) -> Result<String, DomainError> {
        self.validate_tenant(tenant_id)?;
        Ok(format!("T#{}#E#{}", tenant_id, entity_id))
    }

    /// Verifica que el entity_type exista en el CodeRegistry.
    /// [PORTED_FROM: ensure-tenant-schema! — verifica existencia de atributos]
    pub fn ensure_schema(
        &self,
        entity_type: &str,
        registry:    &CodeRegistry,
    ) -> Result<(), DomainError> {
        if registry.get_model(entity_type).is_none() {
            return Err(DomainError::eav(
                ErrorCode::Eav004,
                format!("entity_type '{entity_type}' no registrado en CodeRegistry"),
            ));
        }
        Ok(())
    }
}

impl Default for TenantGuard {
    fn default() -> Self {
        Self::new()
    }
}
