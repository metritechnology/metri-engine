//! Guardia de aislamiento multi-tenant (`TenantGuard`) en la capa de almacenamiento EAV.
//!
//! Valida que toda operación de lectura o escritura incluya un `tenant_id` no vacío,
//! bloquea el uso de `SYSTEM_TENANT_ID` salvo en contextos explícitamente autorizados y
//! construye la clave de partición canónica `T#<tenant_id>#E#<entity_id>`.

use crate::codice::CodeRegistry;
use crate::domain::errors::{DomainError, ErrorCode};

/// ID de sistema para operaciones de bootstrap (seeds, migraciones).
pub const SYSTEM_TENANT_ID: &str = "SYSTEM";

/// TenantGuard — garantiza que todo I/O del EAV engine tenga tenant_id válido.
pub struct TenantGuard {
    /// Prefijo usado en el PK de DynamoDB: "T#<tenant_id>#..."
    /// Verifica que el tenant_id no sea vacío ni "SYSTEM" en operaciones de usuario.
    pub allowed_system_ops: bool,
}

impl TenantGuard {
    /// Construye un `TenantGuard` estándar que rechaza operaciones sobre el tenant `SYSTEM`.
    pub fn new() -> Self {
        TenantGuard {
            allowed_system_ops: false,
        }
    }

    /// Construye un `TenantGuard` que permite operaciones administrativas bajo `SYSTEM_TENANT_ID`.
    pub fn with_system_ops() -> Self {
        TenantGuard {
            allowed_system_ops: true,
        }
    }

    /// Valida que el tenant_id sea un valor no-vacío y bien formado.
    pub fn validate_tenant(&self, tenant_id: &str) -> Result<(), DomainError> {
        if tenant_id.is_empty() {
            return Err(DomainError::auth(
                ErrorCode::Auth403,
                "tenant_id no puede ser vacío",
            ));
        }
        if (tenant_id.eq_ignore_ascii_case(SYSTEM_TENANT_ID)
            || tenant_id.eq_ignore_ascii_case("metri_global")
            || crate::cedar::is_master_tenant(tenant_id))
            && !self.allowed_system_ops
        {
            return Err(DomainError::auth(
                ErrorCode::Auth403,
                "Operaciones del sistema SYSTEM_TENANT_ID no permitidas en este contexto",
            ));
        }
        Ok(())
    }

    /// Genera el Partition Key canónico para la tabla EAV.
    /// Formato: "T#<tenant_id>#E#<entity_id>"
    pub fn eavt_pk(&self, tenant_id: &str, entity_id: &str) -> Result<String, DomainError> {
        self.validate_tenant(tenant_id)?;
        Ok(format!("T#{}#E#{}", tenant_id, entity_id))
    }

    /// Verifica que el entity_type exista en el CodeRegistry.
    pub fn ensure_schema(
        &self,
        entity_type: &str,
        registry: &CodeRegistry,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_guard_bloquea_variantes_system_y_master_sin_permiso_explicito() {
        let guard = TenantGuard::new();
        for forbidden in ["", "SYSTEM", "system", "metri_global", "METRI_GLOBAL"] {
            assert!(
                guard.validate_tenant(forbidden).is_err(),
                "Debió bloquear tenant '{forbidden}'"
            );
        }
        assert!(guard.validate_tenant("tnt_01").is_ok());

        let sys_guard = TenantGuard::with_system_ops();
        assert!(sys_guard.validate_tenant("SYSTEM").is_ok());
        assert!(sys_guard.validate_tenant("system").is_ok());
        assert!(sys_guard.validate_tenant("metri_global").is_ok());
    }
}
