// cedar/rules.rs — Reglas de seguridad de entidades de sistema.
//
// Qué es maestro y qué entidad es de sistema se decide AQUÍ, una vez, sobre
// la configuración del arranque (engine_config). Antes había tres definiciones
// divergentes de "tenant maestro" repartidas por evaluator, principal_graph y
// este módulo — un tenant podía ser maestro para una capa y no para otra.

use crate::domain::errors::{DomainError, ErrorCode};

/// Entidades reservadas al tenant maestro (arquitectura de cuotas y plugins).
pub const MASTER_ONLY_ENTITIES: &[&str] = &[
    "tenant",
    "domain_quota",
    "quota",
    "domain_plugin",
    "tenant_plugin",
];

/// Tenant maestro vigente según la configuración del arranque.
pub fn master_tenant_id() -> String {
    crate::domain::config::engine_config().master_tenant_id.clone()
}

/// El tenant maestro canónico es `system`, además del configurado y su alias
/// histórico `tnt_master`.
pub fn is_master_tenant(tenant_id: &str) -> bool {
    tenant_id == "system" || tenant_id == "tnt_master" || tenant_id == master_tenant_id()
}

/// Reglas de sistema y de entidades del tenant maestro (`tenant`, `domain_quota`, …).
pub struct SystemSecurityRules;

impl SystemSecurityRules {
    /// true si el tipo de entidad solo puede ser tocado por el tenant maestro.
    pub fn is_master_only_entity(entity_type: &str) -> bool {
        MASTER_ONLY_ENTITIES.contains(&entity_type)
    }

    /// Autorización CRUD (Query, Explore, Mutation) sobre un tipo de entidad.
    /// Las entidades maestro-only solo son accesibles a usuarios del tenant
    /// maestro o a la cuenta BFF de sistema.
    pub fn check_crud_authorization(
        entity_type: &str,
        tenant_id: &str,
        user_id: &str,
        action: &str,
    ) -> Result<(), DomainError> {
        if Self::is_master_only_entity(entity_type) {
            let is_master = is_master_tenant(tenant_id);
            let is_system_bff = user_id == "usr_system_bff";
            if !is_master && !is_system_bff {
                let msg = match action {
                    "mutate" => format!(
                        "Auth403: Only master tenant users can mutate {}",
                        entity_type
                    ),
                    "read" => {
                        "Auth403: Only master tenant users can read tenants or quotas".to_string()
                    }
                    "explore" => format!(
                        "Auth403: Only master tenant users can explore {}",
                        entity_type
                    ),
                    _ => format!(
                        "Auth403: Only master tenant users can access {}",
                        entity_type
                    ),
                };
                return Err(DomainError::new(ErrorCode::Auth403, msg).with_stage("security_rules"));
            }
        }
        Ok(())
    }

    /// Aislamiento de tenant: prohíbe acciones cross-tenant salvo maestro o
    /// system BFF.
    pub fn check_tenant_isolation(
        target_tenant_id: &str,
        caller_tenant_id: &str,
        caller_user_id: &str,
    ) -> Result<(), DomainError> {
        let is_master = is_master_tenant(caller_tenant_id);
        let is_system_bff = caller_user_id == "usr_system_bff";
        if target_tenant_id != caller_tenant_id && !is_master && !is_system_bff {
            return Err(
                DomainError::new(ErrorCode::Auth403, "Auth403: Tenant mismatch")
                    .with_stage("security_rules"),
            );
        }
        Ok(())
    }

    /// true si el tipo de entidad está exento de validación e incremento de
    /// cuota (ilimitado). Hoy coincide con la lista de maestro-only — cada
    /// lista tiene su propia razón de cambio; si se separan, se separan aquí.
    pub fn is_quota_exempt(entity_type: &str) -> bool {
        MASTER_ONLY_ENTITIES.contains(&entity_type)
    }
}


// ── step3b: restricción temporal de acceso ─────────────────────────────────

use chrono::{DateTime, Datelike, Timelike, Utc};

use crate::cedar::types::PrincipalData;

pub fn step3b_validate_time_window(
    principal: &PrincipalData,
    now: DateTime<Utc>,
) -> Result<(), DomainError> {
    if principal.time_restrictions.is_empty() {
        return Ok(());
    }

    let day = now.weekday().number_from_monday() as i32;
    let minute = now.hour() * 60 + now.minute();

    let matches_any = principal.time_restrictions.iter().any(|r| {
        r.days_of_week.contains(&day) && minute >= r.start_minute && minute <= r.end_minute
    });

    if !matches_any {
        return Err(
            DomainError::new(ErrorCode::Auth403, "Access outside allowed time window")
                .with_stage("cedar"),
        );
    }

    Ok(())
}



/// Política de autenticación decidida UNA vez en la raíz de composición
/// (`resolve_dev_auth_bypass`, fail-closed). Sustituye al bool
/// `dev_auth_bypass` hilado por toda la pila: el bypass es un valor explícito,
/// no un flag implícito — en producción siempre es `Production`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthenticationPolicy {
    Production,
    DevBypass,
}

impl AuthenticationPolicy {
    pub fn is_bypass(&self) -> bool {
        matches!(self, AuthenticationPolicy::DevBypass)
    }

    /// Desde el bool que resuelve `resolve_dev_auth_bypass` en el arranque.
    pub fn from_bool(bypass: bool) -> Self {
        if bypass {
            AuthenticationPolicy::DevBypass
        } else {
            AuthenticationPolicy::Production
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listas_maestro_only_y_quota_exempt_coinciden_hoy() {
        for entity in MASTER_ONLY_ENTITIES {
            assert!(SystemSecurityRules::is_master_only_entity(entity));
            assert!(SystemSecurityRules::is_quota_exempt(entity));
        }
    }

    #[test]
    fn system_y_tnt_master_son_maestros_siempre() {
        assert!(is_master_tenant("system"));
        assert!(is_master_tenant("tnt_master"));
        assert!(!is_master_tenant("tnt_cualquiera"));
    }
}
