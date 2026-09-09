//! Authorizer domain types — principal, context, invalidation.
//!
//! Tipos de dominio del autorizador Zero-Trust.
//! Contratos de datos que cruzan el módulo: el principal consolidado con sus
//! límites de rol, restricciones temporales, el contexto Cedar que reciben los
//! handlers y el mensaje de invalidación de caché. Solo tipos — nada de lógica.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleBoundary {
    pub role_id: String,
    pub grants: Vec<serde_json::Value>,
    pub permitted_locations: Vec<String>,
    pub permitted_assets: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRestriction {
    pub days_of_week: Vec<i32>,
    pub start_minute: u32,
    pub end_minute: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalData {
    pub user_id: String,
    pub tenant_id: String,
    pub status: String,
    pub user_type: String,
    pub company_id: String,
    pub roles: HashSet<String>,
    pub roles_boundaries: Vec<RoleBoundary>,
    pub time_restrictions: Vec<TimeRestriction>,
    pub group_allowed_locations: Vec<String>,
    pub group_allowed_assets: Vec<String>,
    pub groups: HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct InvalidationMsg {
    pub tenant_id: String,
    pub entity_type: String,
    pub entity_id: String,
}

#[derive(Debug, Clone)]
pub struct CedarContext {
    pub tenant_id: String,
    pub user_id: String,
    pub roles: HashSet<String>,
    pub domain_boundaries: serde_json::Value,
}
