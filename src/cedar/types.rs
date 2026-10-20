//! Tipos de dominio del autorizador Zero-Trust (`PrincipalData`, `CedarContext`, `InvalidationMsg`).
//!
//! Define los contratos de datos puros que cruzan el módulo: el principal consolidado
//! con sus fronteras de rol y restricciones temporales, el contexto Cedar entregado a los
//! handlers y el mensaje de invalidación reactiva de cachés.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// Frontera de permisos y perímetros geográficos/operativos asociada a un rol del principal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleBoundary {
    /// Identificador único del rol.
    pub role_id: String,
    /// Lista de grants en formato JSON (`domain`, `actions`, `scope`) definidos por el rol.
    pub grants: Vec<serde_json::Value>,
    /// Identificadores de ubicaciones (`location`) permitidas tras expandir e intersecar jerarquías.
    pub permitted_locations: Vec<String>,
    /// Identificadores de activos (`asset`) permitidos tras expandir e intersecar jerarquías.
    pub permitted_assets: Vec<String>,
}

/// Ventana horaria de acceso permitida para un usuario o grupo de usuarios.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRestriction {
    /// Días de la semana permitidos según numeración ISO-8601 (`1` = lunes .. `7` = domingo).
    pub days_of_week: Vec<i32>,
    /// Minuto del día UTC en que inicia la ventana permitida (`0..=1439`).
    pub start_minute: u32,
    /// Minuto del día UTC en que finaliza la ventana permitida (`0..=1439`).
    pub end_minute: u32,
}

/// Grafo consolidado de identidad, roles, grupos, perímetros y restricciones horarias de un usuario.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalData {
    /// Identificador único del usuario.
    pub user_id: String,
    /// Identificador del tenant al que pertenece el usuario.
    pub tenant_id: String,
    /// Estado operativo del usuario (`ACTIVE`, `SUSPENDED`, etc.).
    pub status: String,
    /// Clasificación del usuario (`INTERNAL`, `CONTRACTOR`, `SYSTEM`).
    pub user_type: String,
    /// Identificador de la empresa contratista asociada (vacío para usuarios internos).
    pub company_id: String,
    /// Conjunto de identificadores de roles asignados al usuario.
    pub roles: HashSet<String>,
    /// Fronteras de autorización y perímetros por cada rol asignado.
    pub roles_boundaries: Vec<RoleBoundary>,
    /// Restricciones de ventana horaria aplicables al usuario (propias o heredadas de grupos).
    pub time_restrictions: Vec<TimeRestriction>,
    /// Unión de ubicaciones permitidas por los grupos a los que pertenece el usuario.
    pub group_allowed_locations: Vec<String>,
    /// Unión de activos permitidos por los grupos a los que pertenece el usuario.
    pub group_allowed_assets: Vec<String>,
    /// Conjunto completo de grupos visitados en la jerarquía del usuario.
    pub groups: HashSet<String>,
}

/// Mensaje publicado en el bus de invalidación cuando muta una entidad de IAM (`user`, `role`, `user_group`).
#[derive(Debug, Clone)]
pub struct InvalidationMsg {
    /// Identificador del tenant donde ocurrió la mutación.
    pub tenant_id: String,
    /// Tipo de entidad modificada (`user`, `role` o `user_group`).
    pub entity_type: String,
    /// Identificador único de la entidad modificada.
    pub entity_id: String,
}

/// Contexto de autorización producido por el pipeline Cedar y consumido por los handlers y el router Janus.
#[derive(Debug, Clone)]
pub struct CedarContext {
    /// Identificador del tenant autenticado en la sesión.
    pub tenant_id: String,
    /// Identificador del usuario autenticado en la sesión.
    pub user_id: String,
    /// Conjunto de identificadores de roles activos del principal.
    pub roles: HashSet<String>,
    /// Diccionario JSON de fronteras de alcance (`query_scope` y perímetros) por dominio.
    pub domain_boundaries: serde_json::Value,
}
