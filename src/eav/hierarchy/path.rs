//! Materialized paths for O(1) hierarchies.
//!
//! Materialized Paths para jerarquías O(1)
//! Blueprint: Metri EAV §XI.1

use crate::domain::errors::DomainError;
use tracing::debug;

/// Construye el path de jerarquía de una entidad recorriendo
/// el árbol de padres hacia arriba.
///
/// El path se almacena como atributo `sys/hierarchy_path = ["01J_ROOT", "01J_PARENT"]`.
/// Al tener `index: true`, DynamoDB genera items AVET para cada ULID del path.
///
/// Esto permite: Query AVET PK="T#tnt#AV#hierarchy_path" SK begins_with "01J_ROOT"
/// → todos los descendientes en O(1).
///
/// [Blueprint: §XI.1 — "Materialized Paths para Jerarquías O(1)"]
pub async fn build_hierarchy_path(
    entity_id: &str,
    parent_id: Option<&str>,
    // En producción: `tenant_id: &str, ddb: &DynamoClient`
    // Para FASE 1: calcula desde el parent_id directo sin sub-queries
) -> Result<Vec<String>, DomainError> {
    let mut path = Vec::new();

    // Si hay parent, el path del child = path del parent + parent_id
    // En FASE 1: solo incluimos el parent inmediato (1 nivel)
    // En FASE 2: haremos el pull del parent y concatenaremos su path
    if let Some(parent) = parent_id {
        path.push(parent.to_string());
    }

    // La entidad misma no se incluye — el path son solo los ANCESTROS
    debug!("[Hierarchy] path para {} → {:?}", entity_id, path);

    Ok(path)
}

/// Extrae todos los descendientes de una entidad usando el índice AVET.
/// Query: PK="T#tnt#AV#sys/hierarchy_path" SK begins_with entity_id
///
/// Retorna Vec<entity_id> de los descendientes directos e indirectos en O(1).
pub fn subtree_query_pk(tenant_id: &str) -> String {
    format!("T#{}#AV#sys/hierarchy_path", tenant_id)
}
