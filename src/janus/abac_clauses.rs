// [PORTED_FROM: src/metri/janus/abac_clauses.clj]
// janus/abac_clauses.rs — Construcción de cláusulas ABAC desde el CedarCtx.
// SRP: transforma {entity + boundaries + user-id + schema} -> nodo AST IR ABAC.

use serde_json::{json, Value};
use tracing::warn;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::janus::router::CedarCtx; // Import CedarCtx which holds boundaries eventually

pub struct OwnershipFields {
    pub owner_field: Option<String>,
    pub assignee_field: Option<String>,
}

/// Extrae los atributos de propiedad y asignación desde el model schema del Códice.
/// [PORTED_FROM: (ownership-fields entity schema)]
pub fn ownership_fields(entity: &str, schema: &Value) -> OwnershipFields {
    let mut owner_field = None;
    let mut assignee_field = None;

    if let Some(attrs) = schema.get("attributes").and_then(|v| v.as_array()) {
        for attr in attrs {
            if attr.get("is_owner").and_then(|v| v.as_bool()).unwrap_or(false) {
                if let Some(name) = attr.get("name").and_then(|v| v.as_str()) {
                    owner_field = Some(format!("{entity}/{name}"));
                }
            }
            if attr.get("is_assignee").and_then(|v| v.as_bool()).unwrap_or(false) {
                if let Some(name) = attr.get("name").and_then(|v| v.as_str()) {
                    assignee_field = Some(format!("{entity}/{name}"));
                }
            }
        }
    }

    OwnershipFields {
        owner_field,
        assignee_field,
    }
}

/// Retorna el nodo Zero-Trust cardinal. SIEMPRE debe ser el primer nodo del :where.
/// [PORTED_FROM: (tenant-node tenant-id)]
pub fn tenant_node(tenant_id: &str) -> Value {
    json!(["=", "tenant/id", tenant_id])
}

/// Construye el nodo ABAC combinado desde los boundaries (en FASE 2/3 pasados como array).
/// [PORTED_FROM: (build-abac-node entity boundaries owner-field assignee-field user-id)]
pub fn build_abac_node(
    entity: &str,
    boundaries: &[Value],
    owner_field: Option<&str>,
    assignee_field: Option<&str>,
    user_id: &str,
) -> Result<Option<Value>, DomainError> {
    let mut clauses = Vec::new();

    for boundary in boundaries {
        // 4b: Fronteras geográficas (permitted-locations)
        let mut loc_node = None;
        if let Some(locs) = boundary.get("permitted-locations").and_then(|v| v.as_array()) {
            if !locs.is_empty() {
                loc_node = Some(json!(["in", format!("{entity}/location_id"), locs]));
            }
        }

        // 4c: Predicado de scope
        let mut scope_node = None;
        if let Some(scope) = boundary.get("query-scope").and_then(|v| v.as_str()) {
            match scope {
                "ALL" => {}
                "OWN" => {
                    if let Some(own) = owner_field {
                        scope_node = Some(json!(["=", own, user_id]));
                    }
                }
                "ASSIGNED" => {
                    if let Some(ass) = assignee_field {
                        scope_node = Some(json!(["=", ass, user_id]));
                    }
                }
                "OWN_OR_ASSIGNED" => {
                    match (owner_field, assignee_field) {
                        (Some(own), Some(ass)) => {
                            scope_node = Some(json!(["or", ["=", own, user_id], ["=", ass, user_id]]));
                        }
                        (Some(own), None) => {
                            scope_node = Some(json!(["=", own, user_id]));
                        }
                        (None, Some(ass)) => {
                            scope_node = Some(json!(["=", ass, user_id]));
                        }
                        _ => {}
                    }
                }
                "NONE" => {
                    return Err(DomainError::janus(
                        ErrorCode::Janus403,
                        format!("Scope NONE — no grant for domain: {entity}")
                    ));
                }
                _ => {
                    warn!("[ABAC] Scope desconocido: {}", scope);
                }
            }
        }

        match (loc_node, scope_node) {
            (Some(loc), Some(scope)) => clauses.push(json!(["and", loc, scope])),
            (Some(loc), None) => clauses.push(loc),
            (None, Some(scope)) => clauses.push(scope),
            (None, None) => {}
        }
    }

    match clauses.len() {
        0 => Ok(None),
        1 => Ok(Some(clauses.remove(0))),
        _ => {
            let mut or_node = vec![json!("or")];
            or_node.extend(clauses);
            Ok(Some(Value::Array(or_node)))
        }
    }
}
