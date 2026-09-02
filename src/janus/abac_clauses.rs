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
    use std::collections::HashSet;

    let mut has_all = false;
    let mut has_own = false;
    let mut has_assigned = false;
    let mut has_own_or_assigned = false;
    let mut valid_scopes_count = 0;

    let mut all_permitted_locations = HashSet::new();
    let mut location_restricted = true;

    for boundary in boundaries {
        let scope = boundary.get("query-scope").or_else(|| boundary.get("query_scope")).and_then(|v| v.as_str()).unwrap_or("NONE");
        if scope != "NONE" {
            valid_scopes_count += 1;
            match scope {
                "ALL" => has_all = true,
                "OWN" => has_own = true,
                "ASSIGNED" => has_assigned = true,
                "OWN_OR_ASSIGNED" => has_own_or_assigned = true,
                _ => {}
            }

            if let Some(locs) = boundary.get("permitted-locations").or_else(|| boundary.get("permitted_locations")).and_then(|v| v.as_array()) {
                if locs.is_empty() {
                    location_restricted = false;
                } else {
                    for loc in locs {
                        if let Some(s) = loc.as_str() {
                            all_permitted_locations.insert(s.to_string());
                        }
                    }
                }
            } else {
                location_restricted = false;
            }
        }
    }

    if valid_scopes_count == 0 {
        return Err(DomainError::janus(
            ErrorCode::Janus403,
            format!("Scope NONE — no grant for domain: {entity}")
        ));
    }

    let consolidated_scope = if has_all {
        "ALL"
    } else if has_own_or_assigned || (has_own && has_assigned) {
        "OWN_OR_ASSIGNED"
    } else if has_own {
        "OWN"
    } else {
        "ASSIGNED"
    };

    let mut loc_node = None;
    if location_restricted && !all_permitted_locations.is_empty() {
        let locs_val: Vec<Value> = all_permitted_locations.into_iter().map(Value::String).collect();
        loc_node = Some(json!(["in", format!("{entity}/location_id"), locs_val]));
    }

    let mut scope_node = None;
    match consolidated_scope {
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
        _ => {}
    }

    match (loc_node, scope_node) {
        (Some(loc), Some(scope)) => Ok(Some(json!(["and", loc, scope]))),
        (Some(loc), None) => Ok(Some(loc)),
        (None, Some(scope)) => Ok(Some(scope)),
        (None, None) => Ok(None),
    }
}
