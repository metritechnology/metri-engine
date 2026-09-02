// [PORTED_FROM: src/metri/janus/filter_compiler.clj]
// janus/filter_compiler.rs — Compilador de FilterNode -> nodos AST IR.
// SRP: transforma árboles de filtros del cliente en nodos del AST IR inmutable.

use serde_json::{json, Value};

use crate::domain::errors::{DomainError, ErrorCode};

/// Atributos de sistema en OLTP
fn is_system_ts_field(field: &str) -> bool {
    field == "created_at" || field == "updated_at" || field == "createdAt" || field == "updatedAt"
}

/// Mapea campos de sistema al namespace `:meta/`
fn oltp_system_field_map(field: &str) -> Option<&'static str> {
    match field {
        "created_at" | "createdAt" => Some("meta/created_at"),
        "updated_at" | "updatedAt" => Some("meta/updated_at"),
        _ => None,
    }
}

/// Coerce a f64 para valores numéricos en proto que vienen como f64 y se necesitan como f64 o long
fn coerce_epoch(val: &Value) -> Option<f64> {
    val.as_f64()
}

/// Extrae valor simple de proto
fn extract_val(val_map: &Value) -> Option<Value> {
    val_map.get("string_val").cloned()
        .or_else(|| val_map.get("number_val").cloned())
        .or_else(|| val_map.get("bool_val").cloned())
        .or_else(|| val_map.get("timestamp_val").cloned())
        .or_else(|| val_map.get("list_val").cloned())
}

/// Compila un Criteria Hoja
fn build_leaf_node(entity: &str, criteria: &Value) -> Result<Value, DomainError> {
    let field_name = criteria.get("field").and_then(|v| v.as_str()).unwrap_or("");
    let op = criteria.get("op_ref").and_then(|v| v.as_str()).unwrap_or("EQ");
    
    let field = oltp_system_field_map(field_name)
        .map(|s| s.to_string())
        .unwrap_or_else(|| format!("{entity}/{field_name}"));

    let val_map = criteria.get("value").cloned().unwrap_or(json!({}));
    let list_vals = val_map.get("list_val").and_then(|v| v.get("values")).cloned().unwrap_or(json!([]));
    let range_vals = val_map.get("range_values").and_then(|v| v.get("values")).cloned().unwrap_or(json!([]));

    let val = extract_val(&val_map).unwrap_or(Value::Null);

    let node = match op {
        "EQ" => json!(["=", field, val]),
        "NEQ" => json!(["not=", field, val]),
        "GT" => json!([">", field, val]),
        "GTE" => json!([">=", field, val]),
        "LT" => json!(["<", field, val]),
        "LTE" => json!(["<=", field, val]),
        "IN" => json!(["in", field, list_vals]),
        "NOT_IN" => json!(["not-in", field, list_vals]),
        "LIKE" => json!(["like", field, val]),
        "CONTAINS" => json!(["contains", field, val]),
        "IS_NULL" => json!(["is-null", field]),
        "IS_NOT_NULL" => json!(["is-not-null", field]),
        "BETWEEN" => {
            if let Some(arr) = range_vals.as_array() {
                if arr.len() >= 2 {
                    let lo = extract_val(&arr[0]).unwrap_or(Value::Null);
                    let hi = extract_val(&arr[1]).unwrap_or(Value::Null);
                    json!(["between", field, [lo, hi]])
                } else {
                    json!(["=", "1", "1"])
                }
            } else {
                json!(["=", "1", "1"])
            }
        }
        "MATCHES" => json!(["matches", field, val_map.get("string_val").unwrap_or(&Value::Null)]),
        _ => return Err(DomainError::janus(ErrorCode::Janus400, format!("Unknown operator: {op}"))),
    };

    Ok(node)
}

/// Validar enums UNSPECIFIED
fn check_not_unspecified(val: &str, label: &str) -> Result<(), DomainError> {
    if val.ends_with("_UNSPECIFIED") {
        return Err(DomainError::janus(ErrorCode::Janus400, format!("Unspecified enum in {label}")));
    }
    Ok(())
}

fn compile_criteria(criteria: &Value, entity: &str) -> Result<Value, DomainError> {
    let op = criteria.get("op_ref").and_then(|v| v.as_str()).unwrap_or("EQ");
    check_not_unspecified(op, "filter operator")?;

    // FASE 3: En el puerto a Rust simplificamos dot-path por ahora asumiendo que el UI
    // manda ref-filters explícitos o los omitimos temporalmente hasta el integrador completo.
    build_leaf_node(entity, criteria)
}

fn compile_group(group: &Value, entity: &str) -> Result<Value, DomainError> {
    let conj = group.get("conjunction").and_then(|v| v.as_str()).unwrap_or("AND");
    check_not_unspecified(conj, "conjunction")?;

    let op = match conj {
        "OR" => "or",
        "NOT" => "not",
        _ => "and",
    };

    let mut nodes = Vec::new();
    if let Some(children) = group.get("nodes").and_then(|v| v.as_array()) {
        for child in children {
            if let Ok(c_node) = compile_node(child, entity) {
                nodes.push(c_node);
            }
        }
    }

    if nodes.is_empty() {
        return Ok(json!(["=", "1", "1"])); // No-op node
    }

    if op == "not" {
        Ok(json!(["not", nodes.remove(0)]))
    } else {
        let mut group_node = vec![json!(op)];
        group_node.extend(nodes);
        Ok(Value::Array(group_node))
    }
}

pub fn compile_node(node: &Value, entity: &str) -> Result<Value, DomainError> {
    if let Some(criteria) = node.get("criteria") {
        compile_criteria(criteria, entity)
    } else if let Some(group) = node.get("group") {
        compile_group(group, entity)
    } else {
        Ok(json!(["=", "1", "1"])) // Fallback for empty nodes
    }
}

pub fn compile_filters(filters: &[Value], entity: &str) -> Result<Vec<Value>, DomainError> {
    let mut ast_nodes = Vec::new();
    for filter in filters {
        let n = compile_node(filter, entity)?;
        ast_nodes.push(n);
    }
    Ok(ast_nodes)
}
