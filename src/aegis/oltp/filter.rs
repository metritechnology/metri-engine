//! In-memory FilterNode evaluator for the OLTP engine.
//!
//! aegis/oltp/filter.rs
//! Evaluador de FilterNode in-memory para el motor OLTP (EAV).
//! SRP: Evaluar si un row JSON cumple con un conjunto de predicados definidos en el AST/FilterNode.

use serde_json::Value;
use tracing::warn;

use crate::aegis::oltp::fuzzy::{fuzzy_match, remove_accents};
use crate::janus::fbs::{FilterNodeT, FilterOperator};

/// Evalúa un FilterNodeT sobre un row JSON. Retorna true si el row pasa.
///
pub fn eval_filter_node(row: &Value, node: &FilterNodeT) -> bool {
    // Hoja: criteria
    if let Some(crit) = &node.criteria {
        let field = crit.field.as_deref().unwrap_or("");
        let bare_field = field.split('/').next_back().unwrap_or(field);
        // Intentar con key completo y sin namespace
        let mut row_val = row.get(field).or_else(|| row.get(bare_field));
        if row_val.is_none() {
            if let Some(obj) = row.as_object() {
                for (k, v) in obj {
                    if k.split('/').next_back() == Some(bare_field) {
                        row_val = Some(v);
                        break;
                    }
                }
            }
        }

        let op = crit.op_ref;
        let fv = crit.value.as_deref();

        match op {
            FilterOperator::EQ => match (row_val, fv) {
                (Some(Value::String(s)), Some(fv)) if fv.string_val.is_some() => {
                    s == fv.string_val.as_deref().unwrap_or("")
                }
                (Some(Value::Number(n)), Some(fv)) => {
                    let rv = n.as_f64().unwrap_or(0.0);
                    let fval = if fv.timestamp_val != 0 {
                        fv.timestamp_val as f64
                    } else {
                        fv.number_val
                    };
                    rv == fval
                }
                (Some(Value::Bool(b)), Some(fv)) => *b == fv.bool_val,
                _ => false,
            },
            FilterOperator::NEQ => !eval_filter_node(
                row,
                &FilterNodeT {
                    criteria: Some(Box::new(crate::janus::fbs::FilterCriteriaT {
                        field: crit.field.clone(),
                        value: crit.value.clone(),
                        op_ref: FilterOperator::EQ,
                    })),
                    group: None,
                },
            ),
            FilterOperator::GT => {
                let rv = row_val
                    .and_then(|v| v.as_f64())
                    .unwrap_or(f64::NEG_INFINITY);
                let fval = fv
                    .map(|v| {
                        if v.timestamp_val != 0 {
                            v.timestamp_val as f64
                        } else {
                            v.number_val
                        }
                    })
                    .unwrap_or(0.0);
                rv > fval
            }
            FilterOperator::GTE => {
                let rv = row_val
                    .and_then(|v| v.as_f64())
                    .unwrap_or(f64::NEG_INFINITY);
                let fval = fv
                    .map(|v| {
                        if v.timestamp_val != 0 {
                            v.timestamp_val as f64
                        } else {
                            v.number_val
                        }
                    })
                    .unwrap_or(0.0);
                rv >= fval
            }
            FilterOperator::LT => {
                let rv = row_val.and_then(|v| v.as_f64()).unwrap_or(f64::INFINITY);
                let fval = fv
                    .map(|v| {
                        if v.timestamp_val != 0 {
                            v.timestamp_val as f64
                        } else {
                            v.number_val
                        }
                    })
                    .unwrap_or(0.0);
                rv < fval
            }
            FilterOperator::LTE => {
                let rv = row_val.and_then(|v| v.as_f64()).unwrap_or(f64::INFINITY);
                let fval = fv
                    .map(|v| {
                        if v.timestamp_val != 0 {
                            v.timestamp_val as f64
                        } else {
                            v.number_val
                        }
                    })
                    .unwrap_or(0.0);
                rv <= fval
            }
            FilterOperator::IS_NULL => row_val.map(|v| v.is_null()).unwrap_or(true),
            FilterOperator::IS_NOT_NULL => row_val.map(|v| !v.is_null()).unwrap_or(false),
            // Contains semántico por tipo: substring sobre strings (como
            // siempre) y pertenencia sobre arrays. El hidratador entrega los
            // atributos lista (role_ids, …) como Value::Array — el as_str()
            // anterior los colapsaba a "" y el filtro devolvía false siempre,
            // con lo que pestañas como «Usuarios con este rol» quedaban vacías.
            FilterOperator::CONTAINS => {
                let pattern = remove_accents(
                    &fv.and_then(|v| v.string_val.as_deref())
                        .unwrap_or("")
                        .to_lowercase(),
                );
                if pattern.is_empty() {
                    return false;
                }
                let str_contains = |s: &str| remove_accents(&s.to_lowercase()).contains(&pattern);
                match row_val {
                    Some(Value::String(s)) => str_contains(s),
                    Some(Value::Array(items)) => items.iter().any(|item| match item {
                        Value::String(s) => str_contains(s),
                        other => str_contains(&other.to_string()),
                    }),
                    _ => false,
                }
            }
            FilterOperator::LIKE => {
                let rv = remove_accents(
                    &row_val
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_lowercase(),
                );
                let pattern = remove_accents(
                    &fv.and_then(|v| v.string_val.as_deref())
                        .unwrap_or("")
                        .to_lowercase(),
                );
                // Convertir % a wildcard básico
                let regex_pat = pattern.replace('%', ".*").replace('_', ".");
                regex::Regex::new(&format!("^{regex_pat}$"))
                    .map(|re| re.is_match(&rv))
                    .unwrap_or(false)
            }
            FilterOperator::IN => {
                if let Some(fval) = fv {
                    if let Some(lst) = &fval.list_val {
                        if let Some(values) = &lst.values {
                            let rv_str = row_val.and_then(|v| v.as_str()).unwrap_or("");
                            return values.iter().any(|item| item == rv_str);
                        }
                    }
                    if let Some(list_str) = &fval.string_val {
                        let items: Vec<&str> = list_str.split(',').collect();
                        let rv_str = row_val.and_then(|v| v.as_str()).unwrap_or("");
                        return items.iter().any(|item| item.trim() == rv_str);
                    }
                }
                false
            }
            FilterOperator::NOT_IN => {
                if let Some(fval) = fv {
                    if let Some(lst) = &fval.list_val {
                        if let Some(values) = &lst.values {
                            let rv_str = row_val.and_then(|v| v.as_str()).unwrap_or("");
                            return !values.iter().any(|item| item == rv_str);
                        }
                    }
                    if let Some(list_str) = &fval.string_val {
                        let items: Vec<&str> = list_str.split(',').collect();
                        let rv_str = row_val.and_then(|v| v.as_str()).unwrap_or("");
                        return !items.iter().any(|item| item.trim() == rv_str);
                    }
                }
                true
            }
            FilterOperator::BETWEEN => {
                if let Some(fval) = fv {
                    if let Some(rvs) = &fval.range_values {
                        if let Some(values) = &rvs.values {
                            if values.len() >= 2 {
                                let lo_val = &values[0];
                                let hi_val = &values[1];

                                let rv = row_val.and_then(|v| v.as_f64()).unwrap_or(f64::NAN);
                                if !rv.is_nan() {
                                    let lo = if lo_val.timestamp_val != 0 {
                                        lo_val.timestamp_val as f64
                                    } else {
                                        lo_val.number_val
                                    };
                                    let hi = if hi_val.timestamp_val != 0 {
                                        hi_val.timestamp_val as f64
                                    } else {
                                        hi_val.number_val
                                    };
                                    return rv >= lo && rv <= hi;
                                }

                                let rv_str = row_val.and_then(|v| v.as_str()).unwrap_or("");
                                if !rv_str.is_empty() {
                                    let lo_str = lo_val.string_val.as_deref().unwrap_or("");
                                    let hi_str = hi_val.string_val.as_deref().unwrap_or("");
                                    return rv_str >= lo_str && rv_str <= hi_str;
                                }
                            }
                        }
                    }
                }
                false
            }
            FilterOperator::MATCHES => {
                let rv = row_val.and_then(|v| v.as_str()).unwrap_or("");
                let term = fv.and_then(|v| v.string_val.as_deref()).unwrap_or("");
                fuzzy_match(rv, term)
            }
            _ => {
                warn!(
                    "[Aegis Agg] Operador de filtro no soportado in-memory: {:?}",
                    op
                );
                true // pass-through defensivo
            }
        }
    } else if let Some(group) = &node.group {
        // Grupo: AND / OR
        let conjunction = group.conjunction.0;
        let nodes = group.nodes.as_deref().unwrap_or(&[]);

        if nodes.is_empty() {
            return true;
        }

        match conjunction {
            1 => nodes.iter().all(|n| eval_filter_node(row, n)), // AND
            2 => nodes.iter().any(|n| eval_filter_node(row, n)), // OR
            _ => true,
        }
    } else {
        true // nodo vacío → pass-through
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn contains_node(field: &str, pattern: &str) -> FilterNodeT {
        FilterNodeT {
            criteria: Some(Box::new(crate::janus::fbs::FilterCriteriaT {
                field: Some(field.to_string()),
                value: Some(Box::new(crate::janus::fbs::FilterValueT {
                    string_val: Some(pattern.to_string()),
                    ..Default::default()
                })),
                op_ref: FilterOperator::CONTAINS,
            })),
            group: None,
        }
    }

    #[test]
    fn contains_substring_en_string() {
        let row = json!({ "description": "Rol de Administrador" });
        assert!(eval_filter_node(
            &row,
            &contains_node("description", "administrador")
        ));
        assert!(!eval_filter_node(
            &row,
            &contains_node("description", "operador")
        ));
    }

    #[test]
    fn contains_sobre_array_por_elemento() {
        // El hidratador entrega role_ids como Value::Array (regresión de la
        // tab «Usuarios con este rol»: as_str() lo colapsaba a "").
        let row =
            json!({ "role_ids": ["01M12AHKP6RP7SCWBX29J9N4AM", "01KYR7D1X0DN0FAY8MBJFC6KJP"] });
        assert!(eval_filter_node(
            &row,
            &contains_node("role_ids", "01M12AHKP6RP7SCWBX29J9N4AM")
        ));
        assert!(eval_filter_node(
            &row,
            &contains_node("role_ids", "01KYR7D1X0DN0FAY8MBJFC6KJP")
        ));
        assert!(!eval_filter_node(
            &row,
            &contains_node("role_ids", "01M9NOEXISTE")
        ));
    }

    #[test]
    fn contains_array_vacio_y_pattern_vacio() {
        let row = json!({ "role_ids": [] });
        assert!(!eval_filter_node(
            &row,
            &contains_node("role_ids", "01M12AHKP6RP7SCWBX29J9N4AM")
        ));
        let row2 = json!({ "role_ids": ["x"] });
        assert!(!eval_filter_node(&row2, &contains_node("role_ids", "")));
    }

    #[test]
    fn contains_ignora_mayusculas_y_acentos() {
        let row = json!({ "name": "Técnico de Mantenimiento" });
        assert!(eval_filter_node(&row, &contains_node("name", "tecnico")));
    }
}
