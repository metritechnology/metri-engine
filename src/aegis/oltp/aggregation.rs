// [PORTED_FROM: src/metri/aegis/datalog/aggregation.clj]
// aegis/oltp/aggregation.rs — Agregación in-memory con FilterNode support.
//
// Implementa los 14 AggregationFunction del contrato FBS:
//   COUNT SUM AVG MIN MAX MEDIAN PERCENTILE_90/95/99
//   STD_DEV VARIANCE CORRELATION LINEAR_REGRESSION LOGISTIC_REGRESSION
//
// Filtered Aggregation (MetricDefinition.filter):
//   Si la métrica tiene un FilterNode, solo los rows que pasan el predicado
//   in-memory contribuyen al cómputo.
//
// API pública:
//   apply_metrics_fbs(rows, metrics) → Value (JSON object {:alias value})
//   eval_filter_node(row, node)      → bool

use serde_json::Value;
use tracing::warn;

use crate::janus::fbs::{
    AggregationFunction, FilterNodeT, FilterOperator,
};

// ── Evaluador de FilterNode in-memory ─────────────────────────────────────────
//
// [PORTED_FROM: (node->pred node) en aggregation.clj]
// Evalúa un FilterNodeT sobre un row JSON. Retorna true si el row pasa.

pub fn eval_filter_node(row: &Value, node: &FilterNodeT) -> bool {
    // Hoja: criteria
    if let Some(crit) = &node.criteria {
        let field = crit.field.as_deref().unwrap_or("");
        let bare_field = field.split('/').last().unwrap_or(field);
        // Intentar con key completo y sin namespace
        let row_val = row.get(field).or_else(|| row.get(bare_field));

        let op = crit.op_ref;
        let fv = crit.value.as_deref();

        match op {
            FilterOperator::EQ => {
                match (row_val, fv) {
                    (Some(Value::String(s)), Some(fv)) if fv.string_val.is_some() =>
                        s == fv.string_val.as_deref().unwrap_or(""),
                    (Some(Value::Number(n)), Some(fv)) =>
                        n.as_f64().unwrap_or(0.0) == fv.number_val,
                    (Some(Value::Bool(b)), Some(fv)) =>
                        *b == fv.bool_val,
                    _ => false,
                }
            }
            FilterOperator::NEQ => !eval_filter_node(row, &FilterNodeT {
                criteria: Some(Box::new(crate::janus::fbs::FilterCriteriaT {
                    field: crit.field.clone(),
                    value: crit.value.clone(),
                    op_ref: FilterOperator::EQ,
                })),
                group: None,
            }),
            FilterOperator::GT => {
                let rv = row_val.and_then(|v| v.as_f64()).unwrap_or(f64::NEG_INFINITY);
                let fval = fv.map(|v| v.number_val).unwrap_or(0.0);
                rv > fval
            }
            FilterOperator::GTE => {
                let rv = row_val.and_then(|v| v.as_f64()).unwrap_or(f64::NEG_INFINITY);
                let fval = fv.map(|v| v.number_val).unwrap_or(0.0);
                rv >= fval
            }
            FilterOperator::LT => {
                let rv = row_val.and_then(|v| v.as_f64()).unwrap_or(f64::INFINITY);
                let fval = fv.map(|v| v.number_val).unwrap_or(0.0);
                rv < fval
            }
            FilterOperator::LTE => {
                let rv = row_val.and_then(|v| v.as_f64()).unwrap_or(f64::INFINITY);
                let fval = fv.map(|v| v.number_val).unwrap_or(0.0);
                rv <= fval
            }
            FilterOperator::IS_NULL => row_val.map(|v| v.is_null()).unwrap_or(true),
            FilterOperator::IS_NOT_NULL => row_val.map(|v| !v.is_null()).unwrap_or(false),
            FilterOperator::CONTAINS => {
                let rv = row_val.and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
                let pattern = fv.and_then(|v| v.string_val.as_deref()).unwrap_or("").to_lowercase();
                rv.contains(&pattern)
            }
            FilterOperator::LIKE => {
                let rv = row_val.and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
                let pattern = fv.and_then(|v| v.string_val.as_deref()).unwrap_or("").to_lowercase();
                // Convertir % a wildcard básico
                let regex_pat = pattern.replace('%', ".*").replace('_', ".");
                regex::Regex::new(&format!("^{regex_pat}$"))
                    .map(|re| re.is_match(&rv))
                    .unwrap_or(false)
            }
            FilterOperator::IN => {
                // values field en el FilterCriteria para IN
                // Para simplificar, usamos string_val como lista separada por coma
                if let Some(fval) = fv {
                    if let Some(list_str) = &fval.string_val {
                        let items: Vec<&str> = list_str.split(',').collect();
                        let rv_str = row_val.and_then(|v| v.as_str()).unwrap_or("");
                        return items.iter().any(|item| item.trim() == rv_str);
                    }
                }
                false
            }
            FilterOperator::NOT_IN => {
                // Inverso de IN
                if let Some(fval) = fv {
                    if let Some(list_str) = &fval.string_val {
                        let items: Vec<&str> = list_str.split(',').collect();
                        let rv_str = row_val.and_then(|v| v.as_str()).unwrap_or("");
                        return !items.iter().any(|item| item.trim() == rv_str);
                    }
                }
                true
            }
            FilterOperator::MATCHES => {
                // Fuzzy matching con Levenshtein-Wagner-Fischer.
                // [PORTED_FROM: (fuzzy-match? value term) en fuzzy.clj]
                //
                // Pipeline (short-circuit):
                //   1. Fast path: substring case-insensitive
                //   2. Fuzzy path: word-level tokenización + distancia ≤ threshold adaptativo
                let rv = row_val.and_then(|v| v.as_str()).unwrap_or("");
                let term = fv.and_then(|v| v.string_val.as_deref()).unwrap_or("");
                crate::aegis::oltp::fuzzy::fuzzy_match(rv, term)
            }
            _ => {
                warn!("[Aegis Agg] Operador de filtro no soportado in-memory: {:?}", op);
                true // pass-through defensivo
            }
        }
    } else if let Some(group) = &node.group {
        // Grupo: AND / OR
        let conjunction = group.conjunction.0;
        let nodes = group.nodes.as_deref().unwrap_or(&[]);

        if nodes.is_empty() { return true; }

        match conjunction {
            1 => nodes.iter().all(|n| eval_filter_node(row, n)),  // AND
            2 => nodes.iter().any(|n| eval_filter_node(row, n)),  // OR
            _ => true,
        }
    } else {
        true // nodo vacío → pass-through
    }
}

// ── Funciones estadísticas auxiliares ─────────────────────────────────────────

fn safe_mean(nums: &[f64]) -> Option<f64> {
    if nums.is_empty() { return None; }
    Some(nums.iter().sum::<f64>() / nums.len() as f64)
}

fn variance_sample(nums: &[f64]) -> Option<f64> {
    if nums.len() < 2 { return None; }
    let m = safe_mean(nums)?;
    let v = nums.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (nums.len() - 1) as f64;
    Some(v)
}

fn percentile_nearest_rank(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() { return None; }
    let idx = ((p * sorted.len() as f64).floor() as usize).min(sorted.len() - 1);
    Some(sorted[idx])
}

fn pearson_r(xs: &[f64], ys: &[f64]) -> Option<f64> {
    if xs.len() < 2 || xs.len() != ys.len() { return None; }
    let mx = safe_mean(xs)?;
    let my = safe_mean(ys)?;
    let cov: f64 = xs.iter().zip(ys).map(|(x, y)| (x - mx) * (y - my)).sum::<f64>()
        / (xs.len() - 1) as f64;
    let vx = variance_sample(xs)?;
    let vy = variance_sample(ys)?;
    if vx <= 0.0 || vy <= 0.0 { return None; }
    Some(cov / (vx * vy).sqrt())
}

fn regr_slope(xs: &[f64], ys: &[f64]) -> Option<f64> {
    if xs.len() < 2 || xs.len() != ys.len() { return None; }
    let mx = safe_mean(xs)?;
    let my = safe_mean(ys)?;
    let ss_xy: f64 = xs.iter().zip(ys).map(|(x, y)| (x - mx) * (y - my)).sum();
    let ss_xx: f64 = xs.iter().map(|x| (x - mx).powi(2)).sum();
    if ss_xx == 0.0 { return None; }
    Some(ss_xy / ss_xx)
}

// ── Dispatcher de agregación ──────────────────────────────────────────────────
// [PORTED_FROM: (compute-agg fn-kw vals sec-vals)]

fn compute_agg(agg_fn: AggregationFunction, vals: &[f64], sec_vals: &[f64]) -> f64 {
    let mut sorted = vals.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    match agg_fn {
        AggregationFunction::COUNT    => vals.len() as f64,
        AggregationFunction::SUM      => vals.iter().sum(),
        AggregationFunction::AVG      => safe_mean(vals).unwrap_or(0.0),
        AggregationFunction::MIN      => sorted.first().copied().unwrap_or(0.0),
        AggregationFunction::MAX      => sorted.last().copied().unwrap_or(0.0),
        AggregationFunction::MEDIAN   => percentile_nearest_rank(&sorted, 0.5).unwrap_or(0.0),
        AggregationFunction::PERCENTILE_90 => percentile_nearest_rank(&sorted, 0.9).unwrap_or(0.0),
        AggregationFunction::PERCENTILE_95 => percentile_nearest_rank(&sorted, 0.95).unwrap_or(0.0),
        AggregationFunction::PERCENTILE_99 => percentile_nearest_rank(&sorted, 0.99).unwrap_or(0.0),
        AggregationFunction::STD_DEV  => variance_sample(vals).map(f64::sqrt).unwrap_or(0.0),
        AggregationFunction::VARIANCE => variance_sample(vals).unwrap_or(0.0),
        AggregationFunction::CORRELATION => pearson_r(vals, sec_vals).unwrap_or(0.0),
        AggregationFunction::LINEAR_REGRESSION => regr_slope(sec_vals, vals).unwrap_or(0.0),
        AggregationFunction::LOGISTIC_REGRESSION => {
            warn!("[Aegis Agg] LOGISTIC_REGRESSION no nativo — COUNT fallback");
            vals.len() as f64
        }
        _ => {
            warn!("[Aegis Agg] AggregationFunction desconocida {:?} — SUM fallback", agg_fn);
            vals.iter().sum()
        }
    }
}

// ── API pública ───────────────────────────────────────────────────────────────

/// Aplica un vector de MetricDefinitionT sobre rows JSON ya procesados.
///
/// [PORTED_FROM: (apply-metrics rows metrics) en aggregation.clj]
///
/// Filtered aggregation: Si `metric.filter` está presente, solo los rows que
/// pasan `eval_filter_node` contribuyen al cómputo.
///
/// Retorna un JSON Object: { "alias": computed_value, ... }
pub fn apply_metrics_fbs(rows: &[Value], metrics: &[crate::janus::fbs::MetricDefinitionT]) -> Value {
    let mut result = serde_json::Map::new();

    for metric in metrics {
        let agg_fn = metric.aggregation;
        let attr_key = metric.attribute.as_deref().unwrap_or("");
        let sec_key  = metric.secondary_attribute.as_deref().unwrap_or("");

        // Alias: usa :name si presente, si no genera "fn_attr"
        let agg_name = match agg_fn {
            AggregationFunction::COUNT    => "count",
            AggregationFunction::SUM      => "sum",
            AggregationFunction::AVG      => "avg",
            AggregationFunction::MIN      => "min",
            AggregationFunction::MAX      => "max",
            AggregationFunction::MEDIAN   => "median",
            AggregationFunction::STD_DEV  => "std_dev",
            AggregationFunction::VARIANCE => "variance",
            AggregationFunction::PERCENTILE_90 => "p90",
            AggregationFunction::PERCENTILE_95 => "p95",
            AggregationFunction::PERCENTILE_99 => "p99",
            AggregationFunction::CORRELATION => "correlation",
            AggregationFunction::LINEAR_REGRESSION => "linear_regression",
            AggregationFunction::LOGISTIC_REGRESSION => "logistic_regression",
            _ => "agg",
        };
        let alias = metric.name.as_deref()
            .map(String::from)
            .unwrap_or_else(|| {
                if attr_key.is_empty() {
                    agg_name.to_string()
                } else {
                    format!("{}_{}", agg_name, attr_key)
                }
            });

        // Filtered aggregation: aplicar predicado in-memory
        let eff_rows: Vec<&Value> = if let Some(filter_node) = &metric.filter {
            rows.iter().filter(|r| eval_filter_node(r, filter_node)).collect()
        } else {
            rows.iter().collect()
        };

        // Extraer valores numéricos del campo
        // Soporte a namespace: "asset/area_value" → prueba con y sin namespace
        let vals: Vec<f64> = if attr_key.is_empty() {
            // COUNT(*) — contar todos los rows efectivos
            vec![1.0; eff_rows.len()]
        } else {
            let bare_key = attr_key.split('/').last().unwrap_or(attr_key);
            eff_rows.iter().filter_map(|r| {
                // Intentar con key completo primero, luego sin namespace
                r.get(attr_key)
                    .or_else(|| r.get(bare_key))
                    .and_then(|v| v.as_f64())
            }).collect()
        };

        let sec_vals: Vec<f64> = if sec_key.is_empty() {
            vec![]
        } else {
            eff_rows.iter().filter_map(|r| {
                r.get(sec_key).and_then(|v| v.as_f64())
            }).collect()
        };

        // COUNT especial: contar rows efectivos directamente
        // Si attr_key es vacío O si COUNT no encontró valores numéricos → COUNT(*)
        let computed = if agg_fn == AggregationFunction::COUNT {
            if vals.is_empty() || attr_key.is_empty() {
                eff_rows.len() as f64
            } else {
                compute_agg(agg_fn, &vals, &sec_vals)
            }
        } else {
            compute_agg(agg_fn, &vals, &sec_vals)
        };

        result.insert(alias, serde_json::json!(computed));
    }

    Value::Object(result)
}

/// Deriva columnas desde la unión de keys de todos los rows.
/// Anota cada columna con is_dimension / is_measure para VizMeta.
/// [PORTED_FROM: (derive-columns rows ast-ir) en datalog/executor.clj]
pub fn derive_columns(
    rows: &[Value],
    dim_attrs: &[String],
    metric_aliases: &[String],
) -> Vec<Value> {
    if rows.is_empty() { return vec![]; }

    // Unión de todas las keys
    let mut all_keys: Vec<String> = rows.iter()
        .filter_map(|r| r.as_object())
        .flat_map(|obj| obj.keys().cloned())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    // Ordenar: dims primero, luego metrics, luego el resto
    all_keys.sort_by_key(|k| {
        if dim_attrs.contains(k)      { 0u8 }
        else if metric_aliases.contains(k) { 2u8 }
        else                          { 1u8 }
    });

    all_keys.iter().map(|k| {
        let sample_val = rows.iter()
            .find_map(|r| r.get(k).filter(|v| !v.is_null()));
        let col_type = infer_col_type(k, sample_val);
        let is_dim    = dim_attrs.contains(k);
        let is_measure = metric_aliases.contains(k);

        serde_json::json!({
            "key":          k,
            "label":        k,
            "type":         col_type,
            "sortable":     true,
            "is_dimension": is_dim,
            "is_measure":   is_measure,
        })
    }).collect()
}

fn infer_col_type(key: &str, sample: Option<&Value>) -> &'static str {
    // ANO-001: forzar "timestamp" para campos temporales conocidos
    if matches!(key, "created_at" | "updated_at" | "deleted_at" | "ingested_at" | "timestamp") {
        return "timestamp";
    }
    match sample {
        Some(Value::Number(n)) => {
            if n.as_f64().map(|v| v > 1e11).unwrap_or(false) { "timestamp" } else { "number" }
        }
        Some(Value::Bool(_))   => "boolean",
        _                      => "string",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn count_star_with_no_attribute() {
        use crate::janus::fbs::{MetricDefinitionT, AggregationFunction};
        let rows = vec![json!({"status": "ACTIVE"}), json!({"status": "ACTIVE"}), json!({"status": "INACTIVE"})];
        let metrics = vec![MetricDefinitionT {
            aggregation: AggregationFunction::COUNT,
            name: Some("total".to_string()),
            ..Default::default()
        }];
        let result = apply_metrics_fbs(&rows, &metrics);
        assert_eq!(result["total"], json!(3.0));
    }

    #[test]
    fn sum_with_attribute() {
        use crate::janus::fbs::{MetricDefinitionT, AggregationFunction};
        let rows = vec![json!({"area": 100.0}), json!({"area": 200.0}), json!({"area": 300.0})];
        let metrics = vec![MetricDefinitionT {
            aggregation: AggregationFunction::SUM,
            attribute: Some("area".to_string()),
            name: Some("total_area".to_string()),
            ..Default::default()
        }];
        let result = apply_metrics_fbs(&rows, &metrics);
        assert_eq!(result["total_area"], json!(600.0));
    }

    #[test]
    fn filtered_count() {
        use crate::janus::fbs::{MetricDefinitionT, AggregationFunction, FilterNodeT, FilterCriteriaT, FilterOperator, FilterValueT};
        let rows = vec![
            json!({"status": "ACTIVE",   "area": 100.0}),
            json!({"status": "ACTIVE",   "area": 200.0}),
            json!({"status": "INACTIVE", "area": 300.0}),
        ];
        let filter = FilterNodeT {
            criteria: Some(Box::new(FilterCriteriaT {
                field: Some("status".to_string()),
                op_ref: FilterOperator::EQ,
                value: Some(Box::new(FilterValueT {
                    string_val: Some("ACTIVE".to_string()),
                    number_val: 0.0,
                    bool_val: false,
                    list_val: None,
                    range_values: None,
                    timestamp_val: 0,
                })),
            })),
            group: None,
        };
        let metrics = vec![MetricDefinitionT {
            aggregation: AggregationFunction::COUNT,
            name: Some("active_count".to_string()),
            filter: Some(Box::new(filter)),
            ..Default::default()
        }];
        let result = apply_metrics_fbs(&rows, &metrics);
        assert_eq!(result["active_count"], json!(2.0));
    }
}
