// aegis/oltp/caster.rs
// SRP: Encapsular el formateo, agrupamiento y agregación final en memoria de los rows (KPI, TIMESERIES, PIE, BUBBLE).

use crate::aegis::formula::{
    lexer, parser, FormulaEvaluator, FunctionRegistry, OltpVariableResolver,
};
use crate::aegis::oltp::aggregation::apply_metrics_fbs;
use crate::aegis::oltp::comparison::run_comparisons;
use crate::janus::fbs::{AnalyticsRequestT, DimensionDefinitionT, MetricDefinitionT};
use crate::temporal::core::TimeRange;
use serde_json::{json, Value};

/// Aplica las agregaciones de OutputCast (KPI, TIMESERIES, PIE, BUBBLE) sobre los rows finales hidratados.
pub fn apply_output_cast_fbs(
    rows: &[Value],
    all_rows_for_comparison: &[Value],
    ast_ir: &AnalyticsRequestT,
    time_range: &TimeRange,
) -> Vec<Value> {
    let mut rows_cloned = rows.to_vec();
    if let Some(measures) = &ast_ir.measures {
        if !measures.is_empty() {
            let registry = FunctionRegistry::standard();
            let compiled_measures: Vec<(String, FormulaEvaluator)> = measures
                .iter()
                .filter_map(|measure| {
                    let formula_str = measure.formula.as_deref()?;
                    let name = measure.name.as_deref().unwrap_or("measure");
                    match lexer::tokenize(formula_str) {
                        Ok(tokens) => match parser::to_rpn(tokens, &registry) {
                            Ok(rpn) => Some((name.to_string(), FormulaEvaluator::new(rpn))),
                            Err(e) => {
                                tracing::warn!(
                                    "Formula parser failed for '{}' ({}): {:?}",
                                    name,
                                    formula_str,
                                    e
                                );
                                None
                            }
                        },
                        Err(e) => {
                            tracing::warn!(
                                "Formula lexer failed for '{}' ({}): {:?}",
                                name,
                                formula_str,
                                e
                            );
                            None
                        }
                    }
                })
                .collect();

            for row in &mut rows_cloned {
                if let Some(obj) = row.as_object_mut() {
                    for (name, evaluator) in &compiled_measures {
                        let resolver = OltpVariableResolver::new(obj);
                        match evaluator.evaluate(&resolver, &registry) {
                            Ok(result) => {
                                if !result.is_nan() {
                                    obj.insert(name.clone(), serde_json::json!(result));
                                } else {
                                    tracing::info!(
                                        "Formula evaluated to NaN for measure '{}'",
                                        name
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Formula evaluator failed for measure '{}': {:?}",
                                    name,
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    let rows = &rows_cloned;

    let output_cast = ast_ir.output_cast.0;

    if let Some(metrics) = &ast_ir.metrics {
        if !metrics.is_empty() {
            match output_cast {
                1 /* KPI */ => {
                    // KPI: agregar todos los rows hidratados → 1 fila de métricas
                    let agg_result = apply_metrics_fbs(rows, metrics);

                    // ── Integrar AnalyticalComparison → previous_value
                    let comparisons = ast_ir.comparisons.as_deref().unwrap_or(&[]);
                    if !comparisons.is_empty() {
                        let current_map = match &agg_result {
                            Value::Object(m) => m.clone(),
                            _ => serde_json::Map::new(),
                        };
                        let tz = ast_ir.time_frame.as_ref()
                            .and_then(|tf| tf.timezone.as_deref())
                            .unwrap_or("UTC");
                        let cmp_result = run_comparisons(
                            all_rows_for_comparison,
                            metrics,
                            comparisons,
                            Some(time_range),
                            current_map,
                            tz,
                        );
                        // Fusionar resultado de comparaciones en el row KPI
                        let mut merged = match agg_result {
                            Value::Object(m) => m,
                            _ => serde_json::Map::new(),
                        };
                        // previous_value: buscar prev_0_<alias> → fallback benchmark_value
                        let first_alias = metrics.first().and_then(|m| {
                            m.name.as_deref().map(|n| n.to_string()).or_else(|| {
                                let agg = match m.aggregation.0 {
                                    1 => "count", 2 => "sum", 3 => "avg",
                                    4 => "min",   5 => "max", _ => "agg",
                                };
                                m.attribute.as_deref().map(|a| {
                                    format!("{}_{}", agg, a.split('/').next_back().unwrap_or(a))
                                })
                            })
                        });
                        if let Some(alias) = &first_alias {
                            let prev_key = format!("prev_0_{alias}");
                            let prev_val = cmp_result.get(&prev_key)
                                .or_else(|| cmp_result.get("benchmark_value"))
                                .or_else(|| cmp_result.iter()
                                    .find(|(k, _)| k.starts_with("benchmark_"))
                                    .map(|(_, v)| v));
                            if let Some(pv) = prev_val {
                                merged.insert("previous_value".to_string(), pv.clone());
                            }
                        }
                        // Merge all comparison keys into the row
                        for (k, v) in cmp_result {
                            merged.insert(k, v);
                        }
                        vec![Value::Object(merged)]
                    } else {
                        vec![agg_result]
                    }
                }
                4 /* PIE */ | 5 /* BUBBLE */ => {
                    // PIE: group-by dimensiones + métricas por grupo
                    if let Some(dims) = &ast_ir.dimensions {
                        let dim_keys: Vec<String> = dims.iter()
                            .filter_map(|d| d.attribute.clone())
                            .collect();
                        if !dim_keys.is_empty() {
                            if let Some(first_row) = rows.first() {
                                if let Some(obj) = first_row.as_object() {
                                    tracing::debug!("dim_keys={:?} row_keys={:?}",
                                        dim_keys,
                                        obj.keys().take(10).collect::<Vec<_>>());
                                    for dk in &dim_keys {
                                        tracing::debug!("row.get('{}') = {:?}",
                                            dk, obj.get(dk));
                                    }
                                }
                            }
                            let mut groups: std::collections::HashMap<String, Vec<Value>> =
                                std::collections::HashMap::new();
                            for row in rows {
                                let key = dim_keys.iter()
                                    .filter_map(|k| {
                                        row.get(k).and_then(|v| {
                                            v.as_str().map(|s| s.to_string())
                                                .or_else(|| v.as_f64().map(|n| n.to_string()))
                                                .or_else(|| v.as_i64().map(|n| n.to_string()))
                                        })
                                    })
                                    .collect::<Vec<_>>().join("|");
                                if !key.is_empty() {
                                    groups.entry(key).or_default().push(row.clone());
                                }
                            }
                            let mut result: Vec<Value> = groups.into_iter().map(|(_, group_rows)| {
                                let mut agg = apply_metrics_fbs(&group_rows, metrics);
                                if let (Some(first), Some(obj)) = (group_rows.first(), agg.as_object_mut()) {
                                    for dk in &dim_keys {
                                        if let Some(v) = first.get(dk) {
                                            obj.insert(dk.clone(), v.clone());
                                        }
                                    }
                                }
                                agg
                            }).collect();
                            result.sort_by(|a, b| {
                                let ka = a.as_object().and_then(|m| m.values().next()).and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let kb = b.as_object().and_then(|m| m.values().next()).and_then(|v| v.as_f64()).unwrap_or(0.0);
                                kb.partial_cmp(&ka).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            result
                        } else {
                            vec![apply_metrics_fbs(rows, metrics)]
                        }
                    } else {
                        vec![apply_metrics_fbs(rows, metrics)]
                    }
                }
                2 /* TIMESERIES */ => {
                    // TIMESERIES: group-by (interval-bucket, dims) → apply-metrics
                    if let Some(dims) = &ast_ir.dimensions {
                        apply_timeseries_bucketing(rows, metrics, dims)
                    } else {
                        rows.to_vec()
                    }
                }
                _ /* TABLE / CSV_EXPORT */ => {
                    if let (Some(dims), Some(metrics)) = (&ast_ir.dimensions, &ast_ir.metrics) {
                        if !dims.is_empty() && !metrics.is_empty() {
                            let dim_keys: Vec<String> = dims.iter()
                                .filter_map(|d| d.attribute.clone())
                                .collect();
                            if !dim_keys.is_empty() {
                                let mut groups: std::collections::HashMap<String, Vec<Value>> =
                                    std::collections::HashMap::new();
                                for row in rows {
                                    let key = dim_keys.iter()
                                        .filter_map(|k| {
                                            let bare_k = k.split('/').next_back().unwrap_or(k);
                                            row.get(k).or_else(|| row.get(bare_k)).and_then(|v| {
                                                v.as_str().map(|s| s.to_string())
                                                    .or_else(|| v.as_f64().map(|n| n.to_string()))
                                                    .or_else(|| v.as_i64().map(|n| n.to_string()))
                                            })
                                        })
                                        .collect::<Vec<_>>().join("|");
                                    groups.entry(key).or_default().push(row.clone());
                                }
                                let result: Vec<Value> = groups.into_iter().map(|(_, group_rows)| {
                                    let mut agg = apply_metrics_fbs(&group_rows, metrics);
                                    if let (Some(first), Some(obj)) = (group_rows.first(), agg.as_object_mut()) {
                                        for dk in &dim_keys {
                                            let bare_dk = dk.split('/').next_back().unwrap_or(dk);
                                            if let Some(v) = first.get(dk).or_else(|| first.get(bare_dk)) {
                                                obj.insert(dk.clone(), v.clone());
                                            }
                                        }
                                    }
                                    agg
                                }).collect();
                                result
                            } else {
                                rows.to_vec()
                            }
                        } else {
                            rows.to_vec()
                        }
                    } else {
                        rows.to_vec()
                    }
                }
            }
        } else {
            rows.to_vec()
        }
    } else {
        rows.to_vec()
    }
}

/// Trunca epoch-segundos al inicio del intervalo dado (UTC).
/// [PORTED_FROM: truncate-to-interval en executor.clj]
pub fn truncate_to_interval(epoch_secs: i64, interval: &str) -> i64 {
    let unit = interval.parse::<crate::temporal::core::CalUnit>().unwrap();
    crate::temporal::core::truncate_to_unit(epoch_secs, unit, "UTC")
}

/// Bucketing TIMESERIES completo.
///
/// [PORTED_FROM: apply-output-cast :TIMESERIES en executor.clj]
pub fn apply_timeseries_bucketing(
    rows: &[Value],
    metrics: &[MetricDefinitionT],
    dimensions: &[DimensionDefinitionT],
) -> Vec<Value> {
    use std::collections::BTreeMap;

    let time_dim = dimensions.iter().find(|d| {
        d.interval
            .as_deref()
            .map(|i| !i.is_empty())
            .unwrap_or(false)
    });
    let interval = time_dim
        .and_then(|d| d.interval.as_deref())
        .unwrap_or("day");
    let bucket_attr = time_dim
        .and_then(|d| d.attribute.as_deref())
        .unwrap_or("bucket");

    let other_dims: Vec<&str> = dimensions
        .iter()
        .filter(|d| d.interval.as_deref().map(|i| i.is_empty()).unwrap_or(true))
        .filter_map(|d| d.attribute.as_deref())
        .collect();

    let ts_candidates = &[
        "created_at",
        "meta/created_at",
        "timestamp",
        "updated_at",
        "ingested_at",
    ];
    let mut groups: BTreeMap<(i64, Vec<String>), Vec<Value>> = BTreeMap::new();

    for row in rows {
        let ts_raw = ts_candidates
            .iter()
            .find_map(|f| row.get(*f).and_then(|v| v.as_f64()))
            .unwrap_or(0.0);
        let ts_secs = if ts_raw > 1e11 {
            (ts_raw / 1000.0) as i64
        } else {
            ts_raw as i64
        };
        let bucket = truncate_to_interval(ts_secs, interval);

        let dim_vals: Vec<String> = other_dims
            .iter()
            .map(|k| {
                row.get(*k)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            })
            .collect();

        groups
            .entry((bucket, dim_vals))
            .or_default()
            .push(row.clone());
    }

    groups
        .into_iter()
        .map(|((bucket_val, dim_vals), group_rows)| {
            let mut agg = apply_metrics_fbs(&group_rows, metrics);
            if let Some(obj) = agg.as_object_mut() {
                obj.insert(bucket_attr.to_string(), json!(bucket_val));
                for (k, v) in other_dims.iter().zip(&dim_vals) {
                    obj.insert((*k).to_string(), json!(v));
                }
            }
            agg
        })
        .collect()
}
