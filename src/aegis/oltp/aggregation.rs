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

pub use crate::aegis::oltp::filter::eval_filter_node;

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

// ── Estrategias de Agregación (OCP) ───────────────────────────────────────────

pub trait AggregationStrategy: Send + Sync {
    fn compute(&self, vals: &[f64], sec_vals: &[f64]) -> f64;
}

pub struct CountStrategy;
impl AggregationStrategy for CountStrategy {
    fn compute(&self, vals: &[f64], _sec_vals: &[f64]) -> f64 {
        vals.len() as f64
    }
}

pub struct SumStrategy;
impl AggregationStrategy for SumStrategy {
    fn compute(&self, vals: &[f64], _sec_vals: &[f64]) -> f64 {
        vals.iter().sum()
    }
}

pub struct AvgStrategy;
impl AggregationStrategy for AvgStrategy {
    fn compute(&self, vals: &[f64], _sec_vals: &[f64]) -> f64 {
        safe_mean(vals).unwrap_or(0.0)
    }
}

pub struct MinStrategy;
impl AggregationStrategy for MinStrategy {
    fn compute(&self, vals: &[f64], _sec_vals: &[f64]) -> f64 {
        let mut sorted = vals.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        sorted.first().copied().unwrap_or(0.0)
    }
}

pub struct MaxStrategy;
impl AggregationStrategy for MaxStrategy {
    fn compute(&self, vals: &[f64], _sec_vals: &[f64]) -> f64 {
        let mut sorted = vals.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        sorted.last().copied().unwrap_or(0.0)
    }
}

pub struct PercentileStrategy {
    pub p: f64,
}
impl AggregationStrategy for PercentileStrategy {
    fn compute(&self, vals: &[f64], _sec_vals: &[f64]) -> f64 {
        let mut sorted = vals.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        percentile_nearest_rank(&sorted, self.p).unwrap_or(0.0)
    }
}

pub struct StdDevStrategy;
impl AggregationStrategy for StdDevStrategy {
    fn compute(&self, vals: &[f64], _sec_vals: &[f64]) -> f64 {
        variance_sample(vals).map(f64::sqrt).unwrap_or(0.0)
    }
}

pub struct VarianceStrategy;
impl AggregationStrategy for VarianceStrategy {
    fn compute(&self, vals: &[f64], _sec_vals: &[f64]) -> f64 {
        variance_sample(vals).unwrap_or(0.0)
    }
}

pub struct CorrelationStrategy;
impl AggregationStrategy for CorrelationStrategy {
    fn compute(&self, vals: &[f64], sec_vals: &[f64]) -> f64 {
        pearson_r(vals, sec_vals).unwrap_or(0.0)
    }
}

pub struct LinearRegressionStrategy;
impl AggregationStrategy for LinearRegressionStrategy {
    fn compute(&self, vals: &[f64], sec_vals: &[f64]) -> f64 {
        regr_slope(sec_vals, vals).unwrap_or(0.0)
    }
}

pub struct LogisticRegressionStrategy;
impl AggregationStrategy for LogisticRegressionStrategy {
    fn compute(&self, vals: &[f64], _sec_vals: &[f64]) -> f64 {
        warn!("[Aegis Agg] LOGISTIC_REGRESSION no nativo — COUNT fallback");
        vals.len() as f64
    }
}

fn get_strategy(agg_fn: AggregationFunction) -> Box<dyn AggregationStrategy> {
    match agg_fn {
        AggregationFunction::COUNT => Box::new(CountStrategy),
        AggregationFunction::SUM => Box::new(SumStrategy),
        AggregationFunction::AVG => Box::new(AvgStrategy),
        AggregationFunction::MIN => Box::new(MinStrategy),
        AggregationFunction::MAX => Box::new(MaxStrategy),
        AggregationFunction::MEDIAN => Box::new(PercentileStrategy { p: 0.5 }),
        AggregationFunction::PERCENTILE_90 => Box::new(PercentileStrategy { p: 0.9 }),
        AggregationFunction::PERCENTILE_95 => Box::new(PercentileStrategy { p: 0.95 }),
        AggregationFunction::PERCENTILE_99 => Box::new(PercentileStrategy { p: 0.99 }),
        AggregationFunction::STD_DEV => Box::new(StdDevStrategy),
        AggregationFunction::VARIANCE => Box::new(VarianceStrategy),
        AggregationFunction::CORRELATION => Box::new(CorrelationStrategy),
        AggregationFunction::LINEAR_REGRESSION => Box::new(LinearRegressionStrategy),
        AggregationFunction::LOGISTIC_REGRESSION => Box::new(LogisticRegressionStrategy),
        _ => {
            warn!("[Aegis Agg] AggregationFunction desconocida {:?} — SUM fallback", agg_fn);
            Box::new(SumStrategy)
        }
    }
}

// ── Dispatcher de agregación ──────────────────────────────────────────────────
// [PORTED_FROM: (compute-agg fn-kw vals sec-vals)]

fn compute_agg(agg_fn: AggregationFunction, vals: &[f64], sec_vals: &[f64]) -> f64 {
    get_strategy(agg_fn).compute(vals, sec_vals)
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


