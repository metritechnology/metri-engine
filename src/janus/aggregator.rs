// janus/aggregator.rs — Paso 7: Result Assembly + OutputCast Aggregation
// Blueprint: JANUS - Rust.md §I.2 Paso 7 — Result Assembly
//
// Aplica el OutputCast sobre rows de negocio:
//   TABLE      → rows directas (sin agregación)
//   KPI        → SUM/COUNT/AVG/MIN/MAX con StreamAggregator (1 fila)
//   TIMESERIES → epoch bucketing + group_by intervalo
//   PIE/BUBBLE → group_by dimensiones + apply_metrics

use serde_json::{json, Map, Value};
use tracing::debug;
use std::collections::HashMap;

// ── StreamAggregator (Blueprint: §IV.3 — Zero-Allocation KPIs) ───────────────

#[derive(Debug, Default, Clone)]
struct StreamAggregator {
    count: u64,
    sum:   f64,
    min:   f64,
    max:   f64,
}

impl StreamAggregator {
    fn new() -> Self {
        StreamAggregator { count: 0, sum: 0.0, min: f64::MAX, max: f64::MIN }
    }

    fn ingest(&mut self, v: f64) {
        self.count += 1;
        self.sum   += v;
        if v < self.min { self.min = v; }
        if v > self.max { self.max = v; }
    }

    fn avg(&self) -> f64 {
        if self.count == 0 { 0.0 } else { self.sum / self.count as f64 }
    }
}

// ── MetricSpec — describe una métrica del AST IR ─────────────────────────────

#[derive(Debug, Clone)]
pub struct MetricSpec {
    pub field:       String,
    pub aggregation: String, // SUM, COUNT, AVG, MIN, MAX
    pub alias:       String,
}

impl MetricSpec {
    pub fn from_json(m: &Value) -> Option<Self> {
        let field = m.get("field").or(m.get("attribute")).and_then(|v| v.as_str())?.to_string();
        let agg   = m.get("aggregation").and_then(|v| v.as_str()).unwrap_or("SUM").to_string();
        let alias = m.get("alias").or(m.get("name")).and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{}_{}", agg.to_lowercase(), field));
        Some(MetricSpec { field, aggregation: agg, alias })
    }
}

// ── apply_metrics — agrega un conjunto de rows según métricas ─────────────────

pub fn apply_metrics(rows: &[Value], metrics: &[MetricSpec]) -> Value {
    let mut result = Map::new();

    for metric in metrics {
        let mut agg = StreamAggregator::new();
        for row in rows {
            let v = row.get(&metric.field)
                .or_else(|| row.as_object().and_then(|m| m.values().next()))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            agg.ingest(v);
        }

        let computed = match metric.aggregation.as_str() {
            "SUM"            => agg.sum,
            "COUNT"          => agg.count as f64,
            "AVG"            => agg.avg(),
            "MIN"            => if agg.count == 0 { 0.0 } else { agg.min },
            "MAX"            => if agg.count == 0 { 0.0 } else { agg.max },
            "COUNT_DISTINCT" => {
                let unique: std::collections::HashSet<String> = rows.iter()
                    .filter_map(|r| r.get(&metric.field).and_then(|v| v.as_str()).map(String::from))
                    .collect();
                unique.len() as f64
            }
            _ => agg.sum,
        };

        result.insert(metric.alias.clone(), json!(computed));
    }

    Value::Object(result)
}

// ── truncate_to_interval — bucketing para TIMESERIES ─────────────────────────

fn truncate_to_interval(epoch_secs: i64, interval: &str) -> i64 {
    match interval {
        "minute"  => (epoch_secs / 60) * 60,
        "hour"    => (epoch_secs / 3600) * 3600,
        "day"     => (epoch_secs / 86400) * 86400,
        "week"    => {
            // Aproximación: lunes = día de inicio (puede ser más preciso con chrono)
            let day_secs = epoch_secs / 86400;
            // 1970-01-01 fue jueves (4), ajustamos al lunes anterior
            let dow = (day_secs + 3) % 7;  // 0=lunes
            (day_secs - dow) * 86400
        }
        "month"   => {
            // Aproximación: inicio del mes en 30 días
            let day = epoch_secs / 86400;
            let approx_month_start = (day / 30) * 30 * 86400;
            approx_month_start
        }
        _ => epoch_secs, // sin truncado para intervalos desconocidos
    }
}

// ── apply_output_cast — punto de entrada principal ────────────────────────────

/// Aplica el OutputCast sobre los rows de negocio devueltos por el EAV reader.
///
/// [Blueprint: JANUS §I.2 Paso 7 — "Aplicar OutputCast: KPI aggregation, TIMESERIES bucketing, PIE grouping"]
pub fn apply_output_cast(rows: Vec<Value>, ast_ir: &Value) -> Vec<Value> {
    let output_cast = ast_ir.get("output_cast")
        .and_then(|v| v.as_str())
        .unwrap_or("TABLE");

    let metrics: Vec<MetricSpec> = ast_ir.get("metrics")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(MetricSpec::from_json).collect())
        .unwrap_or_default();

    let dimensions: Vec<String> = ast_ir.get("dimensions")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|d| {
            d.get("field").or(d.get("attribute")).and_then(|v| v.as_str()).map(String::from)
        }).collect())
        .unwrap_or_default();

    debug!("[Aggregator] output_cast={output_cast} rows={} metrics={} dims={}",
        rows.len(), metrics.len(), dimensions.len());

    match output_cast {
        // TABLE / CSV_EXPORT → rows directas sin agregación
        "TABLE" | "CSV_EXPORT" => rows,

        // KPI → 1 sola fila con las métricas agregadas
        "KPI" => {
            if metrics.is_empty() {
                // Sin métricas: devolver la primera fila con conteo
                vec![json!({ "count": rows.len() })]
            } else {
                vec![apply_metrics(&rows, &metrics)]
            }
        }

        // TIMESERIES → group_by intervalo de tiempo + métricas por bucket
        "TIMESERIES" => {
            let time_dim = ast_ir.get("dimensions")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.iter().find(|d| d.get("interval").is_some()));

            let (ts_field, interval) = time_dim.map(|d| {
                let f = d.get("field").or(d.get("attribute")).and_then(|v| v.as_str())
                    .unwrap_or("meta/created_at").to_string();
                let i = d.get("interval").and_then(|v| v.as_str()).unwrap_or("day").to_string();
                (f, i)
            }).unwrap_or_else(|| ("meta/created_at".to_string(), "day".to_string()));

            let mut buckets: HashMap<i64, Vec<Value>> = HashMap::new();
            for row in rows {
                let ts_secs = row.get(&ts_field)
                    .and_then(|v| v.as_i64())
                    .map(|ms| if ms > 1_000_000_000_000 { ms / 1000 } else { ms })
                    .unwrap_or(0);
                let bucket = truncate_to_interval(ts_secs, &interval);
                buckets.entry(bucket).or_default().push(row);
            }

            let mut result: Vec<Value> = buckets.into_iter().map(|(bucket, bucket_rows)| {
                let mut row = if metrics.is_empty() {
                    json!({ "count": bucket_rows.len() })
                } else {
                    apply_metrics(&bucket_rows, &metrics)
                };
                if let Some(obj) = row.as_object_mut() {
                    obj.insert("bucket".to_string(), json!(bucket));
                }
                row
            }).collect();

            // Ordenar por bucket ascendente
            result.sort_by_key(|r| r.get("bucket").and_then(|v| v.as_i64()).unwrap_or(0));
            result
        }

        // PIE / BUBBLE → group_by dimensiones + métricas por grupo
        "PIE" | "BUBBLE" => {
            if dimensions.is_empty() || metrics.is_empty() {
                return rows;
            }

            let mut groups: HashMap<String, Vec<Value>> = HashMap::new();
            for row in rows {
                let group_key = dimensions.iter()
                    .filter_map(|d| row.get(d).and_then(|v| v.as_str()).map(String::from))
                    .collect::<Vec<_>>()
                    .join("|");
                groups.entry(group_key).or_default().push(row);
            }

            let mut result: Vec<Value> = groups.into_iter().map(|(key, group_rows)| {
                let mut row = apply_metrics(&group_rows, &metrics);
                // Inyectar valores de las dimensiones en el resultado
                if let Some(first) = group_rows.first() {
                    if let Some(obj) = row.as_object_mut() {
                        for dim in &dimensions {
                            if let Some(val) = first.get(dim) {
                                obj.insert(dim.clone(), val.clone());
                            }
                        }
                    }
                }
                row
            }).collect();

            result.sort_by(|a, b| {
                let ka = a.as_object().and_then(|m| m.values().next()).and_then(|v| v.as_f64()).unwrap_or(0.0);
                let kb = b.as_object().and_then(|m| m.values().next()).and_then(|v| v.as_f64()).unwrap_or(0.0);
                kb.partial_cmp(&ka).unwrap_or(std::cmp::Ordering::Equal)
            });
            result
        }

        _ => rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn kpi_sum_aggregation() {
        let rows = vec![
            json!({"cost": 100.0, "status": "OPEN"}),
            json!({"cost": 200.0, "status": "CLOSED"}),
            json!({"cost": 300.0, "status": "OPEN"}),
        ];
        let ast = json!({
            "output_cast": "KPI",
            "metrics": [{"field": "cost", "aggregation": "SUM", "alias": "total_cost"}]
        });
        let result = apply_output_cast(rows, &ast);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["total_cost"], json!(600.0));
    }

    #[test]
    fn table_passes_rows_through() {
        let rows = vec![json!({"a": 1}), json!({"a": 2})];
        let ast = json!({"output_cast": "TABLE"});
        let result = apply_output_cast(rows.clone(), &ast);
        assert_eq!(result.len(), 2);
    }
}

use crate::janus::fbs;

pub fn apply_output_cast_fbs(rows: Vec<Value>, ast_ir: &fbs::AnalyticsRequestT) -> Vec<Value> {
    let output_cast = match ast_ir.output_cast.0 {
        1 => "KPI",
        2 => "TIMESERIES",
        3 => "TABLE",
        4 => "PIE",
        5 => "BUBBLE",
        6 => "CSV_EXPORT",
        _ => "TABLE",
    };

    let mut metrics = Vec::new();
    if let Some(m_list) = &ast_ir.metrics {
        for m in m_list {
            let field = m.attribute.clone().unwrap_or_default();
            let agg = match m.aggregation.0 {
                1 => "COUNT",
                2 => "SUM",
                3 => "AVG",
                4 => "MIN",
                5 => "MAX",
                _ => "SUM",
            };
            let alias = m.name.clone().unwrap_or_else(|| format!("{}_{}", agg.to_lowercase(), field));
            metrics.push(MetricSpec { field, aggregation: agg.to_string(), alias });
        }
    }

    let mut dimensions = Vec::new();
    if let Some(d_list) = &ast_ir.dimensions {
        for d in d_list {
            if let Some(attr) = &d.attribute {
                dimensions.push(attr.clone());
            }
        }
    }

    match output_cast {
        "KPI" => {
            if metrics.is_empty() {
                vec![json!({ "count": rows.len() })]
            } else {
                vec![apply_metrics(&rows, &metrics)]
            }
        }
        "TIMESERIES" => {
            let mut ts_field = "meta/created_at".to_string();
            let mut interval = "day".to_string();
            if let Some(d_list) = &ast_ir.dimensions {
                if let Some(d) = d_list.iter().find(|x| x.interval.is_some()) {
                    ts_field = d.attribute.clone().unwrap_or(ts_field);
                    interval = d.interval.clone().unwrap_or(interval);
                }
            }

            let mut buckets: HashMap<i64, Vec<Value>> = HashMap::new();
            for row in rows {
                let ts_secs = row.get(&ts_field)
                    .and_then(|v| v.as_i64())
                    .map(|ms| if ms > 1_000_000_000_000 { ms / 1000 } else { ms })
                    .unwrap_or(0);
                let bucket = truncate_to_interval(ts_secs, &interval);
                buckets.entry(bucket).or_default().push(row);
            }

            let mut result: Vec<Value> = buckets.into_iter().map(|(bucket, bucket_rows)| {
                let mut row = if metrics.is_empty() {
                    json!({ "count": bucket_rows.len() })
                } else {
                    apply_metrics(&bucket_rows, &metrics)
                };
                if let Some(obj) = row.as_object_mut() {
                    obj.insert("bucket".to_string(), json!(bucket));
                }
                row
            }).collect();

            result.sort_by_key(|r| r.get("bucket").and_then(|v| v.as_i64()).unwrap_or(0));
            result
        }
        "PIE" | "BUBBLE" => {
            if dimensions.is_empty() || metrics.is_empty() { return rows; }

            let mut groups: HashMap<String, Vec<Value>> = HashMap::new();
            for row in rows {
                let group_key = dimensions.iter()
                    .filter_map(|d| row.get(d).and_then(|v| v.as_str()).map(String::from))
                    .collect::<Vec<_>>()
                    .join("|");
                groups.entry(group_key).or_default().push(row);
            }

            let mut result: Vec<Value> = groups.into_iter().map(|(_key, group_rows)| {
                let mut row = apply_metrics(&group_rows, &metrics);
                if let Some(first) = group_rows.first() {
                    if let Some(obj) = row.as_object_mut() {
                        for dim in &dimensions {
                            if let Some(val) = first.get(dim) {
                                obj.insert(dim.clone(), val.clone());
                            }
                        }
                    }
                }
                row
            }).collect();

            result.sort_by(|a, b| {
                let ka = a.as_object().and_then(|m| m.values().next()).and_then(|v| v.as_f64()).unwrap_or(0.0);
                let kb = b.as_object().and_then(|m| m.values().next()).and_then(|v| v.as_f64()).unwrap_or(0.0);
                kb.partial_cmp(&ka).unwrap_or(std::cmp::Ordering::Equal)
            });
            result
        }
        _ => rows,
    }
}
