// [PORTED_FROM: src/metri/janus/multi_series.clj]
// janus/multi_series.rs — Motor de fusión MultiSeriesGroup — Full Outer Join asintótico.
// SRP: realiza y fusiona resultados de sub-queries agrupados bajo un group-id.

use serde_json::{json, Value, Map};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use tracing::warn;

use crate::janus::router::QueryChunk;

/// Calcula un hash simplificado para un objeto JSON.
fn hash_value(val: &Value) -> u64 {
    let mut hasher = DefaultHasher::new();
    match val {
        Value::Object(map) => {
            // Sort keys to ensure consistent hashing
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for k in keys {
                k.hash(&mut hasher);
                hash_value(map.get(k).unwrap()).hash(&mut hasher);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                hash_value(v).hash(&mut hasher);
            }
        }
        Value::String(s) => s.hash(&mut hasher),
        Value::Number(n) => n.to_string().hash(&mut hasher),
        Value::Bool(b) => b.hash(&mut hasher),
        Value::Null => 0.hash(&mut hasher),
    }
    hasher.finish()
}

/// Full Outer Join de filas de múltiples sub-queries.
/// [PORTED_FROM: (outer-join-data rows-per-qk)]
pub fn outer_join_data(rows_per_qk: HashMap<String, Vec<Value>>) -> Vec<Value> {
    if rows_per_qk.len() == 1 {
        return rows_per_qk.into_values().next().unwrap();
    }

    let mut idx: HashMap<u64, Map<String, Value>> = HashMap::new();

    for (_, rows) in rows_per_qk {
        for row in rows {
            if let Value::Object(row_map) = row {
                let h = hash_value(&Value::Object(row_map.clone()));
                let entry = idx.entry(h).or_insert_with(Map::new);
                // Merge
                for (k, v) in row_map {
                    entry.insert(k, v);
                }
            }
        }
    }

    idx.into_values().map(Value::Object).collect()
}

/// Retorna el subconjunto de queries que NO pertenecen a ningún merge-group.
/// [PORTED_FROM: (partition-standalone queries merge-groups)]
pub fn partition_standalone(mut queries: Map<String, Value>, merge_groups: &[Value]) -> Map<String, Value> {
    if merge_groups.is_empty() {
        return queries;
    }

    let mut merged_qks = HashSet::new();
    for mg in merge_groups {
        if let Some(qks) = mg.get("query_keys").or(mg.get("query-keys")).and_then(|v| v.as_array()) {
            for qk in qks {
                if let Some(s) = qk.as_str() {
                    merged_qks.insert(s.to_string());
                }
            }
        }
    }

    let mut standalone = Map::new();
    for (k, v) in queries {
        if !merged_qks.contains(&k) {
            standalone.insert(k, v);
        }
    }

    standalone
}

// Nota: process-merge-groups se implementa en janus/router.rs directamente
// debido a las restricciones de async y el préstamo de closures en Rust.
