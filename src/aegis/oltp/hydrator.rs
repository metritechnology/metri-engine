// aegis/oltp/hydrator.rs
// SRP: Encapsular hidratación de entidades (EAV → JSON Rows in-memory), control de cache y calentamiento selectivo.

use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::RwLock;
use tracing::{debug, info, warn};

use crate::domain::errors::DomainError;
use crate::eav::reader::pull::{EavReader, EAV_CACHE};
use crate::eav::types::datom::DatomValue;

pub const MAX_WARMED_TENANTS_SIZE: usize = 1_000;

pub static WARMED_TENANTS: Lazy<RwLock<std::collections::HashMap<String, HashSet<String>>>> =
    Lazy::new(|| RwLock::new(std::collections::HashMap::new()));

pub static WARMUP_LOCK: Lazy<tokio::sync::Mutex<()>> = Lazy::new(|| tokio::sync::Mutex::new(()));

#[derive(Clone)]
pub struct OltpEntityHydrator {
    pull_reader: EavReader,
}

impl OltpEntityHydrator {
    pub fn new(pull_reader: EavReader) -> Self {
        Self { pull_reader }
    }

    /// Calentamiento selectivo incremental de cache para consultas analíticas.
    pub async fn warm_cache_incremental(
        &self,
        tenant_id: &str,
        entity_type: &str,
        entity_ids: &[String],
        resolved_attrs: &HashSet<String>,
        is_analytical: bool,
    ) {
        if is_analytical && entity_ids.len() > 100 {
            let cache_key = format!("{}:{}", tenant_id, entity_type);

            let missing_attrs: Vec<String> = if let Ok(warmed) = WARMED_TENANTS.read() {
                if let Some(warmed_attrs) = warmed.get(&cache_key) {
                    resolved_attrs
                        .iter()
                        .filter(|attr| !warmed_attrs.contains(*attr))
                        .cloned()
                        .collect()
                } else {
                    let mut attrs: Vec<String> = resolved_attrs.iter().cloned().collect();
                    if attrs.is_empty() {
                        attrs.push("entity/ulid".to_string());
                    }
                    attrs
                }
            } else {
                let mut attrs: Vec<String> = resolved_attrs.iter().cloned().collect();
                if attrs.is_empty() {
                    attrs.push("entity/ulid".to_string());
                }
                attrs
            };

            if !missing_attrs.is_empty() {
                let _guard = WARMUP_LOCK.lock().await;
                let missing_attrs_lock: Vec<String> = if let Ok(warmed) = WARMED_TENANTS.read() {
                    if let Some(warmed_attrs) = warmed.get(&cache_key) {
                        missing_attrs
                            .iter()
                            .filter(|attr| !warmed_attrs.contains(*attr))
                            .cloned()
                            .collect()
                    } else {
                        missing_attrs.clone()
                    }
                } else {
                    missing_attrs.clone()
                };

                if !missing_attrs_lock.is_empty() {
                    info!(
                        "[Aegis OLTP FBS] Calentando caché selectiva incremental para tenant {} y entidad {}. Atributos faltantes a calentar: {:?}",
                        tenant_id,
                        entity_type,
                        missing_attrs_lock
                    );

                    if let Err(e) = self
                        .pull_reader
                        .warm_cache_for_tenant(
                            tenant_id,
                            Some(entity_type),
                            Some(&missing_attrs_lock),
                        )
                        .await
                    {
                        warn!("[Aegis OLTP FBS] Error calentando caché selectiva incremental para tenant {} y entidad {}: {:?}", tenant_id, entity_type, e);
                    } else {
                        if let Ok(mut warmed) = WARMED_TENANTS.write() {
                            if warmed.len() >= MAX_WARMED_TENANTS_SIZE
                                && !warmed.contains_key(&cache_key)
                            {
                                let to_remove: Vec<String> = warmed
                                    .keys()
                                    .take(MAX_WARMED_TENANTS_SIZE / 5)
                                    .cloned()
                                    .collect();
                                for k in to_remove {
                                    warmed.remove(&k);
                                }
                            }
                            let entry = warmed.entry(cache_key).or_insert_with(HashSet::new);
                            for attr in missing_attrs_lock {
                                entry.insert(attr);
                            }
                        }
                    }
                }
            }
        }
    }

    pub async fn hydrate_entities<F>(
        &self,
        tenant_id: &str,
        entity_ids: &[String],
        resolved_attrs: &HashSet<String>,
        _is_analytical: bool,
        sel_opt: Option<&[&str]>,
        max_hydrations: usize,
        target_break: usize,
        should_break_early: bool,
        filter_fn: F,
    ) -> Result<Vec<Value>, DomainError>
    where
        F: Fn(&Value) -> bool + Send + Sync,
    {
        let tenant_id_str = tenant_id.to_string();
        let sel_opt_owned: Option<Vec<String>> =
            sel_opt.map(|s| s.iter().map(|&x| x.to_string()).collect());

        let total_candidates = entity_ids.len();
        let limit_to_spawn = std::cmp::min(total_candidates, max_hydrations);

        let mut collected_rows: Vec<(usize, Value)> = Vec::new();
        let mut misses: Vec<(usize, String)> = Vec::new();

        // 1. Resolución Sincrónica desde Caché bajo un único read lock
        if let Ok(cache) = EAV_CACHE.read() {
            for idx in 0..limit_to_spawn {
                let entity_id = &entity_ids[idx];
                let pk = format!("T#{}#E#{}", tenant_id_str, entity_id);
                if let Some(entry) = cache.get(&pk) {
                    let is_sufficient = if entry.is_complete {
                        true
                    } else {
                        let mut sufficient = true;
                        for attr in resolved_attrs {
                            if !entry.map.contains_key(attr) {
                                sufficient = false;
                                break;
                            }
                        }
                        sufficient
                    };

                    if is_sufficient {
                        let mut map = entry.map.clone();
                        map.insert("id".to_string(), DatomValue::Str(entity_id.clone()));
                        let row = entity_map_to_json(map);

                        if filter_fn(&row) {
                            collected_rows.push((idx, row));
                            if should_break_early && collected_rows.len() >= target_break {
                                break;
                            }
                        }
                    } else {
                        misses.push((idx, entity_id.clone()));
                    }
                } else {
                    misses.push((idx, entity_id.clone()));
                }
            }
        }

        // 2. Fallback asíncrono para Cache Misses utilizando JoinSet concurrente
        if !misses.is_empty() && collected_rows.len() < target_break {
            debug!("[Aegis OLTP Hydrator] Cache miss para {} de {} candidatos. Hydrating concurrently.", misses.len(), limit_to_spawn);
            let mut join_set: tokio::task::JoinSet<(
                usize,
                String,
                Result<crate::eav::reader::pull::EntityMap, DomainError>,
            )> = tokio::task::JoinSet::new();
            let concurrency_limit = 100;
            let mut idx_to_spawn = 0;

            loop {
                // Spawn tasks up to the concurrency limit
                while join_set.len() < concurrency_limit && idx_to_spawn < misses.len() {
                    let (original_idx, entity_id) = misses[idx_to_spawn].clone();
                    let reader = self.pull_reader.clone();
                    let tenant = tenant_id_str.clone();
                    let sel = sel_opt_owned.clone();

                    join_set.spawn(async move {
                        let sel_refs: Option<Vec<&str>> =
                            sel.as_ref().map(|v| v.iter().map(|s| s.as_str()).collect());
                        let sel_opt_ref = sel_refs.as_ref().map(|v| v.as_slice());
                        let res = reader.pull(&tenant, &entity_id, sel_opt_ref).await;
                        (original_idx, entity_id, res)
                    });
                    idx_to_spawn += 1;
                }

                if join_set.is_empty() {
                    break;
                }

                // Process finished pull tasks and filter on the fly
                if let Some(res) = join_set.join_next().await {
                    match res {
                        Ok((original_idx, entity_id, Ok(mut map))) => {
                            if !map.is_empty() {
                                map.insert("id".to_string(), DatomValue::Str(entity_id.clone()));
                                let row = entity_map_to_json(map);

                                if filter_fn(&row) {
                                    collected_rows.push((original_idx, row));
                                    if should_break_early && collected_rows.len() >= target_break {
                                        break;
                                    }
                                }
                            } else {
                                warn!(
                                    "[Aegis OLTP Hydrator] pull vacío para entity_id={entity_id}"
                                );
                            }
                        }
                        Ok((_original_idx, entity_id, Err(e))) => {
                            warn!("[Aegis OLTP Hydrator] pull falló para entity_id={entity_id}: {e:?}");
                        }
                        Err(e) => {
                            warn!("[Aegis OLTP Hydrator] JoinSet task panicked: {e:?}");
                        }
                    }
                }
            }
        }

        // 3. Ordenar filas recolectadas por su índice original para garantizar orden determinista EAV ULID
        collected_rows.sort_by_key(|(idx, _)| *idx);
        let matching_rows: Vec<Value> = collected_rows.into_iter().map(|(_, row)| row).collect();

        info!(
            "[Aegis OLTP Hydrator] {} de {} candidatos procesados ({} cache hits, {} misses), {} pasaron filtros",
            limit_to_spawn,
            total_candidates,
            limit_to_spawn - misses.len(),
            misses.len(),
            matching_rows.len()
        );

        Ok(matching_rows)
    }
}

/// Convierte un EntityMap (HashMap<String, DatomValue>) a un JSON Object plano.
pub fn entity_map_to_json(map: std::collections::HashMap<String, DatomValue>) -> Value {
    let mut row = serde_json::Map::new();
    for (k, v) in map {
        let json_val = match v {
            DatomValue::Str(s) => {
                if let Some(registry) = crate::codice::registry::global_opt() {
                    let parts: Vec<&str> = k.split('/').collect();
                    if parts.len() == 2 {
                        let entity = parts[0];
                        let attr_name = parts[1];
                        if let Some(attr) = registry.get_attribute(entity, attr_name) {
                            if matches!(attr.attr_type, crate::codice::registry::AttrType::Json) {
                                serde_json::from_str::<Value>(&s).unwrap_or_else(|_| json!(s))
                            } else {
                                json!(s)
                            }
                        } else {
                            json!(s)
                        }
                    } else {
                        if (s.starts_with('{') && s.ends_with('}'))
                            || (s.starts_with('[') && s.ends_with(']'))
                        {
                            serde_json::from_str::<Value>(&s).unwrap_or_else(|_| json!(s))
                        } else {
                            json!(s)
                        }
                    }
                } else {
                    json!(s)
                }
            }
            DatomValue::Uuid(s) => json!(s),
            DatomValue::Long(n) => json!(n),
            DatomValue::Instant(n) => json!(n),
            DatomValue::Double(d) => json!(d),
            DatomValue::Bool(b) => json!(b),
            DatomValue::BigInt(n) => json!(n),
            DatomValue::Ref(r) => json!(r),
            DatomValue::Array(arr) => json!(arr),
            DatomValue::Geo { lat, lon } => json!({"lat": lat, "lon": lon}),
            DatomValue::Null => Value::Null,
            DatomValue::Bytes(_) => json!("_binary_"),
        };
        if k.contains('/') {
            let bare_key = k.split('/').last().unwrap_or(&k).to_string();
            if !bare_key.is_empty() {
                if bare_key == "id" {
                    if k == "entity/id" {
                        row.insert(bare_key, json_val.clone());
                    } else if k == "tenant/id" {
                        row.insert("tenant_id".to_string(), json_val.clone());
                    }
                } else if bare_key == "type" {
                    if k != "entity/type" {
                        row.insert(bare_key, json_val.clone());
                    }
                } else {
                    row.insert(bare_key, json_val.clone());
                }
            }
        }
        row.insert(k, json_val);
    }

    // Asegurar alias para frontend (creado y actualizado)
    if let Some(v) = row.get("meta/created_at").cloned() {
        row.insert("created_at".to_string(), v.clone());
        row.insert("createdAt".to_string(), v);
    }
    if let Some(v) = row.get("meta/updated_at").cloned() {
        row.insert("updated_at".to_string(), v.clone());
        row.insert("updatedAt".to_string(), v);
    }

    Value::Object(row)
}
