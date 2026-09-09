//! HierarchyContext post-processing — has_children injection.
//!
//! HierarchyContext post-processing.
//!
//! SRP: inyectar :has_children en rows EAV, sin I/O ni estado.
//!
//! # Origin
//! el stack anterior implementa 2 modos:
//! Modo 1 (ref): Datahike reverse-ref → pull retorna [{:db/id ...}]
//! Modo 2 (string/EAV): in-memory usando el conjunto de parent_field values
//!
//! Para DynamoDB EAV, parent_location_id es un string ULID.
//! Solo el Modo 2 aplica aquí.

use serde_json::{json, Value};
use std::collections::HashSet;
use tracing::debug;

/// Modo 2 (EAV/string): Calcula `has_children` en memoria.
///
/// Algoritmo:
///   1. Recolectar todos los valores de `parent_field` presentes en `rows`
///      → ese conjunto son los IDs de entidades que tienen al menos un hijo.
///   2. Para cada row: `has_children = (parent_ids.contains(row["id"]))`
///
pub fn inject_has_children_from_rows(rows: &mut [Value], parent_field: &str) {
    if rows.is_empty() {
        return;
    }

    // Paso 1: IDs de todas las entidades referenciadas como padres
    // Probar tanto el campo con namespace como sin namespace
    let bare_parent = parent_field.split('/').next_back().unwrap_or(parent_field);
    let parent_ids: HashSet<String> = rows
        .iter()
        .filter_map(|r| {
            r.get(parent_field)
                .or_else(|| r.get(bare_parent))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .collect();

    debug!(
        "[Hierarchy] {} parent IDs únicos detectados para campo '{}'",
        parent_ids.len(),
        parent_field
    );

    // Paso 2: anotar has_children en cada row
    for row in rows.iter_mut() {
        if let Some(obj) = row.as_object_mut() {
            let id = obj
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let has_children = parent_ids.contains(&id);
            obj.insert("has_children".to_string(), json!(has_children));
        }
    }
}
