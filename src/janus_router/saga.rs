//! SagaBuilder — declarative projection into scheduled_job.
//!
//! SagaBuilder — Proyección declarativa hacia `scheduled_job`.
//!
//! Cuando una entidad madre declara `shadow_sagas_mapping`, cada CREATE proyecta uno o más
//! `scheduled_job` en la MISMA transacción ACID. Metri Schedulers los consume por el bus.
//! Ver docs/architecture/COMPONENTE_EXTERNO_05_METRI_SCHEDULERS.md §3 y 03B §III.2.
//!
//! El builder no inventa valores y NO CONOCE dominios: la madre declara en su
//! mapping de qué atributo sale el disparo (`trigger_source`, un instante
//! ISO-8601) y qué attrs viajan al job. Los dominios con semánticas de ciclo
//! propias (p. ej. el recurrente de las pautas preventivas) implementan su
//! lógica en su plugin — aquí sólo plataforma (PLAN_DESCACOLE_PM_TOTAL.md).

use std::collections::{HashMap, HashSet};

use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::codice::registry::EntityModel;
use crate::codice::validator;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::reader::pull::EavReader;
use crate::eav::types::datom::DatomValue;
use crate::janus_router::ulid;

/// Entidad proyectada que viaja en la misma TX ACID que su madre.
#[derive(Debug, Clone)]
pub struct SagaProjection {
    pub entity_type: String,
    pub entity_id: String,
    pub attrs: HashMap<String, DatomValue>,
}

/// Trigger derivado del mapping DECLARATIVO. La plataforma sólo sabe traducir
/// un instante ISO-8601 a EXACT_TIME — sin vocabulario de dominio: la madre
/// declara de qué attr sale su disparo (`trigger_source`) y los dominios con
/// semánticas propias (cron recurrente, telemetría) implementan su ciclo en su
/// plugin, no aquí (PLAN_DESCACOLE_PM_TOTAL.md: esquema = dato del motor,
/// comportamiento = plugin).
fn resolve_trigger(
    entity: &str,
    mapping: &Map<String, Value>,
    payload: &Value,
) -> Result<i64, DomainError> {
    let source = mapping
        .get("trigger_source")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            DomainError::janus(
                ErrorCode::Jns001,
                format!(
                    "[SagaBuilder] '{entity}' declara shadow_sagas_mapping sin 'trigger_source': \
                     la proyección no puede inventar cuándo disparar."
                ),
            )
        })?;
    let iso = payload
        .get(source)
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            DomainError::janus(
                ErrorCode::Jns001,
                format!(
                    "[SagaBuilder] '{entity}' no expone '{source}' (su trigger_source declarado): \
                     la proyección no puede inventar cuándo disparar."
                ),
            )
        })?;
    iso_to_epoch_secs(source, iso)
}

/// ISO 8601 → epoch en segundos. Acepta `Z` y offsets explícitos.
fn iso_to_epoch_secs(source: &str, iso: &str) -> Result<i64, DomainError> {
    chrono::DateTime::parse_from_rfc3339(iso)
        .map(|dt| dt.timestamp())
        .map_err(|e| {
            DomainError::janus(
                ErrorCode::Jns001,
                format!("[SagaBuilder] trigger_source '{source}' con '{iso}' no es ISO 8601 válido: {e}"),
            )
        })
}

/// Offsets de pre-notificación declarados por la madre, en minutos.
fn prenotify_offsets(payload: &Value) -> Vec<i64> {
    for key in ["prenotify_before_minutes", "prenotify_minutes_array"] {
        if let Some(arr) = payload.get(key).and_then(|v| v.as_array()) {
            let mut v: Vec<i64> = arr
                .iter()
                .filter_map(|x| x.as_i64())
                .filter(|n| *n > 0)
                .collect();
            v.sort_unstable();
            v.dedup();
            return v;
        }
    }
    Vec::new()
}

fn sha256_hex(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update(p.as_bytes());
        h.update(b"|");
    }
    format!("{:x}", h.finalize())
}

/// Escribe `value` en `dest_path` dentro de `obj`; los puntos descienden en objetos anidados.
fn assoc_path(obj: &mut Map<String, Value>, dest_path: &str, value: Value) {
    let parts: Vec<&str> = dest_path.split('.').collect();
    assoc_path_inner(obj, &parts, value);
}

/// La recursión anida los préstamos por nivel: evita el conflicto de
/// reborrows de un cursor iterativo a través de la ruta.
fn assoc_path_inner(cursor: &mut Map<String, Value>, parts: &[&str], value: Value) {
    if parts.len() == 1 {
        cursor.insert(parts[0].to_string(), value);
        return;
    }
    // El mapping exige objetos en la ruta intermedia: un valor preexistente
    // no-objeto se sobreescribe — total, sin pánico (R2).
    let slot = cursor
        .entry(parts[0].to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if !slot.is_object() {
        *slot = Value::Object(Map::new());
    }
    if let Value::Object(map) = slot {
        assoc_path_inner(map, &parts[1..], value);
    }
    // Inalcanzable tras el force: el brazo no-objeto no tiene nada que hacer.
}

/// Resuelve la ruta origen del mapping.
/// Resuelve la ruta origen del mapping.
///
/// `["__const:VALOR"]` → valor literal constante `VALOR`.
/// `["__self"]`        → ID de la entidad madre (`parent_id`).
/// `[campo]`           → lee directo del payload de la madre.
/// `[campo_ref, attr]` → sigue la referencia y lee `attr` de la entidad referenciada.
///                       Exige una lectura EAV: es el precio de un mapping declarativo.
async fn resolve_source(
    reader: &EavReader,
    tenant_id: &str,
    payload: &Value,
    parent_id: &str,
    src: &[Value],
) -> Option<Value> {
    match src.len() {
        0 => None,
        1 => {
            let s = src[0].as_str()?;
            if let Some(const_val) = s.strip_prefix("__const:") {
                Some(json!(const_val))
            } else if s == "__self" {
                Some(json!(parent_id))
            } else {
                payload.get(s).cloned().filter(|v| !v.is_null())
            }
        }
        _ => {
            let ref_field = src[0].as_str()?;
            let target_attr = src[1].as_str()?;
            let ref_id = payload.get(ref_field)?.as_str()?;
            let entity = reader
                .pull(tenant_id, ref_id, Some(&[target_attr]))
                .await
                .ok()?;
            // Reutiliza el único conversor DatomValue→JSON del motor para no divergir
            // en el tratamiento de arrays, refs y json embebido.
            let as_json = crate::eav::writer::outbox::datom_map_to_json(&entity);
            as_json.get(target_attr).cloned().filter(|v| !v.is_null())
        }
    }
}

/// Job vivo de una madre, leído por AVET + pull para el diff de convergencia.
#[derive(Debug, Clone)]
pub struct LiveSagaJob {
    pub job_id: String,
    pub idempotency_hash: Option<String>,
    pub status: String,
}

/// Convergencia de las sagas de una madre que se actualiza (P0c).
#[derive(Debug, Clone, Default)]
pub struct SagaReconciliation {
    /// Jobs que deben nacer en la misma TX que el update de la madre.
    pub to_create: Vec<SagaProjection>,
    /// Jobs huérfanos (hash que la madre ya no declara): se retiran tras el commit.
    pub to_delete: Vec<String>,
    /// Jobs en ciclo activo cuyo estado diverge del dictado por la madre.
    pub to_restatus: Vec<(String, String)>,
}

/// El estado que la salud de la madre dicta para sus trampas: una madre
/// PAUSED/RETIRED no engendra — sus jobs viven SUSPENDED (Chronos los mapea a
/// DISABLED sin destruirlos: reactivar es barato y no recalcula nada).
pub fn desired_job_status(mother_status: Option<&str>) -> &'static str {
    match mother_status {
        Some("ACTIVE") | None => "ACTIVE",
        Some(_) => "SUSPENDED",
    }
}

fn projection_hash(p: &SagaProjection) -> Option<&str> {
    match p.attrs.get("idempotency_hash") {
        Some(DatomValue::Str(h)) => Some(h.as_str()),
        _ => None,
    }
}

/// Diff puro de convergencia (P0c). `desired` ya lleva el estado dictado por la
/// salud de la madre. Un hash presente en lo vivo se conserva (salvo divergencia
/// de estado en ciclo activo); un hash sin job vivo nace; un job con hash
/// huérfano muere. COMPLETED/FAILED cerraron su ciclo y no se tocan: la bitácora
/// del pasado no se reescribe.
pub fn diff_sagas(
    desired: Vec<SagaProjection>,
    live: Vec<LiveSagaJob>,
    desired_status: &str,
) -> SagaReconciliation {
    let mut pending: HashSet<String> = desired
        .iter()
        .filter_map(projection_hash)
        .map(str::to_string)
        .collect();

    let mut to_delete = Vec::new();
    let mut to_restatus = Vec::new();
    for job in &live {
        let Some(hash) = job.idempotency_hash.as_deref() else {
            // Sin hash no hay identidad que comparar: se conserva (conservador).
            continue;
        };
        if pending.remove(hash) {
            if matches!(job.status.as_str(), "ACTIVE" | "SUSPENDED" | "PENDING")
                && job.status != desired_status
            {
                to_restatus.push((job.job_id.clone(), desired_status.to_string()));
            }
        } else {
            to_delete.push(job.job_id.clone());
        }
    }

    let to_create = desired
        .into_iter()
        .filter(|p| {
            projection_hash(p)
                .map(|h| pending.contains(h))
                .unwrap_or(false)
        })
        .collect();

    SagaReconciliation {
        to_create,
        to_delete,
        to_restatus,
    }
}

/// Construye las proyecciones `scheduled_job` de una entidad madre recién creada.
///
/// Emite un Job por cada offset de pre-notificación **más** el disparo principal.
/// `[1440, 30]` produce tres.
pub async fn build_saga_projections(
    reader: &EavReader,
    tenant_id: &str,
    model: &EntityModel,
    payload: &Value,
    parent_id: &str,
    created_by: &str,
) -> Result<Vec<SagaProjection>, DomainError> {
    let Some(mapping) = model
        .shadow_sagas_mapping
        .as_ref()
        .and_then(|m| m.as_object())
    else {
        return Ok(Vec::new());
    };
    if mapping.is_empty() {
        return Ok(Vec::new());
    }

    let job_model = crate::codice::global()
        .get_model("scheduled_job")
        .ok_or_else(|| {
            DomainError::janus(
                ErrorCode::Jns001,
                "[SagaBuilder] 'scheduled_job' no está en el Códice — no se puede proyectar"
                    .to_string(),
            )
        })?;

    let base_epoch = resolve_trigger(&model.entity, mapping, payload)?;
    let tz = payload
        .get("iana_timezone")
        .and_then(|v| v.as_str())
        .unwrap_or("UTC");

    // El disparo principal (offset 0) siempre existe; los avisos previos se le suman.
    let mut offsets = prenotify_offsets(payload);
    offsets.push(0);

    let mut out = Vec::with_capacity(offsets.len());

    for offset_min in offsets {
        // La madre declarativa es de UN instante: el aviso previo es aritmética
        // directa sobre el epoch. El fan-out recurrente de un dominio (p. ej.
        // las pautas) vive en su plugin, no en esta proyección.
        let expr = (base_epoch - offset_min * 60).to_string();

        let job_id = ulid::generate();

        let mut obj = Map::new();
        obj.insert("parent_entity_ref".into(), json!(parent_id));
        obj.insert("created_by".into(), json!(created_by));
        obj.insert("trigger_type".into(), json!("EXACT_TIME"));
        obj.insert("trigger_expression".into(), json!(expr));
        obj.insert("iana_timezone".into(), json!(tz));
        obj.insert("action_type".into(), json!("DISPATCH_NOTIFICATION"));
        obj.insert("status".into(), json!("ACTIVE"));
        obj.insert("run_count".into(), json!(0));

        let mut action_payload = Map::new();
        action_payload.insert("source_entity".into(), json!(model.entity));
        action_payload.insert("source_id".into(), json!(parent_id));
        action_payload.insert("offset_minutes".into(), json!(offset_min));
        action_payload.insert("content".into(), Value::Object(Map::new()));
        obj.insert("action_payload".into(), Value::Object(action_payload));

        // Proyección declarativa: el mapping sobrescribe lo anterior si coincide.
        // `trigger_source` (string) no es una entrada dest→src y se salta solo:
        // el loop sólo consume valores-array.
        for (dest, src) in mapping {
            let Some(src_arr) = src.as_array() else {
                continue;
            };
            if let Some(v) = resolve_source(reader, tenant_id, payload, parent_id, src_arr).await {
                assoc_path(&mut obj, dest, v);
            }
        }

        // El hash de idempotencia cubre expr, offset y AHORA el action_payload
        // completo. El payload es ESTÁTICO en T=0 — AWS lo reproduce meses
        // después tal como nació — así que cualquier cambio del molde (título,
        // prioridad, duración, asignados…) cambia el hash y la convergencia de
        // la saga re-hornea la trampa con el payload fresco. Sin esto, editar
        // la prioridad del plan dejaba la trampa sirviendo el payload viejo
        // para siempre (hallazgo del diseño del plan, 2026-09-23).
        let payload_fingerprint = obj
            .get("action_payload")
            .and_then(|v| serde_json::to_string(v).ok())
            .unwrap_or_default();
        obj.insert(
            "idempotency_hash".into(),
            json!(sha256_hex(&[
                parent_id,
                &expr,
                &offset_min.to_string(),
                &payload_fingerprint
            ])),
        );

        let job_payload = Value::Object(obj);
        let attrs =
            validator::validate_payload(job_model, &job_payload, tenant_id, true).map_err(|e| {
                DomainError::janus(
                    ErrorCode::Jns001,
                    format!(
                        "[SagaBuilder] la proyección de '{}' no valida contra scheduled_job: {e:?}",
                        model.entity
                    ),
                )
            })?;

        out.push(SagaProjection {
            entity_type: "scheduled_job".to_string(),
            entity_id: job_id,
            attrs,
        });
    }

    Ok(out)
}

#[cfg(test)]
#[path = "tests/saga_tests.rs"]
mod tests;
