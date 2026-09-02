// janus_router/saga.rs — SagaBuilder — Proyección declarativa hacia `scheduled_job`.
//
//
// Cuando una entidad madre declara `shadow_sagas_mapping`, cada CREATE proyecta uno o más
// `scheduled_job` en la MISMA transacción ACID. Metri Schedulers los consume por el bus.
// Ver docs/architecture/COMPONENTE_EXTERNO_05_METRI_SCHEDULERS.md §3 y 03B §III.2.
//
// El builder no inventa valores: el trigger se deriva de atributos declarados en la madre.

use std::collections::HashMap;

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

/// Trigger derivado de la entidad madre. Nunca se inventa: sale de atributos declarados.
enum Trigger {
    Cron { expr: String },
    ExactTime { epoch_secs: i64 },
    Telemetry { expr: String },
}

/// Deriva el trigger de la entidad madre.
///
/// Orden deliberado: la condición física gana al calendario. Un mantenimiento con
/// `meter_based_trigger` debe dispararse cuando la máquina lo pida, no cuando toque en el
/// almanaque. `next_due_date` es el último recurso porque es un valor calculado, no una
/// intención declarada por el usuario.
fn resolve_trigger(entity: &str, payload: &Value) -> Result<Trigger, DomainError> {
    let s = |k: &str| {
        payload
            .get(k)
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
    };

    if let Some(meter) = payload.get("meter_based_trigger").filter(|v| !v.is_null()) {
        return Ok(Trigger::Telemetry {
            expr: telemetry_expr(meter)?,
        });
    }
    if let Some(cron) = s("cron_expression") {
        return Ok(Trigger::Cron {
            expr: cron.to_string(),
        });
    }
    if let Some(iso) = s("reminder_datetime") {
        return Ok(Trigger::ExactTime {
            epoch_secs: iso_to_epoch_secs(iso)?,
        });
    }
    if let Some(n) = payload.get("next_due_date").and_then(epoch_secs_of) {
        return Ok(Trigger::ExactTime { epoch_secs: n });
    }

    Err(DomainError::janus(
        ErrorCode::Jns001,
        format!(
            "[SagaBuilder] '{entity}' declara shadow_sagas_mapping pero no expone ninguna fuente \
             de trigger (meter_based_trigger, cron_expression, reminder_datetime, next_due_date). \
             La proyección no puede inventar cuándo disparar."
        ),
    ))
}

/// Traduce `meter_based_trigger` a la gramática `<METRIC_CODE> <OPERADOR> <VALOR>`
/// que exige Metri Schedulers (Componente Externo 05 §3.2).
fn telemetry_expr(meter: &Value) -> Result<String, DomainError> {
    let metric = meter
        .get("metric_code")
        .or_else(|| meter.get("meterId"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            DomainError::janus(
                ErrorCode::Jns001,
                "[SagaBuilder] meter_based_trigger sin metric_code".to_string(),
            )
        })?;
    let op = meter
        .get("operator")
        .and_then(|v| v.as_str())
        .unwrap_or("GT");
    let sym = match op {
        "GT" | ">" => ">",
        "LT" | "<" => "<",
        "GTE" | ">=" => ">=",
        "LTE" | "<=" => "<=",
        "EQ" | "==" => "==",
        other => {
            return Err(DomainError::janus(
                ErrorCode::Jns001,
                format!("[SagaBuilder] operador telemétrico no soportado: '{other}'"),
            ))
        }
    };
    let threshold = meter
        .get("threshold")
        .or_else(|| meter.get("value"))
        .and_then(|v| v.as_f64())
        .ok_or_else(|| {
            DomainError::janus(
                ErrorCode::Jns001,
                "[SagaBuilder] meter_based_trigger sin threshold numérico".to_string(),
            )
        })?;
    Ok(format!("{metric} {sym} {threshold}"))
}

fn epoch_secs_of(v: &Value) -> Option<i64> {
    let n = v
        .as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))?;
    // Heurística de unidad: los epoch en ms de este siglo superan 1e12.
    Some(if n > 1_000_000_000_000 { n / 1000 } else { n })
}

/// ISO 8601 → epoch en segundos. Acepta `Z` y offsets explícitos.
fn iso_to_epoch_secs(iso: &str) -> Result<i64, DomainError> {
    chrono::DateTime::parse_from_rfc3339(iso)
        .map(|dt| dt.timestamp())
        .map_err(|e| {
            DomainError::janus(
                ErrorCode::Jns001,
                format!("[SagaBuilder] reminder_datetime '{iso}' no es ISO 8601 válido: {e}"),
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
    if parts.len() == 1 {
        obj.insert(parts[0].to_string(), value);
        return;
    }
    let mut cursor = obj;
    for seg in &parts[..parts.len() - 1] {
        cursor = cursor
            .entry(seg.to_string())
            .or_insert_with(|| Value::Object(Map::new()))
            .as_object_mut()
            .expect("segmento intermedio del mapping no es objeto");
    }
    cursor.insert(parts[parts.len() - 1].to_string(), value);
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
            let as_json = crate::eav::writer::transact::datom_map_to_json(&entity);
            as_json.get(target_attr).cloned().filter(|v| !v.is_null())
        }
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

    let trigger = resolve_trigger(&model.entity, payload)?;
    let tz = payload
        .get("iana_timezone")
        .and_then(|v| v.as_str())
        .unwrap_or("UTC");

    // El disparo principal (offset 0) siempre existe; los avisos previos se le suman.
    let mut offsets = prenotify_offsets(payload);
    offsets.push(0);

    let mut out = Vec::with_capacity(offsets.len());

    for offset_min in offsets {
        // Un aviso previo es siempre un instante concreto, aunque la madre sea recurrente:
        // desplazar una expresión cron no es expresable en el caso general (cruces de mes).
        // Para la recurrencia, el Job principal se reproyecta en cada disparo.
        let (kind, expr) = match (&trigger, offset_min) {
            (t, 0) => match t {
                Trigger::Cron { expr } => ("CRON", expr.clone()),
                Trigger::Telemetry { expr } => ("TELEMETRY", expr.clone()),
                Trigger::ExactTime { epoch_secs } => ("EXACT_TIME", epoch_secs.to_string()),
            },
            (Trigger::ExactTime { epoch_secs }, off) => {
                ("EXACT_TIME", (epoch_secs - off * 60).to_string())
            }
            (Trigger::Cron { .. }, off) => {
                // La madre es recurrente pero el aviso previo necesita un instante.
                // `next_due_date` es la única referencia temporal concreta disponible.
                match payload.get("next_due_date").and_then(epoch_secs_of) {
                    Some(due) => ("EXACT_TIME", (due - off * 60).to_string()),
                    None => {
                        tracing::warn!(
                            entity = %model.entity, offset = off,
                            "[SagaBuilder] pre-notificación omitida: la madre es CRON y no expone \
                             next_due_date, así que no hay instante concreto que desplazar"
                        );
                        continue;
                    }
                }
            }
            (Trigger::Telemetry { .. }, off) => {
                // Una condición física no tiene 'antes': no se sabe cuándo ocurrirá.
                tracing::warn!(
                    entity = %model.entity, offset = off,
                    "[SagaBuilder] pre-notificación omitida: un trigger TELEMETRY no tiene \
                     instante conocido que desplazar"
                );
                continue;
            }
        };

        let job_id = ulid::generate();

        let mut obj = Map::new();
        obj.insert("parent_entity_ref".into(), json!(parent_id));
        obj.insert("created_by".into(), json!(created_by));
        obj.insert("trigger_type".into(), json!(kind));
        obj.insert("trigger_expression".into(), json!(expr));
        obj.insert("iana_timezone".into(), json!(tz));
        obj.insert("action_type".into(), json!("DISPATCH_NOTIFICATION"));
        obj.insert("status".into(), json!("ACTIVE"));
        obj.insert("run_count".into(), json!(0));
        // El offset entra en el hash: dos avisos del mismo padre no deben deduplicarse entre sí.
        obj.insert(
            "idempotency_hash".into(),
            json!(sha256_hex(&[parent_id, &expr, &offset_min.to_string()])),
        );

        let mut action_payload = Map::new();
        action_payload.insert("source_entity".into(), json!(model.entity));
        action_payload.insert("source_id".into(), json!(parent_id));
        action_payload.insert("offset_minutes".into(), json!(offset_min));
        action_payload.insert("content".into(), Value::Object(Map::new()));
        obj.insert("action_payload".into(), Value::Object(action_payload));

        // Proyección declarativa: el mapping sobrescribe lo anterior si coincide.
        for (dest, src) in mapping {
            let Some(src_arr) = src.as_array() else {
                continue;
            };
            if let Some(v) = resolve_source(reader, tenant_id, payload, parent_id, src_arr).await {
                assoc_path(&mut obj, dest, v);
            }
        }

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
