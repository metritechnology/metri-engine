//! Transactional outbox — the event row is born with the mutation.
//!
//! Patrón outbox: la fila del evento nace en la MISMA
//! transacción que la mutación (Componente Externo 05, metri-schedulers §10).
//!
//! La fila outbox_event ES el registro durable de la mutación: su id es el ULID
//! de la mutación y la llave de orden del ledger del Hub (§10.3). Este módulo es
//! puro — compone datoms, no toca I/O — para que el escritor (transact.rs) sólo
//! orqueste y los tests puedan fijar el sobre sin DynamoDB.
//!
//! Los helpers JSON (`datom_value_to_json`, `datom_map_to_json`) viven aquí
//! porque el sobre `payload` fijó su formato: aplanado y con la misma
//! representación que el delta del contrato metri-contracts. Consumidores
//! externos (saga, domain_event_bus, materialization) comparten la función para
//! no partir la representación en dos.

use std::collections::HashMap;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::datom::{Datom, DatomValue};
use crate::eav::writer::transact::TransactOp;

/// Convierte un valor de datom a JSON del contrato. Extraído de
/// `datom_map_to_json` para reuso — el delta del contrato (before/after) usa la
/// MISMA representación que el payload aplanado, o el guard de bitácora del Hub
/// compararía peras con manzanas.
pub(crate) fn datom_value_to_json(v: &DatomValue) -> serde_json::Value {
    match v {
        DatomValue::Null => serde_json::Value::Null,
        DatomValue::Bool(b) => serde_json::Value::Bool(*b),
        DatomValue::Long(n) | DatomValue::Instant(n) => {
            serde_json::Value::Number(serde_json::Number::from(*n))
        }
        DatomValue::Double(d) => serde_json::Number::from_f64(*d)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        DatomValue::Str(s) | DatomValue::Uuid(s) => {
            if (s.starts_with('{') && s.ends_with('}')) || (s.starts_with('[') && s.ends_with(']'))
            {
                serde_json::from_str::<serde_json::Value>(s)
                    .unwrap_or_else(|_| serde_json::Value::String(s.clone()))
            } else {
                serde_json::Value::String(s.clone())
            }
        }
        DatomValue::Ref(r) => serde_json::Value::String(r.to_string()),
        DatomValue::Array(arr) => serde_json::Value::Array(
            arr.iter()
                .map(|s| {
                    if (s.starts_with('{') && s.ends_with('}'))
                        || (s.starts_with('[') && s.ends_with(']'))
                    {
                        serde_json::from_str::<serde_json::Value>(s)
                            .unwrap_or_else(|_| serde_json::Value::String(s.clone()))
                    } else {
                        serde_json::Value::String(s.clone())
                    }
                })
                .collect(),
        ),
        DatomValue::Bytes(b) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
        DatomValue::BigInt(n) => serde_json::Value::String(n.to_string()),
        DatomValue::Geo { lat, lon } => {
            let mut geo_obj = serde_json::Map::new();
            if let Some(la) = serde_json::Number::from_f64(*lat) {
                geo_obj.insert("lat".to_string(), serde_json::Value::Number(la));
            }
            if let Some(lo) = serde_json::Number::from_f64(*lon) {
                geo_obj.insert("lon".to_string(), serde_json::Value::Number(lo));
            }
            serde_json::Value::Object(geo_obj)
        }
    }
}

pub(crate) fn datom_map_to_json(map: &HashMap<String, DatomValue>) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (k, v) in map {
        obj.insert(k.clone(), datom_value_to_json(v));
    }
    serde_json::Value::Object(obj)
}

/// Genera los datoms de la fila outbox para una mutación, o `None` si el
/// modelo los suprime (`disable_eda`) o la entidad ES un outbox_event (las
/// filas outbox no anidan outbox — terminaría el ciclo de realimentación).
pub(crate) fn generate_outbox_datoms(
    tenant_id: &str,
    entity_type: &str,
    entity_id: &str,
    op: TransactOp,
    attrs: &HashMap<String, DatomValue>,
    tx_id: u64,
    mutation_ulid: &str,
) -> Result<Option<Vec<Datom>>, DomainError> {
    let disable_eda = crate::codice::global()
        .get_model(entity_type)
        .map(|m| m.disable_eda)
        .unwrap_or(false);

    if entity_type == "outbox_event" || disable_eda {
        return Ok(None);
    }

    // La fila outbox nace con el id de la mutación: outbox_id == ulid del
    // sobre. Un evento re-publicado por el sweeper conserva el mismo id, y
    // el ledger del Hub puede ordenar creaciones y borrados del mismo Job.
    let outbox_ulid = mutation_ulid.to_string();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    let op_str = match op {
        TransactOp::Create => "create",
        TransactOp::Update => "update",
        TransactOp::Delete => "delete",
    };

    let detail_type = format!("{}.{}", entity_type, op_str);

    let mut flat_attrs = HashMap::new();
    for (k, v) in attrs {
        flat_attrs.insert(k.clone(), v.clone());
    }
    flat_attrs.insert("id".to_string(), DatomValue::Str(entity_id.to_string()));

    let payload_json = datom_map_to_json(&flat_attrs);
    let payload_str = serde_json::to_string(&payload_json).unwrap_or_default();

    let mut outbox_attrs = HashMap::new();
    outbox_attrs.insert("status".to_string(), DatomValue::Str("PENDING".to_string()));
    outbox_attrs.insert("detail_type".to_string(), DatomValue::Str(detail_type));
    outbox_attrs.insert("payload".to_string(), DatomValue::Str(payload_str));
    outbox_attrs.insert("retry_count".to_string(), DatomValue::Long(0));
    outbox_attrs.insert("created_at".to_string(), DatomValue::Instant(now_ms));

    let mut outbox_datoms = Vec::new();
    for (attr_name, val) in outbox_attrs {
        let attr_desc = crate::codice::global()
            .get_attribute("outbox_event", &attr_name)
            .ok_or_else(|| {
                DomainError::eav(
                    ErrorCode::Eav004,
                    format!("Atributo '{attr_name}' no en registry para 'outbox_event'"),
                )
            })?;
        let assert = Datom::assert(
            tenant_id,
            &outbox_ulid,
            &attr_name,
            Datom::hash_attr_name(&attr_desc.name),
            val,
            tx_id,
        );
        outbox_datoms.push(assert);
    }

    crate::eav::writer::enricher::enrich_datoms(
        &mut outbox_datoms,
        &outbox_ulid,
        "outbox_event",
        tenant_id,
        tx_id,
        true, // is_create
        &HashMap::new(),
    );

    let outbox_type_datom = Datom::assert(
        tenant_id,
        &outbox_ulid,
        "entity_type",
        Datom::hash_attr_name("entity_type"),
        DatomValue::Str("outbox_event".to_string()),
        tx_id,
    );
    outbox_datoms.push(outbox_type_datom);

    Ok(Some(outbox_datoms))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() {
        crate::eav::writer::test_support::init_codice();
    }

    fn attrs_de_ejemplo() -> HashMap<String, DatomValue> {
        let mut attrs = HashMap::new();
        attrs.insert(
            "title".to_string(),
            DatomValue::Str("Bomba hidraulica".to_string()),
        );
        attrs
    }

    #[test]
    fn create_genera_fila_outbox_pendiente_con_detail_type_y_payload_plano() {
        registry();
        let datoms = generate_outbox_datoms(
            "t1",
            "asset",
            "ent_1",
            TransactOp::Create,
            &attrs_de_ejemplo(),
            1000,
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        )
        .expect("sin errores de registry")
        .expect("asset genera outbox");

        let detalle = datoms
            .iter()
            .find(|d| d.attr_name == "detail_type")
            .expect("la fila trae detail_type");
        assert_eq!(detalle.value, DatomValue::Str("asset.create".into()));

        let payload = datoms
            .iter()
            .find(|d| d.attr_name == "payload")
            .expect("la fila trae payload");
        match &payload.value {
            DatomValue::Str(json) => {
                let v: serde_json::Value = serde_json::from_str(json).expect("payload es JSON");
                assert_eq!(v["id"], "ent_1", "el payload aplanado porta el id");
                assert_eq!(v["title"], "Bomba hidraulica");
            }
            otro => panic!("payload debería ser Str, fue {otro:?}"),
        }

        let status = datoms
            .iter()
            .find(|d| d.attr_name == "status")
            .expect("la fila trae status");
        assert_eq!(status.value, DatomValue::Str("PENDING".into()));

        let tipo = datoms
            .iter()
            .find(|d| d.attr_name == "entity_type")
            .expect("la fila trae entity_type");
        assert_eq!(tipo.value, DatomValue::Str("outbox_event".into()));
        assert_eq!(tipo.entity_id, "01ARZ3NDEKTSV4RRFFQ69G5FAV");
    }

    #[test]
    fn la_operacion_cambia_el_detail_type() {
        registry();
        let detalle = |op| {
            let datoms = generate_outbox_datoms(
                "t1",
                "asset",
                "ent_1",
                op,
                &attrs_de_ejemplo(),
                1000,
                "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            )
            .expect("sin errores")
            .expect("genera outbox");
            match &datoms
                .iter()
                .find(|d| d.attr_name == "detail_type")
                .expect("trae detail_type")
                .value
            {
                DatomValue::Str(s) => s.clone(),
                otro => panic!("detail_type debería ser Str, fue {otro:?}"),
            }
        };
        assert_eq!(detalle(TransactOp::Update), "asset.update");
        assert_eq!(detalle(TransactOp::Delete), "asset.delete");
    }

    #[test]
    fn outbox_event_no_anida_outbox() {
        registry();
        let fila = generate_outbox_datoms(
            "t1",
            "outbox_event",
            "ob_1",
            TransactOp::Create,
            &HashMap::new(),
            1000,
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        )
        .expect("sin errores");
        assert!(fila.is_none(), "outbox_event no genera outbox");
    }

    #[test]
    fn disable_eda_suprime_el_outbox() {
        registry();
        let fila = generate_outbox_datoms(
            "t1",
            "audit_log",
            "aud_1",
            TransactOp::Create,
            &HashMap::new(),
            1000,
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        )
        .expect("sin errores");
        assert!(fila.is_none(), "audit_log declara disable_eda");
    }

    #[test]
    fn el_json_del_valor_refleja_el_tipo_del_contrato() {
        assert_eq!(
            datom_value_to_json(&DatomValue::Null),
            serde_json::json!(null)
        );
        assert_eq!(
            datom_value_to_json(&DatomValue::Bool(true)),
            serde_json::json!(true)
        );
        assert_eq!(
            datom_value_to_json(&DatomValue::Long(7)),
            serde_json::json!(7)
        );
        assert_eq!(
            datom_value_to_json(&DatomValue::Instant(1_700_000_000_000)),
            serde_json::json!(1_700_000_000_000_i64)
        );
        assert_eq!(
            datom_value_to_json(&DatomValue::Str("hola".into())),
            serde_json::json!("hola")
        );
        // Un Str que YA es JSON se parsea — el sobre viaja embebido, no escapado.
        assert_eq!(
            datom_value_to_json(&DatomValue::Str("{\"a\":1}".into())),
            serde_json::json!({"a": 1})
        );
        assert_eq!(
            datom_value_to_json(&DatomValue::Uuid(
                "550e8400-e29b-41d4-a716-446655440000".into()
            )),
            serde_json::json!("550e8400-e29b-41d4-a716-446655440000")
        );
        assert_eq!(
            datom_value_to_json(&DatomValue::Ref(42)),
            serde_json::json!("42")
        );
        assert_eq!(
            datom_value_to_json(&DatomValue::BigInt(170141183460469231731687303715884105727)),
            serde_json::json!("170141183460469231731687303715884105727")
        );
        // Bytes viajan base64; Geo como objeto lat/lon.
        assert_eq!(
            datom_value_to_json(&DatomValue::Bytes(vec![1, 2, 3])),
            serde_json::json!("AQID")
        );
        assert_eq!(
            datom_value_to_json(&DatomValue::Geo {
                lat: 4.5,
                lon: -74.0
            }),
            serde_json::json!({"lat": 4.5, "lon": -74.0})
        );
        // Un Double no finito no tiene representación JSON — cae a null.
        assert_eq!(
            datom_value_to_json(&DatomValue::Double(f64::NAN)),
            serde_json::json!(null)
        );
        // Un Str con corchetes que NO es JSON válido se queda string.
        assert_eq!(
            datom_value_to_json(&DatomValue::Str("{no es json".into())),
            serde_json::json!("{no es json")
        );
    }
}
