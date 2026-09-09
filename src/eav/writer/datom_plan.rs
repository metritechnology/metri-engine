//! Pure planner of the write path — payload to datom list.
//!
//! Planificador puro del camino de escritura.
//!
//! Traduce un payload (o una entidad proyectada de saga) a la lista de datoms
//! que la transacción ACID confirmará: retracts del valor previo, asserts del
//! nuevo, atributos de sistema, `entity_type`, delta del contrato y el rescate
//! de `deleted_attrs`. Es la extracción del núcleo que `transact_inner` y
//! `transact_bulk_deferred` duplicaban línea a línea — con esta pieza, el
//! escritor sólo orquesta I/O y este módulo se prueba sin DynamoDB.
//!
//! Invariantes que fija (y los tests de caracterización candan):
//! - Un UPDATE genera par retract+assert por atributo cambiado; los
//! atributos no tocados no generan nada.
//! - Un DELETE genera sólo retracts y NO añade datoms de sistema.
//! - UPDATE/DELETE sobre entidad sin atributos activos es Eav002: el motor
//! no hace upsert (el audit trail del recurso real no se fabrica aquí).
//! - El delta sólo entra atributos que realmente cambiaron.

use std::collections::HashMap;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::datom::{Datom, DatomValue};
use crate::eav::writer::outbox::datom_value_to_json;
use crate::eav::writer::transact::{TransactOp, TransactPayload};

/// Plan de una sola entidad (madre o proyección de saga): sus datoms y los
/// requests FTS que sus asserts generen. El ORQUESTADOR decide si los FTS se
/// despachan — hoy sólo la entidad madre los usa.
#[derive(Debug)]
pub struct EntityPlan {
    pub datoms: Vec<Datom>,
    pub fts_requests: Vec<aws_sdk_dynamodb::types::WriteRequest>,
}

/// Plan completo de la mutación del payload: datoms, FTS, delta del contrato
/// y los atributos previos que un DELETE rescata para el sobre del Hub.
#[derive(Debug)]
pub struct DatomPlan {
    pub datoms: Vec<Datom>,
    pub fts_requests: Vec<aws_sdk_dynamodb::types::WriteRequest>,
    pub delta: Option<crate::domain::events::Delta>,
    pub deleted_attrs: Option<HashMap<String, DatomValue>>,
}

/// Compone los datoms de UNA entidad: valida cada atributo contra el Códice,
/// genera retract del valor previo en UPDATE, assert del nuevo (con FTS si el
/// atributo lo declara), enriquecimiento de sistema y el datom `entity_type`.
///
/// Es el camino compartido por la entidad madre y las proyecciones de saga
/// (éstas siempre llegan como `is_create=true` sin atributos activos).
pub fn plan_entity_datoms(
    tenant_id: &str,
    entity_type: &str,
    entity_id: &str,
    attrs: &HashMap<String, DatomValue>,
    tx_id: u64,
    is_create: bool,
    active_attrs: &HashMap<String, (u16, DatomValue)>,
) -> Result<EntityPlan, DomainError> {
    let mut datoms: Vec<Datom> = Vec::new();
    let mut fts_requests = Vec::new();

    for (attr_name, new_value) in attrs {
        let attr_desc = crate::codice::global()
            .get_attribute(entity_type, attr_name)
            .ok_or_else(|| {
                DomainError::eav(
                    ErrorCode::Eav004,
                    format!("Atributo '{attr_name}' no en registry para '{entity_type}'"),
                )
            })?;

        // Para UPDATE: retract del valor anterior
        if !is_create {
            if let Some((attr_id, old_value)) = active_attrs.get(attr_name) {
                let retract = Datom::retract(
                    tenant_id,
                    entity_id,
                    attr_name,
                    *attr_id,
                    old_value.clone(),
                    tx_id,
                );
                datoms.push(retract);
            }
        }

        // Assert — el nuevo valor
        let assert = Datom::assert(
            tenant_id,
            entity_id,
            attr_name,
            Datom::hash_attr_name(&attr_desc.name),
            new_value.clone(),
            tx_id,
        );

        if attr_desc.fts {
            let reqs = crate::eav::fts::trigram::build_fts_items(&assert, attr_desc);
            fts_requests.extend(reqs);
        }

        datoms.push(assert);
    }

    // Atributos de sistema (ulid, type, tenant, created_at/updated_at)
    crate::eav::writer::enricher::enrich_datoms(
        &mut datoms,
        entity_id,
        entity_type,
        tenant_id,
        tx_id,
        is_create,
        active_attrs,
    );

    // Compatibilidad con consultas legacy que busquen "entity_type" sin slash:
    if !is_create {
        if let Some((_, DatomValue::Str(old_type))) = active_attrs.get("entity_type") {
            if old_type != entity_type {
                let entity_type_retract = Datom::retract(
                    tenant_id,
                    entity_id,
                    "entity_type",
                    Datom::hash_attr_name("entity_type"),
                    DatomValue::Str(old_type.clone()),
                    tx_id,
                );
                datoms.push(entity_type_retract);
            }
        }
    }
    let entity_type_datom = Datom::assert(
        tenant_id,
        entity_id,
        "entity_type",
        Datom::hash_attr_name("entity_type"),
        DatomValue::Str(entity_type.to_string()),
        tx_id,
    );
    datoms.push(entity_type_datom);

    Ok(EntityPlan {
        datoms,
        fts_requests,
    })
}

/// Plan completo del payload: guards de existencia, delta, deleted_attrs y la
/// rama DELETE (sólo retracts, sin datoms de sistema) o Create/Update vía
/// `plan_entity_datoms`.
pub fn plan_payload_datoms(
    payload: &TransactPayload,
    entity_id: &str,
    tx_id: u64,
    active_attrs: &HashMap<String, (u16, DatomValue)>,
) -> Result<DatomPlan, DomainError> {
    // Delta del contrato (before/after) — sólo tiene sentido en UPDATE, y
    // debe computarse ANTES de que las ramas de abajo consuman active_attrs.
    let delta = if payload.op == TransactOp::Update {
        compute_transact_delta(active_attrs, &payload.attrs)
    } else {
        None
    };
    // En DELETE, los atributos previos se van con el retract — el sobre
    // `deleted` del Hub los necesita, así que se rescatan aquí.
    let deleted_attrs = if payload.op == TransactOp::Delete && !active_attrs.is_empty() {
        Some(
            active_attrs
                .iter()
                .map(|(k, (_, v))| (k.clone(), v.clone()))
                .collect(),
        )
    } else {
        None
    };

    // Invariante de existencia: un UPDATE/DELETE presupone una entidad
    // previa. Sin este guard, una entidad fantasma (id equivocado, id de
    // cliente ignorado en el CREATE) se fabrica aquí como upsert silencioso
    // y el audit trail del recurso real queda amputado.
    if matches!(payload.op, TransactOp::Update | TransactOp::Delete) && active_attrs.is_empty() {
        return Err(DomainError::eav(
            ErrorCode::Eav002,
            format!(
                "UPDATE/DELETE sobre entidad inexistente: '{}' (tenant '{}') — el motor no hace upsert",
                entity_id, payload.tenant_id
            ),
        ));
    }

    if payload.op == TransactOp::Delete {
        let mut datoms = Vec::new();
        for (attr_name, (attr_id, val)) in active_attrs {
            let retract = Datom::retract(
                &payload.tenant_id,
                entity_id,
                attr_name,
                *attr_id,
                val.clone(),
                tx_id,
            );
            datoms.push(retract);
        }
        return Ok(DatomPlan {
            datoms,
            fts_requests: Vec::new(),
            delta,
            deleted_attrs,
        });
    }

    let is_create = payload.op == TransactOp::Create;
    let EntityPlan {
        datoms,
        fts_requests,
    } = plan_entity_datoms(
        &payload.tenant_id,
        &payload.entity_type,
        entity_id,
        &payload.attrs,
        tx_id,
        is_create,
        active_attrs,
    )?;

    Ok(DatomPlan {
        datoms,
        fts_requests,
        delta,
        deleted_attrs,
    })
}

/// Delta before/after de un UPDATE, en el formato del contrato
/// metri-contracts. Sólo entran atributos que realmente cambiaron — un UPDATE
/// idempotente del ejecutor no debe parecer cambio ante el guard del Hub.
pub(crate) fn compute_transact_delta(
    active_attrs: &HashMap<String, (u16, DatomValue)>,
    new_attrs: &HashMap<String, DatomValue>,
) -> Option<crate::domain::events::Delta> {
    let mut delta = crate::domain::events::Delta::new();
    for (name, new_val) in new_attrs {
        let old_val = active_attrs.get(name).map(|(_, v)| v);
        if old_val != Some(new_val) {
            delta.insert(
                name.clone(),
                crate::domain::events::DeltaEntry {
                    before: old_val.map(datom_value_to_json),
                    after: Some(datom_value_to_json(new_val)),
                },
            );
        }
    }
    if delta.is_empty() {
        None
    } else {
        Some(delta)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry() {
        crate::eav::writer::test_support::init_codice();
    }

    fn payload(op: TransactOp) -> TransactPayload {
        let mut attrs = HashMap::new();
        attrs.insert("title".to_string(), DatomValue::Str("Bomba 3".to_string()));
        TransactPayload {
            tenant_id: "t1".to_string(),
            entity_id: Some("ent_1".to_string()),
            entity_type: "reminder".to_string(),
            attrs,
            op,
            suppress_events: false,
        }
    }

    fn activos(valores: &[(&str, u16, DatomValue)]) -> HashMap<String, (u16, DatomValue)> {
        valores
            .iter()
            .map(|(k, id, v)| (k.to_string(), (*id, v.clone())))
            .collect()
    }

    fn assert_de(datoms: &[Datom], attr: &str, valor: &str) -> bool {
        datoms.iter().any(|d| {
            d.attr_name == attr && d.op && matches!(&d.value, DatomValue::Str(s) if s == valor)
        })
    }

    fn retract_de(datoms: &[Datom], attr: &str, valor: &str) -> bool {
        datoms.iter().any(|d| {
            d.attr_name == attr && !d.op && matches!(&d.value, DatomValue::Str(s) if s == valor)
        })
    }

    #[test]
    fn create_trae_atributos_sistema_y_entity_type_sin_retracts() {
        registry();
        let plan =
            plan_payload_datoms(&payload(TransactOp::Create), "ent_1", 1000, &HashMap::new())
                .expect("create planifica");

        assert!(plan.delta.is_none(), "create no genera delta");
        assert!(plan.deleted_attrs.is_none());
        assert!(plan.datoms.iter().all(|d| d.op), "create no retracta nada");
        assert!(assert_de(&plan.datoms, "title", "Bomba 3"));
        assert!(assert_de(&plan.datoms, "entity/ulid", "ent_1"));
        assert!(assert_de(&plan.datoms, "entity/type", "reminder"));
        assert!(assert_de(&plan.datoms, "tenant/id", "t1"));
        // created_at sólo en create; updated_at siempre
        assert!(plan.datoms.iter().any(|d| d.attr_name == "meta/created_at"));
        assert!(plan.datoms.iter().any(|d| d.attr_name == "meta/updated_at"));
    }

    #[test]
    fn update_genera_par_retract_assert_solo_para_lo_que_cambia() {
        registry();
        let previos = activos(&[
            ("title", 77, DatomValue::Str("vieja".into())),
            ("status", 88, DatomValue::Str("open".into())),
            ("meta/updated_at", 4, DatomValue::Instant(123)),
        ]);
        let plan = plan_payload_datoms(&payload(TransactOp::Update), "ent_1", 2000, &previos)
            .expect("update planifica");

        assert!(retract_de(&plan.datoms, "title", "vieja"));
        assert!(assert_de(&plan.datoms, "title", "Bomba 3"));
        // `status` no viene en el payload: ni retract ni assert
        assert!(!plan.datoms.iter().any(|d| d.attr_name == "status"));
        // meta/updated_at retracta el instant previo — aquí se verifica la forma
        let retracts_updated = plan
            .datoms
            .iter()
            .filter(|d| d.attr_name == "meta/updated_at" && !d.op)
            .count();
        assert_eq!(retracts_updated, 1, "el updated_at previo se retracta");
    }

    #[test]
    fn update_de_entidad_inexistente_es_eav002() {
        registry();
        let err = plan_payload_datoms(
            &payload(TransactOp::Update),
            "fantasma",
            2000,
            &HashMap::new(),
        )
        .expect_err("sin atributos activos no hay update");
        assert_eq!(err.code, crate::domain::errors::ErrorCode::Eav002);
    }

    #[test]
    fn delete_de_entidad_inexistente_tambien_es_eav002() {
        registry();
        let err = plan_payload_datoms(
            &payload(TransactOp::Delete),
            "fantasma",
            2000,
            &HashMap::new(),
        )
        .expect_err("sin atributos activos no hay delete");
        assert_eq!(err.code, crate::domain::errors::ErrorCode::Eav002);
    }

    #[test]
    fn delete_genera_solo_retracts_y_rescata_deleted_attrs() {
        registry();
        let previos = activos(&[
            ("title", 77, DatomValue::Str("vieja".into())),
            ("status", 88, DatomValue::Str("open".into())),
        ]);
        let plan = plan_payload_datoms(&payload(TransactOp::Delete), "ent_1", 3000, &previos)
            .expect("delete planifica");

        assert_eq!(plan.datoms.len(), 2, "un retract por atributo activo");
        assert!(plan.datoms.iter().all(|d| !d.op), "delete no aserta nada");
        assert!(retract_de(&plan.datoms, "title", "vieja"));
        assert!(retract_de(&plan.datoms, "status", "open"));
        // No hay datoms de sistema en un delete
        assert!(!plan
            .datoms
            .iter()
            .any(|d| d.attr_name == "meta/updated_at" || d.attr_name == "entity_type"));

        let borrados = plan.deleted_attrs.expect("delete rescata los previos");
        assert_eq!(
            borrados.get("title"),
            Some(&DatomValue::Str("vieja".into()))
        );
        assert_eq!(
            borrados.get("status"),
            Some(&DatomValue::Str("open".into()))
        );
    }

    #[test]
    fn atributo_fuera_del_registry_es_eav004() {
        registry();
        let mut p = payload(TransactOp::Create);
        p.attrs
            .insert("attr_inexistente".to_string(), DatomValue::Long(1));
        let err =
            plan_payload_datoms(&p, "ent_1", 1000, &HashMap::new()).expect_err("attr desconocido");
        assert_eq!(err.code, crate::domain::errors::ErrorCode::Eav004);
    }

    #[test]
    fn el_delta_solo_trae_cambios_reales() {
        registry();
        let previos = activos(&[
            ("title", 77, DatomValue::Str("vieja".into())),
            ("status", 88, DatomValue::Str("open".into())),
        ]);
        let mut attrs = HashMap::new();
        // title cambia; status llega idéntico; message es nuevo
        attrs.insert("title".to_string(), DatomValue::Str("Bomba 3".into()));
        attrs.insert("status".to_string(), DatomValue::Str("open".into()));
        attrs.insert("message".to_string(), DatomValue::Str("nota".into()));
        let mut p = payload(TransactOp::Update);
        p.attrs = attrs;

        let plan = plan_payload_datoms(&p, "ent_1", 2000, &previos).expect("planifica");
        let delta = plan.delta.expect("hubo cambios");

        assert_eq!(delta.len(), 2, "status idéntico no entra al delta");
        let title = delta.get("title").expect("title cambió");
        assert_eq!(title.before.as_ref(), Some(&serde_json::json!("vieja")));
        assert_eq!(title.after.as_ref(), Some(&serde_json::json!("Bomba 3")));
        let message = delta.get("message").expect("message es nuevo");
        assert!(message.before.is_none());
        assert_eq!(message.after.as_ref(), Some(&serde_json::json!("nota")));
    }

    #[test]
    fn update_idempotente_no_genera_delta() {
        registry();
        let previos = activos(&[
            ("title", 77, DatomValue::Str("igual".into())),
            ("meta/updated_at", 4, DatomValue::Instant(123)),
        ]);
        let mut attrs = HashMap::new();
        attrs.insert("title".to_string(), DatomValue::Str("igual".into()));
        let mut p = payload(TransactOp::Update);
        p.attrs = attrs;

        let plan = plan_payload_datoms(&p, "ent_1", 2000, &previos).expect("planifica");
        assert!(plan.delta.is_none(), "sin cambios reales no hay delta");
    }

    #[test]
    fn los_atributos_fts_generan_requests_y_los_demas_no() {
        registry();
        // `asset.name` declara fts: true en config/models
        let mut attrs = HashMap::new();
        attrs.insert(
            "name".to_string(),
            DatomValue::Str("Bomba Centrifuga".into()),
        );
        attrs.insert("status".to_string(), DatomValue::Str("active".into()));
        let mut p = payload(TransactOp::Create);
        p.entity_type = "asset".to_string();
        p.attrs = attrs;

        let plan = plan_payload_datoms(&p, "ent_1", 1000, &HashMap::new()).expect("planifica");
        assert!(
            !plan.fts_requests.is_empty(),
            "asset.name es fts y genera requests"
        );
    }
}
