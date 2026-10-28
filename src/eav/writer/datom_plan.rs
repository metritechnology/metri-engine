//! Planificador puro del camino de escritura — traducción determinista de `TransactPayload` a lista de `Datom`.
//!
//! Traduce un payload (o una entidad proyectada de saga) a la lista de datoms
//! que la transacción ACID confirmará: retracciones del valor previo, aserciones del
//! nuevo, atributos de sistema, `entity/type`, delta del contrato y el rescate
//! de `deleted_attrs`.
//!
//! Invariantes garantizados:
//! - Un `UPDATE` genera el par `retract + assert` únicamente por cada atributo modificado; los
//!   atributos no incluidos o sin cambio efectivo no generan mutación.
//! - Un `DELETE` genera exclusivamente retracciones y no añade datoms de sistema.
//! - `UPDATE` o `DELETE` sobre una entidad sin atributos activos falla con `ErrorCode::Eav002`
//!   (prohibición de *upsert* implícito).
//! - El `Delta` sólo incluye los atributos cuyo valor cambió efectivamente.

use std::collections::HashMap;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::datom::{Datom, DatomValue};
use crate::eav::writer::outbox::datom_value_to_json;
use crate::eav::writer::transact::{TransactOp, TransactPayload};

/// Plan de escritura de una entidad individual (entidad madre o proyección de saga).
#[derive(Debug)]
pub struct EntityPlan {
    /// Lista ordenada de datoms (`retract` y `assert`) generados para la entidad.
    pub datoms: Vec<Datom>,
    /// Solicitudes de indexación FTS generadas por los atributos con `fts: true`.
    pub fts_requests: Vec<crate::mbf::storage::WriteRequest>,
}

/// Plan completo de la mutación de un `TransactPayload`.
#[derive(Debug)]
pub struct DatomPlan {
    /// Lista completa de datoms a persistir en la transacción.
    pub datoms: Vec<Datom>,
    /// Solicitudes de escritura para el índice de búsqueda de texto completo (FTS).
    pub fts_requests: Vec<crate::mbf::storage::WriteRequest>,
    /// Diferencial `before/after` de los atributos que cambiaron en un `Update`.
    pub delta: Option<crate::domain::events::Delta>,
    /// Mapa de atributos activos previos rescatados antes de un `Delete` para el sobre de eventos.
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
    actor: &str,
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

        // D1 (PLAN_PM_FECHA_INICIO_FIN): null = SÓLO el retract — limpiar el
        // atributo por parche. Sin assert, sin FTS. En CREATE un null no
        // genera nada: el atributo nace ausente, como si no viajara.
        if matches!(new_value, DatomValue::Null) {
            continue;
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
            let reqs = crate::eav::fts::trigram::build_fts_items_pure(&assert, attr_desc);
            fts_requests.extend(reqs);
        }

        datoms.push(assert);
    }

    // Auto-generación declarativa de vectores (`auto_generate_from`, p. ej. `document_chunk.semantic_embedding`)
    // cuando el cliente envía el texto fuente (`chunk_content`) sin precalcular el vector de bytes.
    if let Some(model_attrs) = crate::codice::global().get_attributes(entity_type) {
        for attr_desc in model_attrs {
            if attr_desc.auto_generate_from.is_empty()
                || attr_desc.attr_type != crate::codice::registry::AttrType::Bytes
            {
                continue;
            }
            let bare_target = attr_desc
                .name
                .split('/')
                .next_back()
                .unwrap_or(attr_desc.name.as_str());
            let already_provided = attrs
                .get(bare_target)
                .or_else(|| attrs.get(&attr_desc.name))
                .is_some_and(|v| {
                    !matches!(v, DatomValue::Null)
                        && !matches!(v, DatomValue::Bytes(b) if b.is_empty())
                });
            if already_provided {
                continue;
            }

            let mut source_touched = false;
            let mut source_parts = Vec::new();
            for src_field in &attr_desc.auto_generate_from {
                let bare_src = src_field
                    .split('/')
                    .next_back()
                    .unwrap_or(src_field.as_str());
                if let Some(DatomValue::Str(s)) =
                    attrs.get(bare_src).or_else(|| attrs.get(src_field))
                {
                    if !s.trim().is_empty() {
                        source_touched = true;
                        source_parts.push(s.trim().to_string());
                    }
                } else if !is_create {
                    if let Some((_, DatomValue::Str(s))) = active_attrs
                        .get(bare_src)
                        .or_else(|| active_attrs.get(src_field))
                    {
                        if !s.trim().is_empty() {
                            source_parts.push(s.trim().to_string());
                        }
                    }
                }
            }

            if source_touched && !source_parts.is_empty() {
                let dim = attr_desc.dimensions.unwrap_or(1024) as usize;
                let combined_text = source_parts.join(" ");
                let emb_vec =
                    crate::mbf::zones::hybrid_search::MbfHybridSearchIndex::compute_deterministic_embedding(
                        &combined_text,
                        dim,
                    );
                let emb_bytes =
                    crate::mbf::zones::hybrid_search::MbfHybridSearchIndex::encode_embedding_le_bytes(
                        &emb_vec,
                    );
                if !is_create {
                    if let Some((old_attr_id, old_val)) = active_attrs
                        .get(bare_target)
                        .or_else(|| active_attrs.get(&attr_desc.name))
                    {
                        datoms.push(Datom::retract(
                            tenant_id,
                            entity_id,
                            bare_target,
                            *old_attr_id,
                            old_val.clone(),
                            tx_id,
                        ));
                    }
                }
                datoms.push(Datom::assert(
                    tenant_id,
                    entity_id,
                    bare_target,
                    Datom::hash_attr_name(&attr_desc.name),
                    DatomValue::Bytes(emb_bytes),
                    tx_id,
                ));
            }
        }
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
        actor,
    );

    // Espejo de historial: si el modelo declara atributos con `history_mirror`,
    // la partición del DUEÑO recibe en esta misma TX el assert/retract del
    // datom `_refs/<entidad>` correspondiente (create y update; el delete lo
    // planifica `plan_payload_datoms`).
    datoms.extend(crate::eav::writer::history_mirror::plan_mirror_datoms(
        (tenant_id, entity_type, entity_id),
        attrs,
        active_attrs,
        tx_id,
        false,
    ));

    Ok(EntityPlan {
        datoms,
        fts_requests,
    })
}

fn build_delete_datoms(
    payload: &TransactPayload,
    entity_id: &str,
    active_attrs: &HashMap<String, (u16, DatomValue)>,
    tx_id: u64,
) -> Vec<Datom> {
    let mut datoms: Vec<Datom> = active_attrs
        .iter()
        .map(|(attr_name, (attr_id, val))| {
            Datom::retract(
                &payload.tenant_id,
                entity_id,
                attr_name,
                *attr_id,
                val.clone(),
                tx_id,
            )
        })
        .collect();
    datoms.extend(crate::eav::writer::history_mirror::plan_mirror_datoms(
        (&payload.tenant_id, &payload.entity_type, entity_id),
        &payload.attrs,
        active_attrs,
        tx_id,
        true,
    ));
    datoms
}

/// Plan completo del payload: guards de existencia, delta, deleted_attrs y la
/// rama DELETE (sólo retracts, sin datoms de sistema) o Create/Update vía
/// `plan_entity_datoms`.
pub fn plan_payload_datoms(
    payload: &TransactPayload,
    entity_id: &str,
    tx_id: u64,
    active_attrs: &HashMap<String, (u16, DatomValue)>,
    actor: &str,
) -> Result<DatomPlan, DomainError> {
    let delta = if payload.op == TransactOp::Update {
        compute_transact_delta(active_attrs, &payload.attrs)
    } else {
        None
    };
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
        return Ok(DatomPlan {
            datoms: build_delete_datoms(payload, entity_id, active_attrs, tx_id),
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
        actor,
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
        // D1: null sobre un atributo AUSENTE no es cambio (el retract de algo
        // que no existe es el no-op que ya era) — sin esta guarda, un null
        // cosmético parecería mutación ante el guard de idempotencia.
        let sin_cambio = old_val.is_none() && matches!(new_val, DatomValue::Null);
        if !sin_cambio && old_val != Some(new_val) {
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

    /// Actor fijo de los planes de prueba.
    const ACTOR_TEST: &str = "usr_test";

    fn registry() {
        crate::eav::writer::test_support::init_codice();
    }

    fn payload(op: TransactOp) -> TransactPayload {
        let mut attrs = HashMap::new();
        attrs.insert(
            "title".to_string(),
            DatomValue::Str("Recordatorio 3".to_string()),
        );
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
        let plan = plan_payload_datoms(
            &payload(TransactOp::Create),
            "ent_1",
            1000,
            &HashMap::new(),
            ACTOR_TEST,
        )
        .expect("create planifica");

        assert!(plan.delta.is_none(), "create no genera delta");
        assert!(plan.deleted_attrs.is_none());
        assert!(plan.datoms.iter().all(|d| d.op), "create no retracta nada");
        assert!(assert_de(&plan.datoms, "title", "Recordatorio 3"));
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
        let plan = plan_payload_datoms(
            &payload(TransactOp::Update),
            "ent_1",
            2000,
            &previos,
            ACTOR_TEST,
        )
        .expect("update planifica");

        assert!(retract_de(&plan.datoms, "title", "vieja"));
        assert!(assert_de(&plan.datoms, "title", "Recordatorio 3"));
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
    fn update_con_null_solo_retracta_sin_assert() {
        registry();
        let mut attrs = HashMap::new();
        attrs.insert("title".to_string(), DatomValue::Null);
        let p = TransactPayload {
            tenant_id: "t1".to_string(),
            entity_id: Some("ent_1".to_string()),
            entity_type: "reminder".to_string(),
            attrs,
            op: TransactOp::Update,
            suppress_events: false,
        };
        let previos = activos(&[("title", 77, DatomValue::Str("vieja".into()))]);
        let plan = plan_payload_datoms(&p, "ent_1", 2000, &previos, ACTOR_TEST).expect("planifica");

        assert!(
            retract_de(&plan.datoms, "title", "vieja"),
            "el valor previo se retracta"
        );
        assert!(
            !plan.datoms.iter().any(|d| d.attr_name == "title" && d.op),
            "null NO genera assert"
        );
        // El contrato del bus: after = null (la limpieza es visible).
        let delta = plan.delta.expect("el null sobre valor vivo es cambio");
        let entry = delta.get("title").expect("title en el delta");
        assert_eq!(entry.after, Some(serde_json::Value::Null));
    }

    #[test]
    fn null_de_atributo_ausente_es_el_no_op_que_ya_era() {
        registry();
        let mut attrs = HashMap::new();
        attrs.insert("title".to_string(), DatomValue::Null);
        let p = TransactPayload {
            tenant_id: "t1".to_string(),
            entity_id: Some("ent_1".to_string()),
            entity_type: "reminder".to_string(),
            attrs,
            op: TransactOp::Update,
            suppress_events: false,
        };
        let previos = activos(&[("status", 88, DatomValue::Str("open".into()))]);
        let plan = plan_payload_datoms(&p, "ent_1", 2000, &previos, ACTOR_TEST).expect("planifica");

        assert!(
            !plan.datoms.iter().any(|d| d.attr_name == "title"),
            "null sin valor previo no genera datom"
        );
        assert!(plan.delta.is_none(), "ausente→null no es cambio");
    }

    #[test]
    fn create_con_null_el_atributo_no_nace() {
        registry();
        let mut attrs = HashMap::new();
        attrs.insert("title".to_string(), DatomValue::Null);
        let p = TransactPayload {
            tenant_id: "t1".to_string(),
            entity_id: Some("ent_1".to_string()),
            entity_type: "reminder".to_string(),
            attrs,
            op: TransactOp::Create,
            suppress_events: false,
        };
        let plan =
            plan_payload_datoms(&p, "ent_1", 1000, &HashMap::new(), ACTOR_TEST).expect("planifica");
        assert!(
            !plan.datoms.iter().any(|d| d.attr_name == "title"),
            "create con null: el atributo nace ausente"
        );
    }

    #[test]
    fn update_de_entidad_inexistente_es_eav002() {
        registry();
        let err = plan_payload_datoms(
            &payload(TransactOp::Update),
            "fantasma",
            2000,
            &HashMap::new(),
            ACTOR_TEST,
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
            ACTOR_TEST,
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
        let plan = plan_payload_datoms(
            &payload(TransactOp::Delete),
            "ent_1",
            3000,
            &previos,
            ACTOR_TEST,
        )
        .expect("delete planifica");

        assert_eq!(plan.datoms.len(), 2, "un retract por atributo activo");
        assert!(plan.datoms.iter().all(|d| !d.op), "delete no aserta nada");
        assert!(retract_de(&plan.datoms, "title", "vieja"));
        assert!(retract_de(&plan.datoms, "status", "open"));
        // No hay datoms de sistema en un delete
        assert!(!plan
            .datoms
            .iter()
            .any(|d| d.attr_name == "meta/updated_at" || d.attr_name == "entity/type"));

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
        let err = plan_payload_datoms(&p, "ent_1", 1000, &HashMap::new(), ACTOR_TEST)
            .expect_err("attr desconocido");
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
        attrs.insert(
            "title".to_string(),
            DatomValue::Str("Recordatorio 3".into()),
        );
        attrs.insert("status".to_string(), DatomValue::Str("open".into()));
        attrs.insert("message".to_string(), DatomValue::Str("nota".into()));
        let mut p = payload(TransactOp::Update);
        p.attrs = attrs;

        let plan = plan_payload_datoms(&p, "ent_1", 2000, &previos, ACTOR_TEST).expect("planifica");
        let delta = plan.delta.expect("hubo cambios");

        assert_eq!(delta.len(), 2, "status idéntico no entra al delta");
        let title = delta.get("title").expect("title cambió");
        assert_eq!(title.before.as_ref(), Some(&serde_json::json!("vieja")));
        assert_eq!(
            title.after.as_ref(),
            Some(&serde_json::json!("Recordatorio 3"))
        );
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

        let plan = plan_payload_datoms(&p, "ent_1", 2000, &previos, ACTOR_TEST).expect("planifica");
        assert!(plan.delta.is_none(), "sin cambios reales no hay delta");
    }

    #[test]
    fn los_atributos_fts_generan_requests_y_los_demas_no() {
        registry();
        // `reminder.title` declara fts: true en config/models
        let mut attrs = HashMap::new();
        attrs.insert(
            "title".to_string(),
            DatomValue::Str("Revision Trimestral".into()),
        );
        attrs.insert("status".to_string(), DatomValue::Str("open".into()));
        let mut p = payload(TransactOp::Create);
        p.entity_type = "reminder".to_string();
        p.attrs = attrs;

        let plan =
            plan_payload_datoms(&p, "ent_1", 1000, &HashMap::new(), ACTOR_TEST).expect("planifica");
        assert!(
            !plan.fts_requests.is_empty(),
            "reminder.title es fts y genera requests"
        );
    }

    #[test]
    fn create_persiste_actor_en_created_by_y_updated_by() {
        registry();
        let plan = plan_payload_datoms(
            &payload(TransactOp::Create),
            "ent_1",
            1000,
            &HashMap::new(),
            "usr_42",
        )
        .expect("create planifica");

        assert!(
            assert_de(&plan.datoms, "meta/created_by", "usr_42"),
            "create asienta meta/created_by con el actor"
        );
        assert!(
            assert_de(&plan.datoms, "meta/updated_by", "usr_42"),
            "create asienta meta/updated_by con el actor"
        );
    }

    #[test]
    fn update_retracta_updated_by_previo_y_asienta_el_nuevo() {
        registry();
        let previos = activos(&[(
            "meta/updated_by",
            crate::eav::writer::system_attrs::UPDATED_BY_ID,
            DatomValue::Str("usr_viejo".to_string()),
        )]);
        let plan = plan_payload_datoms(
            &payload(TransactOp::Update),
            "ent_1",
            2000,
            &previos,
            "usr_nuevo",
        )
        .expect("update planifica");

        assert!(
            retract_de(&plan.datoms, "meta/updated_by", "usr_viejo"),
            "update retracta el updated_by previo"
        );
        assert!(
            assert_de(&plan.datoms, "meta/updated_by", "usr_nuevo"),
            "update asienta el actor nuevo"
        );
        assert!(
            !plan.datoms.iter().any(|d| d.attr_name == "meta/created_by"),
            "update no toca created_by"
        );
    }

    #[test]
    fn update_sin_updated_by_previo_no_retracta_nada() {
        registry();
        let previos = activos(&[("title", 99, DatomValue::Str("Recordatorio 3".to_string()))]);
        let plan = plan_payload_datoms(
            &payload(TransactOp::Update),
            "ent_1",
            2000,
            &previos,
            "usr_nuevo",
        )
        .expect("update planifica");

        assert!(assert_de(&plan.datoms, "meta/updated_by", "usr_nuevo"));
        assert!(
            plan.datoms
                .iter()
                .filter(|d| d.attr_name == "meta/updated_by")
                .all(|d| d.op),
            "sin valor previo no hay retract de updated_by"
        );
    }

    #[test]
    fn actor_vacio_no_emite_atribucion() {
        registry();
        let plan = plan_payload_datoms(
            &payload(TransactOp::Create),
            "ent_1",
            1000,
            &HashMap::new(),
            "",
        )
        .expect("create planifica");

        assert!(
            !plan
                .datoms
                .iter()
                .any(|d| d.attr_name == "meta/created_by" || d.attr_name == "meta/updated_by"),
            "sin actor no se emite atribución: mejor ausente que valor inventado"
        );
    }

    #[test]
    fn atributo_negocio_entity_type_no_colisiona_con_entity_slash_type() {
        registry();
        let mut attrs = HashMap::new();
        attrs.insert("entity_type".to_string(), DatomValue::Str("asset".into()));
        let p = TransactPayload {
            tenant_id: "t1".to_string(),
            entity_id: Some("fault_1".to_string()),
            entity_type: "domain_fault".to_string(),
            attrs,
            op: TransactOp::Create,
            suppress_events: false,
        };
        let plan = plan_payload_datoms(&p, "fault_1", 1000, &HashMap::new(), ACTOR_TEST)
            .expect("planifica");

        assert!(
            assert_de(&plan.datoms, "entity_type", "asset"),
            "el atributo de negocio entity_type conserva su valor 'asset'"
        );
        assert!(
            assert_de(&plan.datoms, "entity/type", "domain_fault"),
            "el atributo de sistema entity/type registra el tipo del modelo 'domain_fault'"
        );
        assert_eq!(
            plan.datoms
                .iter()
                .filter(|d| d.attr_name == "entity_type")
                .count(),
            1,
            "solo debe existir un datom para el atributo de negocio entity_type"
        );
    }

    #[test]
    fn document_chunk_autogenera_semantic_embedding_de_1024_floats_desde_chunk_content() {
        registry();
        let mut attrs = HashMap::new();
        attrs.insert(
            "id".to_string(),
            DatomValue::Uuid("01M5D9Q0KMWNJ9JRAFQS29G6DK#chunk-0".into()),
        );
        attrs.insert(
            "parent_file".to_string(),
            DatomValue::Str("01M5D9Q0KMWNJ9JRAFQS29G6DK".into()),
        );
        attrs.insert("chunk_index".to_string(), DatomValue::Long(0));
        attrs.insert(
            "chunk_content".to_string(),
            DatomValue::Str("Manual técnico de bomba centrífuga KSB-500 150 PSI".into()),
        );
        attrs.insert(
            "metadata_tags".to_string(),
            DatomValue::Str("manual-bomba.pdf".into()),
        );

        let p = TransactPayload {
            tenant_id: "t1".to_string(),
            entity_id: Some("01M5D9Q0KMWNJ9JRAFQS29G6DK#chunk-0".to_string()),
            entity_type: "document_chunk".to_string(),
            attrs,
            op: TransactOp::Create,
            suppress_events: true,
        };
        let plan = plan_payload_datoms(
            &p,
            "01M5D9Q0KMWNJ9JRAFQS29G6DK#chunk-0",
            1000,
            &HashMap::new(),
            ACTOR_TEST,
        )
        .expect("planifica document_chunk");

        let emb_datom = plan
            .datoms
            .iter()
            .find(|d| d.attr_name == "semantic_embedding" && d.op)
            .expect("debe emitir datom assert de semantic_embedding");
        match &emb_datom.value {
            DatomValue::Bytes(bytes) => {
                assert_eq!(
                    bytes.len(),
                    4096,
                    "1024 dimensiones * 4 bytes f32 LE = 4096 bytes"
                );
            }
            other => panic!("esperaba DatomValue::Bytes(4096), obtuvo: {other:?}"),
        }
    }
}
