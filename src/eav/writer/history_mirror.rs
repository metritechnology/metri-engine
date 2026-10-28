//! Espejo de relaciones en la partición del dueño para historial agregado (`_history`).
//!
//! Cuando un atributo de un modelo está marcado con `history_mirror: true` en el
//! Códice (hoy: `file.owner_entity_id`), el writer escribe EN LA MISMA transacción
//! un datom espejo en la partición del dueño:
//!
//! - atributo: `_refs/<entidad_hija>` (ej. `_refs/file`),
//! - entidad: el valor del atributo espejo (el `owner_entity_id`),
//! - valor: el id de la entidad hija.
//!
//! Semántica:
//! - `assert` cuando la relación nace (create) o cambia A ese dueño (update),
//! - `retract` cuando la relación se retracta (update a null), cambia DE ese
//!   dueño a otro (retract del viejo + assert del nuevo) o la hija se elimina.
//!
//! Como el espejo comparte `tx_id` con la mutación de la hija, el `_history`
//! del dueño muestra el rastro completo —altas, cambios y borrados— sin GSI
//! nuevo. Los atributos `_refs/…` no se declaran en el modelo del dueño: el
//! Códice registra sus `attr_id` al construirse (ver `registry::build`) y el
//! índice FTS los excluye (`mbf::storage::is_non_searchable_attr`).
//!
//! El espejo sólo registra eventos DESDE su despliegue; no hay backfill.

use std::collections::HashMap;

use crate::eav::types::datom::{Datom, DatomValue};

/// Prefijo reservado de los atributos espejo de historial (`_refs/<entidad_hija>`).
pub const REFS_PREFIX: &str = "_refs/";

/// Nombre del atributo espejo que la partición del dueño recibe para una
/// entidad hija: `_refs/file` para las hijas de tipo `file`.
pub fn mirror_attr_name(child_entity_type: &str) -> String {
    format!("{REFS_PREFIX}{child_entity_type}")
}

/// Nombre del atributo hermano que declara el TIPO del dueño, por convención
/// del par polimórfico: `owner_entity_id` → `owner_entity_type`.
fn sibling_type_attr(mirror_attr: &str) -> Option<String> {
    mirror_attr
        .strip_suffix("_id")
        .map(|base| format!("{base}_type"))
}

/// Resuelve un id de dueño desde un valor de datom: sólo strings no vacíos.
fn owner_id_of(value: Option<&DatomValue>) -> Option<&str> {
    match value {
        Some(DatomValue::Str(s)) if !s.is_empty() => Some(s.as_str()),
        _ => None,
    }
}

/// Planifica los datoms espejo de UNA entidad hija para la transacción en curso.
///
/// - `attrs`: atributos del payload de la hija (vacío en DELETE).
/// - `active_attrs`: estado activo previo de la hija (vacío en CREATE).
/// - `is_delete`: la hija se está eliminando — todo dueño previo se retracta.
///
/// Devuelve datoms en las particiones de los dueños con el MISMO `tx_id`; el
/// orquestador los añade a la misma `TransactWriteItems`.
pub fn plan_mirror_datoms(
    (tenant_id, child_entity_type, child_entity_id): (&str, &str, &str),
    attrs: &HashMap<String, DatomValue>,
    active_attrs: &HashMap<String, (u16, DatomValue)>,
    tx_id: u64,
    is_delete: bool,
) -> Vec<Datom> {
    let mut datoms = Vec::new();

    let Some(model_attrs) = crate::codice::global().get_attributes(child_entity_type) else {
        return datoms;
    };

    for attr_desc in model_attrs.iter().filter(|a| a.history_mirror) {
        let mirror_name = mirror_attr_name(child_entity_type);
        let mirror_id = Datom::hash_attr_name(&mirror_name);

        let old_owner = owner_id_of(active_attrs.get(&attr_desc.name).map(|(_, v)| v));
        let new_owner = if is_delete {
            None
        } else {
            match attrs.get(&attr_desc.name) {
                // El atributo no viaja en el payload: la relación no cambia.
                None => continue,
                // Null explícito: la relación se desata (sólo retract del viejo).
                Some(DatomValue::Null) => None,
                Some(v) => owner_id_of(Some(v)),
            }
        };

        if old_owner == new_owner {
            continue;
        }

        if let Some(old) = old_owner {
            datoms.push(Datom::retract(
                tenant_id,
                old,
                mirror_name.clone(),
                mirror_id,
                DatomValue::Str(child_entity_id.to_string()),
                tx_id,
            ));
        }

        if let Some(new) = new_owner {
            // El assert exige el par polimórfico completo: el tipo del dueño
            // (atributo hermano `<base>_type`) debe resolverse del payload o
            // del estado activo de la hija. Sin tipo, un id colgado no
            // identifica ninguna partición con sentido y el espejo sería ruido.
            let tipo_resuelto = sibling_type_attr(&attr_desc.name).is_some_and(|sibling| {
                owner_id_of(attrs.get(&sibling)).is_some()
                    || owner_id_of(active_attrs.get(&sibling).map(|(_, v)| v)).is_some()
            });
            if !tipo_resuelto {
                tracing::debug!(
                    "[history_mirror] '{child_entity_type}#{}' sin tipo de dueño resoluble; \
                     se omite el assert espejo hacia '{new}'",
                    attr_desc.name
                );
                continue;
            }
            datoms.push(Datom::assert(
                tenant_id,
                new,
                mirror_name,
                mirror_id,
                DatomValue::Str(child_entity_id.to_string()),
                tx_id,
            ));
        }
    }

    datoms
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eav::writer::transact::TransactOp;

    const ACTOR_TEST: &str = "usr_test";

    fn registry() {
        crate::eav::writer::test_support::init_codice();
    }

    fn attrs_file(pares: &[(&str, DatomValue)]) -> HashMap<String, DatomValue> {
        pares
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn activos(pares: &[(&str, DatomValue)]) -> HashMap<String, (u16, DatomValue)> {
        pares
            .iter()
            .map(|(k, v)| (k.to_string(), (Datom::hash_attr_name(k), v.clone())))
            .collect()
    }

    fn espejo<'a>(datoms: &'a [Datom], owner: &str, op: bool) -> Option<&'a Datom> {
        datoms
            .iter()
            .find(|d| d.entity_id == owner && d.attr_name == "_refs/file" && d.op == op)
    }

    #[test]
    fn create_con_dueno_aserta_espejo_en_la_particion_del_dueno() {
        registry();
        let attrs = attrs_file(&[
            ("owner_entity_id", DatomValue::Str("asset_1".into())),
            ("owner_entity_type", DatomValue::Str("asset".into())),
        ]);
        let datoms = plan_mirror_datoms(
            ("t1", "file", "file_9"),
            &attrs,
            &HashMap::new(),
            777,
            false,
        );

        let d = espejo(&datoms, "asset_1", true).expect("assert espejo en asset_1");
        assert_eq!(d.value, DatomValue::Str("file_9".into()));
        assert_eq!(d.tenant_id, "t1");
        assert_eq!(d.tx_id, 777, "el espejo comparte el tx de la mutación");
        assert_eq!(datoms.len(), 1, "un solo datom espejo");
    }

    #[test]
    fn delete_retracta_el_espejo_del_dueno_previo() {
        registry();
        let previos = activos(&[
            ("owner_entity_id", DatomValue::Str("asset_1".into())),
            ("owner_entity_type", DatomValue::Str("asset".into())),
        ]);
        let datoms = plan_mirror_datoms(
            ("t1", "file", "file_9"),
            &HashMap::new(),
            &previos,
            888,
            true,
        );

        let d = espejo(&datoms, "asset_1", false).expect("retract espejo en asset_1");
        assert_eq!(d.value, DatomValue::Str("file_9".into()));
        assert_eq!(datoms.len(), 1);
        assert!(
            espejo(&datoms, "asset_1", true).is_none(),
            "delete no aserta"
        );
    }

    #[test]
    fn cambio_de_dueno_retracta_el_viejo_y_aserta_el_nuevo() {
        registry();
        let attrs = attrs_file(&[("owner_entity_id", DatomValue::Str("asset_2".into()))]);
        let previos = activos(&[
            ("owner_entity_id", DatomValue::Str("asset_1".into())),
            ("owner_entity_type", DatomValue::Str("asset".into())),
        ]);
        let datoms = plan_mirror_datoms(("t1", "file", "file_9"), &attrs, &previos, 999, false);

        assert!(
            espejo(&datoms, "asset_1", false).is_some(),
            "retract en el viejo"
        );
        assert!(
            espejo(&datoms, "asset_2", true).is_some(),
            "assert en el nuevo"
        );
        assert_eq!(datoms.len(), 2);
    }

    #[test]
    fn update_sin_cambio_de_dueno_no_genera_espejo() {
        registry();
        // owner_entity_id no viaja en el payload: la relación no cambia.
        let attrs = attrs_file(&[("file_name", DatomValue::Str("nuevo.pdf".into()))]);
        let previos = activos(&[("owner_entity_id", DatomValue::Str("asset_1".into()))]);
        let datoms = plan_mirror_datoms(("t1", "file", "file_9"), &attrs, &previos, 999, false);
        assert!(datoms.is_empty());
    }

    #[test]
    fn update_con_mismo_dueno_es_idempotente() {
        registry();
        let attrs = attrs_file(&[("owner_entity_id", DatomValue::Str("asset_1".into()))]);
        let previos = activos(&[("owner_entity_id", DatomValue::Str("asset_1".into()))]);
        let datoms = plan_mirror_datoms(("t1", "file", "file_9"), &attrs, &previos, 999, false);
        assert!(datoms.is_empty(), "mismo dueño: ni retract ni assert");
    }

    #[test]
    fn null_desata_la_relacion_y_solo_retracta() {
        registry();
        let attrs = attrs_file(&[("owner_entity_id", DatomValue::Null)]);
        let previos = activos(&[("owner_entity_id", DatomValue::Str("asset_1".into()))]);
        let datoms = plan_mirror_datoms(("t1", "file", "file_9"), &attrs, &previos, 999, false);

        assert!(espejo(&datoms, "asset_1", false).is_some());
        assert_eq!(datoms.len(), 1, "sin assert: la relación quedó desatada");
    }

    #[test]
    fn assert_espejo_exige_el_tipo_de_dueno_resoluble() {
        registry();
        // owner_entity_id sin owner_entity_type ni en payload ni en estado: no hay espejo.
        let attrs = attrs_file(&[("owner_entity_id", DatomValue::Str("asset_1".into()))]);
        let datoms = plan_mirror_datoms(
            ("t1", "file", "file_9"),
            &attrs,
            &HashMap::new(),
            777,
            false,
        );
        assert!(datoms.is_empty(), "id colgado sin tipo: espejo omitido");

        // ...pero el tipo puede venir del estado activo en un update de re-punteo.
        let previos = activos(&[("owner_entity_type", DatomValue::Str("asset".into()))]);
        let datoms = plan_mirror_datoms(("t1", "file", "file_9"), &attrs, &previos, 777, false);
        assert!(espejo(&datoms, "asset_1", true).is_some());
    }

    #[test]
    fn atributo_sin_history_mirror_no_genera_espejo() {
        registry();
        // `reminder` no declara history_mirror en ningún atributo.
        let attrs = attrs_file(&[("title", DatomValue::Str("Recordatorio".into()))]);
        let datoms = plan_mirror_datoms(
            ("t1", "reminder", "rem_1"),
            &attrs,
            &HashMap::new(),
            777,
            false,
        );
        assert!(datoms.is_empty());
    }

    #[test]
    fn el_codice_registra_el_nombre_del_espejo_para_los_caminos_de_lectura() {
        registry();
        let id = Datom::hash_attr_name("_refs/file");
        assert_eq!(
            crate::codice::global().get_attr_name(id),
            Some("_refs/file"),
            "history/pull resuelven _refs/file en lugar de unknown_attr"
        );
        // Un modelo sin history_mirror no registra espejo.
        let id_reminder = Datom::hash_attr_name("_refs/reminder");
        assert_eq!(crate::codice::global().get_attr_name(id_reminder), None);
    }

    #[test]
    fn plan_completo_incluye_el_espejo_en_create_update_y_delete() {
        registry();
        // (a) CREATE
        let payload = crate::eav::writer::transact::TransactPayload {
            tenant_id: "t1".into(),
            entity_id: Some("file_9".into()),
            entity_type: "file".into(),
            attrs: attrs_file(&[
                ("owner_entity_id", DatomValue::Str("asset_1".into())),
                ("owner_entity_type", DatomValue::Str("asset".into())),
            ]),
            op: TransactOp::Create,
            suppress_events: false,
        };
        let plan = crate::eav::writer::datom_plan::plan_payload_datoms(
            &payload,
            "file_9",
            1000,
            &HashMap::new(),
            ACTOR_TEST,
        )
        .expect("create planifica");
        assert!(espejo(&plan.datoms, "asset_1", true).is_some());
        assert!(
            plan.datoms.iter().all(|d| d.tx_id == 1000),
            "espejo y datoms de la hija comparten tx_id"
        );

        // (c) UPDATE con cambio de dueño
        let previos = activos(&[
            ("owner_entity_id", DatomValue::Str("asset_1".into())),
            ("owner_entity_type", DatomValue::Str("asset".into())),
        ]);
        let payload = crate::eav::writer::transact::TransactPayload {
            tenant_id: "t1".into(),
            entity_id: Some("file_9".into()),
            entity_type: "file".into(),
            attrs: attrs_file(&[("owner_entity_id", DatomValue::Str("asset_2".into()))]),
            op: TransactOp::Update,
            suppress_events: false,
        };
        let plan = crate::eav::writer::datom_plan::plan_payload_datoms(
            &payload, "file_9", 2000, &previos, ACTOR_TEST,
        )
        .expect("update planifica");
        assert!(espejo(&plan.datoms, "asset_1", false).is_some());
        assert!(espejo(&plan.datoms, "asset_2", true).is_some());

        // (b) DELETE
        let payload = crate::eav::writer::transact::TransactPayload {
            tenant_id: "t1".into(),
            entity_id: Some("file_9".into()),
            entity_type: "file".into(),
            attrs: HashMap::new(),
            op: TransactOp::Delete,
            suppress_events: false,
        };
        let plan = crate::eav::writer::datom_plan::plan_payload_datoms(
            &payload, "file_9", 3000, &previos, ACTOR_TEST,
        )
        .expect("delete planifica");
        assert!(espejo(&plan.datoms, "asset_1", false).is_some());
        assert!(
            plan.datoms.iter().all(|d| !d.op),
            "delete sigue siendo puro retract, espejo incluido"
        );
    }

    #[test]
    fn update_del_dueno_con_refs_activo_no_rompe_el_plan() {
        registry();
        // La partición del dueño acumula datoms `_refs/file` que su modelo NO
        // declara: un update normal del dueño convive con ellos (se resuelven
        // por attr_id registrado, no se validan contra el modelo ni se retractan).
        let mut previos = activos(&[("name", DatomValue::Str("Compresor".into()))]);
        previos.insert(
            "_refs/file".to_string(),
            (
                Datom::hash_attr_name("_refs/file"),
                DatomValue::Str("file_9".into()),
            ),
        );
        let payload = crate::eav::writer::transact::TransactPayload {
            tenant_id: "t1".into(),
            entity_id: Some("asset_1".into()),
            entity_type: "asset".into(),
            attrs: attrs_file(&[("name", DatomValue::Str("Compresor B".into()))]),
            op: TransactOp::Update,
            suppress_events: false,
        };
        let plan = crate::eav::writer::datom_plan::plan_payload_datoms(
            &payload, "asset_1", 2000, &previos, ACTOR_TEST,
        )
        .expect("el dueño con _refs/ activo planifica sin error");
        assert!(
            !plan.datoms.iter().any(|d| d.attr_name == "_refs/file"),
            "el update del dueño no toca sus espejos"
        );
    }
}
