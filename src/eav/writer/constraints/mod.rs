//! Restricciones de integridad e invariantes de estado evaluadas dentro de la transacción ACID sobre `.mbf`.
//!
//! ## Propósito del módulo
//!
//! Garantiza que las restricciones de unicidad (`unique`) se apliquen como invariantes
//! atómicos mediante un **ítem de reclamación** escrito en la misma operación
//! `TransactWriteItems` con la condición `attribute_not_exists`, evitando condiciones
//! de carrera tipo *check-then-act* entre escritores concurrentes.
//!
//! Asimismo, evalúa las restricciones declarativas de estado (`requires_when`,
//! `at_most`, `at_least`, `requires_any`) y de estado de referencia (`ref_state`)
//! definidas en los modelos del Códice.

mod unique_claim;

pub use unique_claim::UniqueClaimPlanner;

use crate::codice::registry::EntityModel;
use crate::domain::errors::DomainError;
use crate::eav::types::datom::DatomValue;
use crate::eav::writer::transact::TransactOp;
use crate::mbf::storage::TransactWriteItem;
use std::collections::HashMap;

/// Contexto inmutable con los datos de una entidad que se está escribiendo para planificar sus restricciones.
pub struct ConstraintContext<'a> {
    /// Identificador del tenant propietario de la transacción.
    pub tenant_id: &'a str,
    /// Identificador único (ULID) de la entidad evaluada.
    pub entity_id: &'a str,
    /// Descriptor del modelo de la entidad en el Códice.
    pub model: &'a EntityModel,
    /// Mapa de atributos y valores incluidos en la escritura.
    pub attrs: &'a HashMap<String, DatomValue>,
    /// Tipo de operación transaccional (`Create`, `Update`, `Delete`).
    pub op: TransactOp,
    /// Nombre lógico de la tabla o partición `.mbf`.
    pub table: &'a str,
}

/// Traduce una restricción declarada en el modelo en ítems condicionales dentro de la misma transacción.
///
/// Opera de forma pura (sin lecturas previas) para evitar ventanas de carrera.
pub trait ConstraintPlanner: Send + Sync {
    /// Nombre identificador del planificador para trazas y mensajes de error.
    fn name(&self) -> &'static str;

    /// Indica si este planificador aplica sobre las restricciones declaradas en `model`.
    fn applies_to(&self, model: &EntityModel) -> bool;

    /// Construye los `TransactWriteItem` condicionales que garantizan la restricción durante el commit.
    fn plan(&self, ctx: &ConstraintContext) -> Result<Vec<TransactWriteItem>, DomainError>;
}

/// Construye la lista predeterminada de planificadores de restricciones activos del motor.
pub fn default_planners() -> Vec<Box<dyn ConstraintPlanner>> {
    vec![Box::new(UniqueClaimPlanner::new())]
}

/// Entidad participante dentro de una transacción (ya sea la entidad madre o una proyección de saga).
pub struct EntidadEnTx<'a> {
    /// Nombre del tipo de entidad en el Códice.
    pub entity_type: &'a str,
    /// Identificador único (ULID) de la entidad.
    pub entity_id: &'a str,
    /// Atributos que se están escribiendo para esta entidad.
    pub attrs: &'a HashMap<String, DatomValue>,
    /// Operación transaccional aplicada a la entidad.
    pub op: TransactOp,
    /// Modelo del Códice correspondiente a la entidad.
    pub model: &'a EntityModel,
}

/// Planifica los ítems de reclamación (`unique`) de todas las entidades participantes en la
/// transacción, detectando conflictos intra-transacción antes del commit.
///
/// Si dos entidades distintas reclaman la misma clave única dentro de la misma transacción,
/// retorna `ErrorCode::Cod001`. Si la misma entidad reclama dos veces la misma clave, se deduplica.
pub fn plan_claims_para(
    planners: &[Box<dyn ConstraintPlanner>],
    tenant_id: &str,
    table: &str,
    entidades: &[EntidadEnTx<'_>],
) -> Result<Vec<TransactWriteItem>, DomainError> {
    let mut items: Vec<TransactWriteItem> = Vec::new();
    let mut reclamado_por: HashMap<String, &str> = HashMap::new();

    for entidad in entidades {
        let ctx = ConstraintContext {
            tenant_id,
            entity_id: entidad.entity_id,
            model: entidad.model,
            attrs: entidad.attrs,
            op: entidad.op.clone(),
            table,
        };
        for planner in planners {
            if !planner.applies_to(entidad.model) {
                continue;
            }
            for item in planner.plan(&ctx)? {
                match item_pk(&item) {
                    Some(pk) => match reclamado_por.get(pk) {
                        Some(previo) if *previo != entidad.entity_id => {
                            return Err(DomainError::codice(
                                crate::domain::errors::ErrorCode::Cod001,
                                format!(
                                    "conflicto de restricción unique dentro del composite: \
                                     '{}' y '{}' reclaman la misma clave ({pk})",
                                    previo, entidad.entity_id
                                ),
                            ));
                        }
                        Some(_) => {} // la misma entidad: dedupe idempotente
                        None => {
                            reclamado_por.insert(pk.to_string(), entidad.entity_id);
                            items.push(item);
                        }
                    },
                    None => items.push(item),
                }
            }
        }
    }
    Ok(items)
}

/// PK del item de un `TransactWriteItem` de tipo Put (para dedupe/conflicto).
fn item_pk(item: &TransactWriteItem) -> Option<&str> {
    item.put()
        .and_then(|put| put.item().get("PK"))
        .and_then(|av| av.as_s().ok())
        .map(|s| s.as_str())
}

/// Planifica los items de todos los planificadores que apliquen.
pub fn plan_all(
    planners: &[Box<dyn ConstraintPlanner>],
    ctx: &ConstraintContext,
) -> Result<Vec<TransactWriteItem>, DomainError> {
    let mut items = Vec::new();
    for planner in planners {
        if planner.applies_to(ctx.model) {
            items.extend(planner.plan(ctx)?);
        }
    }
    Ok(items)
}

/// Evalúa las restricciones de ESTADO declaradas en el modelo
/// (`requires_when`, `at_most`) sobre la vista fusionada.
///
/// La vista fusionada es el estado previo (`active`) sobrescrito por el
/// payload (`written`): en Create `active` viene vacío y el payload debe
/// contener todo; en Update el camino de escritura ya leyó el estado previo
/// para el retract+assert, así que esta comprobación no añade lecturas.
///
/// Nota honesta sobre la regla del módulo («lo que lee no es invariante»):
/// aquí no se lee nada *nuevo* — se reusa la lectura que el update ya hace —
/// pero sí hay una ventana mínima entre esa lectura y la transacción. Para
/// estas reglas de negocio la carrera es tolerada por diseño: deciden sobre
/// una foto coherente del estado, no sobre reclamos físicos.
pub fn check_state_constraints(
    model: &EntityModel,
    op: TransactOp,
    written: &HashMap<String, DatomValue>,
    active: &HashMap<String, DatomValue>,
) -> Result<(), DomainError> {
    if op == TransactOp::Delete {
        return Ok(());
    }

    // Vista fusionada: lo escrito gana sobre lo previo.
    let mut merged = active.clone();
    for (k, v) in written {
        merged.insert(k.clone(), v.clone());
    }

    for c in &model.constraints {
        match c.kind {
            crate::codice::registry::ConstraintKind::RequiresWhen => {
                let cuando = c.when.as_deref().unwrap_or(&[]);
                if cuando
                    .iter()
                    .all(|(k, v)| como_texto(merged.get(k)).as_deref() == Some(v.as_str()))
                {
                    for requerido in &c.attributes {
                        if presente(&merged, requerido) {
                            return Err(DomainError::codice(
                                crate::domain::errors::ErrorCode::Cod001,
                                format!(
                                    "restricción '{}': con {} el campo '{}' es obligatorio",
                                    model.entity,
                                    cuando
                                        .iter()
                                        .map(|(k, v)| format!("{k}={v}"))
                                        .collect::<Vec<_>>()
                                        .join(" y "),
                                    requerido
                                ),
                            ));
                        }
                    }
                }
            }
            crate::codice::registry::ConstraintKind::AtMost => {
                if let [campo, techo] = c.attributes.as_slice() {
                    if let (Some(valor), Some(maximo)) =
                        (como_numero(&merged, campo), como_numero(&merged, techo))
                    {
                        if valor > maximo {
                            return Err(DomainError::codice(
                                crate::domain::errors::ErrorCode::Cod001,
                                format!(
                                    "restricción '{}': {campo} ({valor}) no puede exceder {techo} ({maximo})",
                                    model.entity
                                ),
                            ));
                        }
                    }
                }
            }
            crate::codice::registry::ConstraintKind::AtLeast => {
                if let [campo, piso] = c.attributes.as_slice() {
                    if let (Some(valor), Some(minimo)) =
                        (como_numero(&merged, campo), como_numero(&merged, piso))
                    {
                        if valor < minimo {
                            return Err(DomainError::codice(
                                crate::domain::errors::ErrorCode::Cod001,
                                format!(
                                    "restricción '{}': {campo} ({valor}) no puede ser anterior/menor a {piso} ({minimo})",
                                    model.entity
                                ),
                            ));
                        }
                    }
                }
            }
            crate::codice::registry::ConstraintKind::RequiresAny => {
                if !c.attributes.iter().any(|campo| !presente(&merged, campo)) {
                    return Err(DomainError::codice(
                        crate::domain::errors::ErrorCode::Cod001,
                        format!(
                            "restricción '{}': exige al menos uno de [{}]",
                            model.entity,
                            c.attributes.join(", ")
                        ),
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Evalúa los pares `conditions` de una restricción `ref_state` contra los
/// atributos activos de la entidad apuntada.
///
/// Es una comprobación con lectura, tolerada por diseño: entre la lectura del
/// referenciado y la transacción cabe que cambie de estado (p. ej. alguien
/// retira el procedure justo después de la comprobación). La ventana es de
/// negocio, no de integridad — no corrompe datos, solo permite instanciar un
/// procedure que acaba de pasar a RETIRED.
pub fn check_ref_conditions(
    entity: &str,
    ref_id: &str,
    ref_attrs: &HashMap<String, DatomValue>,
    when: &[(String, String)],
) -> Result<(), DomainError> {
    for (k, v) in when {
        if como_texto(ref_attrs.get(k)).as_deref() != Some(v.as_str()) {
            return Err(DomainError::codice(
                crate::domain::errors::ErrorCode::Cod001,
                format!(
                    "restricción '{entity}': la entidad referenciada '{ref_id}' no cumple \
                     la condición {k}={v} (¿existe? {})",
                    if ref_attrs.is_empty() { "no" } else { "sí" }
                ),
            ));
        }
    }
    Ok(())
}

fn como_texto(v: Option<&DatomValue>) -> Option<String> {
    match v? {
        DatomValue::Str(s) => Some(s.clone()),
        DatomValue::Ref(r) => Some(r.to_string()),
        _ => None,
    }
}

/// ¿Falta el campo en la vista fusionada (ausente o nulo)?
fn presente(merged: &HashMap<String, DatomValue>, campo: &str) -> bool {
    matches!(merged.get(campo), None | Some(DatomValue::Null))
}

fn como_numero(merged: &HashMap<String, DatomValue>, campo: &str) -> Option<f64> {
    match merged.get(campo)? {
        DatomValue::Long(i) => Some(*i as f64),
        DatomValue::Double(f) => Some(*f),
        // Epoch: los instantes son comparables numéricamente (end ≥ start).
        DatomValue::Instant(i) => Some(*i as f64),
        _ => None,
    }
}

#[cfg(test)]
mod state_tests {
    use super::*;
    use crate::codice::registry::{Constraint, ConstraintKind, ConstraintScope, EngineChannel};

    fn modelo(constraints: Vec<Constraint>) -> EntityModel {
        EntityModel {
            entity: "composite_link".to_string(),
            label: None,
            icon: None,
            primary_key: None,
            fts_fields: vec![],
            engine: EngineChannel::Oltp,
            attributes: vec![],
            event_rules: vec![],
            is_sequence_scope_provider: false,
            write_path_locked: false,
            is_system: false,
            disable_eda: false,
            shadow_sagas_mapping: None,
            constraints,
            ..Default::default()
        }
    }

    fn requires_when(cuando: (&str, &str), requeridos: &[&str]) -> Constraint {
        Constraint {
            kind: ConstraintKind::RequiresWhen,
            scope: ConstraintScope::Tenant,
            attributes: requeridos.iter().map(|s| s.to_string()).collect(),
            when: Some(vec![(cuando.0.to_string(), cuando.1.to_string())]),
        }
    }

    fn at_most(campo: &str, techo: &str) -> Constraint {
        Constraint {
            kind: ConstraintKind::AtMost,
            scope: ConstraintScope::Tenant,
            attributes: vec![campo.to_string(), techo.to_string()],
            when: None,
        }
    }

    fn pares(vals: &[(&str, DatomValue)]) -> HashMap<String, DatomValue> {
        vals.iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn completed_sin_actor_en_create_falla() {
        let m = modelo(vec![requires_when(
            ("status", "COMPLETED"),
            &["completed_by", "completed_at"],
        )]);
        let escrito = pares(&[("status", DatomValue::Str("COMPLETED".into()))]);
        let err = check_state_constraints(&m, TransactOp::Create, &escrito, &HashMap::new())
            .expect_err("COMPLETED sin actor debe fallar");
        assert!(err.detail.contains("completed_by"), "{err:?}");
    }

    #[test]
    fn completed_con_actor_en_create_pasa() {
        let m = modelo(vec![requires_when(
            ("status", "COMPLETED"),
            &["completed_by", "completed_at"],
        )]);
        let escrito = pares(&[
            ("status", DatomValue::Str("COMPLETED".into())),
            ("completed_by", DatomValue::Str("01CARLOS".into())),
            ("completed_at", DatomValue::Instant(1_788_000_000_000)),
        ]);
        check_state_constraints(&m, TransactOp::Create, &escrito, &HashMap::new())
            .expect("COMPLETED con actor debe pasar");
    }

    #[test]
    fn update_parcial_usa_el_estado_previo() {
        let m = modelo(vec![requires_when(
            ("status", "COMPLETED"),
            &["completed_by", "completed_at"],
        )]);
        // El payload solo marca status; completed_by/at ya estaban en la entidad.
        let escrito = pares(&[("status", DatomValue::Str("COMPLETED".into()))]);
        let previo = pares(&[
            ("completed_by", DatomValue::Str("01CARLOS".into())),
            ("completed_at", DatomValue::Instant(1)),
        ]);
        check_state_constraints(&m, TransactOp::Update, &escrito, &previo)
            .expect("con completed_* previos el update parcial debe pasar");

        // Y si el update RETRACTA completed_by, la vista fusionada lo delata.
        let escrito = pares(&[
            ("status", DatomValue::Str("COMPLETED".into())),
            ("completed_by", DatomValue::Null),
        ]);
        assert!(check_state_constraints(&m, TransactOp::Update, &escrito, &previo).is_err());
    }

    #[test]
    fn status_distinto_no_dispara_la_exigencia() {
        let m = modelo(vec![requires_when(
            ("status", "COMPLETED"),
            &["completed_by"],
        )]);
        let escrito = pares(&[("status", DatomValue::Str("IN_PROGRESS".into()))]);
        check_state_constraints(&m, TransactOp::Create, &escrito, &HashMap::new())
            .expect("IN_PROGRESS no exige actor");
    }

    #[test]
    fn score_sobre_max_score_falla_con_ambos_tipos_numericos() {
        let m = modelo(vec![at_most("score", "max_score")]);

        let err = check_state_constraints(
            &m,
            TransactOp::Create,
            &pares(&[
                ("score", DatomValue::Long(150)),
                ("max_score", DatomValue::Long(100)),
            ]),
            &HashMap::new(),
        )
        .expect_err("150 > 100 debe fallar");
        assert!(err.detail.contains("no puede exceder"), "{err:?}");

        // Decimal contra entero: mismo chequeo, tipos distintos.
        let escrito = pares(&[
            ("score", DatomValue::Double(100.5)),
            ("max_score", DatomValue::Long(100)),
        ]);
        assert!(
            check_state_constraints(&m, TransactOp::Create, &escrito, &HashMap::new()).is_err()
        );

        // Dentro del techo pasa.
        let escrito = pares(&[
            ("score", DatomValue::Long(100)),
            ("max_score", DatomValue::Long(100)),
        ]);
        check_state_constraints(&m, TransactOp::Create, &escrito, &HashMap::new())
            .expect("igualar el máximo es válido");
    }

    #[test]
    fn at_most_sin_techo_en_vista_no_chequea() {
        let m = modelo(vec![at_most("score", "max_score")]);
        let escrito = pares(&[("score", DatomValue::Long(500))]);
        check_state_constraints(&m, TransactOp::Create, &escrito, &HashMap::new())
            .expect("sin max_score en la vista no hay contra qué comparar");
    }

    #[test]
    fn delete_nunca_dispara_restricciones_de_estado() {
        let m = modelo(vec![requires_when(
            ("status", "COMPLETED"),
            &["completed_by"],
        )]);
        let escrito = pares(&[("status", DatomValue::Str("COMPLETED".into()))]);
        check_state_constraints(&m, TransactOp::Delete, &escrito, &HashMap::new())
            .expect("el borrado no evalúa estado");
    }

    #[test]
    fn ref_state_evalua_las_condiciones_del_referenciado() {
        let publicado = pares(&[
            ("entity/type", DatomValue::Str("template_record".into())),
            ("lifecycle_state", DatomValue::Str("PUBLISHED".into())),
        ]);
        check_ref_conditions(
            "template_record",
            "01TMPL",
            &publicado,
            &[("lifecycle_state".to_string(), "PUBLISHED".to_string())],
        )
        .expect("PUBLISHED cumple");

        let borrador = pares(&[("lifecycle_state", DatomValue::Str("DRAFT".into()))]);
        assert!(check_ref_conditions(
            "template_record",
            "01TMPL",
            &borrador,
            &[("lifecycle_state".to_string(), "PUBLISHED".to_string())],
        )
        .is_err());

        // Entidad inexistente: vista vacía, no cumple nada.
        assert!(check_ref_conditions(
            "template_record",
            "01FANTASMA",
            &HashMap::new(),
            &[("lifecycle_state".to_string(), "PUBLISHED".to_string())],
        )
        .is_err());
    }
}

#[cfg(test)]
mod claims_tests {
    use super::*;
    use crate::codice::registry::{Constraint, ConstraintKind, ConstraintScope, EngineChannel};
    use crate::eav::writer::constraints::UniqueClaimPlanner;

    fn modelo_unico() -> EntityModel {
        EntityModel {
            entity: "composite_link".to_string(),
            label: None,
            icon: None,
            primary_key: None,
            fts_fields: vec![],
            engine: EngineChannel::Oltp,
            attributes: vec![],
            event_rules: vec![],
            is_sequence_scope_provider: false,
            write_path_locked: false,
            is_system: false,
            disable_eda: false,
            shadow_sagas_mapping: None,
            constraints: vec![Constraint {
                kind: ConstraintKind::Unique,
                scope: ConstraintScope::Tenant,
                attributes: vec!["parent_id".into(), "child_id".into()],
                when: None,
            }],
            ..Default::default()
        }
    }

    fn entidad<'a>(
        eid: &'a str,
        parent: &'a str,
        child: &'a str,
        m: &'a EntityModel,
    ) -> EntidadEnTx<'a> {
        let attrs = HashMap::from([
            ("parent_id".to_string(), DatomValue::Str(parent.into())),
            ("child_id".to_string(), DatomValue::Str(child.into())),
        ]);
        EntidadEnTx {
            entity_type: "composite_link",
            entity_id: eid,
            attrs: Box::leak(Box::new(attrs)),
            op: TransactOp::Create,
            model: m,
        }
    }

    fn planners() -> Vec<Box<dyn ConstraintPlanner>> {
        vec![Box::new(UniqueClaimPlanner::new())]
    }

    #[test]
    fn composite_conflicto_intra_tx_sale_como_error_de_dominio() {
        let m = modelo_unico();
        let a = entidad("01A", "01P", "01C", &m);
        let b = entidad("01B", "01P", "01C", &m);
        let err = plan_claims_para(&planners(), "tnt_1", "tbl", &[a, b])
            .expect_err("misma clave, entidades distintas: conflicto");
        assert!(err.detail.contains("conflicto"), "{err:?}");
    }

    #[test]
    fn composite_deduplica_el_reintento_de_la_misma_entidad() {
        let m = modelo_unico();
        let a = entidad("01A", "01P", "01C", &m);
        let a2 = entidad("01A", "01P", "01C", &m);
        let items = plan_claims_para(&planners(), "tnt_1", "tbl", &[a, a2])
            .expect("misma entidad: reintento idempotente");
        assert_eq!(items.len(), 1, "una sola reclamación");
    }

    #[test]
    fn composite_planifica_un_claim_por_entidad_distinta() {
        let m = modelo_unico();
        let a = entidad("01A", "01P", "01C", &m);
        let b = entidad("01B", "01P", "01C2", &m);
        let items = plan_claims_para(&planners(), "tnt_1", "tbl", &[a, b])
            .expect("claves distintas: ambas reclaman");
        assert_eq!(items.len(), 2);
    }
}

#[cfg(test)]
mod at_least_any_tests {
    use super::*;
    use crate::codice::registry::{Constraint, ConstraintKind, ConstraintScope, EngineChannel};

    fn modelo(constraints: Vec<Constraint>) -> EntityModel {
        EntityModel {
            entity: "schedule_window".to_string(),
            label: None,
            icon: None,
            primary_key: None,
            fts_fields: vec![],
            engine: EngineChannel::Oltp,
            attributes: vec![],
            event_rules: vec![],
            is_sequence_scope_provider: false,
            write_path_locked: false,
            is_system: false,
            disable_eda: false,
            shadow_sagas_mapping: None,
            constraints,
            ..Default::default()
        }
    }

    fn at_least(campo: &str, piso: &str) -> Constraint {
        Constraint {
            kind: ConstraintKind::AtLeast,
            scope: ConstraintScope::Tenant,
            attributes: vec![campo.to_string(), piso.to_string()],
            when: None,
        }
    }

    fn requires_any(campos: &[&str]) -> Constraint {
        Constraint {
            kind: ConstraintKind::RequiresAny,
            scope: ConstraintScope::Tenant,
            attributes: campos.iter().map(|s| s.to_string()).collect(),
            when: None,
        }
    }

    fn pares(vals: &[(&str, DatomValue)]) -> HashMap<String, DatomValue> {
        vals.iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn end_time_anterior_a_start_time_falla() {
        let m = modelo(vec![at_least("end_time", "start_time")]);
        // Epoch en segundos de un mismo día: end 10:00 < start 14:00.
        let escrito = pares(&[
            ("start_time", DatomValue::Instant(50_400)),
            ("end_time", DatomValue::Instant(36_000)),
        ]);
        let err = check_state_constraints(&m, TransactOp::Create, &escrito, &HashMap::new())
            .expect_err("end < start debe fallar");
        assert!(err.detail.contains("no puede ser anterior"), "{err:?}");
    }

    #[test]
    fn turnos_que_cruzan_medianoche_pasan() {
        let m = modelo(vec![at_least("end_time", "start_time")]);
        // Guardia 24x48: start 22:00 del día 1, end 22:00 del día 2.
        let escrito = pares(&[
            ("start_time", DatomValue::Instant(79_200)),
            ("end_time", DatomValue::Instant(165_600)),
        ]);
        check_state_constraints(&m, TransactOp::Create, &escrito, &HashMap::new())
            .expect("el turno que cruza medianoche es end > start");
    }

    #[test]
    fn requires_any_exige_al_menos_un_dueno() {
        let m = modelo(vec![requires_any(&["user_id", "user_group_id"])]);

        // Patrón huérfano: ni técnico ni cuadrilla.
        let err = check_state_constraints(
            &m,
            TransactOp::Create,
            &pares(&[("name", DatomValue::Str("fantasma".into()))]),
            &HashMap::new(),
        )
        .expect_err("patrón sin dueño debe fallar");
        assert!(err.detail.contains("al menos uno"), "{err:?}");

        // Técnico: pasa. Cuadrilla: pasa.
        for dueño in [
            ("user_id", DatomValue::Str("01CARLOS".into())),
            ("user_group_id", DatomValue::Str("01CUADRILLA".into())),
        ] {
            check_state_constraints(&m, TransactOp::Create, &pares(&[dueño]), &HashMap::new())
                .expect("con un dueño basta");
        }
    }
}
