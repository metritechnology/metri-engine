//! Tests for `codice::registry`.
use super::*;
use std::fs;

fn create_temp_models_dir() -> (std::path::PathBuf, impl FnOnce()) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let count = COUNTER.fetch_add(1, Ordering::SeqCst);
    let path = std::env::temp_dir().join(format!(
        "metri_test_models_{}_{}",
        std::process::id(),
        count
    ));
    fs::create_dir_all(&path).unwrap();

    let cleanup_path = path.clone();
    let cleanup = move || {
        let _ = fs::remove_dir_all(cleanup_path);
    };
    (path, cleanup)
}

#[test]
fn test_registry_compilation() {
    let (dir, cleanup) = create_temp_models_dir();

    let model_json = serde_json::json!({
        "entity": "test_note",
        "engine": "oltp",
        "is_system": false,
        "attributes": [
            {
                "name": "content",
                "type": "string",
                "required": true
            }
        ]
    });

    fs::write(
        dir.join("test_note.json"),
        serde_json::to_string(&model_json).unwrap(),
    )
    .unwrap();

    let res = CodeRegistry::build(&dir);
    assert!(
        res.is_ok(),
        "Expected compilation to succeed, got {:?}",
        res.err()
    );
    let (registry, _rules) = res.unwrap();

    assert_eq!(registry.entity_count(), 1);
    let model = registry.get_model("test_note");
    assert!(model.is_some());
    let model = model.unwrap();
    assert_eq!(model.entity, "test_note");
    assert_eq!(model.attributes.len(), 1);
    assert_eq!(model.attributes[0].name, "content");

    cleanup();
}

#[test]
fn test_duplicate_entity_prevention() {
    let (dir, cleanup) = create_temp_models_dir();

    let model_1 = serde_json::json!({
        "entity": "dup_entity",
        "engine": "oltp",
        "attributes": [
            { "name": "field_a", "type": "string" }
        ]
    });
    let model_2 = serde_json::json!({
        "entity": "dup_entity",
        "engine": "oltp",
        "attributes": [
            { "name": "field_b", "type": "string" }
        ]
    });

    fs::write(
        dir.join("m1.json"),
        serde_json::to_string(&model_1).unwrap(),
    )
    .unwrap();
    fs::write(
        dir.join("m2.json"),
        serde_json::to_string(&model_2).unwrap(),
    )
    .unwrap();

    let res = CodeRegistry::build(&dir);
    assert!(
        res.is_err(),
        "Expected compilation to fail due to duplicate entity"
    );
    let err = res.err().unwrap();
    assert_eq!(err.code, ErrorCode::Cod002);

    cleanup();
}

#[test]
fn test_invalid_scope_provider() {
    let (dir, cleanup) = create_temp_models_dir();

    // m1 define una entidad con is_sequence_scope_provider: false (por defecto)
    let model_1 = serde_json::json!({
        "entity": "location_non_provider",
        "engine": "oltp",
        "is_sequence_scope_provider": false,
        "attributes": []
    });

    // m2 referencia a location_non_provider como scope, lo cual debe fallar porque no es provider!
    let model_2 = serde_json::json!({
        "entity": "work_order_invalid",
        "engine": "oltp",
        "attributes": [
            {
                "name": "scope_location",
                "type": "reference",
                "entityRef": "location_non_provider",
                "is_sequence_scope": true
            }
        ]
    });

    fs::write(
        dir.join("m1.json"),
        serde_json::to_string(&model_1).unwrap(),
    )
    .unwrap();
    fs::write(
        dir.join("m2.json"),
        serde_json::to_string(&model_2).unwrap(),
    )
    .unwrap();

    let res = CodeRegistry::build(&dir);
    assert!(
        res.is_err(),
        "Expected compilation to fail due to invalid sequence scope provider"
    );
    let err = res.err().unwrap();
    assert_eq!(err.code, ErrorCode::CodScope001);

    cleanup();
}

#[test]
fn test_stub_injection_activation() {
    std::env::set_var("ATHENA_MODE", "stub");
    let mode = std::env::var("ATHENA_MODE").unwrap_or_default();
    assert_eq!(mode, "stub");

    std::env::set_var("SQS_MODE", "stub");
    let sqs_mode = std::env::var("SQS_MODE").unwrap_or_default();
    assert_eq!(sqs_mode, "stub");
}

#[test]
fn test_dashboard_bi_entity_registered() {
    let models_dir = std::path::Path::new("config/models");
    let res = CodeRegistry::build(models_dir);
    assert!(
        res.is_ok(),
        "Expected main registry compilation to succeed, got {:?}",
        res.err()
    );
    let (registry, _rules) = res.unwrap();

    let model = registry.get_model("dashboardBI");
    assert!(
        model.is_some(),
        "dashboardBI entity should be registered in the CodeRegistry"
    );
    let model = model.unwrap();
    assert_eq!(model.entity, "dashboardBI");

    assert!(model
        .attributes
        .iter()
        .any(|a| a.name == "name" && a.attr_type == AttrType::String));
    assert!(model
        .attributes
        .iter()
        .any(|a| a.name == "description" && a.attr_type == AttrType::String));
    assert!(model
        .attributes
        .iter()
        .any(|a| a.name == "widgets" && a.attr_type == AttrType::Json));
    assert!(model
        .attributes
        .iter()
        .any(|a| a.name == "created_at" && a.attr_type == AttrType::Epoch));
    assert!(model
        .attributes
        .iter()
        .any(|a| a.name == "updated_at" && a.attr_type == AttrType::Epoch));
}

/// Candado de la familia de procedimientos formales (estilo MaintainX) y de la
/// jerarquía OT → sub-OTs: el registry REAL debe compilar con las entidades
/// nuevas. La instanciación vive en metri-cmms-plugin; aquí sólo esquema.
#[test]
fn test_procedure_family_and_wo_hierarchy_registered() {
    let models_dir = std::path::Path::new("config/models");
    let (registry, _rules) = CodeRegistry::build(models_dir)
        .expect("config/models debe compilar con la familia procedure");

    let procedure = registry
        .get_model("procedure_template")
        .expect("procedure_template debe estar registrado");
    assert!(procedure
        .attributes
        .iter()
        .any(|a| a.name == "lifecycle_state"
            && a.attr_type == AttrType::Enum
            && a.options.contains(&"PUBLISHED".to_string())));

    let pfield = registry
        .get_model("procedure_template_field")
        .expect("procedure_template_field debe estar registrado");
    assert!(pfield
        .attributes
        .iter()
        .any(|a| a.name == "procedure_template_id" && a.required && a.indexed));
    assert!(pfield
        .attributes
        .iter()
        .any(|a| a.name == "field_type" && a.attr_type == AttrType::Enum));
    assert!(pfield
        .attributes
        .iter()
        .any(|a| a.name == "choices" && a.attr_type == AttrType::Array));
    // Anidación de secciones: auto-referencia a la misma entidad.
    assert!(pfield.attributes.iter().any(|a| a.name == "parent_field_id"
        && a.entity_ref.as_deref() == Some("procedure_template_field")));

    let wop = registry
        .get_model("work_order_procedure")
        .expect("work_order_procedure debe estar registrado");
    assert!(wop
        .attributes
        .iter()
        .any(|a| a.name == "work_order_id" && a.required && a.indexed));
    assert!(wop
        .attributes
        .iter()
        .any(|a| a.name == "procedure_template_id"
            && a.entity_ref.as_deref() == Some("procedure_template")));
    assert!(wop.attributes.iter().any(|a| a.name == "score"));
    assert!(wop.attributes.iter().any(|a| a.name == "max_score"));

    let wopf = registry
        .get_model("work_order_procedure_field")
        .expect("work_order_procedure_field debe estar registrado");
    assert!(wopf
        .attributes
        .iter()
        .any(|a| a.name == "work_order_procedure_id" && a.required && a.indexed));
    assert!(wopf
        .attributes
        .iter()
        .any(|a| a.name == "procedure_template_field_id"));
    // Captura de valor tipada: un attr por familia de respuesta. La evidencia
    // de fichero no va en array: la vía única es file.owner_entity_* (Fase 3
    // de PLAN_REFACTORIZACION_MODELO).
    for value_attr in [
        "value_text",
        "value_number",
        "value_boolean",
        "value_epoch",
        "value_choice",
    ] {
        assert!(
            wopf.attributes.iter().any(|a| a.name == value_attr),
            "work_order_procedure_field debe declarar {value_attr}"
        );
    }
    assert!(
        !wopf.attributes.iter().any(|a| a.name == "value_file_ids"),
        "value_file_ids fue retirado: los adjuntos van vía file.owner_entity_*"
    );

    let wo = registry
        .get_model("work_order")
        .expect("work_order debe estar registrado");
    assert!(wo
        .attributes
        .iter()
        .any(|a| a.name == "completed_at" && a.indexed));
}

/// La OT lleva 1..N procedimientos (Fase 2 del PLAN_REFACTORIZACION_MODELO).
/// El orden de ejecución vive en la instancia y las constraints impiden
/// duplicar la misma plantilla en la misma OT. La familia de checklists de
/// instancia (work_order_checklist*) fue retirada del catálogo el 2026-09-24.
#[test]
fn la_ot_lleva_n_procedimientos_ordenados() {
    let models_dir = std::path::Path::new("config/models");
    let (registry, _rules) =
        CodeRegistry::build(models_dir).expect("config/models debe compilar con la familia 1-a-N");

    let wop = registry
        .get_model("work_order_procedure")
        .expect("work_order_procedure debe estar registrado");
    assert!(wop
        .attributes
        .iter()
        .any(|a| a.name == "procedure_order" && a.indexed));
    let wop_unique = wop
        .constraints
        .iter()
        .find(|c| c.attributes == ["work_order_id", "procedure_template_id"])
        .expect("el mismo procedure no debe instanciarse dos veces en una OT");
    assert_eq!(
        wop_unique.scope,
        crate::codice::registry::ConstraintScope::Tenant
    );
    // Contrato de integridad: plantilla PUBLISHED, cierre con actor y puntaje acotado.
    assert!(wop.constraints.iter().any(|c| c.kind
        == crate::codice::registry::ConstraintKind::RefState
        && c.attributes == ["procedure_template_id"]
        && c.when.as_deref().is_some_and(
            |w| w.contains(&("lifecycle_state".to_string(), "PUBLISHED".to_string()))
        )));
    assert!(wop.constraints.iter().any(|c| c.kind
        == crate::codice::registry::ConstraintKind::RequiresWhen
        && c.attributes == ["completed_by", "completed_at"]));
    assert!(wop.constraints.iter().any(|c| c.kind
        == crate::codice::registry::ConstraintKind::AtMost
        && c.attributes == ["score", "max_score"]));

    // Toda la capa de definición de formularios está retirada, y con ella la
    // familia de checklists de instancia (2026-09-24): la verificación vive
    // en las tareas y sus ítems.
    for retirada in [
        "form_template",
        "form_template_section",
        "form_template_field",
        "work_order_checklist",
        "work_order_checklist_section",
        "work_order_checklist_item",
    ] {
        assert!(
            registry.get_model(retirada).is_none(),
            "{retirada} fue retirada del catálogo"
        );
    }

    // La plantilla de checklists conserva ciclo de vida PUBLISHED y tipado
    // por pregunta; quedó sin instancia desde el retiro de la familia
    // work_order_checklist* (2026-09-24).
    let clt = registry
        .get_model("checklist_template")
        .expect("checklist_template debe estar registrado");
    assert!(clt.attributes.iter().any(|a| a.name == "status"
        && a.attr_type == AttrType::Enum
        && a.options.contains(&"PUBLISHED".to_string())));
    let clt_item = registry
        .get_model("checklist_template_item")
        .expect("checklist_template_item debe estar registrado");
    assert!(clt_item
        .attributes
        .iter()
        .any(|a| a.name == "item_order" && a.attr_type == AttrType::Number));
    assert!(clt_item.attributes.iter().any(|a| a.name == "type"
        && a.attr_type == AttrType::Enum
        && a.options.contains(&"PASS_FAIL".to_string())));
}

/// La placa de datos del activo dejó de ser un flat_map libre: cada
/// especificación técnica es una fila propia (technical_specification) con un
/// concepto del sistema terminológico UN/CEFACT y un valor numérico. El
/// atributo `specifications` del asset fue retirado en el mismo movimiento.
#[test]
fn la_placa_de_datos_del_activo_es_una_entidad_hija_terminologica() {
    let models_dir = std::path::Path::new("config/models");
    let (registry, _rules) = CodeRegistry::build(models_dir)
        .expect("config/models debe compilar con la placa de datos terminológica");

    let tspec = registry
        .get_model("technical_specification")
        .expect("technical_specification debe estar registrado");
    assert!(tspec.attributes.iter().any(|a| a.name == "asset_id"
        && a.required
        && a.indexed
        && a.entity_ref.as_deref() == Some("asset")));
    // La fila ES la cantidad: el concepto nombra la magnitud, el valor la mide.
    assert!(tspec
        .attributes
        .iter()
        .any(|a| a.name == "concept_code" && a.required && a.attr_type == AttrType::String));
    assert!(tspec
        .attributes
        .iter()
        .any(|a| a.name == "value" && a.required && a.attr_type == AttrType::Decimal));
    // Companion denormalizado (patrón omniclass_name) y orden de fila.
    assert!(tspec
        .attributes
        .iter()
        .any(|a| a.name == "concept_name" && a.attr_type == AttrType::String));
    assert!(tspec
        .attributes
        .iter()
        .any(|a| a.name == "spec_order" && a.attr_type == AttrType::Number && a.indexed));
    // Un valor por magnitud: el mismo concepto no se declara dos veces.
    let unique = tspec
        .constraints
        .iter()
        .find(|c| {
            c.kind == crate::codice::registry::ConstraintKind::Unique
                && c.attributes == ["asset_id", "concept_code"]
        })
        .expect("el mismo concepto UN/CEFACT no debe declararse dos veces por activo");
    assert_eq!(
        unique.scope,
        crate::codice::registry::ConstraintScope::Tenant
    );

    let asset = registry
        .get_model("asset")
        .expect("asset debe estar registrado");
    assert!(
        !asset.attributes.iter().any(|a| a.name == "specifications"),
        "asset.specifications fue retirado: la placa de datos vive en technical_specification"
    );
}

/// La familia de turnos declara sus contratos de capacidad: el override cita
/// patrón, la ausencia tiene motivo, el cierre lleva actor, el horario es
/// coherente y el patrón tiene dueño.
#[test]
fn la_familia_de_turnos_declara_los_contratos_de_capacidad() {
    let models_dir = std::path::Path::new("config/models");
    let (registry, _rules) = CodeRegistry::build(models_dir)
        .expect("config/models debe compilar con la familia de turnos");

    let ts = registry
        .get_model("technician_shift")
        .expect("technician_shift debe estar registrado");
    let tipos: Vec<(crate::codice::registry::ConstraintKind, Vec<String>)> = ts
        .constraints
        .iter()
        .map(|c| (c.kind, c.attributes.clone()))
        .collect();
    for esperado in [
        (
            crate::codice::registry::ConstraintKind::RequiresWhen,
            vec!["shift_pattern_id".to_string()],
        ),
        (
            crate::codice::registry::ConstraintKind::RequiresWhen,
            vec!["absence_reason".to_string()],
        ),
        (
            crate::codice::registry::ConstraintKind::RequiresWhen,
            vec!["completed_by".to_string(), "completed_at".to_string()],
        ),
        (
            crate::codice::registry::ConstraintKind::AtLeast,
            vec!["end_time".to_string(), "start_time".to_string()],
        ),
    ] {
        assert!(
            tipos
                .iter()
                .any(|(k, a)| *k == esperado.0 && *a == esperado.1),
            "falta la constraint {:?} {:?} en technician_shift: {tipos:?}",
            esperado.0,
            esperado.1
        );
    }
    assert!(ts.attributes.iter().any(|a| a.name == "status"
        && a.attr_type == AttrType::Enum
        && a.options.contains(&"CANCELLED".to_string())));
    assert_eq!(
        ts.event_rules.len(),
        3,
        "create/update/delete emiten la señal de reproyección"
    );

    let sp = registry
        .get_model("shift_pattern")
        .expect("shift_pattern debe estar registrado");
    assert!(sp.constraints.iter().any(|c| c.kind
        == crate::codice::registry::ConstraintKind::RequiresAny
        && c.attributes == ["user_id", "user_group_id"]));
    assert!(sp.attributes.iter().any(|a| a.name == "status"
        && a.attr_type == AttrType::Enum
        && a.options.contains(&"PUBLISHED".to_string())));
}
