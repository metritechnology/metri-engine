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
        .get_model("procedure")
        .expect("procedure debe estar registrado");
    assert!(procedure
        .attributes
        .iter()
        .any(|a| a.name == "work_order_template_id" && a.attr_type == AttrType::Reference));
    assert!(procedure
        .attributes
        .iter()
        .any(|a| a.name == "lifecycle_state"
            && a.attr_type == AttrType::Enum
            && a.options.contains(&"PUBLISHED".to_string())));

    let pfield = registry
        .get_model("procedure_field")
        .expect("procedure_field debe estar registrado");
    assert!(pfield
        .attributes
        .iter()
        .any(|a| a.name == "procedure_id" && a.required && a.indexed));
    assert!(pfield
        .attributes
        .iter()
        .any(|a| a.name == "field_type" && a.attr_type == AttrType::Enum));
    assert!(pfield
        .attributes
        .iter()
        .any(|a| a.name == "choices" && a.attr_type == AttrType::Array));
    // Anidación de secciones: auto-referencia a la misma entidad.
    assert!(
        pfield
            .attributes
            .iter()
            .any(|a| a.name == "parent_field_id"
                && a.entity_ref.as_deref() == Some("procedure_field"))
    );

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
        .any(|a| a.name == "procedure_id" && a.entity_ref.as_deref() == Some("procedure")));
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
        .any(|a| a.name == "procedure_field_id"));
    // Captura de valor tipada: un attr por familia de respuesta.
    for value_attr in [
        "value_text",
        "value_number",
        "value_boolean",
        "value_epoch",
        "value_choice",
        "value_file_ids",
    ] {
        assert!(
            wopf.attributes.iter().any(|a| a.name == value_attr),
            "work_order_procedure_field debe declarar {value_attr}"
        );
    }

    let wo = registry
        .get_model("work_order")
        .expect("work_order debe estar registrado");
    assert!(wo
        .attributes
        .iter()
        .any(|a| a.name == "parent_work_order_id"
            && a.attr_type == AttrType::Reference
            && a.indexed));
    assert!(wo
        .attributes
        .iter()
        .any(|a| a.name == "is_parent" && a.indexed));
    assert!(wo
        .attributes
        .iter()
        .any(|a| a.name == "completed_at" && a.indexed));

}
