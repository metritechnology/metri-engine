use super::*;
use crate::codice::registry::{AttributeDescriptor, EngineChannel};
use serde_json::json;

fn make_test_model() -> EntityModel {
    EntityModel {
        entity: "test_asset".to_string(),
        label: None,
        icon: None,
        primary_key: Some("id".to_string()),
        fts_fields: vec!["name".to_string()],
        engine: EngineChannel::Oltp,
        attributes: vec![
            AttributeDescriptor {
                name: "id".to_string(),
                attr_type: AttrType::String,
                label: None,
                required: true,
                unique: None,
                indexed: false,
                fts: false,
                is_dimension: false,
                is_metric: false,
                entity_ref: None,
                options: vec![],
                is_sequence_scope: false,
                is_sequence_scope_via: false,
                sensitive: false,
                auto_generate: None,
                validation_regex: None,
                default_value: None,
            },
            AttributeDescriptor {
                name: "name".to_string(),
                attr_type: AttrType::String,
                label: None,
                required: true,
                unique: None,
                indexed: false,
                fts: true,
                is_dimension: false,
                is_metric: false,
                entity_ref: None,
                options: vec![],
                is_sequence_scope: false,
                is_sequence_scope_via: false,
                sensitive: false,
                auto_generate: None,
                validation_regex: None,
                default_value: None,
            },
            AttributeDescriptor {
                name: "cost".to_string(),
                attr_type: AttrType::Decimal,
                label: None,
                required: false,
                unique: None,
                indexed: false,
                fts: false,
                is_dimension: false,
                is_metric: false,
                entity_ref: None,
                options: vec![],
                is_sequence_scope: false,
                is_sequence_scope_via: false,
                sensitive: false,
                auto_generate: None,
                validation_regex: None,
                default_value: None,
            },
            AttributeDescriptor {
                name: "is_active".to_string(),
                attr_type: AttrType::Boolean,
                label: None,
                required: true,
                unique: None,
                indexed: false,
                fts: false,
                is_dimension: false,
                is_metric: false,
                entity_ref: None,
                options: vec![],
                is_sequence_scope: false,
                is_sequence_scope_via: false,
                sensitive: false,
                auto_generate: None,
                validation_regex: None,
                default_value: None,
            },
            AttributeDescriptor {
                name: "created_at".to_string(),
                attr_type: AttrType::Epoch,
                label: None,
                required: false,
                unique: None,
                indexed: false,
                fts: false,
                is_dimension: false,
                is_metric: false,
                entity_ref: None,
                options: vec![],
                is_sequence_scope: false,
                is_sequence_scope_via: false,
                sensitive: false,
                auto_generate: None,
                validation_regex: None,
                default_value: None,
            },
            AttributeDescriptor {
                name: "tags".to_string(),
                attr_type: AttrType::Array,
                label: None,
                required: false,
                unique: None,
                indexed: false,
                fts: false,
                is_dimension: false,
                is_metric: false,
                entity_ref: None,
                options: vec![],
                is_sequence_scope: false,
                is_sequence_scope_via: false,
                sensitive: false,
                auto_generate: None,
                validation_regex: None,
                default_value: None,
            },
            AttributeDescriptor {
                name: "location_ref".to_string(),
                attr_type: AttrType::Reference,
                label: None,
                required: false,
                unique: None,
                indexed: false,
                fts: false,
                is_dimension: false,
                is_metric: false,
                entity_ref: Some("location".to_string()),
                options: vec![],
                is_sequence_scope: false,
                is_sequence_scope_via: false,
                sensitive: false,
                auto_generate: None,
                validation_regex: None,
                default_value: None,
            },
            AttributeDescriptor {
                name: "status".to_string(),
                attr_type: AttrType::Enum,
                label: None,
                required: false,
                unique: None,
                indexed: false,
                fts: false,
                is_dimension: true,
                is_metric: false,
                entity_ref: None,
                options: vec!["ACTIVE".to_string(), "INACTIVE".to_string()],
                is_sequence_scope: false,
                is_sequence_scope_via: false,
                sensitive: false,
                auto_generate: None,
                validation_regex: None,
                default_value: Some("ACTIVE".to_string()),
            },
            AttributeDescriptor {
                name: "tag".to_string(),
                attr_type: AttrType::String,
                label: None,
                required: false,
                unique: None,
                indexed: false,
                fts: true,
                is_dimension: true,
                is_metric: false,
                entity_ref: None,
                options: vec![],
                is_sequence_scope: false,
                is_sequence_scope_via: false,
                sensitive: false,
                auto_generate: None,
                validation_regex: Some("^[A-Z]{3}-\\d{3}$".to_string()),
                default_value: None,
            },
        ],
        event_rules: vec![],
        is_sequence_scope_provider: false,
        write_path_locked: false,
        is_system: false,
        disable_eda: false,
        shadow_sagas_mapping: None,
        constraints: vec![],
    }
}

#[test]
fn test_validate_payload_success() {
    let model = make_test_model();
    let payload = json!({
        "id": "asset-001",
        "name": "Compressor A",
        "cost": 1500.50,
        "is_active": true,
        "created_at": "1622000000000",
        "tags": ["hvac", "critical"],
        "location_ref": "loc-100",
        "status": "ACTIVE",
        "tag": "AST-101"
    });

    let result = validate_payload(&model, &payload, "tnt_01", true);
    assert!(result.is_ok());

    let attrs = result.unwrap();
    assert_eq!(
        attrs.get("id").unwrap(),
        &DatomValue::Str("asset-001".to_string())
    );
    assert_eq!(
        attrs.get("name").unwrap(),
        &DatomValue::Str("Compressor A".to_string())
    );
    assert_eq!(attrs.get("cost").unwrap(), &DatomValue::Double(1500.50));
    assert_eq!(attrs.get("is_active").unwrap(), &DatomValue::Bool(true));
    assert_eq!(
        attrs.get("created_at").unwrap(),
        &DatomValue::Instant(1622000000000)
    );
    assert_eq!(
        attrs.get("tags").unwrap(),
        &DatomValue::Array(vec!["hvac".to_string(), "critical".to_string()])
    );
    assert_eq!(
        attrs.get("location_ref").unwrap(),
        &DatomValue::Str("loc-100".to_string())
    );
    assert_eq!(
        attrs.get("status").unwrap(),
        &DatomValue::Str("ACTIVE".to_string())
    );
    assert_eq!(
        attrs.get("tag").unwrap(),
        &DatomValue::Str("AST-101".to_string())
    );
}

#[test]
fn test_validate_payload_missing_required() {
    let model = make_test_model();
    // Missing 'is_active' (required) and 'name' (required)
    let payload = json!({
        "id": "asset-001"
    });

    let result = validate_payload(&model, &payload, "tnt_01", true);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code, ErrorCode::Cod001);
    assert!(err.detail.contains("Campo requerido 'name' está ausente"));
    assert!(err
        .detail
        .contains("Campo requerido 'is_active' está ausente"));
}

#[test]
fn test_validate_payload_type_mismatch() {
    let model = make_test_model();
    let payload = json!({
        "id": "asset-001",
        "name": "Compressor A",
        "is_active": "not_a_bool" // Should be a boolean
    });

    let result = validate_payload(&model, &payload, "tnt_01", true);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code, ErrorCode::Cod001);
    assert!(err.detail.contains("Debe ser un booleano"));
}

#[test]
fn test_validate_payload_enum_validation() {
    let model = make_test_model();
    let payload = json!({
        "id": "asset-001",
        "name": "Compressor A",
        "is_active": true,
        "status": "BANANA" // Invalid enum option
    });

    let result = validate_payload(&model, &payload, "tnt_01", true);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code, ErrorCode::Cod001);
    assert!(err.detail.contains("Valor 'BANANA' no permitido para enum"));
}

#[test]
fn test_validate_payload_pattern_validation() {
    let model = make_test_model();
    let payload = json!({
        "id": "asset-001",
        "name": "Compressor A",
        "is_active": true,
        "tag": "invalid_tag" // Does not match ^[A-Z]{3}-\\d{3}$
    });

    let result = validate_payload(&model, &payload, "tnt_01", true);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code, ErrorCode::Cod001);
    assert!(err.detail.contains("no cumple con el patrón"));
}
