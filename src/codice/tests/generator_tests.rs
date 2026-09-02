use super::*;
use crate::codice::registry::{AttrType, AttributeDescriptor, EngineChannel};
use serde_json::json;

#[tokio::test]
async fn test_generator_inject_empty() {
    let ddb = DynamoClient::new("dummy-table").await;
    let model = EntityModel {
        entity: "empty_entity".to_string(),
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
        constraints: vec![],
    };

    let mut payload = serde_json::Map::new();
    payload.insert("name".to_string(), Value::String("foo".to_string()));

    let result = inject(&ddb, &model, "tnt_01", payload.clone()).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), payload);
}

#[tokio::test]
async fn test_generator_inject_stochastic_base36() {
    let ddb = DynamoClient::new("dummy-table").await;

    let auto_gen_config = json!({
        "strategy": "stochastic_base36",
        "prefix": "WO-",
        "length": 7
    });

    let model = EntityModel {
        entity: "work_order".to_string(),
        label: None,
        icon: None,
        primary_key: Some("code".to_string()),
        fts_fields: vec![],
        engine: EngineChannel::Oltp,
        attributes: vec![AttributeDescriptor {
            name: "code".to_string(),
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
            auto_generate: Some(auto_gen_config),
            validation_regex: None,
            default_value: None,
        }],
        event_rules: vec![],
        is_sequence_scope_provider: false,
        write_path_locked: false,
        is_system: false,
        disable_eda: false,
        shadow_sagas_mapping: None,
        constraints: vec![],
    };

    // Case A: Payload does not have "code". It should inject it.
    let payload = serde_json::Map::new();
    let result = inject(&ddb, &model, "tnt_01", payload).await;
    assert!(result.is_ok());
    let enriched = result.unwrap();
    assert!(enriched.contains_key("code"));
    let code_val = enriched.get("code").unwrap().as_str().unwrap();
    assert!(code_val.starts_with("WO-"));
    assert_eq!(code_val.len(), 10); // "WO-" (3) + 7 = 10

    // Case B: Payload already has "code". It should not overwrite it.
    let mut payload_existing = serde_json::Map::new();
    payload_existing.insert(
        "code".to_string(),
        Value::String("WO-PRESERVED".to_string()),
    );
    let result_existing = inject(&ddb, &model, "tnt_01", payload_existing.clone()).await;
    assert!(result_existing.is_ok());
    assert_eq!(result_existing.unwrap(), payload_existing);
}
