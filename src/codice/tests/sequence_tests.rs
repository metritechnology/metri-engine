//! Tests for `codice::sequence`.
use super::*;

#[test]
fn build_sequence_code_without_scope() {
    let code = build_sequence_code("tenant-1", "work_order_code", None);
    assert_eq!(code, "tenant-1:work_order_code_seq");
}

#[test]
fn build_sequence_code_with_scope() {
    let code = build_sequence_code("tenant-1", "work_order_code", Some("L1"));
    assert_eq!(code, "tenant-1:work_order_code:L1_seq");
}

#[test]
fn format_code_zero_pads() {
    assert_eq!(format_code("WO-", None, 4, 7), "WO-0007");
    assert_eq!(format_code("WO-", None, 4, 100), "WO-0100");
    assert_eq!(format_code("", None, 6, 1), "000001");
    // Con segmento (el tag de la location): el formato del pattern del
    // Códice (^[A-Z0-9][A-Z0-9-]*\d{2,8}$) → "WO-L-K92MXA-0043".
    assert_eq!(
        format_code("WO-", Some("L-K92MXA"), 4, 43),
        "WO-L-K92MXA-0043"
    );
    // Un segmento vacío es sin segmento.
    assert_eq!(format_code("WO-", Some(""), 4, 7), "WO-0007");
    // Un prefijo del tenant distinto del default del Códice (política).
    assert_eq!(format_code("OT-", Some("L-1"), 2, 5), "OT-L-1-05");
}

// ── Política de numeración del tenant (fila global) ──────────────────────────

use crate::codice::registry::EntityModel;
use crate::codice::sequence::{
    global_sequence_code, parse_seq_defaults, scope_tag_from_pk, SequencePolicy,
};

#[test]
fn la_politica_del_tenant_manda_sobre_el_codice() {
    let mut config = SeqAttrConfig {
        name: "work_order_number".to_string(),
        prefix: "WO-".to_string(),
        padding: 4,
        scope_resolution: ScopeResolution::NearestRegistered,
    };

    config.apply_policy(&SequencePolicy::default());
    assert_eq!(config.prefix, "WO-", "política vacía = todo cae al Códice");
    assert_eq!(config.padding, 4);
    assert_eq!(config.scope_resolution, ScopeResolution::NearestRegistered);

    config.apply_policy(&SequencePolicy {
        prefix: Some("OT-".to_string()),
        padding: Some(2),
        scope_resolution: Some(ScopeResolution::Exact),
    });
    assert_eq!(config.prefix, "OT-");
    assert_eq!(config.padding, 2);
    assert_eq!(config.scope_resolution, ScopeResolution::Exact);
}

#[test]
fn el_codigo_global_y_el_scope_tag_viajan_en_el_pk() {
    assert_eq!(
        global_sequence_code("tnt_01", "work_order_number"),
        "tnt_01:work_order_number_seq"
    );
    assert_eq!(
        scope_tag_from_pk(
            "tnt_01:work_order_number:L-K92MXA_seq",
            "tnt_01",
            "work_order_number"
        )
        .as_deref(),
        Some("L-K92MXA")
    );
    assert_eq!(
        scope_tag_from_pk(
            "tnt_01:work_order_number_seq",
            "tnt_01",
            "work_order_number"
        ),
        None,
        "la fila global no tiene scope_tag"
    );
    assert_eq!(
        scope_tag_from_pk(
            "otro_tenant:work_order_number:L-1_seq",
            "tnt_01",
            "work_order_number"
        ),
        None,
        "un PK de otro tenant no produce scope_tag"
    );
}

#[test]
fn parse_seq_defaults_lee_el_codice_y_descarta_lo_no_secuencial() {
    let model: EntityModel = serde_json::from_value(serde_json::json!({
        "entity": "work_order",
        "engine": "oltp",
        "fts_fields": [],
        "event_rules": [],
        "constraints": [],
        "is_sequence_scope_provider": false,
        "attributes": [
            {
                "name": "title",
                "attr_type": "string",
                "label": null,
                "required": false,
                "unique": null,
                "indexed": false,
                "fts": true,
                "is_dimension": false,
                "is_metric": false,
                "entity_ref": null,
                "options": [],
                "is_sequence_scope": false,
                "is_sequence_scope_via": false,
                "sensitive": false,
                "auto_generate": null,
                "validation_regex": null,
                "default_value": null
            },
            {
                "name": "work_order_number",
                "attr_type": "string",
                "label": null,
                "required": false,
                "unique": "tenant",
                "indexed": false,
                "fts": true,
                "is_dimension": false,
                "is_metric": false,
                "entity_ref": null,
                "options": [],
                "is_sequence_scope": false,
                "is_sequence_scope_via": false,
                "sensitive": false,
                "auto_generate": {
                    "strategy": "sequential",
                    "prefix": "WO-",
                    "padding": 4,
                    "scope_resolution": "nearest_registered"
                },
                "validation_regex": null,
                "default_value": null
            }
        ]
    }))
    .expect("modelo deserializa");

    let config = parse_seq_defaults(&model, "work_order_number").expect("sequential declarado");
    assert_eq!(config.prefix, "WO-");
    assert_eq!(config.padding, 4);
    assert_eq!(config.scope_resolution, ScopeResolution::NearestRegistered);

    assert!(
        parse_seq_defaults(&model, "title").is_none(),
        "sin auto_generate"
    );
    assert!(
        parse_seq_defaults(&model, "missing").is_none(),
        "atributo inexistente"
    );
}
