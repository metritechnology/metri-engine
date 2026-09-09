//! Tests for `eav::writer::writer`.
// Tests del writer EAV — Puerta 1 del plan de refactorización.
//
// El writer es la capa de escritura: una regresión aquí es corrupción silenciosa
// de datos, no un error visible. Cubren el ciclo completo contra DynamoDB Local:
// `make test-integration` los ejecuta (están marcados #[ignore] porque exigen
// infra viva; `make infra` la levanta).

use super::*;
use crate::infrastructure::dynamodb::DynamoClient;
use std::collections::HashMap;
use std::sync::Arc;

const TABLA: &str = "metri-eav-local";

async fn writer() -> EavWriter {
    std::env::set_var("DYNAMODB_ENDPOINT", "http://localhost:8000");
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", "us-east-1");
    // El writer valida entity_type contra el Códice global: inicializarlo una
    // sola vez por proceso (los tests del writer no comparten setup con otros).
    static CODICE: std::sync::Once = std::sync::Once::new();
    CODICE.call_once(|| {
        if crate::codice::registry::global_opt().is_none() {
            let models = std::path::Path::new("config/models");
            let (registry, _) = crate::codice::CodeRegistry::build(models)
                .expect("Códice: registry de config/models");
            crate::codice::init_global(registry);
        }
    });
    EavWriter::new(Arc::new(DynamoClient::new(TABLA).await), TABLA)
}

/// Cada test estrena tenant: así son repetibles sin limpiar la tabla.
fn tenant_nuevo() -> String {
    format!("tnt_wtest_{}", ulid::Ulid::new())
}

fn payload_nueva(tenant: &str) -> TransactPayload {
    let mut attrs = HashMap::new();
    attrs.insert(
        "title".to_string(),
        DatomValue::Str("Bomba hidraulica 3".to_string()),
    );
    attrs.insert("status".to_string(), DatomValue::Str("open".to_string()));
    TransactPayload {
        tenant_id: tenant.to_string(),
        entity_id: None,
        entity_type: "work_order".to_string(),
        attrs,
        op: TransactOp::Create,
        suppress_events: false,
    }
}

#[tokio::test]
#[ignore]
async fn create_escribe_y_el_pull_lo_devuelve() {
    let w = writer().await;
    let tenant = tenant_nuevo();

    let res = w.transact(payload_nueva(&tenant)).await.expect("Create");
    assert!(
        res.datoms >= 2,
        "Create debe escribir al menos los 2 atributos"
    );
    assert!(!res.entity_id.is_empty(), "Create genera ULID");

    let map = w
        .reader()
        .pull(&tenant, &res.entity_id, None)
        .await
        .expect("Pull tras Create");
    assert_eq!(
        map.get("title"),
        Some(&DatomValue::Str("Bomba hidraulica 3".to_string()))
    );
    assert_eq!(
        map.get("status"),
        Some(&DatomValue::Str("open".to_string()))
    );
}

#[tokio::test]
#[ignore]
async fn update_genera_retract_y_el_pull_muestra_el_nuevo_valor() {
    let w = writer().await;
    let tenant = tenant_nuevo();
    let res = w.transact(payload_nueva(&tenant)).await.expect("Create");

    let mut attrs = HashMap::new();
    attrs.insert("status".to_string(), DatomValue::Str("closed".to_string()));
    let upd = TransactPayload {
        tenant_id: tenant.clone(),
        entity_id: Some(res.entity_id.clone()),
        entity_type: "work_order".to_string(),
        attrs,
        op: TransactOp::Update,
        suppress_events: false,
    };
    let res2 = w.transact(upd).await.expect("Update");
    assert!(res2.datoms >= 2, "Update genera retract + assert");

    let map = w
        .reader()
        .pull(&tenant, &res.entity_id, None)
        .await
        .expect("Pull tras Update");
    assert_eq!(
        map.get("status"),
        Some(&DatomValue::Str("closed".to_string())),
        "El pull debe devolver el valor vigente, no el retractado"
    );
}

#[tokio::test]
#[ignore]
async fn delete_retracta_los_atributos() {
    let w = writer().await;
    let tenant = tenant_nuevo();
    let res = w.transact(payload_nueva(&tenant)).await.expect("Create");

    let del = TransactPayload {
        tenant_id: tenant.clone(),
        entity_id: Some(res.entity_id.clone()),
        entity_type: "work_order".to_string(),
        attrs: HashMap::new(),
        op: TransactOp::Delete,
        suppress_events: false,
    };
    w.transact(del).await.expect("Delete");

    let map = w
        .reader()
        .pull(&tenant, &res.entity_id, None)
        .await
        .expect("Pull tras Delete");
    assert!(
        map.get("status").is_none(),
        "Delete debe retractar los atributos; quedó {:?}",
        map.get("status")
    );
}

#[tokio::test]
#[ignore]
async fn entity_id_provido_se_respeta() {
    let w = writer().await;
    let tenant = tenant_nuevo();
    let id_fijo = format!("01JWTEST{}", ulid::Ulid::new());

    let payload = TransactPayload {
        entity_id: Some(id_fijo.clone()),
        ..payload_nueva(&tenant)
    };
    let res = w.transact(payload).await.expect("Create con id provisto");
    assert_eq!(res.entity_id, id_fijo, "No debe regenerar el ULID");
}

#[tokio::test]
#[ignore]
async fn bulk_deferred_escribe_y_es_visible() {
    let w = writer().await;
    let tenant = tenant_nuevo();

    let res = w
        .transact_bulk_deferred(payload_nueva(&tenant), None)
        .await
        .expect("Bulk deferred");
    assert!(res.datoms >= 2);

    let map = w
        .reader()
        .pull(&tenant, &res.entity_id, None)
        .await
        .expect("Pull tras bulk deferred");
    assert_eq!(
        map.get("status"),
        Some(&DatomValue::Str("open".to_string()))
    );
}

#[tokio::test]
#[ignore]
async fn entity_type_desconocido_rechaza_con_eav004() {
    let w = writer().await;
    let tenant = tenant_nuevo();

    let mut attrs = HashMap::new();
    attrs.insert("x".to_string(), DatomValue::Str("y".to_string()));
    let payload = TransactPayload {
        tenant_id: tenant,
        entity_id: None,
        entity_type: "entidad_inexistente_en_codice".to_string(),
        attrs,
        op: TransactOp::Create,
        suppress_events: false,
    };
    let err = w.transact(payload).await.expect_err("Debe rechazar");
    assert_eq!(err.code, crate::domain::errors::ErrorCode::Eav004);
}

#[tokio::test]
#[ignore]
async fn update_de_entidad_inexistente_falla_con_eav002() {
    let w = writer().await;
    let tenant = tenant_nuevo();

    let mut attrs = HashMap::new();
    attrs.insert("status".to_string(), DatomValue::Str("open".to_string()));
    let upd = TransactPayload {
        tenant_id: tenant,
        entity_id: Some("entidad_fantasma".to_string()),
        entity_type: "work_order".to_string(),
        attrs,
        op: TransactOp::Update,
        suppress_events: false,
    };

    let err = w
        .transact(upd)
        .await
        .expect_err("Un UPDATE sobre entidad inexistente NO puede hacer upsert");
    assert_eq!(err.code, crate::domain::errors::ErrorCode::Eav002);
}

#[tokio::test]
#[ignore]
async fn colision_de_tx_se_rechaza_y_el_historico_queda_intacto() {
    let w = writer().await;
    let tenant = tenant_nuevo();
    let res = w.transact(payload_nueva(&tenant)).await.expect("Create");
    let tx_create = res.tx_id;

    // Reutilizar el tx del CREATE fuerza la colisión: el assert del nuevo
    // valor caería sobre un SK (attr, tx, op) ya ocupado. La condición
    // append-only debe anular la TX completa con EAV_TX_004.
    let mut attrs = HashMap::new();
    attrs.insert("status".to_string(), DatomValue::Str("closed".to_string()));
    let upd = TransactPayload {
        tenant_id: tenant.clone(),
        entity_id: Some(res.entity_id.clone()),
        entity_type: "work_order".to_string(),
        attrs,
        op: TransactOp::Update,
        suppress_events: false,
    };
    let err = w
        .transact_with_tx(upd, Vec::new(), None, tx_create)
        .await
        .expect_err("La colisión de datom debe rechazarse, no sobrescribirse");
    assert_eq!(err.code, crate::domain::errors::ErrorCode::EavTx004);

    // El UPDATE falló entero: el estado vigente sigue siendo el del CREATE.
    let map = w
        .reader()
        .pull(&tenant, &res.entity_id, None)
        .await
        .expect("Pull tras colisión");
    assert_eq!(
        map.get("status"),
        Some(&DatomValue::Str("open".to_string())),
        "El UPDATE anulado no puede alterar el estado vigente"
    );
}

#[tokio::test]
#[ignore]
async fn update_preserva_el_historico_con_par_retract_assert() {
    let w = writer().await;
    let tenant = tenant_nuevo();
    let res = w.transact(payload_nueva(&tenant)).await.expect("Create");

    let mut attrs = HashMap::new();
    attrs.insert("status".to_string(), DatomValue::Str("closed".to_string()));
    let upd = TransactPayload {
        tenant_id: tenant.clone(),
        entity_id: Some(res.entity_id.clone()),
        entity_type: "work_order".to_string(),
        attrs,
        op: TransactOp::Update,
        suppress_events: false,
    };
    w.transact(upd).await.expect("Update");

    let entries = w
        .reader()
        .history(&tenant, &res.entity_id, Some("status"))
        .await
        .expect("History");

    let asserts: Vec<_> = entries.iter().filter(|e| e.op).collect();
    let retracts: Vec<_> = entries.iter().filter(|e| !e.op).collect();
    assert_eq!(asserts.len(), 2, "assert open + assert closed");
    assert_eq!(retracts.len(), 1, "un retract del valor open");
    assert!(
        !entries.is_empty() && entries.windows(2).all(|w| w[0].tx_id <= w[1].tx_id),
        "el historial queda ordenado por tx"
    );
    // El assert del valor "open" sobrevive: el histórico NO se sobrescribe.
    assert!(
        asserts
            .iter()
            .any(|e| e.value == Some(DatomValue::Str("open".to_string()))),
        "el valor original debe seguir consultable en el histórico"
    );
}
