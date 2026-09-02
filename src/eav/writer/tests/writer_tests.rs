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
    }
}

#[tokio::test]
#[ignore]
async fn create_escribe_y_el_pull_lo_devuelve() {
    let w = writer().await;
    let tenant = tenant_nuevo();

    let res = w.transact(payload_nueva(&tenant)).await.expect("Create");
    assert!(res.datoms >= 2, "Create debe escribir al menos los 2 atributos");
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
    assert_eq!(map.get("status"), Some(&DatomValue::Str("open".to_string())));
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
        .transact_bulk_deferred(payload_nueva(&tenant))
        .await
        .expect("Bulk deferred");
    assert!(res.datoms >= 2);

    let map = w
        .reader()
        .pull(&tenant, &res.entity_id, None)
        .await
        .expect("Pull tras bulk deferred");
    assert_eq!(map.get("status"), Some(&DatomValue::Str("open".to_string())));
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
    };
    let err = w.transact(payload).await.expect_err("Debe rechazar");
    assert_eq!(err.code, crate::domain::errors::ErrorCode::Eav004);
}
