// eav/writer/optimistic.rs
// Optimistic Locking vía ConditionExpression de DynamoDB.
// Blueprint: Metri EAV §XIII.2

use aws_sdk_dynamodb::types::{AttributeValue, ConditionCheck, TransactWriteItem};
use crate::eav::types::encoding::build_eavt_sk;

const SYS_VERSION_ATTR_ID: u16 = 0x0001;

/// Construye un ConditionCheck que valida que la versión de la entidad
/// no ha cambiado desde que el cliente la leyó.
///
/// Si la condición falla → DynamoDB rechaza TODA la transacción
/// → el cliente recibe EAV_TX_003: ConcurrentModification.
///
/// [Blueprint: §XIII.2 — "TX-Version Optimistic Locking vía ConditionExpression"]
pub fn build_version_condition_check(
    tenant_id:        &str,
    entity_id:        &str,
    expected_version: u64,
    table_name:       &str,
) -> TransactWriteItem {
    let pk = format!("T#{}#E#{}", tenant_id, entity_id);
    // SK apunta al attr SYS_VERSION con el tx_id más alto conocido
    let sk = build_eavt_sk(SYS_VERSION_ATTR_ID, u64::MAX, true);

    let check = ConditionCheck::builder()
        .table_name(table_name)
        .key("PK", AttributeValue::S(pk))
        .key("SK", AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(sk)))
        // El valor almacenado en "v" debe coincidir con la versión esperada
        .condition_expression("v = :expected_v")
        .expression_attribute_values(
            ":expected_v",
            AttributeValue::N(expected_version.to_string()),
        )
        .build()
        .expect("ConditionCheck build never fails with valid inputs");

    TransactWriteItem::builder()
        .condition_check(check)
        .build()
}
