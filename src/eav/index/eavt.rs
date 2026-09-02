// eav/index/eavt.rs
// GSI helpers para el índice EAVT (tabla principal).
// Blueprint: Metri EAV §MÓDULO 5 + §II.2

use crate::eav::types::datom::Datom;
use crate::eav::types::encoding::build_eavt_sk;
use aws_sdk_dynamodb::types::{AttributeValue, Put};

/// Construye el Put para el índice EAVT (tabla principal).
pub fn build_eavt_item(datom: &Datom, table: &str) -> Result<Put, String> {
    let mut item = std::collections::HashMap::new();
    item.insert("PK".to_string(), AttributeValue::S(datom.eavt_pk()));
    item.insert(
        "SK".to_string(),
        AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(build_eavt_sk(
            datom.attr_id,
            datom.tx_id,
            datom.op,
        ))),
    );
    item.insert("a".to_string(), AttributeValue::S(datom.attr_name.clone()));
    item.insert("tx".to_string(), AttributeValue::N(datom.tx_id.to_string()));
    item.insert("op".to_string(), AttributeValue::Bool(datom.op));
    item.insert("et".to_string(), AttributeValue::S("entity".to_string())); // entity_type

    Put::builder()
        .table_name(table)
        .set_item(Some(item))
        .build()
        .map_err(|e| e.to_string())
}
