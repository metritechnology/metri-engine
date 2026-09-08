// eav/index/aevt.rs — GSI-AEVT: Atributo-Entidad-Valor-TX
// Blueprint: Metri EAV §II.3

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::datom::Datom;
use crate::eav::types::encoding::build_aevt_sk;
use aws_sdk_dynamodb::types::{AttributeValue, Put};

/// Construye el Put para el GSI-AEVT con soporte de sharding.
/// shard=0 → sin sharding; shard>0 → PK sufijo "#N"
pub fn build_aevt_item(datom: &Datom, entity_type: &str, shard: u8) -> Result<Put, DomainError> {
    let pk = if shard == 0 {
        format!("T#{}#A#{}", datom.tenant_id, entity_type)
    } else {
        format!("T#{}#A#{}#{}", datom.tenant_id, entity_type, shard)
    };

    let sk = build_aevt_sk(&datom.entity_id, datom.tx_id);

    let mut item = std::collections::HashMap::new();
    item.insert("AEVT_PK".to_string(), AttributeValue::S(pk));
    item.insert(
        "AEVT_SK".to_string(),
        AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(sk)),
    );
    item.insert(
        "entity_id".to_string(),
        AttributeValue::S(datom.entity_id.clone()),
    );
    item.insert("tx".to_string(), AttributeValue::N(datom.tx_id.to_string()));
    item.insert("op".to_string(), AttributeValue::Bool(datom.op));

    Put::builder().set_item(Some(item)).build().map_err(|e| {
        DomainError::eav(
            ErrorCode::Eav001,
            format!("AEVT put build failed para '{entity_type}': {e}"),
        )
    })
}
