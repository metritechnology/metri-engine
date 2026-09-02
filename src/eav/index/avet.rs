// eav/index/avet.rs — GSI-AVET: Atributo-Valor-Entidad-TX
// Blueprint: Metri EAV §II.4

use crate::eav::registry::descriptor::AttributeDescriptor;
use crate::eav::types::datom::Datom;
use crate::eav::types::encoding::build_avet_sk;
use aws_sdk_dynamodb::types::{AttributeValue, Put};

/// Construye el Put para el GSI-AVET.
/// Retorna None si el atributo no es indexable (no genera item AVET).
pub fn build_avet_item(datom: &Datom, attr: &AttributeDescriptor) -> Option<Put> {
    if !attr.is_avet_indexable() {
        return None;
    }

    let pk = format!("T#{}#AV#{}", datom.tenant_id, datom.attr_name);
    let sk = build_avet_sk(&datom.value, &datom.entity_id);

    let mut item = std::collections::HashMap::new();
    item.insert("AVET_PK".to_string(), AttributeValue::S(pk));
    item.insert(
        "AVET_SK".to_string(),
        AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(sk)),
    );
    item.insert(
        "entity_id".to_string(),
        AttributeValue::S(datom.entity_id.clone()),
    );
    item.insert("tx".to_string(), AttributeValue::N(datom.tx_id.to_string()));

    Put::builder().set_item(Some(item)).build().ok()
}
