// eav/index/vaet.rs — GSI-VAET: Valor(Ref)-Atributo-Entidad-TX (grafo inverso)
// Blueprint: Metri EAV §II.5

use crate::eav::registry::descriptor::AttributeDescriptor;
use crate::eav::types::datom::{Datom, DatomValue};
use crate::eav::types::encoding::build_vaet_sk;
use aws_sdk_dynamodb::types::{AttributeValue, Put};

/// Construye el Put para el GSI-VAET.
/// Solo aplica para datoms con value = DatomValue::Ref(_).
/// Retorna None si el atributo no es una referencia.
pub fn build_vaet_item(datom: &Datom, attr: &AttributeDescriptor) -> Option<Put> {
    if !attr.is_ref {
        return None;
    }

    let ref_eid = match &datom.value {
        DatomValue::Ref(r) => *r,
        _ => return None,
    };

    let pk = format!("T#{}#V#{}", datom.tenant_id, ref_eid);
    let sk = build_vaet_sk(datom.attr_id, &datom.entity_id);

    let mut item = std::collections::HashMap::new();
    item.insert("VAET_PK".to_string(), AttributeValue::S(pk));
    item.insert(
        "VAET_SK".to_string(),
        AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(sk)),
    );
    item.insert(
        "entity_id".to_string(),
        AttributeValue::S(datom.entity_id.clone()),
    );
    item.insert("tx".to_string(), AttributeValue::N(datom.tx_id.to_string()));
    item.insert("op".to_string(), AttributeValue::Bool(datom.op));

    Put::builder().set_item(Some(item)).build().ok()
}
