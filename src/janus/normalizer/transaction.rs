use crate::janus::normalizer::strategy::NormalizerStrategy;
use serde_json::{json, Value};

pub struct TransactionNormalizer;

impl NormalizerStrategy for TransactionNormalizer {
    fn normalize(&self, body: &mut Value, _success: bool) {
        let Some(obj) = body.as_object_mut() else {
            return;
        };
        // entity_id siempre string
        let eid = obj
            .get("entity_id")
            .or_else(|| obj.get("entity-id"))
            .or_else(|| obj.get("result").and_then(|r| r.get("entity_id")))
            .or_else(|| obj.get("result").and_then(|r| r.get("entity-id")))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        obj.insert("entity_id".to_string(), json!(eid));
    }
}
