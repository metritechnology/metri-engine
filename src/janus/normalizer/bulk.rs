//! Normalizer for BulkIngest requests.
use crate::janus::normalizer::strategy::NormalizerStrategy;
use serde_json::{json, Value};

pub struct BulkNormalizer;

impl NormalizerStrategy for BulkNormalizer {
    fn normalize(&self, body: &mut Value, _success: bool) {
        let Some(obj) = body.as_object_mut() else {
            return;
        };
        let count = obj
            .get("ingested_count")
            .or_else(|| obj.get("result").and_then(|r| r.get("ingested_count")))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let outbox = obj
            .get("outbox_count")
            .or_else(|| obj.get("result").and_then(|r| r.get("outbox_count")))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        obj.insert("ingested_count".to_string(), json!(count));
        obj.insert("outbox_count".to_string(), json!(outbox));
    }
}
