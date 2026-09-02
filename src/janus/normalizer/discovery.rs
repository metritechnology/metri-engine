use crate::janus::normalizer::strategy::NormalizerStrategy;
use serde_json::{json, Value};

pub struct DiscoveryNormalizer;

impl NormalizerStrategy for DiscoveryNormalizer {
    fn normalize(&self, body: &mut Value, _success: bool) {
        let Some(obj) = body.as_object_mut() else {
            return;
        };
        let schemas = obj.get("schemas").cloned().unwrap_or(Value::Null);
        if schemas.is_null()
            || (schemas.is_array()
                && schemas.as_array().map(|a| a.is_empty()).unwrap_or(false)
                && !obj.contains_key("schemas"))
        {
            obj.insert("schemas".to_string(), json!([]));
        }
        if obj.get("has_next").map(|v| v.is_null()).unwrap_or(true) {
            obj.insert("has_next".to_string(), json!(false));
        }
        if obj.get("next_cursor").map(|v| v.is_null()).unwrap_or(true) {
            obj.insert("next_cursor".to_string(), json!(""));
        }
    }
}
