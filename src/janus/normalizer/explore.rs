use crate::janus::normalizer::strategy::NormalizerStrategy;
use serde_json::{json, Value};

pub struct ExploreNormalizer;

impl NormalizerStrategy for ExploreNormalizer {
    fn normalize(&self, body: &mut Value, _success: bool) {
        let Some(obj) = body.as_object_mut() else {
            return;
        };
        obj.entry("values").or_insert(json!([]));
    }
}
