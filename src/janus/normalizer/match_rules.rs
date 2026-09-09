//! Normalizer for MatchRoutingRulesBatch requests.
use crate::janus::normalizer::strategy::NormalizerStrategy;
use serde_json::{json, Value};

pub struct MatchRulesNormalizer;

impl NormalizerStrategy for MatchRulesNormalizer {
    fn normalize(&self, body: &mut Value, _success: bool) {
        let Some(obj) = body.as_object_mut() else {
            return;
        };
        obj.entry("matched_rules").or_insert(json!([]));
    }
}

pub struct MatchRulesBatchNormalizer;

impl NormalizerStrategy for MatchRulesBatchNormalizer {
    fn normalize(&self, body: &mut Value, _success: bool) {
        let Some(obj) = body.as_object_mut() else {
            return;
        };
        obj.entry("responses").or_insert(json!([]));
    }
}
