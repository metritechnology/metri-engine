//! Normalizadores de respuestas para evaluación de reglas de enrutamiento (`MatchRoutingRules`).
use crate::janus::normalizer::strategy::NormalizerStrategy;
use serde_json::{json, Value};

/// Estrategia de normalización para respuestas individuales de evaluación de reglas (`MatchRoutingRules`).
pub struct MatchRulesNormalizer;

impl NormalizerStrategy for MatchRulesNormalizer {
    fn normalize(&self, body: &mut Value, _success: bool) {
        let Some(obj) = body.as_object_mut() else {
            return;
        };
        obj.entry("matched_rules").or_insert(json!([]));
    }
}

/// Estrategia de normalización para respuestas por lotes de evaluación de reglas (`MatchRoutingRulesBatch`).
pub struct MatchRulesBatchNormalizer;

impl NormalizerStrategy for MatchRulesBatchNormalizer {
    fn normalize(&self, body: &mut Value, _success: bool) {
        let Some(obj) = body.as_object_mut() else {
            return;
        };
        obj.entry("responses").or_insert(json!([]));
    }
}
