//! Normalizador de respuestas para solicitudes de exploración de facetas (`Explore`).
use crate::janus::normalizer::strategy::NormalizerStrategy;
use serde_json::{json, Value};

/// Estrategia de normalización para respuestas de exploración de valores (`Explore`).
pub struct ExploreNormalizer;

impl NormalizerStrategy for ExploreNormalizer {
    fn normalize(&self, body: &mut Value, _success: bool) {
        let Some(obj) = body.as_object_mut() else {
            return;
        };
        obj.entry("values").or_insert(json!([]));
    }
}
