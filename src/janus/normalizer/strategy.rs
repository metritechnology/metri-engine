//! Normalizer strategy trait — one implementation per RPC shape.
use serde_json::Value;

pub trait NormalizerStrategy {
    fn normalize(&self, body: &mut Value, success: bool);
}
