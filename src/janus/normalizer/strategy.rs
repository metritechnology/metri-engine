use serde_json::Value;

pub trait NormalizerStrategy {
    fn normalize(&self, body: &mut Value, success: bool);
}
