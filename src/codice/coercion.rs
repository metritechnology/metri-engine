// codice/coercion.rs — Motor centralizado de coerción de tipos para Códice.
// SRP: Maneja conversiones y normalizaciones seguras de strings/tipos en payloads.

use crate::codice::registry::AttrType;
use serde_json::Value;

/// Coerce un valor JSON individual basándose en su tipo de atributo del Códice.
/// Retorna Some(nuevo_valor) si se requiere coerción y fue exitosa, o None.
pub fn coerce_value(val: &Value, attr_type: &AttrType) -> Option<Value> {
    match attr_type {
        AttrType::Epoch | AttrType::Number | AttrType::Decimal => {
            if let Value::String(s) = val {
                if let Ok(n) = s.parse::<i64>() {
                    Some(Value::Number(n.into()))
                } else if let Ok(f) = s.parse::<f64>() {
                    serde_json::Number::from_f64(f).map(Value::Number)
                } else {
                    None
                }
            } else {
                None
            }
        }
        AttrType::Boolean => {
            if let Value::String(s) = val {
                match s.to_lowercase().as_str() {
                    "true" => Some(Value::Bool(true)),
                    "false" => Some(Value::Bool(false)),
                    _ => None,
                }
            } else {
                None
            }
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "tests/coercion_tests.rs"]
mod tests;
