// src/codice/validator.rs
// SRP: Validador estructural y semántico de payloads JSON contra los modelos Códice.
// Reemplaza la funcionalidad de `malli.clj` y `api.clj` (`validate-payload`) de Clojure.

use std::collections::HashMap;
use serde_json::Value;
use tracing::{error, info, warn};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::codice::registry::{EntityModel, AttrType};
use crate::eav::types::datom::DatomValue;

/// Valida un payload JSON contra el modelo de la entidad y convierte los valores a DatomValue.
pub fn validate_payload(
    model: &EntityModel,
    payload: &Value,
    tenant_id: &str,
) -> Result<HashMap<String, DatomValue>, DomainError> {
    let obj = payload.as_object().ok_or_else(|| {
        DomainError::codice(
            ErrorCode::Cod001,
            format!("Payload para '{}' no es un objeto JSON válido", model.entity),
        )
    })?;

    let mut attrs = HashMap::new();
    let mut violations = Vec::new();

    for attr_desc in &model.attributes {
        let attr_name = &attr_desc.name;
        
        match obj.get(attr_name) {
            Some(val) if !val.is_null() => {
                match map_to_datom_value(val, &attr_desc.attr_type) {
                    Ok(datom_val) => {
                        attrs.insert(attr_name.clone(), datom_val);
                    }
                    Err(e) => {
                        violations.push(format!("Campo '{}': {}", attr_name, e));
                    }
                }
            }
            _ => {
                // Si no está presente o es null
                if attr_desc.required {
                    violations.push(format!("Campo requerido '{}' está ausente", attr_name));
                }
            }
        }
    }

    if !violations.is_empty() {
        warn!("[Codice Validator] {} violaciones para la entidad {}", violations.len(), model.entity);
        return Err(DomainError::codice(
            ErrorCode::Cod001,
            format!("Violaciones de validación: {}", violations.join(" | ")),
        ));
    }

    Ok(attrs)
}

fn map_to_datom_value(val: &Value, attr_type: &AttrType) -> Result<DatomValue, String> {
    match attr_type {
        AttrType::String | AttrType::Enum => {
            val.as_str()
                .map(|s| DatomValue::Str(s.to_string()))
                .ok_or_else(|| "Debe ser un texto (string)".to_string())
        }
        AttrType::Number | AttrType::Decimal => {
            if let Some(f) = val.as_f64() {
                Ok(DatomValue::Double(f))
            } else if let Some(i) = val.as_i64() {
                Ok(DatomValue::Long(i))
            } else {
                Err("Debe ser un número".to_string())
            }
        }
        AttrType::Epoch => {
            if let Some(i) = val.as_i64() {
                Ok(DatomValue::Instant(i))
            } else if let Some(s) = val.as_str() {
                s.parse::<i64>()
                    .map(|v| DatomValue::Instant(v))
                    .map_err(|_| "Epoch debe ser un entero válido".to_string())
            } else {
                Err("Debe ser un número entero (epoch ms)".to_string())
            }
        }
        AttrType::Boolean => {
            val.as_bool()
                .map(DatomValue::Bool)
                .ok_or_else(|| "Debe ser un booleano (true/false)".to_string())
        }
        AttrType::Array => {
            if let Some(arr) = val.as_array() {
                let mut vec = Vec::new();
                for item in arr {
                    if let Some(s) = item.as_str() {
                        vec.push(s.to_string());
                    } else {
                        return Err("Array solo soporta elementos string".to_string());
                    }
                }
                Ok(DatomValue::Array(vec))
            } else {
                Err("Debe ser un arreglo (array)".to_string())
            }
        }
        AttrType::Reference => {
            // Referencias ahora son UUIDs (ulid o uuid) según el sistema viejo
            val.as_str()
                .map(|s| DatomValue::Str(s.to_string()))
                .ok_or_else(|| "Referencia debe ser un string".to_string())
        }
        AttrType::Uuid => {
            val.as_str()
                .map(|s| DatomValue::Uuid(s.to_string()))
                .ok_or_else(|| "Debe ser un UUID válido".to_string())
        }
        AttrType::Bytes => {
            Err("Mapeo de Bytes no implementado directamente desde JSON".to_string())
        }
        AttrType::Unknown(u) => {
            Err(format!("Tipo desconocido en esquema: {}", u))
        }
    }
}
