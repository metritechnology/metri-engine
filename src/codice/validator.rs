// src/codice/validator.rs
// SRP: Validador estructural y semántico de payloads JSON contra los modelos Códice.
// Reemplaza la funcionalidad de `malli.clj` y `api.clj` (`validate-payload`) de Clojure.

use serde_json::Value;
use std::collections::HashMap;
use tracing::{error, info, warn};

use crate::codice::registry::{AttrType, EntityModel};
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::datom::DatomValue;

/// Valida un payload JSON contra el modelo de la entidad y convierte los valores a DatomValue.
pub fn validate_payload(
    model: &EntityModel,
    payload: &Value,
    tenant_id: &str,
    is_create: bool,
) -> Result<HashMap<String, DatomValue>, DomainError> {
    let obj = payload.as_object().ok_or_else(|| {
        DomainError::codice(
            ErrorCode::Cod001,
            format!(
                "Payload para '{}' no es un objeto JSON válido",
                model.entity
            ),
        )
    })?;

    let mut attrs = HashMap::new();
    let mut violations = Vec::new();

    for attr_desc in &model.attributes {
        let attr_name = &attr_desc.name;

        match obj.get(attr_name) {
            Some(val) => {
                if !val.is_null() {
                    // X-01: Validar enum options
                    if matches!(attr_desc.attr_type, AttrType::Enum)
                        && !attr_desc.options.is_empty()
                    {
                        if let Some(s) = val.as_str() {
                            if !attr_desc.options.iter().any(|o| o == s) {
                                violations.push(format!(
                                    "Campo '{}': Valor '{}' no permitido para enum. Opciones válidas: {}",
                                    attr_name, s, attr_desc.options.join(", ")
                                ));
                            }
                        }
                    }

                    // X-02: Validar validation_regex
                    if let Some(ref pattern) = attr_desc.validation_regex {
                        if let Some(s) = val.as_str() {
                            match regex::Regex::new(pattern) {
                                Ok(re) => {
                                    if !re.is_match(s) {
                                        violations.push(format!(
                                            "Campo '{}': Valor '{}' no cumple con el patrón requerido: {}",
                                            attr_name, s, pattern
                                        ));
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "Expresión regular inválida en modelo para campo '{}': {}",
                                        attr_name, e
                                    );
                                }
                            }
                        }
                    }

                    match map_to_datom_value(val, &attr_desc.attr_type) {
                        Ok(datom_val) => {
                            attrs.insert(attr_name.clone(), datom_val);
                        }
                        Err(e) => {
                            violations.push(format!("Campo '{}': {}", attr_name, e));
                        }
                    }
                } else if attr_desc.required {
                    violations.push(format!("Campo requerido '{}' no puede ser nulo", attr_name));
                }
            }
            None => {
                if is_create && attr_desc.required {
                    violations.push(format!("Campo requerido '{}' está ausente", attr_name));
                }
            }
        }
    }

    if !violations.is_empty() {
        warn!(
            "[Codice Validator] {} violaciones para la entidad {}",
            violations.len(),
            model.entity
        );
        return Err(DomainError::codice(
            ErrorCode::Cod001,
            format!("Violaciones de validación: {}", violations.join(" | ")),
        ));
    }

    Ok(attrs)
}

pub fn map_to_datom_value(val: &Value, attr_type: &AttrType) -> Result<DatomValue, String> {
    match attr_type {
        AttrType::String | AttrType::Enum => val
            .as_str()
            .map(|s| DatomValue::Str(s.to_string()))
            .ok_or_else(|| "Debe ser un texto (string)".to_string()),
        AttrType::Number | AttrType::Decimal => {
            if matches!(attr_type, AttrType::Number) {
                if let Some(i) = val.as_i64() {
                    Ok(DatomValue::Long(i))
                } else if let Some(f) = val.as_f64() {
                    Ok(DatomValue::Long(f as i64))
                } else {
                    Err("Debe ser un número entero".to_string())
                }
            } else {
                if let Some(f) = val.as_f64() {
                    Ok(DatomValue::Double(f))
                } else if let Some(i) = val.as_i64() {
                    Ok(DatomValue::Double(i as f64))
                } else {
                    Err("Debe ser un número decimal".to_string())
                }
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
        AttrType::Boolean => val
            .as_bool()
            .map(DatomValue::Bool)
            .ok_or_else(|| "Debe ser un booleano (true/false)".to_string()),
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
            if let Some(arr) = val.as_array() {
                let mut vec = Vec::new();
                for item in arr {
                    if let Some(s) = item.as_str() {
                        vec.push(s.to_string());
                    } else {
                        return Err("Referencia en array debe ser un string".to_string());
                    }
                }
                Ok(DatomValue::Array(vec))
            } else {
                val.as_str()
                    .map(|s| DatomValue::Str(s.to_string()))
                    .ok_or_else(|| {
                        "Referencia debe ser un string o un arreglo de strings".to_string()
                    })
            }
        }
        AttrType::Uuid => val
            .as_str()
            .map(|s| DatomValue::Uuid(s.to_string()))
            .ok_or_else(|| "Debe ser un UUID válido".to_string()),
        AttrType::Bytes => {
            Err("Mapeo de Bytes no implementado directamente desde JSON".to_string())
        }
        AttrType::Json => Ok(DatomValue::Str(val.to_string())),
        AttrType::Unknown(u) => Err(format!("Tipo desconocido en esquema: {}", u)),
    }
}

#[cfg(test)]
#[path = "tests/validator_tests.rs"]
mod tests;
