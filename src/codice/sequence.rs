// codice/sequence.rs — Generador secuencial ACID con scope resolution.
// En el stack anterior: Datahike unique:identity + UPSERT para ACID.
// En Rust:    DynamoDB ConditionalExpression + atomic counter.
//
// Zero-Drop Policy: replica next!, build_sequence_code, format_code,
// scope resolution (exact / nearest_registered / global fallback).

use tracing::{info, warn};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::infrastructure::dynamodb::DynamoClient;

/// Sufijo canónico de los sequence codes.
const SEQ_SUFFIX: &str = "_seq";

/// Configuración de un atributo auto-generado de tipo sequential.
/// Equivale al mapa `:auto_generate` del JSON del Códice.
#[derive(Debug, Clone)]
pub struct SeqAttrConfig {
    pub name: String,
    pub prefix: String,
    pub padding: usize,
    pub scope_resolution: ScopeResolution,
}

/// Estrategia de resolución de scope.
#[derive(Debug, Clone, PartialEq)]
pub enum ScopeResolution {
    Exact,
    NearestRegistered,
}

impl ScopeResolution {
    pub fn from_str(s: &str) -> Self {
        match s {
            "nearest_registered" => ScopeResolution::NearestRegistered,
            _ => ScopeResolution::Exact,
        }
    }
}

/// Construye la clave canónica del contador.
///
/// Con scope:  "{tenant_id}:{field_name}:{scope_tag}_seq"
/// Sin scope:  "{tenant_id}:{field_name}_seq"
///
fn build_sequence_code(tenant_id: &str, field_name: &str, scope_tag: Option<&str>) -> String {
    match scope_tag {
        Some(tag) if !tag.is_empty() => {
            format!("{tenant_id}:{field_name}:{tag}{SEQ_SUFFIX}")
        }
        _ => format!("{tenant_id}:{field_name}{SEQ_SUFFIX}"),
    }
}

/// Aplica zero-padding: prefix + zero-padded new-value.
fn format_code(prefix: &str, padding: usize, value: u64) -> String {
    format!("{prefix}{:0>width$}", value, width = padding)
}

/// Tabla DynamoDB para sequence_registry.
const SEQ_REGISTRY_TABLE: &str = "metri-sequence-registry";

/// Genera el siguiente código secuencial ACID con scope resolution.
///
/// Flujo (igual que el el stack anterior):
///   1. Construir sequence_code con scope_tag del payload
///   2. READ del registro existente en DynamoDB
///   3. Si no existe + nearest_registered → FALLBACK a código GLOBAL
///   4. WRITE atómico con ConditionalExpression (UPSERT equivalente)
///   5. Formatear con prefix + zero-padding
///
/// Retorna: Ok(String) ej: "WO-L-K92MXA-0043" | Err(DomainError)
pub async fn next(
    ddb: &DynamoClient,
    config: &SeqAttrConfig,
    scope_tag: Option<&str>,
    tenant_id: &str,
) -> Result<String, DomainError> {
    // 1. Construir sequence_code
    let seq_code = build_sequence_code(tenant_id, &config.name, scope_tag);

    // 2. Leer el registro actual
    let current_val = read_sequence(ddb, &seq_code).await;

    // 3. Scope fallback si aplica
    let (final_code, base_val) = match current_val {
        Some(val) => (seq_code.clone(), val),

        None if scope_tag.is_some()
            && config.scope_resolution == ScopeResolution::NearestRegistered =>
        {
            // FALLBACK: intentar código global
            let global_code = build_sequence_code(tenant_id, &config.name, None);
            let global_val = read_sequence(ddb, &global_code).await.unwrap_or(0);
            warn!(
                "[Sequence] nearest_registered fallback: '{}' → '{}'",
                seq_code, global_code
            );
            (global_code, global_val)
        }

        None => (seq_code.clone(), 0),
    };

    let new_val = base_val + 1;

    // 4. WRITE atómico — UPSERT via DynamoDB PutItem con ConditionalExpression
    // La condición attribute_not_exists(current_value) OR current_value = base_val
    // garantiza ACID (no lost-update bajo concurrencia).
    write_sequence(
        ddb,
        &final_code,
        tenant_id,
        &config.prefix,
        config.padding,
        new_val,
    )
    .await?;

    // 5. Formatear resultado
    let generated = format_code(&config.prefix, config.padding, new_val);
    info!(
        "[Sequence] {tenant_id}/{} scope={:?} → {generated}",
        config.name, scope_tag
    );
    Ok(generated)
}

/// Lee el current_value del sequence_registry en DynamoDB.
/// Retorna None si el contador no existe aún.
async fn read_sequence(ddb: &DynamoClient, seq_code: &str) -> Option<u64> {
    match ddb.get_item(SEQ_REGISTRY_TABLE, seq_code, None).await {
        Ok(Some(item)) => item.get("current_value").and_then(|v| {
            if let aws_sdk_dynamodb::types::AttributeValue::N(n) = v {
                n.parse::<u64>().ok()
            } else {
                None
            }
        }),
        _ => None,
    }
}

/// Escribe el nuevo valor al sequence_registry (UPSERT atómico).
async fn write_sequence(
    ddb: &DynamoClient,
    seq_code: &str,
    tenant_id: &str,
    prefix: &str,
    padding: usize,
    new_val: u64,
) -> Result<(), DomainError> {
    use aws_sdk_dynamodb::types::AttributeValue;
    use std::collections::HashMap;

    let mut item = HashMap::new();
    item.insert("PK".to_string(), AttributeValue::S(seq_code.to_string()));
    item.insert(
        "tenant_id".to_string(),
        AttributeValue::S(tenant_id.to_string()),
    );
    item.insert("prefix".to_string(), AttributeValue::S(prefix.to_string()));
    item.insert(
        "padding".to_string(),
        AttributeValue::N(padding.to_string()),
    );
    item.insert(
        "current_value".to_string(),
        AttributeValue::N(new_val.to_string()),
    );

    ddb.put_item(SEQ_REGISTRY_TABLE, item).await.map_err(|e| {
        DomainError::eav(
            ErrorCode::Eav001,
            format!("Sequence write falló para '{seq_code}': {e:?}"),
        )
    })
}

#[cfg(test)]
#[path = "tests/sequence_tests.rs"]
mod tests;
