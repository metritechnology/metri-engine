//! ACID sequential code generator with scope resolution.
//!
//! Generador secuencial ACID con scope resolution.
//!
//! # Origin
//! En el stack anterior: Datahike unique:identity + UPSERT para ACID.
//! En Rust:    DynamoDB ConditionalExpression + atomic counter.
//!
//! Zero-Drop Policy: replica next!, build_sequence_code, format_code,
//! scope resolution (exact / nearest_registered / global fallback).

use tracing::info;

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

/// Aplica zero-padding: prefix [+ segmento-] + número.
///
/// Sin segmento: "WO-0042". Con segmento (el `tag` de la location,
/// 'L-K92MXA'): "WO-L-K92MXA-0043" — el formato que declara el `pattern`
/// `^WO(-[A-Z0-9-]+)?-\d{4,6}$` del Códice. La rama anterior ignoraba el
/// scope al formatear: hasta una OT con ubicación salía "WO-0043".
fn format_code(prefix: &str, segment: Option<&str>, padding: usize, value: u64) -> String {
    match segment {
        Some(seg) if !seg.is_empty() => {
            format!("{prefix}{seg}-{:0>width$}", value, width = padding)
        }
        _ => format!("{prefix}{:0>width$}", value, width = padding),
    }
}

/// Tabla DynamoDB para sequence_registry.
/// El nombre viaja por env (`SEQUENCE_REGISTRY_TABLE`, IaC: MetriSequenceRegistryTable
/// en template.yaml) con el valor canónico como fallback: que el stack y el
/// binario no puedan divergir es lo que evita repetir el incidente de prod del
/// 2026-09 — tabla ausente, `inject` en Err, OT creada sin número.
fn sequence_registry_table() -> String {
    std::env::var("SEQUENCE_REGISTRY_TABLE")
        .unwrap_or_else(|_| "metri-sequence-registry".to_string())
}

/// Genera el siguiente código secuencial ACID con scope resolution.
///
/// Flujo:
///   1. Construir sequence_code con scope_tag del payload.
///   2. READ del registro existente — y si no existe y hay
///      `ancestor_scopes`, caminar del ancestro más cercano a la raíz:
///      el contador se HEREDA del primero que ya tenga actividad
///      (`nearest_registered`, doc del Códice: "el contador se hereda del
///      ancestro más cercano que ya tiene actividad").
///   3. Sin actividad en ninguna parte → contador GLOBAL, base 0.
///   4. WRITE atómico sobre el contador elegido (PutItem con
///      ConditionalExpression — no lost-update bajo concurrencia).
///   5. Formatear: prefix + [segmento-] + número con padding. El segmento es
///      el `tag` de la propia ubicación de la orden, venga el contador de
///      donde venga: "WO-L-K92MXA-0043".
///
/// Retorna: Ok(String) ej: "WO-0042" | "WO-L-K92MXA-0043" | Err(DomainError)
pub async fn next(
    ddb: &DynamoClient,
    config: &SeqAttrConfig,
    scope_tag: Option<&str>,
    tenant_id: &str,
    ancestor_scopes: &[String],
    segment: Option<&str>,
) -> Result<String, DomainError> {
    // 1. Contador propio + contadores ancestro (más cercano primero) + global.
    let own_code = build_sequence_code(tenant_id, &config.name, scope_tag);
    let mut candidates: Vec<String> = Vec::with_capacity(ancestor_scopes.len() + 1);
    if scope_tag.is_some() {
        candidates.push(own_code.clone());
    }
    for ancestor in ancestor_scopes {
        candidates.push(build_sequence_code(tenant_id, &config.name, Some(ancestor)));
    }

    // 2. El primer contador CON actividad es la base (nearest_registered).
    let mut base_val: Option<(String, u64)> = None;
    for candidate in &candidates {
        if let Some(val) = read_sequence(ddb, candidate).await {
            base_val = Some((candidate.clone(), val));
            break;
        }
    }

    // 3. Sin actividad en ninguna parte → contador global, base 0.
    let (final_code, base_val) = match base_val {
        Some((code, val)) => (code, val),
        None => {
            let global = build_sequence_code(tenant_id, &config.name, None);
            if let Some(val) = read_sequence(ddb, &global).await {
                (global, val)
            } else {
                (global, 0)
            }
        }
    };

    let new_val = base_val + 1;

    // 4. WRITE atómico sobre el contador elegido.
    write_sequence(
        ddb,
        &final_code,
        tenant_id,
        &config.prefix,
        config.padding,
        new_val,
    )
    .await?;

    // 5. Formatear resultado.
    let generated = format_code(&config.prefix, segment, config.padding, new_val);
    info!(
        "[Sequence] {tenant_id}/{} scope={:?} contador={} → {generated}",
        config.name, scope_tag, final_code
    );
    Ok(generated)
}

/// Lee el current_value del sequence_registry en DynamoDB.
/// Retorna None si el contador no existe aún.
async fn read_sequence(ddb: &DynamoClient, seq_code: &str) -> Option<u64> {
    match ddb
        .get_item(&sequence_registry_table(), seq_code, None)
        .await
    {
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

    ddb.put_item(&sequence_registry_table(), item)
        .await
        .map_err(|e| {
            DomainError::eav(
                ErrorCode::Eav001,
                format!("Sequence write falló para '{seq_code}': {e:?}"),
            )
        })
}

#[cfg(test)]
#[path = "tests/sequence_tests.rs"]
mod tests;
