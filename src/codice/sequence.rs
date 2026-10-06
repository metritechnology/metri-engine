//! ACID sequential code generator with scope resolution.
//!
//! Generador secuencial ACID con scope resolution.
//!
//! # Origin
//! En el stack anterior: Datahike unique:identity + UPSERT para ACID.
//! En Rust:    DynamoDB ADD atómico sobre el contador.
//!
//! Zero-Drop Policy: replica next!, build_sequence_code, format_code,
//! scope resolution (exact / nearest_registered / global fallback).
//!
//! # Política de numeración del tenant
//! La config de formateo (prefix/padding/scope_resolution) del JSON del
//! Códice es el DEFAULT estático: la fila GLOBAL del contador
//! (`{tenant}:{field}_seq`, escrita SOLO por el RPC de configuración —
//! grpc/handlers/sequence_config.rs) manda sobre ella al mintear. Los
//! contadores por ubicación solo guardan el valor; la política es una
//! por tenant.

use tracing::info;

use crate::codice::registry::EntityModel;
use crate::domain::errors::DomainError;
use crate::infrastructure::dynamodb::{av_number, av_string, DynamoClient};

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

    /// Serialización canónica para la fila de política y el RPC.
    pub fn as_str(&self) -> &'static str {
        match self {
            ScopeResolution::Exact => "exact",
            ScopeResolution::NearestRegistered => "nearest_registered",
        }
    }
}

/// Política de numeración del tenant, leída de la fila GLOBAL del contador.
/// `None` = ese aspecto cae al default estático del Códice.
#[derive(Debug, Clone, Default)]
pub struct SequencePolicy {
    pub prefix: Option<String>,
    pub padding: Option<usize>,
    pub scope_resolution: Option<ScopeResolution>,
}

impl SeqAttrConfig {
    /// Merge: la fila global del tenant manda sobre la config del Códice.
    pub fn apply_policy(&mut self, policy: &SequencePolicy) {
        if let Some(prefix) = &policy.prefix {
            self.prefix = prefix.clone();
        }
        if let Some(padding) = policy.padding {
            self.padding = padding;
        }
        if let Some(scope_resolution) = &policy.scope_resolution {
            self.scope_resolution = scope_resolution.clone();
        }
    }
}

/// Parsea la config `sequential` declarada en el Códice para un atributo —
/// los defaults efectivos que el RPC de configuración expone a la UI
/// (SequenceDefaults) y que aplican cuando el tenant aún no tiene fila.
pub fn parse_seq_defaults(model: &EntityModel, field_name: &str) -> Option<SeqAttrConfig> {
    let attr = model.attributes.iter().find(|a| a.name == field_name)?;
    let auto_gen = attr.auto_generate.as_ref()?;
    if auto_gen.get("strategy").and_then(|v| v.as_str()) != Some("sequential") {
        return None;
    }
    Some(SeqAttrConfig {
        name: attr.name.clone(),
        prefix: auto_gen
            .get("prefix")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        padding: auto_gen
            .get("padding")
            .and_then(|v| v.as_u64())
            .unwrap_or(4) as usize,
        scope_resolution: ScopeResolution::from_str(
            auto_gen
                .get("scope_resolution")
                .and_then(|v| v.as_str())
                .unwrap_or("exact"),
        ),
    })
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

/// Clave del contador GLOBAL del tenant (la fila de política) — la usa el
/// RPC de configuración; el generador la compone internamente.
pub fn global_sequence_code(tenant_id: &str, field_name: &str) -> String {
    build_sequence_code(tenant_id, field_name, None)
}

/// Extrae el scope_tag de un PK scoped: `{tenant}:{field}:{tag}_seq` → tag.
/// `None` si el PK es la fila global.
pub fn scope_tag_from_pk(pk: &str, tenant_id: &str, field_name: &str) -> Option<String> {
    let prefix = format!("{tenant_id}:{field_name}:");
    let rest = pk.strip_prefix(&prefix)?;
    let tag = rest.strip_suffix(SEQ_SUFFIX)?;
    if tag.is_empty() {
        None
    } else {
        Some(tag.to_string())
    }
}

/// Aplica zero-padding: prefix [+ segmento-] + número.
///
/// Sin segmento: "WO-0042". Con segmento (el `tag` de la location,
/// 'L-K92MXA'): "WO-L-K92MXA-0043". El `pattern` del Códice ahora es
/// `^[A-Z0-9][A-Z0-9-]*\d{2,8}$` — tolera cualquier prefijo del tenant.
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
pub(crate) fn sequence_registry_table() -> String {
    std::env::var("SEQUENCE_REGISTRY_TABLE")
        .unwrap_or_else(|_| "metri-sequence-registry".to_string())
}

/// Fila cruda de un contador (el item de Dynamo, si existe).
async fn read_counter_item(
    ddb: &DynamoClient,
    seq_code: &str,
) -> Result<
    Option<std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>,
    DomainError,
> {
    ddb.get_item(&sequence_registry_table(), seq_code, None)
        .await
}

/// Lee la política de una fila (prefix/padding/scope_resolution), los tres
/// opcionales: las filas históricas del writer viejo los pineaban estáticos;
/// las nuevas los escribe solo el RPC de configuración.
fn policy_from_item(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> SequencePolicy {
    SequencePolicy {
        prefix: item
            .get("prefix")
            .and_then(|v| av_string(v))
            .map(String::from),
        padding: item
            .get("padding")
            .and_then(|v| av_number(v))
            .and_then(|n| n.parse::<usize>().ok()),
        scope_resolution: item
            .get("scope_resolution")
            .and_then(|v| av_string(v))
            .map(ScopeResolution::from_str),
    }
}

/// Lee la política GLOBAL del tenant para un campo secuencial. Fila ausente
/// → política vacía (todo cae al Códice). Un fallo de infraestructura sube:
/// Zero-Drop — el CREATE aborta antes que nacer sin número.
pub async fn read_global_policy(
    ddb: &DynamoClient,
    tenant_id: &str,
    field_name: &str,
) -> Result<SequencePolicy, DomainError> {
    let code = global_sequence_code(tenant_id, field_name);
    match read_counter_item(ddb, &code).await? {
        Some(item) => Ok(policy_from_item(&item)),
        None => Ok(SequencePolicy::default()),
    }
}

/// Parsea una fila de contador para el RPC de configuración:
/// (política, current_value), ambos opcionales — las filas históricas del
/// writer viejo no siempre traen todo.
pub(crate) fn parse_counter_item(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> (SequencePolicy, Option<i64>) {
    let policy = policy_from_item(item);
    let current_value = item
        .get("current_value")
        .and_then(|v| av_number(v))
        .and_then(|n| n.parse::<i64>().ok());
    (policy, current_value)
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
///   4. INCREMENTO atómico (`ADD current_value 1`, UpdateItem upsert) sobre
///      el contador elegido — sin read-modify-write, sin lost-update, y sin
///      pisar la política del tenant con la config estática.
///   5. Formatear con la config YA MERGEADA (política del tenant sobre el
///      Códice): prefix + [segmento-] + número con padding. El segmento es
///      el `tag` de la propia ubicación de la orden, venga el contador de
///      donde venga: "WO-L-K92MXA-0043".
///
/// `config` llega mergeada (ver `SeqAttrConfig::apply_policy`): el llamador
/// (generator::inject) leyó `read_global_policy` antes de invocar.
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
    //    Un fallo de lectura sube: mintear con base 0 fabricaría duplicados.
    let mut base: Option<(String, u64)> = None;
    for candidate in &candidates {
        if let Some(val) = read_sequence(ddb, candidate).await? {
            base = Some((candidate.clone(), val));
            break;
        }
    }

    // 3. Sin actividad en ninguna parte → contador global, base 0.
    let (final_code, base_val) = match base {
        Some(pair) => pair,
        None => {
            let global = build_sequence_code(tenant_id, &config.name, None);
            let val = read_sequence(ddb, &global).await?.unwrap_or(0);
            (global, val)
        }
    };

    // 4. INCREMENTO atómico sobre el contador elegido: el valor nuevo lo
    //    decide DynamoDB (`ADD current_value 1`), no este proceso — bajo
    //    concurrencia cada CREATE obtiene un número DISTINTO, sin
    //    read-modify-write ni lost-update, y sin pisar la política del
    //    tenant con la config estática.
    let new_val = ddb
        .sequence_increment(&sequence_registry_table(), &final_code, tenant_id)
        .await?;

    // 5. Formatear resultado con la config mergeada.
    let generated = format_code(&config.prefix, segment, config.padding, new_val as u64);
    info!(
        "[Sequence] {tenant_id}/{} scope={:?} contador={final_code} base={base_val} → {generated}",
        config.name, scope_tag
    );
    Ok(generated)
}

/// Lee el current_value del sequence_registry en DynamoDB.
/// Retorna Ok(None) si el contador no existe aún; un fallo de infra sube.
async fn read_sequence(ddb: &DynamoClient, seq_code: &str) -> Result<Option<u64>, DomainError> {
    Ok(read_counter_item(ddb, seq_code).await?.and_then(|item| {
        item.get("current_value").and_then(|v| {
            if let aws_sdk_dynamodb::types::AttributeValue::N(n) = v {
                n.parse::<u64>().ok()
            } else {
                None
            }
        })
    }))
}

#[cfg(test)]
#[path = "tests/sequence_tests.rs"]
mod tests;
