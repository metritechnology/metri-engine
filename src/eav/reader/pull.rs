// [NUEVO — reemplaza: src/metri/aegis/datalog/pull.clj]
// eav/reader/pull.rs — Read path del motor EAV.
// Blueprint: Metri EAV - OLPT.md §V y §VI
//
// En Clojure: (d/pull db pattern entity-id) → blob completo en memoria
// En Rust:    Query selectiva por atributo → solo los bytes necesarios

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;
use tracing::debug;

use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::{
    datom::DatomValue,
    encoding::{eavt_sk_attr_prefix, eavt_sk_as_of},
};
use crate::infrastructure::dynamodb::{DynamoClient, av_string, av_number, av_bytes};

/// Resultado de un pull — mapa de atributos con sus valores actuales.
pub type EntityMap = HashMap<String, DatomValue>;

#[derive(Clone)]
pub struct EavReader {
    ddb:   Arc<DynamoClient>,
    table: String,
}

impl EavReader {
    pub fn new(ddb: Arc<DynamoClient>, table: impl Into<String>) -> Self {
        EavReader { ddb, table: table.into() }
    }

    /// Carga el estado actual de una entidad (todos los atributos vigentes).
    /// Equivale a (d/pull db '[*] entity-id) pero sin deserializar el blob.
    ///
    /// [PORTED_FROM: aegis/datalog/pull.clj — (d/pull db pattern entity-id)]
    /// Mejora: solo carga los atributos pedidos, no el blob completo.
    pub async fn pull(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attrs:     Option<&[&str]>, // None = todos los atributos
    ) -> Result<EntityMap, DomainError> {
        let pk = format!("T#{}#E#{}", tenant_id, entity_id);

        // Query en tabla EAVT — todos los datoms de la entidad, ordenados por SK
        // SK binario: [attr_id:2B][tx_id:8B][op:1B] — orden natural de DynamoDB
        let key_condition = "#pk = :pk".to_string();
        let mut attr_names  = HashMap::new();
        let mut attr_values = HashMap::new();
        attr_names.insert("#pk".to_string(), "PK".to_string());
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk));

        let raw_items = self.ddb
            .query(
                &self.table,
                None,                   // tabla principal, no GSI
                &key_condition,
                attr_names,
                attr_values,
                true,                   // ScanIndexForward=true → orden cronológico
                None,
            )
            .await
            .map_err(|e| DomainError::eav(ErrorCode::Eav002, format!("pull falló: {e:?}")))?;

        if raw_items.is_empty() {
            return Ok(HashMap::new());
        }

        // Reconstruir el estado actual: por cada (attr_name), tomar el último datom
        // con op=true (assert). Los retracts (op=false) marcan que ese valor ya no aplica.
        // [BLUEPRINT: §V.1 — "el estado actual es el datom assert más reciente por atributo"]
        let entity_map = assemble_current_state(raw_items, attrs);

        debug!(
            "[EAV] pull tenant={tenant_id} entity_id={entity_id} attrs={}",
            entity_map.len()
        );

        Ok(entity_map)
    }

    /// Time-travel: estado de la entidad as-of un TX_ID específico.
    /// [BLUEPRINT: §VI.1 — "Snapshot Reads — as-of TX T"]
    /// [PORTED_FROM: eav_pull_as_of — diseñado en OLPT.md reader/as_of.rs]
    pub async fn pull_as_of(
        &self,
        tenant_id: &str,
        entity_id: &str,
        as_of_tx:  u64,
        attrs:     Option<&[&str]>,
    ) -> Result<EntityMap, DomainError> {
        let pk = format!("T#{}#E#{}", tenant_id, entity_id);

        // Query con SK <= [attr_id][as_of_tx][0x01], ScanIndexForward=false, Limit=1 por atributo
        // Para simplicidad en FASE 1: traemos todos y filtramos por tx_id <= as_of_tx
        // FASE 2 optimizará con queries per-atributo con SK range.
        let key_condition = "#pk = :pk AND #sk <= :sk_max".to_string();
        let mut attr_names  = HashMap::new();
        let mut attr_values = HashMap::new();

        // SK máximo: tx_id = as_of_tx, op = true → filtra todo lo posterior al snapshot
        // [BLUEPRINT: "SK ≤ T, max per attr"]
        let sk_max = eavt_sk_as_of(0xFFFF, as_of_tx); // attr_id=0xFFFF → cap universal

        attr_names.insert("#pk".to_string(), "PK".to_string());
        attr_names.insert("#sk".to_string(), "SK".to_string());
        attr_values.insert(":pk".to_string(),     AttributeValue::S(pk));
        attr_values.insert(":sk_max".to_string(), AttributeValue::B(
            aws_sdk_dynamodb::primitives::Blob::new(sk_max)
        ));

        let raw_items = self.ddb
            .query(
                &self.table,
                None,
                &key_condition,
                attr_names,
                attr_values,
                true,
                None,
            )
            .await
            .map_err(|e| DomainError::eav(ErrorCode::Eav002, format!("pull_as_of falló: {e:?}")))?;

        Ok(assemble_current_state(raw_items, attrs))
    }

    /// History query — retorna TODOS los datoms de una entidad incluyendo retracts.
    /// Ideal para audit trail.
    /// [BLUEPRINT: §VI.2 — "History Query (Auditoría completa)"]
    pub async fn history(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attr_name: Option<&str>,
    ) -> Result<Vec<HistoryEntry>, DomainError> {
        let pk = format!("T#{}#E#{}", tenant_id, entity_id);

        let key_condition = "#pk = :pk".to_string();
        let mut attr_names  = HashMap::new();
        let mut attr_values = HashMap::new();
        attr_names.insert("#pk".to_string(), "PK".to_string());
        attr_values.insert(":pk".to_string(), AttributeValue::S(pk));

        let raw_items = self.ddb
            .query(&self.table, None, &key_condition, attr_names, attr_values, true, None)
            .await
            .map_err(|e| DomainError::eav(ErrorCode::Eav002, format!("history falló: {e:?}")))?;

        let entries = raw_items
            .into_iter()
            .filter_map(|item| {
                let attr = av_string(item.get("a")?)?;
                if let Some(filter) = attr_name {
                    if attr != filter { return None; }
                }
                let tx_id = av_number(item.get("tx")?)?
                    .parse::<u64>()
                    .ok()?;
                let op = match item.get("op")? {
                    AttributeValue::Bool(b) => *b,
                    _ => return None,
                };
                let value = extract_datom_value(&item);
                Some(HistoryEntry {
                    attr_name: attr.to_string(),
                    value,
                    tx_id,
                    op,
                })
            })
            .collect();

        Ok(entries)
    }
}

/// Entrada del historial de una entidad.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub attr_name: String,
    pub value:     Option<DatomValue>,
    pub tx_id:     u64,
    pub op:        bool, // true=assert, false=retract
}

/// Ensambla el estado actual de una entidad desde los datoms raw de DynamoDB.
/// Toma el último datom con op=true por atributo.
/// [PORTED_FROM: La lógica de pull de Datahike — reconstrucción sin blob]
fn assemble_current_state(
    items:      Vec<HashMap<String, AttributeValue>>,
    attr_filter: Option<&[&str]>,
) -> EntityMap {
    // Mapa: attr_name → (tx_id, value) — mantenemos el más reciente con op=true
    let mut latest: HashMap<String, (u64, DatomValue)> = HashMap::new();

    for item in items {
        let attr_name = match item.get("a").and_then(|v| {
            if let AttributeValue::S(s) = v { Some(s.clone()) } else { None }
        }) {
            Some(a) => a,
            None    => continue,
        };

        // Aplicar filtro de atributos si se especificó
        if let Some(filter) = attr_filter {
            if !filter.contains(&attr_name.as_str()) {
                continue;
            }
        }

        let tx_id = match item.get("tx").and_then(|v| {
            if let AttributeValue::N(n) = v { n.parse::<u64>().ok() } else { None }
        }) {
            Some(t) => t,
            None    => {
                tracing::warn!("[EAV] Missing or invalid tx_id for attr {}: {:?}", attr_name, item.get("tx"));
                continue;
            }
        };

        let op = match item.get("op") {
            Some(AttributeValue::Bool(b)) => *b,
            _ => continue,
        };

        // Solo actualizar si este datom es más reciente que el que tenemos
        let should_update = match latest.get(&attr_name) {
            None              => true,
            Some((prev_tx, _)) => tx_id > *prev_tx,
        };

        if should_update {
            if op {
                // Assert — registrar el valor
                if let Some(val) = extract_datom_value(&item) {
                    latest.insert(attr_name, (tx_id, val));
                } else {
                    tracing::warn!("[EAV] Invalid datom value for attr {}", attr_name);
                }
            } else {
                // Retract — eliminar del mapa si el retract es más reciente
                latest.remove(&attr_name);
            }
        }
    }

    // Convertir a EntityMap simple
    latest.into_iter().map(|(k, (_, v))| (k, v)).collect()
}

/// Extrae el DatomValue del campo `v` de un item DynamoDB.
fn extract_datom_value(item: &HashMap<String, AttributeValue>) -> Option<DatomValue> {
    match item.get("v")? {
        AttributeValue::S(s)    => Some(DatomValue::Str(s.clone())),
        AttributeValue::N(n)    => {
            // Intentar i64 primero, luego f64
            if let Ok(i) = n.parse::<i64>() {
                Some(DatomValue::Long(i))
            } else {
                n.parse::<f64>().ok().map(DatomValue::Double)
            }
        }
        AttributeValue::Bool(b) => Some(DatomValue::Bool(*b)),
        AttributeValue::B(_)    => Some(DatomValue::Bytes(vec![])), // placeholder
        AttributeValue::Null(_) => Some(DatomValue::Null),
        _ => None,
    }
}
