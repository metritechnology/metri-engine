// [NUEVO — reemplaza: src/metri/infrastructure/datahike.clj + tenant_guard.clj]
// eav/writer/transact.rs — Write path ACID del motor EAV.
// Blueprint: Metri EAV - OLPT.md §IV — ACID Write Path
//
// En Clojure: d/transact → DynamoDB (blob monolítico via Datahike)
// En Rust:    TransactWriteItems con datoms EAVT/AEVT/AVET/VAET individuales
//
// Este módulo es el reemplazo directo de la corrupción de blobs de Datahike.
// Cada atributo es un datom independiente — sin contención bajo concurrencia.

use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::{
    AttributeValue, Put, TransactWriteItem,
};
use tracing::{info, warn};
use ulid::Ulid;

use crate::codice::CodeRegistry;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::{
    datom::{Datom, DatomValue},
    encoding::{build_eavt_sk, build_aevt_sk, build_avet_sk, build_vaet_sk},
    value_type::ValueType,
};
use crate::infrastructure::dynamodb::DynamoClient;

/// Contexto de una transacción EAV.
/// Equivale al `TransactPayload` implícito en el pipeline IOP de Clojure.
#[derive(Debug, Clone)]
pub struct TransactPayload {
    pub tenant_id:   String,
    pub entity_id:   Option<String>, // None = nuevo (se genera ULID)
    pub entity_type: String,
    pub attrs:       HashMap<String, DatomValue>, // nombre_attr → valor nuevo
    pub op:          TransactOp,
}

/// Operación de la transacción.
#[derive(Debug, Clone, PartialEq)]
pub enum TransactOp {
    /// Crear entidad nueva
    Create,
    /// Actualizar atributos existentes (genera retract + assert)
    Update,
    /// Retract completo (marca todos los atributos como inactivos)
    Delete,
}

/// Resultado de una transacción exitosa.
#[derive(Debug, Clone)]
pub struct TransactResult {
    pub entity_id: String,
    pub tx_id:     u64,
    pub datoms:    usize, // número de datoms escritos
}

/// EavWriter — motor de escritura ACID.
/// Reemplaza d/transact de Datahike con TransactWriteItems de DynamoDB.
pub struct EavWriter {
    ddb:      Arc<DynamoClient>,
    table:    String,
}

impl EavWriter {
    pub fn new(
        ddb:      Arc<DynamoClient>,
        table:    impl Into<String>,
    ) -> Self {
        EavWriter {
            ddb,
            table: table.into(),
        }
    }

    /// Ejecuta una transacción ACID.
    /// [PORTED_FROM: (d/transact conn tx-data) → DynamoDB blob]
    ///
    /// Proceso:
    /// 1. Generar TX_ID (ULID epoch ms monotónico)
    /// 2. Generar entity_id si es creación nueva
    /// 3. Para UPDATE: generar par retract+assert por cada atributo
    /// 4. Construir TransactWriteItems para EAVT + GSIs activos
    /// 5. Chunk a 100 items por transacción (DynamoDB limit)
    /// 6. Ejecutar TransactWriteItems
    pub async fn transact(
        &self,
        payload: TransactPayload,
    ) -> Result<TransactResult, DomainError> {
        // 1. Verificar entity_type en registry
        crate::codice::global().get_model(&payload.entity_type)
            .ok_or_else(|| DomainError::eav(
                ErrorCode::Eav004,
                format!("entity_type '{}' no en registry", payload.entity_type),
            ))?;

        // 2. Generar TX_ID — ULID epoch ms garantiza monotonía
        let tx_id = Ulid::new().timestamp_ms();

        // 3. Generar entity_id si es creación
        let entity_id = match &payload.entity_id {
            Some(id) => id.clone(),
            None     => Ulid::new().to_string(),
        };

        // 4. Construir datoms y FTS requests
        let mut datoms: Vec<Datom> = Vec::new();
        let mut fts_write_requests = Vec::new();

        for (attr_name, new_value) in &payload.attrs {
            let attr_desc = crate::codice::global()
                .get_attribute(&payload.entity_type, attr_name)
                .ok_or_else(|| DomainError::eav(
                    ErrorCode::Eav004,
                    format!("Atributo '{attr_name}' no en registry para '{}'", payload.entity_type),
                ))?;

            // Para UPDATE: añadir retract del valor anterior
            if payload.op == TransactOp::Update {
                let retract = Datom::retract(
                    &payload.tenant_id,
                    &entity_id,
                    attr_name,
                    Self::hash_attr_name(&attr_desc.name),
                    new_value.clone(),
                    tx_id - 1, // TX anterior
                );
                datoms.push(retract);
            }

            // Assert — el nuevo valor
            let assert = Datom::assert(
                &payload.tenant_id,
                &entity_id,
                attr_name,
                Self::hash_attr_name(&attr_desc.name),
                new_value.clone(),
                tx_id,
            );
            
            // Generar FTS si el atributo lo requiere
            if attr_desc.fts {
                let reqs = crate::eav::fts::trigram::build_fts_items(&assert, attr_desc);
                fts_write_requests.extend(reqs);
            }
            
            datoms.push(assert);
        }

        // 4.5. Inyectar el atributo de sistema `entity_type`
        let entity_type_datom = Datom::assert(
            &payload.tenant_id,
            &entity_id,
            "entity_type",
            Self::hash_attr_name("entity_type"),
            DatomValue::Str(payload.entity_type.clone()),
            tx_id,
        );
        datoms.push(entity_type_datom);

        // 5. Construir TransactWriteItems para la capa ACID
        let write_items = self.build_write_items(&datoms)?;
        let datom_count  = datoms.len();

        info!(
            "[EAV] transact entity_id={entity_id} tx_id={tx_id} datoms={} ACID_items={} FTS_items={}",
            datom_count, write_items.len(), fts_write_requests.len()
        );

        // 6. Lanzar FTS de forma asíncrona concurrente (Degraded Consistency)
        let mut fts_handles = Vec::new();
        for chunk in fts_write_requests.chunks(25) {
            let chunk_vec = chunk.to_vec();
            let ddb_clone = self.ddb.clone();
            let table_clone = self.table.clone();
            fts_handles.push(tokio::spawn(async move {
                ddb_clone.batch_write_item(&table_clone, chunk_vec).await
            }));
        }

        // 7. Ejecutar capa ACID (síncrona)
        self.ddb.transact_write(write_items).await?;

        // 8. Esperar a que terminen los FTS (no bloquean el ACID pero deben terminar antes de responder)
        for handle in fts_handles {
            if let Ok(Err(e)) = handle.await {
                warn!("[EAV] Error en escritura Batch FTS asíncrona: {e:?}");
            }
        }

        Ok(TransactResult {
            entity_id,
            tx_id,
            datoms: datom_count,
        })
    }

    /// Construye todos los TransactWriteItems para los 4 índices EAV.
    /// EAVT = tabla principal, AEVT+AVET+VAET = GSIs separados.
    ///
    /// Blueprint: §III — "Single Table Design con 4 GSIs canónicos"
    fn build_write_items(
        &self,
        datoms: &[Datom],
    ) -> Result<Vec<TransactWriteItem>, DomainError> {
        let mut items = Vec::with_capacity(datoms.len() * 4);

        for datom in datoms {
            // ── EAVT (tabla principal) ────────────────────────────────────────
            // PK = "T#<tenant>#E#<eid>"
            // SK = [attr_id:2B][tx_id:8B][op:1B] — binario
            let eavt_pk = datom.eavt_pk();
            let eavt_sk = build_eavt_sk(datom.attr_id, datom.tx_id, datom.op);
            let mut eavt_item = self.base_datom_attrs(datom);
            eavt_item.insert("PK".to_string(), AttributeValue::S(eavt_pk));
            eavt_item.insert("SK".to_string(), av_binary(eavt_sk));
            // GSI-AEVT projection keys
            eavt_item.insert("AEVT_PK".to_string(), AttributeValue::S(datom.aevt_pk()));
            eavt_item.insert("AEVT_SK".to_string(), av_binary(build_aevt_sk(&datom.entity_id, datom.tx_id)));
            // GSI-AVET projection keys (solo si el tipo es indexable)
            if datom.value.value_type().is_avet_indexable() {
                eavt_item.insert("AVET_PK".to_string(), AttributeValue::S(datom.avet_pk()));
                eavt_item.insert("AVET_SK".to_string(), av_binary(build_avet_sk(&datom.value, &datom.entity_id)));
            }
            // GSI-VAET (solo para Reference)
            if let Some(vaet_pk) = datom.vaet_pk() {
                eavt_item.insert("VAET_PK".to_string(), AttributeValue::S(vaet_pk));
                eavt_item.insert("VAET_SK".to_string(), av_binary(build_vaet_sk(datom.attr_id, &datom.entity_id)));
            }

            items.push(TransactWriteItem::builder()
                .put(Put::builder()
                    .table_name(&self.table)
                    .set_item(Some(eavt_item))
                    .build()
                    .map_err(|e| DomainError::eav(ErrorCode::Eav001, format!("Put builder error: {e}")))?)
                .build());
        }

        Ok(items)
    }

    /// Construye los atributos base comunes a todos los índices de un datom.
    fn base_datom_attrs(&self, datom: &Datom) -> HashMap<String, AttributeValue> {
        let mut map = HashMap::new();
        map.insert("tid".to_string(), AttributeValue::S(datom.tenant_id.clone()));
        map.insert("eid".to_string(), AttributeValue::S(datom.entity_id.clone()));
        map.insert("a".to_string(),   AttributeValue::S(datom.attr_name.clone()));
        map.insert("tx".to_string(),  AttributeValue::N(datom.tx_id.to_string()));
        map.insert("op".to_string(),  AttributeValue::Bool(datom.op));

        // Valor según el tipo del DatomValue
        // [BLUEPRINT: §II.3 — "v = tipo nativo DynamoDB según el tipo del attr"]
        match &datom.value {
            DatomValue::Str(s) | DatomValue::Uuid(s) => {
                map.insert("v".to_string(), AttributeValue::S(s.clone()));
            }
            DatomValue::Long(n) | DatomValue::Instant(n) => {
                map.insert("v".to_string(), AttributeValue::N(n.to_string()));
            }
            DatomValue::Double(d) => {
                map.insert("v".to_string(), AttributeValue::N(d.to_string()));
            }
            DatomValue::Bool(b) => {
                map.insert("v".to_string(), AttributeValue::Bool(*b));
            }
            DatomValue::Ref(eid) => {
                map.insert("v".to_string(), AttributeValue::N(eid.to_string()));
            }
            DatomValue::Array(arr) => {
                map.insert("v".to_string(), AttributeValue::S(
                    serde_json::to_string(arr).unwrap_or_default()
                ));
            }
            DatomValue::Bytes(b) => {
                map.insert("v".to_string(), av_binary(b.clone()));
            }
            DatomValue::BigInt(n) => {
                map.insert("v".to_string(), AttributeValue::N(n.to_string()));
            }
            DatomValue::Geo { lat, lon } => {
                map.insert("v".to_string(), AttributeValue::S(
                    format!("{lat},{lon}")
                ));
            }
            DatomValue::Null => {
                map.insert("v".to_string(), AttributeValue::Null(true));
            }
        }

        map
    }

    /// FASE 1: Genera un attr_id seudo-aleatorio basado en el nombre del atributo.
    /// Reemplaza la longitud para evitar colisiones de SK en DynamoDB.
    fn hash_attr_name(name: &str) -> u16 {
        let mut h = 5381u32;
        for b in name.as_bytes() {
            h = h.wrapping_mul(33).wrapping_add(*b as u32);
        }
        (h ^ (h >> 16)) as u16
    }
}

/// Helper: AttributeValue::B desde Vec<u8>
fn av_binary(bytes: Vec<u8>) -> AttributeValue {
    AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new(bytes))
}
