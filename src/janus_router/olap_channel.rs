// janus_router/olap_channel.rs — OLAPChannel — Canal OLAP Columnar Nativo.
//
// Arquitectura: Columnar Nativo (sin Raw Zone genérica).
// Cada entidad OLAP → stream Firehose dedicado:
//   meter_reading  → <prefix>-meter-reading
//   audit_log      → <prefix>-audit-log
//
// Solo acepta BulkIngest (data=[...]). Bloquea rpc Transact con JNS_OLAP_001.
//
// Pipeline:
//   1. Guard: data != nil (solo BulkIngest)
//   2. Lookup atributos del Códice para coerción de tipos
//   3. Por cada registro: coerce numéricos + decorate (ULID + metadatos)
//   4. Emit al stream Firehose de la entidad
//
// La coerción de tipos usa AttrType del Códice para garantizar que los campos
// numéricos (epoch/number/decimal) sean nativos al serializarse en Parquet/Iceberg.

use chrono::Utc;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::codice::global as codice_global;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::iop::core::IopContext;
use crate::janus_router::router::IWriteChannel;
use crate::janus_router::ulid;

use crate::domain::protocols::IStreamWriter;
use std::sync::Arc;

// ── OlapChannel ───────────────────────────────────────────────────────────────

/// Canal de escritura OLAP vía Kinesis Firehose (Columnar Nativo).
pub struct OlapChannel {
    /// Prefijo del stream Firehose — ej: "metri-olap-stream"
    stream_prefix: String,
    stream_writer: Arc<dyn IStreamWriter>,
}

impl OlapChannel {
    pub fn new(stream_prefix: impl Into<String>, stream_writer: Arc<dyn IStreamWriter>) -> Self {
        let stream_prefix = stream_prefix.into();
        info!("[OlapChannel] Columnar Nativo activo | prefix: {stream_prefix}");
        Self {
            stream_prefix,
            stream_writer,
        }
    }

    /// Nombre del stream Firehose para la entidad.
    /// underscore → hyphen: meter_reading → <prefix>-meter-reading
    fn stream_name(&self, entity_type: &str) -> String {
        format!("{}-{}", self.stream_prefix, entity_type.replace('_', "-"))
    }
}

#[async_trait::async_trait]
impl IWriteChannel for OlapChannel {
    /// Enruta BulkIngest al stream Firehose de la entidad.
    /// Bloquea rpc Transact (data = None).
    ///
    async fn route(&self, ctx: IopContext) -> Result<Value, DomainError> {
        let entity_type = &ctx.entity_type;
        let tenant_id = &ctx.tenant_id;

        // Guard: rpc Transact (data = nil) → JNS_OLAP_001
        let data_opt = ctx.request.get("data");
        if data_opt.is_none() {
            warn!(
                entity = %entity_type,
                tenant = %tenant_id,
                "[OlapChannel] rpc Transact bloqueado — usar BulkIngest"
            );
            return Err(DomainError::janus(
                ErrorCode::JnsOlap001,
                format!(
                    "rpc Transact prohibido para engine:olap en '{entity_type}' — usar BulkIngest"
                ),
            ));
        }

        let records: Vec<Value> = data_opt
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let stream_name = self.stream_name(entity_type);
        let created_at = Utc::now().timestamp_millis();

        // Lookup atributos del Códice para coerción de tipos.
        let registry = codice_global();
        let attributes = registry.get_attributes(entity_type);

        info!(
            stream = %stream_name,
            count  = records.len(),
            tenant = %tenant_id,
            "[OlapChannel] Iniciando ingestión OLAP"
        );

        let mut ingested = 0usize;

        // Procesar en chunks de 50 para evitar saturar el pool de conexiones del servidor LocalStack/AWS
        for chunk in records.chunks(50) {
            let mut tasks = Vec::new();

            for record in chunk {
                let record_ulid = ulid::generate();

                // 1. Coerción de tipos numéricos (string → number).
                let coerced = coerce_numeric_fields(record.clone(), attributes);

                // 2. Aplanar: campos de dominio + metadatos del sistema.
                let decorated = decorate_record(coerced, tenant_id, created_at, &record_ulid);

                // FASE 4: Real Firehose PutRecord
                let payload_bytes = serde_json::to_vec(&decorated).map_err(|e| {
                    DomainError::janus(
                        ErrorCode::Jns001,
                        format!("Error al serializar registro OLAP a JSON: {e}"),
                    )
                })?;

                let writer = Arc::clone(&self.stream_writer);
                let stream_name_clone = stream_name.clone();
                tasks.push(async move {
                    writer
                        .put_record(&stream_name_clone, &record_ulid, payload_bytes)
                        .await
                });
            }

            let results = futures::future::join_all(tasks).await;
            for res in results {
                res?;
                ingested += 1;
            }
        }

        info!(
            stream   = %stream_name,
            ingested = ingested,
            tenant   = %tenant_id,
            "[OlapChannel] ✅ Ingestión OLAP exitosa"
        );

        Ok(json!({
            "ingested_count": ingested,
            "outbox_count":   0,
            "stream":         stream_name,
            "entity_type":    entity_type,
            "tenant_id":      tenant_id,
            "channel":        "olap",
        }))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Coerce campos numéricos del Códice que lleguen como strings.
/// Crítico para Parquet/Iceberg: los tipos deben coincidir exactamente.
///
/// Tipos numéricos: Number | Decimal | Epoch
/// Estrategia: si el valor es String y el tipo es numérico → parsear a f64 o i64.
///
fn coerce_numeric_fields(
    record: Value,
    attributes: Option<&[crate::codice::registry::AttributeDescriptor]>,
) -> Value {
    let Some(attrs) = attributes else {
        return record;
    };
    let Some(obj) = record.as_object() else {
        return record;
    };

    let mut out = obj.clone();

    for attr in attrs {
        if let Some(val) = out.get(&attr.name) {
            if let Some(coerced_val) = crate::codice::coercion::coerce_value(val, &attr.attr_type) {
                out.insert(attr.name.clone(), coerced_val);
            }
        }
    }

    Value::Object(out)
}

/// Aplana el payload con metadatos del sistema como campos nativos.
/// Produce mapa plano compatible con el esquema Iceberg de la entidad.
///
/// Columnas del sistema inyectadas:
///   id         → ULID del registro (clave única Iceberg)
///   _tenant    → tenant_id (columna de partición)
///   created_at → epoch-ms server-side
///
fn decorate_record(record: Value, tenant_id: &str, created_at: i64, record_ulid: &str) -> Value {
    let mut obj = match record {
        Value::Object(m) => m,
        other => {
            let mut m = serde_json::Map::new();
            m.insert("_raw".to_string(), other);
            m
        }
    };

    obj.insert("id".to_string(), Value::String(record_ulid.to_string()));
    obj.insert("_tenant".to_string(), Value::String(tenant_id.to_string()));
    obj.insert("created_at".to_string(), Value::Number(created_at.into()));

    Value::Object(obj)
}
