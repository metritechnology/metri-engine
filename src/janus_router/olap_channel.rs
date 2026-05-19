// [PORTED_FROM: src/metri/janus_router/channels/olap.clj]
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

use serde_json::{json, Value};
use tracing::{info, warn, error};
use chrono::Utc;

use crate::codice::global as codice_global;
use crate::codice::registry::AttrType;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::iop::core::IopContext;
use crate::janus_router::router::IWriteChannel;
use crate::janus_router::ulid;

// ── OlapChannel ───────────────────────────────────────────────────────────────

/// Canal de escritura OLAP vía Kinesis Firehose (Columnar Nativo).
/// [PORTED_FROM: (defrecord OLAPChannel [stream-writer stream-prefix])]
pub struct OlapChannel {
    /// Prefijo del stream Firehose — ej: "metri-olap-stream"
    stream_prefix: String,
    // FASE 4: aws_sdk_firehose::Client
}

impl OlapChannel {
    pub fn new(stream_prefix: impl Into<String>) -> Self {
        let stream_prefix = stream_prefix.into();
        info!("[OlapChannel] Columnar Nativo activo | prefix: {stream_prefix}");
        Self { stream_prefix }
    }

    /// Nombre del stream Firehose para la entidad.
    /// underscore → hyphen: meter_reading → <prefix>-meter-reading
    /// [PORTED_FROM: (defn- entity->stream-name [stream-prefix entity-type])]
    fn stream_name(&self, entity_type: &str) -> String {
        format!("{}-{}", self.stream_prefix, entity_type.replace('_', "-"))
    }
}

#[async_trait::async_trait]
impl IWriteChannel for OlapChannel {
    /// Enruta BulkIngest al stream Firehose de la entidad.
    /// Bloquea rpc Transact (data = None).
    ///
    /// [PORTED_FROM: (route [_ ctx] ...)]
    async fn route(&self, ctx: IopContext) -> Result<Value, DomainError> {
        let entity_type = &ctx.entity_type;
        let tenant_id   = &ctx.tenant_id;

        // Guard: rpc Transact (data = nil) → JNS_OLAP_001
        // [PORTED_FROM: (if (nil? data) (errors/error :JNS_OLAP_001 ...))]
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
        let created_at  = Utc::now().timestamp_millis();

        // Lookup atributos del Códice para coerción de tipos.
        // [PORTED_FROM: (codice/describe-attributes entity-type ctx)]
        let registry   = codice_global();
        let attributes = registry.get_attributes(entity_type);

        info!(
            stream = %stream_name,
            count  = records.len(),
            tenant = %tenant_id,
            "[OlapChannel] Iniciando ingestión OLAP"
        );

        let mut ingested = 0usize;

        for record in &records {
            let record_ulid = ulid::generate();

            // 1. Coerción de tipos numéricos (string → number).
            // Crítico para Parquet/Iceberg donde los tipos deben coincidir exactamente.
            // [PORTED_FROM: (coerce-numeric-fields record attributes)]
            let coerced = coerce_numeric_fields(record.clone(), attributes);

            // 2. Aplanar: campos de dominio + metadatos del sistema.
            // Produce mapa plano compatible con esquema Iceberg de la entidad.
            // [PORTED_FROM: (decorate-record coerced tenant-id created-at record-ulid)]
            let decorated = decorate_record(coerced, tenant_id, created_at, &record_ulid);

            // FASE 4: aws_sdk_firehose::Client::put_record()
            // Formato: PutRecordInput { delivery_stream_name, record: base64(JSON) }
            // [PORTED_FROM: (proto/put-record! stream-writer stream-name record-ulid decorated)]
            info!(
                stream = %stream_name,
                ulid   = %record_ulid,
                "[OlapChannel] STUB Firehose PutRecord (FASE 4)"
            );

            ingested += 1;
        }

        info!(
            stream   = %stream_name,
            ingested = ingested,
            tenant   = %tenant_id,
            "[OlapChannel] ✅ Ingestión OLAP exitosa"
        );

        // [PORTED_FROM: [:ok {:ingested-count (count records) :outbox-count 0}]]
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
/// [PORTED_FROM: (defn- coerce-numeric-fields [record attributes])]
fn coerce_numeric_fields(
    record:     Value,
    attributes: Option<&[crate::codice::registry::AttributeDescriptor]>,
) -> Value {
    let Some(attrs) = attributes else { return record; };
    let Some(obj)   = record.as_object() else { return record; };

    let mut out = obj.clone();

    for attr in attrs {
        let is_numeric = matches!(
            attr.attr_type,
            AttrType::Number | AttrType::Epoch | AttrType::Decimal
        );
        if !is_numeric { continue; }

        if let Some(Value::String(s)) = out.get(&attr.name) {
            // Intentar parsear como entero primero (epoch/long), luego como decimal.
            let parsed = if let Ok(i) = s.parse::<i64>() {
                Some(Value::Number(i.into()))
            } else if let Ok(f) = s.parse::<f64>() {
                serde_json::Number::from_f64(f).map(Value::Number)
            } else {
                None
            };

            if let Some(v) = parsed {
                out.insert(attr.name.clone(), v);
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
/// [PORTED_FROM: (defn- decorate-record [record tenant-id created-at ulid])]
fn decorate_record(record: Value, tenant_id: &str, created_at: i64, record_ulid: &str) -> Value {
    let mut obj = match record {
        Value::Object(m) => m,
        other            => {
            let mut m = serde_json::Map::new();
            m.insert("_raw".to_string(), other);
            m
        }
    };

    obj.insert("id".to_string(),         Value::String(record_ulid.to_string()));
    obj.insert("_tenant".to_string(),    Value::String(tenant_id.to_string()));
    obj.insert("created_at".to_string(), Value::Number(created_at.into()));

    Value::Object(obj)
}
