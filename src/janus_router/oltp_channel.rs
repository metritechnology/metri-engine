// [PORTED_FROM: src/metri/janus_router/channels/oltp.clj]
// janus_router/oltp_channel.rs — OLTPChannel — Canal ACID vía EAV (DynamoDB).
//
// Responsabilidades (igual que OLTPChannel Clojure):
//   1. Validar payload contra el Códice → HashMap<attr, DatomValue>
//   2. Enriquecer con auto-generados (ULID, meta/created_at, meta/updated_at)
//   3. Coerción de tipos numéricos (epoch/number strings → DatomValue correcto)
//   4. TX ACID via EavWriter (entidad principal)
//   5. Soporte de BULK: procesamiento de N registros con cortocircuito en error
//
// Reemplaza d/transact de Datahike con TransactWriteItems DynamoDB.
// Sin imports directos de negocio — todo llega en IopContext o inyectado.

use std::collections::HashMap;

use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::codice::{global as codice_global, validator};
use crate::codice::registry::AttrType;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::datom::DatomValue;
use crate::eav::writer::{EavWriter, TransactPayload, TransactOp, TransactResult};
use crate::iop::core::IopContext;
use crate::janus_router::router::IWriteChannel;
use crate::janus_router::ulid;

// ── OltpChannel ───────────────────────────────────────────────────────────────

/// Canal de escritura ACID via EAV/DynamoDB.
/// [PORTED_FROM: (defrecord OLTPChannel [datahike-conn projections codice-generator-fn])]
pub struct OltpChannel {
    writer: EavWriter,
}

impl OltpChannel {
    pub fn new(writer: EavWriter) -> Self {
        info!("[OltpChannel] Canal OLTP EAV activo");
        Self { writer }
    }
}

#[async_trait::async_trait]
impl IWriteChannel for OltpChannel {
    /// Pipeline Write Path OLTP.
    ///
    /// Para Transact (payload = objeto):
    ///   validate → enrich → transact → [:ok {:entity_id ... :tx_id ...}]
    ///
    /// Para BulkIngest (data = array):
    ///   por cada row: validate → enrich → transact → cortocircuito en error
    ///   → [:ok {:ingested_count N}]
    ///
    /// [PORTED_FROM: (route [_ ctx] ...)]
    async fn route(&self, ctx: IopContext) -> Result<Value, DomainError> {
        let entity_type = &ctx.entity_type;
        let tenant_id   = &ctx.tenant_id;
        let operation   = ctx.operation.as_str();

        let registry = codice_global();
        let model = registry.get_model(entity_type).ok_or_else(|| {
            DomainError::janus(
                ErrorCode::Jns001,
                format!("[OltpChannel] Entidad desconocida en Códice: '{entity_type}'"),
            )
        })?;

        // Determinar si es Bulk (data=[...]) o Transact (payload={...})
        // [PORTED_FROM: (or (= :bulk op) (some? (get-in ctx [:request :data])))]
        let is_bulk = ctx.request.contains_key("data");

        // Operación EAV
        let op = parse_op(operation)?;

        if is_bulk {
            // ── Bulk Path ─────────────────────────────────────────────────────
            // [PORTED_FROM: (get-in ctx [:request :data] [])]
            let records: Vec<Value> = ctx.request
                .get("data")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();

            info!(
                entity = %entity_type,
                tenant = %tenant_id,
                count  = records.len(),
                "[OltpChannel] Bulk OLTP ingestión iniciada"
            );

            let mut ingested = 0usize;

            for raw_record in &records {
                // Coerción + validación
                let coerced = coerce_payload(raw_record.clone(), model);
                let validated = validator::validate_payload(model, &coerced, tenant_id)?;

                // entity_id: del payload o ULID nuevo
                // [PORTED_FROM: (or (:id payload) (generate-ulid))]
                // Respetar ID provisto por el cliente, si no, generar ULID
                let entity_id = extract_entity_id(raw_record).unwrap_or_else(|| ulid::generate());

                let transact = TransactPayload {
                    tenant_id:   tenant_id.clone(),
                    entity_type: entity_type.clone(),
                    entity_id:   Some(entity_id),
                    op:          TransactOp::Create,
                    attrs:       validated,
                };

                // Cortocircuito en primer error de TX
                // [PORTED_FROM: (if (= :error (first gen-res)) (reduced gen-res) ...)]
                self.writer.transact(transact).await.map_err(|e| {
                    error!(
                        entity = %entity_type,
                        err    = ?e,
                        "[OltpChannel] Bulk TX ACID falló — cortocircuito"
                    );
                    e
                })?;

                ingested += 1;
            }

            info!(
                entity   = %entity_type,
                tenant   = %tenant_id,
                ingested = ingested,
                "[OltpChannel] ✅ Bulk ACID exitosa"
            );

            // [PORTED_FROM: [:ok {:ingested-count (count records) :outbox-count 0}]]
            Ok(json!({
                "ingested_count": ingested,
                "outbox_count":   0,
                "entity_type":    entity_type,
                "tenant_id":      tenant_id,
                "channel":        "oltp",
            }))

        } else {
            // ── Transact Path (rpc Transact) ──────────────────────────────────
            let payload = ctx.request
                .get("payload")
                .cloned()
                .unwrap_or(Value::Object(serde_json::Map::new()));

            // Coerción de tipos numéricos
            // [PORTED_FROM: (coerce-payload raw-payload model)]
            let coerced = coerce_payload(payload.clone(), model);

            // Validación contra el Códice
            let validated = validator::validate_payload(model, &coerced, tenant_id)?;

            // entity_id para UPDATE/DELETE
            // [PORTED_FROM: (or (:id payload) (generate-ulid))]
            let entity_id = match op {
                TransactOp::Create => extract_entity_id(&payload).unwrap_or_else(|| ulid::generate()),
                TransactOp::Update | TransactOp::Delete => {
                    extract_entity_id(&payload).ok_or_else(|| {
                        DomainError::janus(
                            ErrorCode::JanusVal001,
                            format!("Operación {operation} requiere entity_id en el payload"),
                        )
                    })?
                }
            };

            let transact = TransactPayload {
                tenant_id:   tenant_id.clone(),
                entity_type: entity_type.clone(),
                entity_id:   Some(entity_id.clone()),
                op,
                attrs:       validated,
            };

            match self.writer.transact(transact).await {
                Ok(result) => {
                    info!(
                        entity_id = %result.entity_id,
                        tx_id     = %result.tx_id,
                        datoms    = %result.datoms,
                        entity    = %entity_type,
                        "[OltpChannel] ✅ TX ACID exitosa"
                    );

                    // [PORTED_FROM: [:ok {:entity-id ulid :ulid ulid :channel :oltp ...}]]
                    Ok(json!({
                        "entity_id":   result.entity_id,
                        "tx_id":       result.tx_id,
                        "datoms":      result.datoms,
                        "entity_type": entity_type,
                        "tenant_id":   tenant_id,
                        "channel":     "oltp",
                    }))
                }
                Err(e) => {
                    error!(
                        entity = %entity_type,
                        err    = ?e,
                        "[OltpChannel] TX ACID falló"
                    );
                    Err(e)
                }
            }
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Parsea la operación del string al enum EAV.
fn parse_op(op: &str) -> Result<TransactOp, DomainError> {
    match op.to_uppercase().as_str() {
        "CREATE" | "BULK_CREATE" => Ok(TransactOp::Create),
        "UPDATE"                 => Ok(TransactOp::Update),
        "DELETE"                 => Ok(TransactOp::Delete),
        other => Err(DomainError::janus(
            ErrorCode::JanusVal001,
            format!("Operación no soportada por OLTP: '{other}'"),
        )),
    }
}

/// Extrae el entity_id del payload (field: "entity_id" | "id" | "ulid").
fn extract_entity_id(payload: &Value) -> Option<String> {
    ["entity_id", "id", "ulid"]
        .iter()
        .find_map(|k| payload.get(*k).and_then(|v| v.as_str()).map(str::to_string))
}

/// Coerce campos numéricos del Códice que lleguen como strings al tipo correcto.
/// Crítico para evitar errores de validación cuando los clientes envían números como strings.
///
/// [PORTED_FROM: (defn- coerce-payload [payload model])]
fn coerce_payload(record: Value, model: &crate::codice::registry::EntityModel) -> Value {
    let Some(obj) = record.as_object() else { return record; };
    let mut out = obj.clone();

    for attr in &model.attributes {
        let Some(val) = out.get(&attr.name) else { continue; };

        let coerced = match &attr.attr_type {
            AttrType::Epoch | AttrType::Number => {
                if let Value::String(s) = val {
                    // string → número
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
                        "true"  => Some(Value::Bool(true)),
                        "false" => Some(Value::Bool(false)),
                        _       => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        };

        if let Some(v) = coerced {
            out.insert(attr.name.clone(), v);
        }
    }

    Value::Object(out)
}
