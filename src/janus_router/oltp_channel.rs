// janus_router/oltp_channel.rs — OLTPChannel — Canal ACID vía EAV (DynamoDB).
//
// Responsabilidades (igual que OLTPChannel el stack anterior):
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
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::types::datom::DatomValue;
use crate::eav::writer::{EavWriter, TransactOp, TransactPayload};
use crate::iop::core::IopContext;
use crate::janus_router::router::IWriteChannel;
use crate::janus_router::ulid;

// ── OltpChannel ───────────────────────────────────────────────────────────────

/// Canal de escritura ACID via EAV/DynamoDB.
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
    async fn route(&self, ctx: IopContext) -> Result<Value, DomainError> {
        let entity_type = &ctx.entity_type;

        let registry = codice_global();
        let model = registry.get_model(entity_type).ok_or_else(|| {
            DomainError::janus(
                ErrorCode::Jns001,
                format!("[OltpChannel] Entidad desconocida en Códice: '{entity_type}'"),
            )
        })?;

        // Determinar si es Bulk (data=[...]) o Transact (payload={...})
        let is_bulk = ctx.request.contains_key("data");

        // Operación EAV
        let op = parse_op(ctx.operation.as_str())?;

        if is_bulk {
            self.route_bulk(ctx, model).await
        } else {
            self.route_single(ctx, model, op).await
        }
    }
}

impl OltpChannel {
    /// Procesa de forma ACID una secuencia masiva de registros en orden Railway.
    async fn route_bulk(
        &self,
        ctx: IopContext,
        model: &crate::codice::registry::EntityModel,
    ) -> Result<Value, DomainError> {
        let entity_type = &ctx.entity_type;
        let tenant_id = &ctx.tenant_id;

        let records: Vec<Value> = ctx
            .request
            .get("data")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        info!(
            entity = %entity_type,
            tenant = %tenant_id,
            count  = records.len(),
            "[OltpChannel] Bulk OLTP ingestión iniciada (optimized: chunks=100, skip_uniqueness=true)"
        );

        let mut ingested = 0usize;
        let mut outbox_count = 0usize;
        let actor = ctx.user_id; // atribución de auditoría para todo el bulk

        // Concurrent ingestion of chunks to leverage in-memory DynamoDB performance
        for chunk in records.chunks(100) {
            let mut futures = Vec::with_capacity(chunk.len());
            for raw_record in chunk {
                let writer = self.writer.clone();
                let model = model.clone();
                let tenant_id = tenant_id.clone();
                let entity_type = entity_type.clone();
                let raw_record = raw_record.clone();
                let actor = actor.clone();

                futures.push(tokio::spawn(async move {
                    let validated = Self::prepare_and_validate_payload_static(
                        writer.clone(),
                        raw_record.clone(),
                        model.clone(),
                        tenant_id.clone(),
                        true,
                        true, // skip_uniqueness: bulk CREATE skips per-row AVET reads
                    )
                    .await?;

                    let entity_id = if model.is_system {
                        extract_entity_id(&raw_record).unwrap_or_else(ulid::generate)
                    } else {
                        ulid::generate()
                    };

                    let transact = TransactPayload {
                        tenant_id,
                        entity_type,
                        entity_id: Some(entity_id),
                        op: TransactOp::Create,
                        attrs: validated,
                    };

                    writer
                        .transact_bulk_deferred(transact, Some(actor.as_str()))
                        .await
                }));
            }

            let results = futures::future::join_all(futures).await;
            for res in results {
                match res {
                    Ok(Ok(tx_res)) => {
                        ingested += 1;
                        outbox_count += tx_res.outbox_count;
                    }
                    Ok(Err(e)) => {
                        error!(
                            entity = %entity_type,
                            err    = ?e,
                            "[OltpChannel] Bulk TX ACID falló — cortocircuito"
                        );
                        return Err(e);
                    }
                    Err(e) => {
                        error!(
                            entity = %entity_type,
                            err    = ?e,
                            "[OltpChannel] Tokio join task failed"
                        );
                        return Err(DomainError::janus(
                            ErrorCode::Jns001,
                            format!("Tokio join error: {:?}", e),
                        ));
                    }
                }
            }
        }

        // Deferred cache invalidation: single invalidation for the entire bulk operation
        if let Ok(mut cache) = crate::eav::reader::query::AEVT_SCAN_CACHE.write() {
            let key = (tenant_id.clone(), entity_type.clone());
            cache.remove(&key);
        }

        info!(
            entity   = %entity_type,
            tenant   = %tenant_id,
            ingested = ingested,
            "[OltpChannel] ✅ Bulk ACID exitosa"
        );

        Ok(json!({
            "ingested_count": ingested,
            "outbox_count":   outbox_count,
            "entity_type":    entity_type,
            "tenant_id":      tenant_id,
            "channel":        "oltp",
        }))
    }

    /// Procesa de forma transaccional ACID una petición unitaria del IOP.
    async fn route_single(
        &self,
        ctx: IopContext,
        model: &crate::codice::registry::EntityModel,
        op: TransactOp,
    ) -> Result<Value, DomainError> {
        let entity_type = &ctx.entity_type;
        let tenant_id = &ctx.tenant_id;
        let operation = ctx.operation.as_str();

        let payload = ctx
            .request
            .get("payload")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));

        let validated = if op == TransactOp::Delete {
            HashMap::new()
        } else {
            let is_create = op == TransactOp::Create;
            self.prepare_and_validate_payload(payload.clone(), model, tenant_id, is_create)
                .await?
        };

        let entity_id = match op {
            TransactOp::Create => {
                if model.is_system {
                    // System entities can have custom/human-readable IDs (like tenant_id, role_id, etc.)
                    extract_entity_id(&payload).unwrap_or_else(ulid::generate)
                } else {
                    // Non-system entities MUST have their IDs generated by metri-engine.
                    // We ignore any client-supplied ID.
                    ulid::generate()
                }
            }
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
            tenant_id: tenant_id.clone(),
            entity_type: entity_type.clone(),
            entity_id: Some(entity_id.clone()),
            op: op.clone(),
            attrs: validated,
        };

        // Proyección de sagas: sólo en CREATE y sólo si la madre declara el mapping.
        // Los scheduled_job resultantes viajan en la MISMA TX ACID (Componente Externo 05 §6).
        let projections = if op == TransactOp::Create && model.shadow_sagas_mapping.is_some() {
            match crate::janus_router::saga::build_saga_projections(
                &self.writer.reader(),
                tenant_id,
                model,
                &payload,
                &entity_id,
                &ctx.user_id,
            )
            .await
            {
                Ok(p) => {
                    if !p.is_empty() {
                        info!(
                            entity = %entity_type,
                            parent = %entity_id,
                            jobs   = p.len(),
                            "[OltpChannel] SagaBuilder proyecta scheduled_job(s) en la misma TX"
                        );
                    }
                    p
                }
                Err(e) => {
                    // Falla la TX entera: una madre sin sus sagas es un mantenimiento que
                    // nunca se ejecutará, y nadie se enteraría hasta el día que tocaba.
                    error!(entity = %entity_type, err = ?e, "[OltpChannel] SagaBuilder falló");
                    return Err(e);
                }
            }
        } else {
            Vec::new()
        };

        // Colisión de tx (EAV_TX_004): la condición append-only protegió el
        // histórico — el reintento mintea un tx nuevo y es siempre seguro.
        let mut write_result = self
            .writer
            .transact_with_projections(transact.clone(), projections.clone(), Some(&ctx.user_id))
            .await;
        for attempt in 1..=2 {
            match write_result {
                Err(ref e) if e.code == ErrorCode::EavTx004 => {
                    warn!(
                        intento = attempt,
                        "[OltpChannel] Colisión de tx (EAV_TX_004) — reintentando con tx nuevo"
                    );
                    write_result = self
                        .writer
                        .transact_with_projections(
                            transact.clone(),
                            projections.clone(),
                            Some(&ctx.user_id),
                        )
                        .await;
                }
                _ => break,
            }
        }

        match write_result {
            Ok(result) => {
                info!(
                    entity_id = %result.entity_id,
                    tx_id     = %result.tx_id,
                    datoms    = %result.datoms,
                    entity    = %entity_type,
                    "[OltpChannel] ✅ TX ACID exitosa"
                );

                // Emite evento de dominio a EventBridge asíncronamente
                let bus_name = std::env::var("EVENTBRIDGE_BUS_NAME")
                    .unwrap_or_else(|_| "metri-events".to_string());
                let source = "metri.engine".to_string();
                let detail_type = if entity_type == "tenant" && op == TransactOp::Create {
                    "system.tenant.created".to_string()
                } else {
                    format!("{entity_type}.{}", operation.to_lowercase())
                };

                let event_tenant_id = if entity_type == "tenant" && op == TransactOp::Create {
                    result.entity_id.clone()
                } else {
                    tenant_id.clone()
                };

                let mut detail_map = serde_json::Map::new();
                detail_map.insert("tenant_id".to_string(), Value::String(event_tenant_id));
                detail_map.insert(
                    "entity_type".to_string(),
                    Value::String(entity_type.clone()),
                );
                detail_map.insert(
                    "entity_id".to_string(),
                    Value::String(result.entity_id.clone()),
                );
                detail_map.insert("op".to_string(), Value::String(operation.to_string()));
                if let Value::Object(ref p_map) = payload {
                    for (k, v) in p_map {
                        detail_map.insert(k.clone(), v.clone());
                    }
                }

                crate::infrastructure::eventbridge::publish_domain_event_async(
                    bus_name,
                    source,
                    detail_type,
                    Value::Object(detail_map),
                );

                if let Ok(mut cache) = crate::eav::reader::query::AEVT_SCAN_CACHE.write() {
                    cache.remove(&(tenant_id.clone(), entity_type.clone()));
                }

                Ok(json!({
                    "entity_id":   result.entity_id,
                    "tx_id":       result.tx_id,
                    "datoms":      result.datoms,
                    "outbox_count": result.outbox_count,
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

    /// Prepara el payload convirtiendo strings a números/booleanos nativos y ejecutando lógicas auto-generadas de Códice (versión estática para tokio::spawn).
    async fn prepare_and_validate_payload_static(
        writer: EavWriter,
        payload: Value,
        model: crate::codice::registry::EntityModel,
        tenant_id: String,
        is_create: bool,
        skip_uniqueness: bool,
    ) -> Result<HashMap<String, DatomValue>, DomainError> {
        let mut coerced = coerce_payload(payload.clone(), &model);

        if is_create {
            if let Ok(Value::Object(map)) = serde_json::to_value(&coerced) {
                if let Ok(injected) =
                    crate::codice::generator::inject(writer.client(), &model, &tenant_id, map).await
                {
                    if let Ok(val) = serde_json::to_value(injected) {
                        coerced = val;
                    }
                }
            }
        }

        // ── Uniqueness constraint checks (identity) ──
        // Optimization: skip per-row AVET reads during bulk CREATE to avoid N DynamoDB reads
        if !skip_uniqueness {
            let entity_id = extract_entity_id(&payload).unwrap_or_default();
            let query_executor = crate::eav::reader::query::EavQueryExecutor::new(
                writer.client().clone(),
                writer.table().to_string(),
            );

            for attr in &model.attributes {
                if let Some(unique_type) = attr.unique.as_deref() {
                    if unique_type == "tenant"
                        || unique_type == "identity"
                        || unique_type == "value"
                        || unique_type == "tenant_scoped"
                    {
                        if let Some(val) = coerced.get(&attr.name) {
                            if !val.is_null() {
                                let datom_val = match crate::codice::validator::map_to_datom_value(
                                    val,
                                    &attr.attr_type,
                                ) {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                };

                                let query_tenant = if unique_type == "identity" {
                                    "GLOBAL_SYSTEM".to_string()
                                } else {
                                    tenant_id.clone()
                                };

                                let plan = crate::eav::reader::query::NativeQueryPlan::AvetSingle {
                                    tenant_id: query_tenant,
                                    attr_name: attr.name.clone(),
                                    value: datom_val,
                                };

                                if let Ok(existing_ids) =
                                    query_executor.execute_native_plan(&plan).await
                                {
                                    let is_duplicate = if is_create {
                                        !existing_ids.is_empty()
                                    } else {
                                        existing_ids.iter().any(|id| id != &entity_id)
                                    };

                                    if is_duplicate {
                                        return Err(DomainError::janus(
                                            ErrorCode::JnsConflict001,
                                            format!("unique:{} constraint violated for attribute '{}' — value already exists", unique_type, attr.name),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        validator::validate_payload(&model, &coerced, &tenant_id, is_create)
    }

    /// Prepara el payload convirtiendo strings a números/booleanos nativos y ejecutando lógicas auto-generadas de Códice.
    async fn prepare_and_validate_payload(
        &self,
        payload: Value,
        model: &crate::codice::registry::EntityModel,
        tenant_id: &str,
        is_create: bool,
    ) -> Result<HashMap<String, DatomValue>, DomainError> {
        Self::prepare_and_validate_payload_static(
            self.writer.clone(),
            payload,
            model.clone(),
            tenant_id.to_string(),
            is_create,
            false, // skip_uniqueness: individual operations always check uniqueness
        )
        .await
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Parsea la operación del string al enum EAV.
fn parse_op(op: &str) -> Result<TransactOp, DomainError> {
    match op.to_uppercase().as_str() {
        "CREATE" | "BULK_CREATE" | "UPSERT" => Ok(TransactOp::Create),
        "UPDATE" => Ok(TransactOp::Update),
        "DELETE" => Ok(TransactOp::Delete),
        other => Err(DomainError::janus(
            ErrorCode::JanusVal001,
            format!("Operación no soportada por OLTP: '{other}'"),
        )),
    }
}

/// Extrae el entity_id del payload (field: "entity_id" | "id" | "ulid").
pub(crate) fn extract_entity_id(payload: &Value) -> Option<String> {
    ["entity_id", "id", "ulid"]
        .iter()
        .find_map(|k| payload.get(*k).and_then(|v| v.as_str()).map(str::to_string))
}

/// Coerce campos numéricos del Códice que lleguen como strings al tipo correcto.
/// Crítico para evitar errores de validación cuando los clientes envían números como strings.
///
fn coerce_payload(record: Value, model: &crate::codice::registry::EntityModel) -> Value {
    let Some(obj) = record.as_object() else {
        return record;
    };
    let mut out = obj.clone();

    for attr in &model.attributes {
        if let Some(val) = out.get(&attr.name) {
            if let Some(coerced_val) = crate::codice::coercion::coerce_value(val, &attr.attr_type) {
                out.insert(attr.name.clone(), coerced_val);
            }
        }
    }

    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codice::registry::EntityModel;
    use serde_json::json;

    #[test]
    fn test_extract_entity_id_fallback_to_ulid_when_invalid() {
        let system_model = EntityModel {
            entity: "tenant".to_string(),
            label: None,
            icon: None,
            primary_key: None,
            fts_fields: vec![],
            engine: crate::codice::EngineChannel::Oltp,
            attributes: vec![],
            event_rules: vec![],
            is_sequence_scope_provider: false,
            write_path_locked: false,
            is_system: true,
            disable_eda: false,
            shadow_sagas_mapping: None,
            constraints: vec![],
        };

        let normal_model = EntityModel {
            entity: "dashboardBI".to_string(),
            label: None,
            icon: None,
            primary_key: None,
            fts_fields: vec![],
            engine: crate::codice::EngineChannel::Oltp,
            attributes: vec![],
            event_rules: vec![],
            is_sequence_scope_provider: false,
            write_path_locked: false,
            is_system: false,
            disable_eda: false,
            shadow_sagas_mapping: None,
            constraints: vec![],
        };

        // Case 1: System model permits custom human-readable ID
        let payload = json!({ "id": "tnt_regular" });
        let resolved_id = if system_model.is_system {
            extract_entity_id(&payload).unwrap_or_else(ulid::generate)
        } else {
            ulid::generate()
        };
        assert_eq!(resolved_id, "tnt_regular");

        // Case 2: Non-system model IGNORES any client-provided ID entirely
        let resolved_id2 = if normal_model.is_system {
            extract_entity_id(&payload).unwrap_or_else(ulid::generate)
        } else {
            ulid::generate()
        };
        assert_ne!(resolved_id2, "tnt_regular");
        assert!(ulid::is_ulid(&resolved_id2));
    }
}
