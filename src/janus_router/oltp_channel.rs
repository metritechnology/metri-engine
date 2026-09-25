//! OLTPChannel — ACID channel via the EAV engine.
//!
//! OLTPChannel — Canal ACID vía EAV (DynamoDB).
//!
//! # Origin
//! Responsabilidades (igual que OLTPChannel el stack anterior):
//! 1. Validar payload contra el Códice → HashMap<attr, DatomValue>
//! 2. Enriquecer con auto-generados (ULID, meta/created_at, meta/updated_at)
//! 3. Coerción de tipos numéricos (epoch/number strings → DatomValue correcto)
//! 4. TX ACID via EavWriter (entidad principal)
//! 5. Soporte de BULK: procesamiento de N registros con cortocircuito en error
//!
//! Reemplaza d/transact de Datahike con TransactWriteItems DynamoDB.
//! Sin imports directos de negocio — todo llega en IopContext o inyectado.

use std::collections::HashMap;

use serde_json::{json, Value};
use tracing::{error, info, warn};

use crate::codice::{global as codice_global, validator};
use crate::domain::errors::{DomainError, ErrorCode};
use crate::eav::reader::query::{EavQueryExecutor, NativeQueryPlan};
use crate::eav::types::datom::DatomValue;
use crate::eav::writer::{EavWriter, TransactOp, TransactPayload};
use crate::iop::core::IopContext;
use crate::janus_router::router::IWriteChannel;
use crate::janus_router::saga::{
    self, build_saga_projections, desired_job_status, diff_sagas, LiveSagaJob, SagaReconciliation,
};
use crate::janus_router::ulid;

// ── OltpChannel ───────────────────────────────────────────────────────────────

/// Canal de escritura ACID via EAV/DynamoDB.
pub struct OltpChannel {
    writer: EavWriter,
    /// Puerto de eventos del contrato metri-contracts (DIP). `new` compone el
    /// adaptador EventBridge detached; `new_with_publisher` permite fakes en
    /// tests y un futuro publisher por outbox sin tocar este canal.
    event_publisher: std::sync::Arc<dyn crate::application::ports::DomainEventPublisher>,
}

impl OltpChannel {
    pub fn new(writer: EavWriter) -> Self {
        info!("[OltpChannel] Canal OLTP EAV activo");
        Self {
            writer,
            event_publisher: std::sync::Arc::new(
                crate::infrastructure::domain_event_bus::DetachedEventBridgePublisher,
            ),
        }
    }

    pub fn new_with_publisher(
        writer: EavWriter,
        event_publisher: std::sync::Arc<dyn crate::application::ports::DomainEventPublisher>,
    ) -> Self {
        info!("[OltpChannel] Canal OLTP EAV activo (publisher inyectado)");
        Self {
            writer,
            event_publisher,
        }
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
                        // ADR-006: el id de negocio lo mintea el engine; un
                        // registro con 'id' del cliente se rechaza.
                        if extract_entity_id(&raw_record).is_some() {
                            return Err(DomainError::janus(
                                ErrorCode::JanusVal001,
                                "La entidad de negocio no acepta 'id' del cliente:                                  el engine genera el ULID y lo devuelve en entity_id (ADR-006)"
                                    .to_string(),
                            ));
                        }
                        ulid::generate()
                    };

                    let transact = TransactPayload {
                        tenant_id,
                        entity_type,
                        entity_id: Some(entity_id),
                        op: TransactOp::Create,
                        suppress_events: false,
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
        crate::eav::writer::cache_policy::invalidate_aevt_scan(tenant_id, entity_type);
        // La caché de consultas distribuida sigue el mismo principio del
        // diferido: UN bump de generación por lote completo, no por fila.
        self.writer.invalidate_query_cache(tenant_id).await;

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
                    // ADR-006: la identidad de una entidad de negocio la mintea
                    // el engine y se devuelve en entity_id. Un 'id' del cliente
                    // en el payload es una violación de contrato — se rechaza
                    // (fail-closed), nunca se ignora en silencio: quien cree
                    // controlar el id acaba operando sobre una entidad fantasma.
                    if extract_entity_id(&payload).is_some() {
                        return Err(DomainError::janus(
                            ErrorCode::JanusVal001,
                            "La entidad de negocio no acepta 'id' del cliente:                              el engine genera el ULID y lo devuelve en entity_id (ADR-006)"
                                .to_string(),
                        ));
                    }
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

        // §10.4 — primera barrera del ciclo de realimentación: el ejecutor del
        // Hub escribe bitácora (last_run_at, run_count, last_error) sin generar
        // eventos. La segunda barrera, independiente del escritor, es el guard
        // de delta en el sobre.
        let suppress_events = ctx
            .request
            .get("suppress_events")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let transact = TransactPayload {
            tenant_id: tenant_id.clone(),
            entity_type: entity_type.clone(),
            entity_id: Some(entity_id.clone()),
            op: op.clone(),
            attrs: validated,
            suppress_events,
        };

        // Proyección y convergencia de sagas:
        //
        // CREATE — los jobs nacen en la MISMA TX ACID que la madre (Componente
        //          Externo 05 §6), como siempre.
        // UPDATE — diff por idempotency_hash: los faltantes nacen en la misma TX;
        //          huérfanos y estados divergentes se reconcilian DESPUÉS del
        //          commit. La madre nunca queda sin sus trampas nuevas; si la
        //          reconciliación muere a medias el error es ruidoso y el
        //          reintento del usuario sana (retirar huérfanos es idempotente).
        // DELETE — los jobs se retiran ANTES que la madre: una trampa huérfana
        //          seguiría engendrando OTs para una madre que ya no existe.
        let mut projections: Vec<saga::SagaProjection> = Vec::new();
        let mut pending_reconciliation: Option<SagaReconciliation> = None;
        if model.shadow_sagas_mapping.is_some() {
            match op {
                TransactOp::Create => {
                    projections = match build_saga_projections(
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
                    };
                }
                TransactOp::Update => {
                    let recon = self
                        .plan_saga_reconciliation(
                            tenant_id,
                            model,
                            &payload,
                            &entity_id,
                            &ctx.user_id,
                        )
                        .await?;
                    if !recon.to_create.is_empty() {
                        info!(
                            entity = %entity_type,
                            parent = %entity_id,
                            creates = recon.to_create.len(),
                            "[OltpChannel] reproyección: trampas nuevas nacen con el update"
                        );
                    }
                    if !recon.to_delete.is_empty() || !recon.to_restatus.is_empty() {
                        warn!(
                            entity = %entity_type,
                            parent = %entity_id,
                            deletes = recon.to_delete.len(),
                            restatus = recon.to_restatus.len(),
                            "[OltpChannel] reproyección: trampas viejas se retiran tras el commit"
                        );
                    }
                    projections = recon.to_create.clone();
                    pending_reconciliation = Some(recon);
                }
                TransactOp::Delete => {
                    self.purge_saga_children(tenant_id, &entity_id, &ctx.user_id)
                        .await?;
                }
            }
        }

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

                // §10.4 — con suppress_events no hay outbox (writer) NI emisiones:
                // son las escrituras de bitácora del ejecutor del Hub.
                if !suppress_events {
                    // Emite evento de dominio legacy a EventBridge asíncronamente
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

                    // Sobre del contrato metri-contracts v1.0 (Tramo 1) — lo que
                    // metri-schedulers parsea. Aditivo al emit legacy: el Hub
                    // ignora lo que no conoce, el motor no puede dejar de
                    // publicar lo que el Hub ya consume.
                    if entity_type == "scheduled_job" {
                        if let Some((detail_type, detail)) =
                            crate::infrastructure::domain_event_bus::build_scheduled_job_detail(
                                &result, operation, &payload, tenant_id,
                            )
                        {
                            crate::application::ports::publish_detached(
                                self.event_publisher.clone(),
                                detail_type,
                                detail,
                            );
                        }
                    }

                    // Las PROYECCIONES también son scheduled_job y el Hub sólo
                    // aprende de ellas por el sobre del contrato: el outbox
                    // viaja en formato legacy ({entity}.{op}) que la regla de
                    // Chronos no matchea. Sin esto, la trampa jamás se arma y
                    // el mantenimiento no dispara (hallazgo E2E 2026-09-22).
                    for proj in &projections {
                        if proj.entity_type != "scheduled_job" {
                            continue;
                        }
                        let proj_result = crate::eav::writer::TransactResult {
                            entity_id: proj.entity_id.clone(),
                            tx_id: result.tx_id,
                            datoms: 0,
                            outbox_count: 0,
                            mutation_ulid: ulid::generate(),
                            delta: None,
                            deleted_attrs: None,
                        };
                        let proj_payload =
                            crate::eav::writer::outbox::datom_map_to_json(&proj.attrs);
                        if let Some((detail_type, detail)) =
                            crate::infrastructure::domain_event_bus::build_scheduled_job_detail(
                                &proj_result,
                                "CREATE",
                                &proj_payload,
                                tenant_id,
                            )
                        {
                            crate::application::ports::publish_detached(
                                self.event_publisher.clone(),
                                detail_type,
                                detail,
                            );
                        }
                    }
                }

                if let Ok(mut cache) = crate::eav::reader::query::AEVT_SCAN_CACHE.write() {
                    cache.remove(&(tenant_id.clone(), entity_type.clone()));
                }

                // Convergencia post-commit (P0c): la madre y sus trampas nuevas
                // ya viven; retirar las viejas puede fallar sin corromper nada.
                // Ruidoso a propósito — el reintento del update sana porque el
                // diff recalcula y retirar huérfanos es idempotente.
                if let Some(recon) = pending_reconciliation {
                    if let Err(e) = self
                        .apply_saga_reconciliation(tenant_id, &ctx.user_id, recon)
                        .await
                    {
                        error!(
                            entity = %entity_type,
                            err = ?e,
                            "[OltpChannel] ⚠️ el update vivió pero sus trampas viejas siguen armadas — reintentar el update para converger"
                        );
                        return Err(e);
                    }
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

    /// P0c — plan de convergencia de las sagas de una madre que se actualiza.
    ///
    /// Falla el update si el estado post-update no puede derivar trigger: la
    /// misma doctrina fail-closed del CREATE (una madre sin sagas es un
    /// mantenimiento que nunca se ejecutará).
    async fn plan_saga_reconciliation(
        &self,
        tenant_id: &str,
        model: &crate::codice::registry::EntityModel,
        delta: &Value,
        entity_id: &str,
        actor: &str,
    ) -> Result<SagaReconciliation, DomainError> {
        let reader = self.writer.reader();

        // Estado post-update: lo vivo hoy + el parche que esta TX va a escribir.
        let current = reader.pull(tenant_id, entity_id, None).await?;
        let mut merged = crate::eav::writer::outbox::datom_map_to_json(&current);
        if let (Value::Object(merged_map), Value::Object(delta_map)) = (&mut merged, delta) {
            for (k, v) in delta_map {
                if !v.is_null() {
                    merged_map.insert(k.clone(), v.clone());
                }
            }
        }

        let mut desired =
            build_saga_projections(&reader, tenant_id, model, &merged, entity_id, actor).await?;

        // La salud de la madre manda sobre el estado de sus trampas: una madre
        // PAUSED no engendra — los jobs que nazcan llegan SUSPENDED, y los vivos
        // convergen a SUSPENDED sin ser destruidos (reactivar es barato).
        let mother_status = desired_job_status(merged.get("status").and_then(|v| v.as_str()));
        for proj in &mut desired {
            proj.attrs.insert(
                "status".to_string(),
                DatomValue::Str(mother_status.to_string()),
            );
        }

        let live = self.list_saga_jobs(tenant_id, entity_id).await?;
        Ok(diff_sagas(desired, live, mother_status))
    }

    /// P0c — los jobs vivos de una madre (AVET por `parent_entity_ref`).
    async fn list_saga_jobs(
        &self,
        tenant_id: &str,
        parent_id: &str,
    ) -> Result<Vec<LiveSagaJob>, DomainError> {
        let query = EavQueryExecutor::new(
            std::sync::Arc::clone(self.writer.client()),
            self.writer.table().to_string(),
        );
        let job_ids = query
            .execute_native_plan(&NativeQueryPlan::AvetSingle {
                tenant_id: tenant_id.to_string(),
                attr_name: "parent_entity_ref".to_string(),
                value: DatomValue::Uuid(parent_id.to_string()),
            })
            .await?;

        let reader = self.writer.reader();
        let mut live = Vec::with_capacity(job_ids.len());
        for job_id in job_ids {
            let attrs = reader
                .pull(tenant_id, &job_id, Some(&["idempotency_hash", "status"]))
                .await?;
            // El índice AVET puede exhumar ids de jobs ya borrados (sus datoms
            // retractados siguen indexados por valor). Un pull sin attrs vivos
            // es un cadáver: NO es hijo que purgar — borrarlo de nuevo sería
            // EAV_005 y abortaría el retiro de la madre entera (hallazgo E2E).
            if attrs.is_empty() {
                continue;
            }
            let hash = match attrs.get("idempotency_hash") {
                Some(DatomValue::Str(h)) => Some(h.clone()),
                _ => None,
            };
            if hash.is_none() {
                warn!(
                    job = %job_id,
                    parent = %parent_id,
                    "[OltpChannel] job sin idempotency_hash — se conserva sin converger (conservador)"
                );
            }
            let status = match attrs.get("status") {
                Some(DatomValue::Str(s)) => s.clone(),
                _ => String::new(),
            };
            live.push(LiveSagaJob {
                job_id,
                idempotency_hash: hash,
                status,
            });
        }
        Ok(live)
    }

    /// P0c — aplica la convergencia calculada. Cada escritura es un Transact
    /// completo del canal: valida, escribe y PUBLICA el sobre que metri-schedulers
    /// consume (`scheduled_job.updated/deleted`) para mover las trampas de AWS.
    async fn apply_saga_reconciliation(
        &self,
        tenant_id: &str,
        actor: &str,
        recon: SagaReconciliation,
    ) -> Result<(), DomainError> {
        for (job_id, status) in recon.to_restatus {
            self.transact_scheduled_job(
                tenant_id,
                actor,
                "UPDATE",
                json!({ "id": job_id, "status": status }),
            )
            .await?;
        }
        for job_id in recon.to_delete {
            self.transact_scheduled_job(tenant_id, actor, "DELETE", json!({ "id": job_id }))
                .await?;
        }
        Ok(())
    }

    /// P0c — retiro de la madre: sus trampas mueren primero, mientras la madre
    /// aún vive (si el borrado de la madre fallara, el reintento sana; al
    /// revés quedarían trampas fantasma engendrando OTs de una madre muerta).
    async fn purge_saga_children(
        &self,
        tenant_id: &str,
        entity_id: &str,
        actor: &str,
    ) -> Result<(), DomainError> {
        let live = self.list_saga_jobs(tenant_id, entity_id).await?;
        for job in &live {
            self.transact_scheduled_job(tenant_id, actor, "DELETE", json!({ "id": job.job_id }))
                .await?;
        }
        if !live.is_empty() {
            info!(
                parent = %entity_id,
                jobs = live.len(),
                "[OltpChannel] sagas retiradas antes que la madre"
            );
        }
        Ok(())
    }

    /// Transact sintético sobre un scheduled_job, reentrando por el canal:
    /// hereda validación, bitácora y — lo decisivo — la publicación del sobre
    /// metri-contracts que mueve la trampa de EventBridge. Sin reentrada, el
    /// job moriría en DynamoDB y la trampa de AWS seguiría viva.
    async fn transact_scheduled_job(
        &self,
        tenant_id: &str,
        actor: &str,
        operation: &str,
        payload: Value,
    ) -> Result<(), DomainError> {
        let model = codice_global().get_model("scheduled_job").ok_or_else(|| {
            DomainError::janus(
                ErrorCode::Jns001,
                "[OltpChannel] 'scheduled_job' no está en el Códice".to_string(),
            )
        })?;
        let op = parse_op(operation)?;
        let mut request = serde_json::Map::new();
        request.insert("payload".to_string(), payload);
        let ctx = IopContext::new(
            tenant_id.to_string(),
            actor.to_string(),
            "scheduled_job".to_string(),
            operation.to_string(),
            request,
        );
        // La reentrada es finita por construcción (scheduled_job no declara
        // shadow_sagas_mapping), pero el compilador no lo sabe: Box::pin corta
        // el tamaño infinito del future recursivo.
        Box::pin(self.route_single(ctx, model, op))
            .await
            .map(|_| ())
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
                // Zero-Drop: un fallo del generador — contador caído, tabla de
                // secuencias ausente — sube y frena el CREATE. Tragarlo aquí
                // creó OTs sin `work_order_number` con un 200 OK (incidente
                // 2026-09-17); la fila sin su código de negocio no debe nacer.
                let scope_ctx = resolve_scope_context(&writer, &model, &coerced, &tenant_id).await;
                let injected = crate::codice::generator::inject(
                    writer.client(),
                    &model,
                    &tenant_id,
                    map,
                    scope_ctx,
                )
                .await?;
                if let Ok(val) = serde_json::to_value(injected) {
                    coerced = val;
                }
            }
            // Los `default_value` del Códice son valor DE NACIMIENTO: el seeder
            // los usaba pero el plano transaccional no, y cada OT nueva nacía
            // sin `status` — la UI la lee «—». Solo enum/string: el default
            // tipado de boolean/number/epoch exige negociar su JSON y hoy no
            // hay modelo que lo pida.
            apply_declared_defaults(&mut coerced, &model);
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

/// Materializa los `default_value` declarados en el Códice para atributos
/// enum/string que el CREATE no trae. El `status: OPEN` de work_order es el
/// caso de uso: sin esto la OT nace sin estado y toda la UI la lee «—».
fn apply_declared_defaults(payload: &mut Value, model: &crate::codice::registry::EntityModel) {
    let Some(obj) = payload.as_object_mut() else {
        return;
    };
    for attr in &model.attributes {
        let Some(default) = &attr.default_value else {
            continue;
        };
        if !matches!(
            attr.attr_type,
            crate::codice::registry::AttrType::Enum | crate::codice::registry::AttrType::String
        ) {
            continue;
        }
        let missing = matches!(obj.get(&attr.name), None | Some(Value::Null));
        if missing {
            obj.insert(attr.name.clone(), Value::String(default.clone()));
        }
    }
}

/// Resuelve el contexto de secuenciación para un CREATE con ámbito de
/// ubicación: el `tag` de la ubicación propia —el segmento del número,
/// 'L-K92MXA'— y la cadena `parent_location_id` hacia la raíz, para que el
/// contador se herede del ancestro más cercano con actividad
/// (`nearest_registered`, doc del Códice). Sin ubicación en el payload, o si
/// la lectura falla, None: el número sale global — la lectura del árbol no
/// bloquea el alta, el contador global es la válvula.
async fn resolve_scope_context(
    writer: &EavWriter,
    model: &crate::codice::registry::EntityModel,
    payload: &Value,
    tenant_id: &str,
) -> Option<crate::codice::generator::ScopeContext> {
    let scope_field = model
        .attributes
        .iter()
        .find(|a| a.is_sequence_scope || a.is_sequence_scope_via)?
        .name
        .clone();
    let mut root = payload
        .get(&scope_field)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Scope indirecto: sin ubicación pero con el campo `via` (asset_id), el
    // ámbito es la ubicación del asset — doc del Códice: «Cuando el payload
    // contiene asset_id pero NO location_id, el generador hace un READ del
    // asset y usa su location_id como scope».
    if root.is_empty() {
        let via_attr = model
            .attributes
            .iter()
            .find(|a| a.is_sequence_scope_via)
            .map(|a| a.name.clone());
        if let Some(via_name) = via_attr {
            let asset = payload
                .get(&via_name)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if !asset.is_empty() {
                if let Ok(attrs) = writer.get_active_attributes(tenant_id, asset).await {
                    if let Some((_, v)) = attrs.get(&scope_field) {
                        if let DatomValue::Str(s) = v {
                            root = s.clone();
                        }
                    }
                }
            }
        }
    }

    if root.is_empty() {
        return None;
    }

    let mut segment: Option<String> = None;
    let mut ancestors: Vec<String> = Vec::new();
    let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut cursor = root;

    // Profundidad acotada: un ciclo del árbol no puede colgar el CREATE.
    for _ in 0..16 {
        if visited.contains(&cursor) {
            break;
        }
        visited.insert(cursor.clone());
        let attrs = writer
            .get_active_attributes(tenant_id, &cursor)
            .await
            .ok()?;
        if segment.is_none() {
            segment = attrs.get("tag").and_then(|(_, v)| match v {
                DatomValue::Str(s) => Some(s.clone()),
                _ => None,
            });
        }
        let parent = attrs.get("parent_location_id").and_then(|(_, v)| match v {
            DatomValue::Str(s) if !s.is_empty() => Some(s.clone()),
            _ => None,
        });
        match parent {
            Some(p) if !visited.contains(&p) => {
                ancestors.push(p.clone());
                cursor = p;
            }
            _ => break,
        }
    }

    Some(crate::codice::generator::ScopeContext { segment, ancestors })
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
    fn apply_declared_defaults_solo_enum_string_ausentes() {
        let model: EntityModel = serde_json::from_value(json!({
            "entity": "work_order",
            "engine": "oltp",
            "fts_fields": [],
            "event_rules": [],
            "constraints": [],
            "label": null,
            "icon": null,
            "primary_key": null,
            "is_sequence_scope_provider": false,
            "write_path_locked": false,
            "is_system": false,
            "disable_eda": false,
            "shadow_sagas_mapping": null,
            "attributes": [
                {"name": "status", "attr_type": "enum", "label": null, "required": false, "unique": null, "indexed": false, "fts": false, "is_dimension": false, "is_metric": false, "entity_ref": null, "options": ["OPEN", "CLOSED"], "is_sequence_scope": false, "is_sequence_scope_via": false, "sensitive": false, "auto_generate": null, "validation_regex": null, "default_value": "OPEN"},
                {"name": "title", "attr_type": "string", "label": null, "required": false, "unique": null, "indexed": false, "fts": false, "is_dimension": false, "is_metric": false, "entity_ref": null, "options": [], "is_sequence_scope": false, "is_sequence_scope_via": false, "sensitive": false, "auto_generate": null, "validation_regex": null, "default_value": "sin título"},
                {"name": "require_location_verification", "attr_type": "boolean", "label": null, "required": false, "unique": null, "indexed": false, "fts": false, "is_dimension": false, "is_metric": false, "entity_ref": null, "options": [], "is_sequence_scope": false, "is_sequence_scope_via": false, "sensitive": false, "auto_generate": null, "validation_regex": null, "default_value": "false"},
                {"name": "category", "attr_type": "enum", "label": null, "required": true, "unique": null, "indexed": false, "fts": false, "is_dimension": false, "is_metric": false, "entity_ref": null, "options": ["CORRECTIVE"], "is_sequence_scope": false, "is_sequence_scope_via": false, "sensitive": false, "auto_generate": null, "validation_regex": null, "default_value": null}
            ]
        }))
        .expect("modelo de prueba deserializa");

        // El title presente MANDA sobre el default; el status ausente lo recibe.
        let mut payload = json!({ "title": "OT real del cliente", "category": "CORRECTIVE" });
        apply_declared_defaults(&mut payload, &model);

        assert_eq!(payload["status"], json!("OPEN"));
        assert_eq!(payload["title"], json!("OT real del cliente"));
        // Un default tipado (boolean "false") NO se materializa: insertar el
        // string crudo corrompería el tipo — exige su propio contrato.
        assert!(payload.get("require_location_verification").is_none());

        // Un null explícito también es ausente: valor DE NACIMIENTO.
        let mut con_null = json!({ "status": null });
        apply_declared_defaults(&mut con_null, &model);
        assert_eq!(con_null["status"], json!("OPEN"));
    }

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
