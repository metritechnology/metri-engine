//! Moira emitter — async event dispatch decoupling the write path.
//!
//! MoiraEmitterImpl
//! Transmite eventos asíncronos para desacoplar procesos del Write Path.

use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, instrument};

use crate::aegis::oltp::executor::OltpExecutor;
use crate::aegis::oltp::hydrator::entity_map_to_json;
use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::ISqsBus;
use crate::eav::reader::pull::EavReader;
use crate::eav::types::datom::DatomValue;
use crate::eav::writer::{EavWriter, TransactOp, TransactPayload};
use crate::iop::core::{IopContext, MoiraEmitter};

#[async_trait]
pub trait EavPullReader: Send + Sync {
    async fn pull(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attributes: Option<&[&str]>,
    ) -> Result<HashMap<String, DatomValue>, DomainError>;
}

#[async_trait]
pub trait EavTransacter: Send + Sync {
    async fn transact(
        &self,
        payload: TransactPayload,
    ) -> Result<crate::eav::writer::TransactResult, DomainError>;
}

#[async_trait]
pub trait OltpQueryRunner: Send + Sync {
    async fn run_oltp_query(&self, tenant_id: &str, ast_ir: &Value) -> Result<Value, DomainError>;
}

#[async_trait]
impl EavPullReader for Arc<EavReader> {
    async fn pull(
        &self,
        tenant_id: &str,
        entity_id: &str,
        attributes: Option<&[&str]>,
    ) -> Result<HashMap<String, DatomValue>, DomainError> {
        self.as_ref().pull(tenant_id, entity_id, attributes).await
    }
}

#[async_trait]
impl EavTransacter for Arc<EavWriter> {
    async fn transact(
        &self,
        payload: TransactPayload,
    ) -> Result<crate::eav::writer::TransactResult, DomainError> {
        self.as_ref().transact(payload).await
    }
}

#[async_trait]
impl OltpQueryRunner for OltpExecutor {
    async fn run_oltp_query(&self, tenant_id: &str, ast_ir: &Value) -> Result<Value, DomainError> {
        self.run_oltp_query(tenant_id, ast_ir).await
    }
}

pub struct MoiraEmitterImpl<R = Arc<EavReader>, W = Arc<EavWriter>, O = OltpExecutor> {
    eav_reader: R,
    eav_writer: W,
    sqs_bus: Arc<dyn ISqsBus>,
    oltp_executor: O,
}

impl<R, W, O> MoiraEmitterImpl<R, W, O>
where
    R: EavPullReader,
    W: EavTransacter,
    O: OltpQueryRunner,
{
    pub fn new(eav_reader: R, eav_writer: W, sqs_bus: Arc<dyn ISqsBus>, oltp_executor: O) -> Self {
        Self {
            eav_reader,
            eav_writer,
            sqs_bus,
            oltp_executor,
        }
    }

    /// Obtiene eventos PENDING elegibles para despacho
    async fn fetch_pending_events(&self, tenant_id: &str) -> Result<Vec<Value>, DomainError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        // Buscar status == "PENDING"
        let query_pending = json!({
            "entity": "outbox_event",
            "select": ["id", "status", "detail_type", "payload", "retry_count", "retry_at", "claimed_at", "created_at"],
            "where": [
                "and",
                ["=", "status", "PENDING"]
            ],
            "limit": 100
        });

        let pending_rows = self
            .oltp_executor
            .run_oltp_query(tenant_id, &query_pending)
            .await?;
        let mut events = pending_rows.as_array().cloned().unwrap_or_default();

        // Buscar status == "FAILED" con retry_at <= now
        let query_failed = json!({
            "entity": "outbox_event",
            "select": ["id", "status", "detail_type", "payload", "retry_count", "retry_at", "claimed_at", "created_at"],
            "where": [
                "and",
                ["=", "status", "FAILED"]
            ],
            "limit": 100
        });

        if let Ok(failed_rows) = self
            .oltp_executor
            .run_oltp_query(tenant_id, &query_failed)
            .await
        {
            if let Some(arr) = failed_rows.as_array() {
                for item in arr {
                    let retry_at = item.get("retry_at").and_then(|v| v.as_i64()).unwrap_or(0);
                    let retry_count = item
                        .get("retry_count")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    if retry_count < 5 && retry_at <= now {
                        events.push(item.clone());
                    }
                }
            }
        }

        // Ordenar por created_at (FIFO)
        events.sort_by_key(|e| e.get("created_at").and_then(|v| v.as_i64()).unwrap_or(0));
        Ok(events)
    }

    /// Intenta marcar un evento de PENDING a PROCESSING de forma atómica.
    async fn try_claim_event(
        &self,
        tenant_id: &str,
        event_id: &str,
    ) -> Result<Option<Value>, DomainError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        // 1. Pull del estado del evento
        let event = self.eav_reader.pull(tenant_id, event_id, None).await?;
        if event.is_empty() {
            return Ok(None);
        }

        let status = event
            .get("status")
            .and_then(|v| match v {
                DatomValue::Str(s) => Some(s.as_str()),
                _ => None,
            })
            .unwrap_or("PENDING");

        // El evento solo se puede reclamar si está PENDING o FAILED
        if status != "PENDING" && status != "FAILED" {
            return Ok(None);
        }

        // 2. Transact para cambiar status a PROCESSING y setear claimed_at
        let mut attrs = HashMap::new();
        attrs.insert(
            "status".to_string(),
            DatomValue::Str("PROCESSING".to_string()),
        );
        attrs.insert("claimed_at".to_string(), DatomValue::Instant(now));

        let transact = TransactPayload {
            tenant_id: tenant_id.to_string(),
            entity_type: "outbox_event".to_string(),
            entity_id: Some(event_id.to_string()),
            attrs,
            op: TransactOp::Update,
            suppress_events: false,
        };

        self.eav_writer.transact(transact).await?;

        let mut event_hydrated = self.eav_reader.pull(tenant_id, event_id, None).await?;
        if !event_hydrated.is_empty() {
            event_hydrated.insert("id".to_string(), DatomValue::Str(event_id.to_string()));
            event_hydrated.insert(
                "outbox_id".to_string(),
                DatomValue::Str(event_id.to_string()),
            );
        }
        let json_val = entity_map_to_json(event_hydrated);
        Ok(Some(json_val))
    }

    async fn mark_delivered(&self, tenant_id: &str, event_id: &str) -> Result<(), DomainError> {
        let mut attrs = HashMap::new();
        attrs.insert(
            "status".to_string(),
            DatomValue::Str("DELIVERED".to_string()),
        );

        let transact = TransactPayload {
            tenant_id: tenant_id.to_string(),
            entity_type: "outbox_event".to_string(),
            entity_id: Some(event_id.to_string()),
            attrs,
            op: TransactOp::Update,
            suppress_events: false,
        };
        self.eav_writer.transact(transact).await?;
        Ok(())
    }

    async fn mark_failed(
        &self,
        tenant_id: &str,
        event_id: &str,
        max_retries: i64,
    ) -> Result<(), DomainError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let event = self.eav_reader.pull(tenant_id, event_id, None).await?;
        let current_retries = event
            .get("retry_count")
            .and_then(|v| match v {
                DatomValue::Long(n) => Some(*n),
                _ => None,
            })
            .unwrap_or(0);

        let next_retry = current_retries + 1;
        let is_exhausted = next_retry >= max_retries;

        // Backoff exponencial: delay = min(2^retry_count × 30s, 30min)
        let delay_ms = if is_exhausted {
            0
        } else {
            let base = 2_f64.powi(current_retries as i32) * 30_000.0;
            base.min(1_800_000.0) as i64
        };
        // El estado resultante es siempre FAILED: si aún hay reintentos, el
        // fetcher lo relee cuando `retry_at` expire; si se agotó, permanente.
        let retry_at = now + delay_ms;

        let mut attrs = HashMap::new();
        // Si no se agota, sigue en FAILED pero con retry_at incrementado. El fetcher leerá FAILED con retry_at expirado
        attrs.insert("status".to_string(), DatomValue::Str("FAILED".to_string()));
        attrs.insert("retry_count".to_string(), DatomValue::Long(next_retry));
        attrs.insert("retry_at".to_string(), DatomValue::Instant(retry_at));

        let transact = TransactPayload {
            tenant_id: tenant_id.to_string(),
            entity_type: "outbox_event".to_string(),
            entity_id: Some(event_id.to_string()),
            attrs,
            op: TransactOp::Update,
            suppress_events: false,
        };
        self.eav_writer.transact(transact).await?;

        if is_exhausted {
            error!(
                tenant_id = %tenant_id,
                event_id = %event_id,
                "[Moira] Outbox event superó max_retries y pasó a estado FAILED permanente"
            );
        }
        Ok(())
    }

    /// Watchdog para eventos huérfanos que quedaron PROCESSING
    pub async fn reset_orphaned_processing(
        &self,
        tenant_id: &str,
        ttl_ms: i64,
    ) -> Result<usize, DomainError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let cutoff = now - ttl_ms;

        let query = json!({
            "entity": "outbox_event",
            "select": ["id", "status", "claimed_at"],
            "where": [
                "and",
                ["=", "status", "PROCESSING"]
            ],
            "limit": 100
        });

        let rows = self.oltp_executor.run_oltp_query(tenant_id, &query).await?;
        let candidates = rows.as_array().cloned().unwrap_or_default();
        let mut reset_count = 0;

        for event in candidates {
            let claimed_at = event
                .get("claimed_at")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if claimed_at <= cutoff {
                let event_id = event.get("id").and_then(|v| v.as_str()).unwrap_or("");

                // Revertir a PENDING
                let mut attrs = HashMap::new();
                attrs.insert("status".to_string(), DatomValue::Str("PENDING".to_string()));

                let transact = TransactPayload {
                    tenant_id: tenant_id.to_string(),
                    entity_type: "outbox_event".to_string(),
                    entity_id: Some(event_id.to_string()),
                    attrs,
                    op: TransactOp::Update,
                    suppress_events: false,
                };
                self.eav_writer.transact(transact).await?;
                reset_count += 1;
            }
        }

        Ok(reset_count)
    }
}

#[async_trait]
impl<R, W, O> MoiraEmitter for MoiraEmitterImpl<R, W, O>
where
    R: EavPullReader,
    W: EavTransacter,
    O: OltpQueryRunner,
{
    #[instrument(name = "moira.emit.start", skip(self, ctx), fields(tenant_id = %ctx.tenant_id))]
    async fn emit(&self, ctx: IopContext) -> Result<(), DomainError> {
        let tenant_id = &ctx.tenant_id;

        // 1. Obtener eventos pendientes
        let pending_events = self.fetch_pending_events(tenant_id).await?;
        if pending_events.is_empty() {
            return Ok(());
        }

        for event in pending_events {
            let event_id = match event.get("id").and_then(|v| v.as_str()) {
                Some(id) => id,
                None => continue,
            };

            // 2. Claim atómico
            match self.try_claim_event(tenant_id, event_id).await {
                Ok(Some(claimed_event)) => {
                    // 3. Publicar a SQS FIFO
                    let payload_str = serde_json::to_string(&claimed_event)
                        .map_err(|e| DomainError::eav(ErrorCode::Eav004, e.to_string()))?;

                    let detail_type = claimed_event
                        .get("detail_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown");
                    let group_id = format!("tnt_{tenant_id}_{detail_type}");
                    let dedup_id = format!("{event_id}_{detail_type}");

                    match self
                        .sqs_bus
                        .publish(&payload_str, &group_id, &dedup_id)
                        .await
                    {
                        Ok(msg_id) => {
                            info!(event_id = %event_id, message_id = %msg_id, "[Moira] Publicado en SQS FIFO");
                            // 4. Confirmar entregado
                            self.mark_delivered(tenant_id, event_id).await?;
                        }
                        Err(e) => {
                            error!(event_id = %event_id, error = ?e, "[Moira] Fallo de envío a SQS FIFO");
                            // 5. Reintento con backoff
                            self.mark_failed(tenant_id, event_id, 5).await?;
                        }
                    }
                }
                Ok(None) => {
                    // Otra réplica ganó la carrera
                    continue;
                }
                Err(e) => {
                    error!(event_id = %event_id, error = ?e, "[Moira] Error intentando hacer claim");
                }
            }
        }

        Ok(())
    }

    async fn reset_orphaned_processing(
        &self,
        tenant_id: &str,
        ttl_ms: i64,
    ) -> Result<usize, DomainError> {
        self.reset_orphaned_processing(tenant_id, ttl_ms).await
    }
}

#[cfg(test)]
#[path = "tests/moira_tests.rs"]
mod tests;
