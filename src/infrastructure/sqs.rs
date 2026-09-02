// [PORTED_FROM: src/metri/infrastructure/sqs.clj]
// infrastructure/sqs.rs — SQSFifoBus implementando ISqsBus.
// En Clojure: (defrecord SQSFifoBus [client queue-url])
// En Rust:    aws-sdk-sqs + ISqsBus trait

use async_trait::async_trait;
use aws_sdk_sqs::Client;
use serde_json::Value;
use tracing::{info, warn};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::{ISqsBus, SqsMessage};

/// [PORTED_FROM: (defrecord SQSFifoBus [client queue-url])]
pub struct SqsFifoBus {
    client: Client,
    queue_url: String,
}

impl SqsFifoBus {
    /// [PORTED_FROM: ig/init-key :moira/sqs-bus]
    pub async fn new(queue_url: impl Into<String>) -> Self {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
        let url = queue_url.into();
        info!("[SQS] cliente FIFO activo | queue: {url}");
        SqsFifoBus {
            client,
            queue_url: url,
        }
    }

    /// Health check: verifica que la cola existe.
    /// [PORTED_FROM: (aws/invoke client {:op :GetQueueAttributes ...})]
    pub async fn health_check(&self) -> bool {
        match self
            .client
            .get_queue_attributes()
            .queue_url(&self.queue_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
        {
            Ok(resp) => {
                let msgs = resp
                    .attributes
                    .as_ref()
                    .and_then(|m| {
                        m.get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
                    })
                    .cloned()
                    .unwrap_or_default();
                info!("[SQS] cola activa | msgs ~{msgs}");
                true
            }
            Err(e) => {
                warn!("[SQS] GetQueueAttributes falló (puede ser normal en inicio): {e}");
                false
            }
        }
    }
}

#[async_trait]
impl ISqsBus for SqsFifoBus {
    /// Publica un mensaje en la cola FIFO.
    /// [PORTED_FROM: (publish! [_ payload group-id dedup-id])]
    async fn publish(
        &self,
        payload: &str,
        group_id: &str,
        dedup_id: &str,
    ) -> Result<String, DomainError> {
        let resp = self
            .client
            .send_message()
            .queue_url(&self.queue_url)
            .message_body(payload)
            .message_group_id(group_id)
            .message_deduplication_id(dedup_id)
            .send()
            .await
            .map_err(|e| {
                let msg = format!("{e:?}");
                let code = if msg.contains("AccessDeniedException") {
                    ErrorCode::Infra003
                } else {
                    ErrorCode::Infra003
                };
                DomainError::infra(code, format!("SQS SendMessage falló: {msg}"))
            })?;

        resp.message_id
            .ok_or_else(|| DomainError::infra(ErrorCode::Infra003, "SQS: sin message_id"))
    }

    /// Recibe mensajes de la cola (long-polling 5s).
    /// [PORTED_FROM: (receive-messages [_ max-count])]
    async fn receive_messages(&self, max_count: u32) -> Result<Vec<SqsMessage>, DomainError> {
        // Límite AWS: máximo 10 mensajes por ReceiveMessage
        // [PORTED_FROM: (min max-count 10) ;; límite AWS]
        let count = (max_count.min(10)) as i32;

        let resp = self
            .client
            .receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(count)
            .wait_time_seconds(5) // long-polling
            .send()
            .await
            .map_err(|e| {
                DomainError::infra(
                    ErrorCode::Infra003,
                    format!("SQS ReceiveMessage falló: {e}"),
                )
            })?;

        let messages = resp
            .messages
            .unwrap_or_default()
            .into_iter()
            .filter_map(|m| {
                Some(SqsMessage {
                    receipt_handle: m.receipt_handle?,
                    body: m.body.unwrap_or_default(),
                    message_id: m.message_id.unwrap_or_default(),
                })
            })
            .collect();

        Ok(messages)
    }

    /// Confirma y elimina un mensaje procesado.
    /// [PORTED_FROM: (delete-message! [_ receipt-handle])]
    async fn delete_message(&self, receipt_handle: &str) -> Result<(), DomainError> {
        self.client
            .delete_message()
            .queue_url(&self.queue_url)
            .receipt_handle(receipt_handle)
            .send()
            .await
            .map_err(|e| {
                DomainError::infra(ErrorCode::Infra003, format!("SQS DeleteMessage falló: {e}"))
            })?;
        Ok(())
    }
}

/// StubSqsBus — Simulación local en memoria del SQS FIFO.
pub struct StubSqsBus {
    pub messages: std::sync::Mutex<Vec<SqsMessage>>,
}

impl StubSqsBus {
    pub fn new() -> Self {
        StubSqsBus {
            messages: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl ISqsBus for StubSqsBus {
    async fn publish(
        &self,
        payload: &str,
        group_id: &str,
        dedup_id: &str,
    ) -> Result<String, DomainError> {
        info!(
            group_id = %group_id,
            dedup_id = %dedup_id,
            payload = %payload,
            "[StubSqsBus] Mensaje publicado en local"
        );
        let msg_id = format!("stub-msg-{}", uuid::Uuid::new_v4());
        self.messages.lock().unwrap().push(SqsMessage {
            receipt_handle: format!("receipt-{}", msg_id),
            body: payload.to_string(),
            message_id: msg_id.clone(),
        });
        Ok(msg_id)
    }

    async fn receive_messages(&self, max_count: u32) -> Result<Vec<SqsMessage>, DomainError> {
        let mut msgs = self.messages.lock().unwrap();
        let count = (max_count as usize).min(msgs.len());
        let drained = msgs.drain(..count).collect();
        Ok(drained)
    }

    async fn delete_message(&self, _receipt_handle: &str) -> Result<(), DomainError> {
        Ok(())
    }
}
