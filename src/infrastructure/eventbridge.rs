// [PORTED_FROM: src/metri/infrastructure/eventbridge.clj]
// infrastructure/eventbridge.rs — EventBridgeClient implementando IEventBus.
// En Clojure: cognitect/aws :events PutEvents
// En Rust:    aws-sdk-eventbridge

use async_trait::async_trait;
use aws_sdk_eventbridge::Client;
use serde_json::Value;
use tracing::{error, info};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::IEventBus;

/// [PORTED_FROM: (defrecord EventBridgeClient [client])]
pub struct EventBridgeClient {
    client: Client,
}

impl EventBridgeClient {
    /// [PORTED_FROM: ig/init-key :infra/eventbridge]
    pub async fn new() -> Self {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
        info!("[EventBridge] cliente activo");
        EventBridgeClient { client }
    }
}

#[async_trait]
impl IEventBus for EventBridgeClient {
    /// Publica un evento de dominio en EventBridge.
    /// [PORTED_FROM: (put-event! [_ bus-name source detail-type detail])]
    async fn put_event(
        &self,
        event_bus_name: &str,
        source:         &str,
        detail_type:    &str,
        detail:         Value,
    ) -> Result<String, DomainError> {
        let detail_json = serde_json::to_string(&detail).unwrap_or_default();

        let entry = aws_sdk_eventbridge::types::PutEventsRequestEntry::builder()
            .event_bus_name(event_bus_name)
            .source(source)
            .detail_type(detail_type)
            .detail(detail_json)
            .build();

        let resp = self
            .client
            .put_events()
            .entries(entry)
            .send()
            .await
            .map_err(|e| {
                let msg = format!("{e:?}");
                // [PORTED_FROM: código de error AccessDeniedException → INFRA_EVENTBRIDGE_003]
                let code = if msg.contains("AccessDeniedException") {
                    ErrorCode::Infra004
                } else {
                    ErrorCode::Infra004
                };
                DomainError::infra(code, format!("EventBridge PutEvents falló: {msg}"))
            })?;

        // Verificar errores por-entry
        // [PORTED_FROM: (when (:ErrorCode entry-resp) (errors/error :INFRA_EVENTBRIDGE_002 ...))]
        if let Some(first_entry) = resp.entries().first() {
            if let Some(error_code) = first_entry.error_code() {
                let error_message = first_entry.error_message().unwrap_or("sin mensaje");
                return Err(DomainError::infra(
                    ErrorCode::Infra004,
                    format!("EventBridge entry error [{error_code}]: {error_message}"),
                ));
            }
            if let Some(event_id) = first_entry.event_id() {
                return Ok(event_id.to_string());
            }
        }

        Err(DomainError::infra(ErrorCode::Infra004, "EventBridge: sin event_id en respuesta"))
    }
}
