//! EventBridgeClient — IEventBus implementation.
//!
//! EventBridgeClient implementando IEventBus.
//!
//! # Origin
//! En el stack anterior: cognitect/aws :events PutEvents
//! En Rust:    aws-sdk-eventbridge

use async_trait::async_trait;
use aws_sdk_eventbridge::Client;
use serde_json::Value;
use tracing::{error, info};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::domain::protocols::IEventBus;

pub struct EventBridgeClient {
    client: Client,
}

impl EventBridgeClient {
    pub async fn new() -> Self {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
        info!("[EventBridge] cliente activo");
        EventBridgeClient { client }
    }
}

/// Emite un evento a EventBridge de forma asíncrona fire-and-forget (tokio::spawn)
pub fn publish_domain_event_async(
    event_bus_name: String,
    source: String,
    detail_type: String,
    detail: Value,
) {
    tokio::spawn(async move {
        let config = aws_config::load_from_env().await;
        let client = Client::new(&config);
        let detail_json = serde_json::to_string(&detail).unwrap_or_default();

        let entry = aws_sdk_eventbridge::types::PutEventsRequestEntry::builder()
            .event_bus_name(&event_bus_name)
            .source(&source)
            .detail_type(&detail_type)
            .detail(detail_json)
            .build();

        match client.put_events().entries(entry).send().await {
            Ok(resp) => {
                if let Some(first_entry) = resp.entries().first() {
                    if let Some(event_id) = first_entry.event_id() {
                        info!(event_id = %event_id, bus = %event_bus_name, detail_type = %detail_type, "[EventBridge] Domain event publicado con éxito");
                    } else if let Some(err_code) = first_entry.error_code() {
                        error!(err_code = %err_code, bus = %event_bus_name, "[EventBridge] PutEvents entry error");
                    }
                }
            }
            Err(e) => {
                error!(error = ?e, bus = %event_bus_name, "[EventBridge] Error enviando evento de dominio");
            }
        }
    });
}

#[async_trait]
impl IEventBus for EventBridgeClient {
    /// Publica un evento de dominio en EventBridge.
    async fn put_event(
        &self,
        event_bus_name: &str,
        source: &str,
        detail_type: &str,
        detail: Value,
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
                // Clasificación pendiente: hoy toda falla mapea a INFRA_004.
                // La rama por `AccessDeniedException` se documentó en el port.
                DomainError::infra(
                    ErrorCode::Infra004,
                    format!("EventBridge PutEvents falló: {msg}"),
                )
            })?;

        // Verificar errores por-entry
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

        Err(DomainError::infra(
            ErrorCode::Infra004,
            "EventBridge: sin event_id en respuesta",
        ))
    }
}
