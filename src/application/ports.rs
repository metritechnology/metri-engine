//! Puertos de salida de la aplicación — el contrato de error gobierna reintentos frente a DLQ.
//!
//! El tipo de error forma parte del contrato arquitectónico: `PublishError::retryable`
//! determina si el evento se encola para reintento con backoff exponencial (outbox sweeper)
//! o si se descarta hacia la cola de mensajes fallidos (DLQ).

use async_trait::async_trait;
use serde_json::Value;

/// Clasificación de errores de publicación de eventos de dominio según su recuperabilidad.
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// Fallo de infraestructura: el sweeper del outbox reintenta con backoff.
    #[error("transitorio (reintentable): {0}")]
    Transient(String),
    /// El evento viola el contrato: reintentar no lo arregla — DLQ y humano.
    #[error("poison (DLQ): {0}")]
    Poison(String),
}

impl PublishError {
    /// Indica si el fallo es transitorio y admite reintento automático mediante el outbox.
    pub fn retryable(&self) -> bool {
        matches!(self, PublishError::Transient(_))
    }
}

/// Puerto de publicación de eventos de dominio del contrato
/// `metri-contracts` (`system.scheduled_job.*` y familia).
///
/// Los adaptadores concretos viven en `infrastructure/domain_event_bus.rs`.
#[async_trait]
pub trait DomainEventPublisher: Send + Sync {
    /// Publica un evento de dominio tipado hacia el bus de eventos configurado.
    async fn publish(&self, detail_type: &str, detail: &Value) -> Result<(), PublishError>;
}

/// Publicación desacoplada de la latencia de la transacción: el canal de
/// escritura llama esto para no bloquear la respuesta del Transact. El
/// adaptador decide cómo (spawn, cola local…); los errores se registran con su
/// clasificación retryable.
pub fn publish_detached(
    publisher: std::sync::Arc<dyn DomainEventPublisher>,
    detail_type: String,
    detail: Value,
) {
    tokio::spawn(async move {
        if let Err(e) = publisher.publish(&detail_type, &detail).await {
            if e.retryable() {
                tracing::warn!(detail_type = %detail_type, error = %e, "[events] publicación reintentará vía outbox");
            } else {
                tracing::error!(detail_type = %detail_type, error = %e, "[events] evento poison — requiere corrección manual");
            }
        }
    });
}
