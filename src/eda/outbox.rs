// eda/outbox.rs — Outbox pattern + retry
// SRP: Asegura consistencia eventual guardando eventos fallidos para reintentos.

use serde_json::Value;
use tracing::warn;
use crate::domain::errors::DomainError;

pub struct OutboxManager {
    // FASE 3/4: Conexión a DynamoDB para la tabla de Outbox
}

impl OutboxManager {
    pub fn new() -> Self {
        Self {}
    }

    /// Guarda un evento en el Outbox para reintento posterior.
    pub async fn save_for_retry(&self, event_type: &str, payload: &Value) -> Result<(), DomainError> {
        // STUB implementation
        warn!("[Outbox] Guardando evento fallido para reintento: {}", event_type);
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/outbox_tests.rs"]
mod tests;

