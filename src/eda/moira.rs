// eda/moira.rs — MoiraEmitter -> EventBridge/SQS
// SRP: Transmite eventos asíncronos para desacoplar procesos del Write Path.

use serde_json::Value;
use tracing::info;
use crate::domain::errors::DomainError;

pub struct MoiraEmitter {
    // FASE 3/4: Cliente de EventBridge / SQS
}

impl MoiraEmitter {
    pub fn new() -> Self {
        Self {}
    }

    /// Emite un evento fire-and-forget.
    pub async fn emit_event(&self, event_type: &str, payload: &Value) -> Result<(), DomainError> {
        // STUB implementation
        info!("[Moira] Evento emitido: {} | Payload keys: {:?}", event_type, payload.as_object().map(|o| o.keys().collect::<Vec<_>>()));
        Ok(())
    }
}
