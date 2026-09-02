// infrastructure/audit/interceptor.rs — AuditInterceptorImpl
//
// Se invoca SIEMPRE al final de cada request (Ok o Err).
// Captura el request completo, calcula ActionType y envía un registro al
// stream de Kinesis (OLAP) audit_log.
//
// Es fire-and-forget, nunca falla el request original.

use chrono::Utc;
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

use crate::domain::audit::action_type::derive_action_type;
use crate::domain::audit::protocol::IAuditInterceptor;
use crate::iop::core::IopContext;
use crate::janus_router::router::IWriteChannel;

/// Implementación del AuditInterceptor.
pub struct AuditInterceptorImpl {
    olap_channel: Arc<dyn IWriteChannel>,
    // FASE 10: fault_notifier para emitir AUD_001 si falla la base transaccional
}

impl AuditInterceptorImpl {
    pub fn new(olap_channel: Arc<dyn IWriteChannel>) -> Self {
        info!("[AuditInterceptor] Activado (Fire-and-Forget, OLAP-Mode)");
        Self { olap_channel }
    }

    /// Construye el snapshot de seguridad (domain_boundaries, etc).
    fn build_security_snapshot(&self, request: &Value) -> Value {
        // En Rust extraemos lo relevante del request gRPC si estuviera inyectado.
        // Por simplicidad, retornamos el nodo "metadata" si existe.
        request.get("metadata").cloned().unwrap_or(json!({}))
    }
}

#[async_trait::async_trait]
impl IAuditInterceptor for AuditInterceptorImpl {
    /// Ejecuta la auditoría de forma asíncrona (fire-and-forget).
    async fn audit(&self, request: &Value, succeeded: bool, error_stage: Option<&str>) {
        let action_type = derive_action_type(succeeded, error_stage);

        let tenant_id = request
            .get("tenant_id")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN");

        let user_id = request
            .get("user_id")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN");

        let entity_type = request
            .get("entity_type")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN");

        // Payload de auditoría (esquema audit_log en Códice)
        let audit_payload = json!({
            "action_type":      action_type.as_str(),
            "resource_domain":  entity_type,
            "tenant_id":        tenant_id,
            "user_id":          user_id,
            "timestamp":        Utc::now().timestamp_millis(),
            "security_context": self.build_security_snapshot(request),
            "request_payload":  request.get("payload").cloned().unwrap_or(Value::Null),
            "status":           if succeeded { "SUCCESS" } else { "FAILURE" }
        });

        info!(
            action = %action_type.as_str(),
            tenant = %tenant_id,
            domain = %entity_type,
            "[AuditInterceptor] Registrando evento en canal OLAP (Kinesis)"
        );

        // Disparar escritura asíncrona hacia OLAP
        // Creamos un IopContext artificial apuntando a la entidad 'audit_log'
        let mut req_map = serde_json::Map::new();
        req_map.insert("data".to_string(), Value::Array(vec![audit_payload]));

        let ctx = IopContext::new(tenant_id, user_id, "audit_log", "BULK_CREATE", req_map);

        let olap = Arc::clone(&self.olap_channel);
        tokio::spawn(async move {
            if let Err(e) = olap.route(ctx).await {
                error!(
                    "[AuditInterceptor] Falla al escribir en canal OLAP (Kinesis): {:?}",
                    e
                );
                // FASE 10: sherlog.emit_fault(AUD_001)
            }
        });
    }
}
