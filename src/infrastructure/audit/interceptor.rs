//! AuditInterceptorImpl — always invoked, absorbs its own errors.
//!
//! AuditInterceptorImpl
//!
//! Se invoca SIEMPRE al final de cada request (Ok o Err).
//! Captura el request completo, calcula ActionType y persiste un registro
//! `audit_log` en el canal de almacenamiento MBF.
//!
//! Es fire-and-forget, nunca falla el request original.

use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{error, info};

use crate::domain::audit::action_type::derive_action_type;
use crate::domain::audit::protocol::IAuditInterceptor;
use crate::iop::core::IopContext;
use crate::janus_router::router::IWriteChannel;

/// Implementación del AuditInterceptor.
pub struct AuditInterceptorImpl {
    telemetry_channel: Arc<dyn IWriteChannel>,
    // FASE 10: fault_notifier para emitir AUD_001 si falla la base transaccional
}

impl AuditInterceptorImpl {
    pub fn new(telemetry_channel: Arc<dyn IWriteChannel>) -> Self {
        info!("[AuditInterceptor] Activado (Fire-and-Forget, MBF-Mode)");
        Self { telemetry_channel }
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

        let resolved_user_id = match request.get("user_id").and_then(|v| v.as_str()) {
            Some(u) if !u.is_empty() && u != "UNKNOWN" => u,
            _ => "usr_system_bff",
        };

        let entity_type = request
            .get("entity_type")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN");

        // Payload de auditoría (esquema audit_log en Códice)
        let audit_payload = json!({
            "action_type":      action_type.as_str(),
            "resource_domain":  entity_type,
            "tenant_id":        tenant_id,
            "user_id":          resolved_user_id,
            "security_context": self.build_security_snapshot(request).to_string(),
        });

        info!(
            action = %action_type.as_str(),
            tenant = %tenant_id,
            domain = %entity_type,
            "[AuditInterceptor] Registrando evento en canal de persistencia MBF"
        );

        // Disparar escritura asíncrona hacia el almacenamiento MBF
        // Creamos un IopContext artificial apuntando a la entidad 'audit_log'
        let mut req_map = serde_json::Map::new();
        req_map.insert("data".to_string(), Value::Array(vec![audit_payload]));

        let ctx = IopContext::new(
            tenant_id,
            resolved_user_id,
            "audit_log",
            "BULK_CREATE",
            req_map,
        );

        if let Err(e) = self.telemetry_channel.route(ctx).await {
            error!(
                "[AuditInterceptor] Falla al escribir en canal de persistencia MBF: {:?}",
                e
            );
            // FASE 10: sherlog.emit_fault(AUD_001)
        }
    }
}
