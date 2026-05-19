// src/aegis/oltp/channel.rs
// SRP: Implementación de IWriteChannel para el Motor OLTP (EAV).

use serde_json::{json, Value};
use tracing::{info, error};

use crate::domain::errors::{DomainError, ErrorCode};
use crate::iop::core::IopContext;
use crate::janus_router::router::IWriteChannel;
use crate::eav::writer::{EavWriter, TransactPayload, TransactOp};
use crate::codice::global as codice_global;
use crate::codice::validator;

pub struct OltpChannel {
    writer: EavWriter,
}

impl OltpChannel {
    pub fn new(writer: EavWriter) -> Self {
        Self { writer }
    }
}

#[async_trait::async_trait]
impl IWriteChannel for OltpChannel {
    async fn route(&self, mut ctx: IopContext) -> Result<Value, DomainError> {
        info!("[OltpChannel] Ejecutando Write Path para {}", ctx.entity_type);

        let registry = codice_global();
        
        let model = registry.get_model(&ctx.entity_type).ok_or_else(|| {
            DomainError::janus(ErrorCode::Jns001, format!("Entidad desconocida: '{}'", ctx.entity_type))
        })?;

        let payload_val = ctx.request.get("payload")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));

        // FASE 3: Validación estructural contra Códice
        let validated_attrs = validator::validate_payload(&model, &payload_val, &ctx.tenant_id)?;

        let op = match ctx.operation.as_str() {
            "CREATE" => TransactOp::Create,
            "UPDATE" => TransactOp::Update,
            "DELETE" => TransactOp::Delete,
            other => {
                return Err(DomainError::janus(
                    ErrorCode::JanusVal001,
                    format!("Operación no soportada por OLTP: {}", other),
                ));
            }
        };

        // Extraer entity_id opcional (requerido para UPDATE/DELETE)
        let entity_id = ctx.request.get("entity_id")
            .or_else(|| payload_val.get("entity_id"))
            .or_else(|| payload_val.get("id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        if (op == TransactOp::Update || op == TransactOp::Delete) && entity_id.is_none() {
            return Err(DomainError::janus(
                ErrorCode::JanusVal001,
                format!("Operación {} requiere entity_id", ctx.operation),
            ));
        }

        let transact_payload = TransactPayload {
            tenant_id: ctx.tenant_id.clone(),
            entity_type: ctx.entity_type.clone(),
            entity_id,
            op,
            attrs: validated_attrs,
        };

        match self.writer.transact(transact_payload).await {
            Ok(result) => {
                info!("[OltpChannel] Transacción EAV exitosa: tx_id={} entity_id={}", result.tx_id, result.entity_id);
                // Retornar información clave en la respuesta (ID, TX_ID)
                Ok(json!({
                    "entity_id": result.entity_id,
                    "tx_id": result.tx_id
                }))
            }
            Err(e) => {
                error!("[OltpChannel] Fallo EAV: {:?}", e);
                Err(e)
            }
        }
    }
}
