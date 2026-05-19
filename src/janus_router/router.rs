// [PORTED_FROM: src/metri/janus_router/core.clj]
// janus_router/router.rs — JanusRouter — Write Path del Metri Engine.
// Invocado exclusivamente por el IOP, después de Cedar + Quota.
//
// Pipeline interno (6 pasos):
//   1. load-schema  (Códice O(1))
//   2. validate-payload (validator Rust)
//   3. Pre-checks: write_path_locked, is_system_seeded
//   4. Resolve engine (oltp | olap)
//   5. Enriquecer ctx — tenant_id inyectado (NUNCA del cliente)
//   6. Despachar al canal del registry

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{json, Map, Value};
use tracing::{error, info, warn};

use crate::codice::{global as codice_global, EngineChannel};
use crate::domain::errors::{DomainError, ErrorCode};
use crate::iop::core::IopContext;
use crate::janus::validator;

// ── Trait del canal de escritura ─────────────────────────────────────────────

/// Canal de escritura para una entidad (OLTP | OLAP).
/// [PORTED_FROM: IJanusWriteChannel → (.route channel safe-ctx)]
#[async_trait::async_trait]
pub trait IWriteChannel: Send + Sync {
    async fn route(&self, ctx: IopContext) -> Result<Value, DomainError>;
}

// ── JanusRouter ──────────────────────────────────────────────────────────────

/// JanusRouter — enrutador del Write Path.
/// [PORTED_FROM: (defn route [ctx {:keys [channel-registry]}] ...)]
pub struct JanusRouter {
    channel_registry: HashMap<EngineChannel, Arc<dyn IWriteChannel>>,
}

impl JanusRouter {
    pub fn new(channel_registry: HashMap<EngineChannel, Arc<dyn IWriteChannel>>) -> Self {
        info!(
            "[JanusRouter] Router activo | canales: {:?}",
            channel_registry.keys().collect::<Vec<_>>()
        );
        JanusRouter { channel_registry }
    }

    /// Pipeline completo del Write Path (6 pasos).
    ///
    /// [PORTED_FROM: (defn route [ctx {:keys [channel-registry]}] ...)]
    pub async fn route(&self, mut ctx: IopContext) -> Result<Value, DomainError> {
        let entity_type = ctx.entity_type.clone();
        let operation   = ctx.operation.clone();

        info!(
            entity = %entity_type,
            op     = %operation,
            tenant = %ctx.tenant_id,
            "[JanusRouter] Enrutando Write Path"
        );

        let registry = codice_global();

        // 1. Cargar schema desde el Códice (O(1))
        // [PORTED_FROM: (codice/load-schema entity-type {})]
        let model = registry.get_model(&entity_type).ok_or_else(|| {
            warn!("[JanusRouter] Entidad desconocida en Códice: {entity_type}");
            DomainError::janus(
                ErrorCode::Jns001,
                format!("Entidad desconocida: '{entity_type}'"),
            )
        })?;

        let schema_json = serde_json::to_value(model).unwrap_or(json!({}));

        // 2. Validar payload (solo si no es Bulk)
        // [PORTED_FROM: (codice/validate-payload schema payload entity-type {})]
        let is_bulk = ctx.request.contains_key("data");
        if !is_bulk {
            validator::validate_entity_type(&entity_type)?;
        }

        // 3. Pre-checks del Códice
        // [PORTED_FROM: (cond (true? (get schema :write_path_locked)) ...)]
        if schema_json.get("write_path_locked").and_then(|v| v.as_bool()).unwrap_or(false) {
            warn!("[JanusRouter] write_path_locked=true | entity: {entity_type}");
            return Err(DomainError::janus(
                ErrorCode::JnsLock001,
                format!("write_path_locked: '{entity_type}' no acepta mutaciones"),
            ));
        }
        if schema_json.get("is_system_seeded").and_then(|v| v.as_bool()).unwrap_or(false) {
            warn!("[JanusRouter] is_system_seeded=true | entity: {entity_type}");
            return Err(DomainError::janus(
                ErrorCode::JnsSeed001,
                format!("is_system_seeded: '{entity_type}' es de solo-lectura"),
            ));
        }

        // 4. Resolver engine desde el Códice
        let engine = registry.get_engine(&entity_type).ok_or_else(|| {
            DomainError::janus(ErrorCode::Jns001, format!("Sin engine para '{entity_type}'"))
        })?;

        let channel = self.channel_registry.get(engine).ok_or_else(|| {
            error!("[JanusRouter] Sin canal para engine: {:?}", engine);
            DomainError::janus(
                ErrorCode::Jns001,
                format!("Sin canal de escritura para engine: {:?}", engine),
            )
        })?;

        // 5. Enriquecer ctx — tenant_id inyectado (NUNCA del cliente)
        // [PORTED_FROM: (assoc :schema schema :entity-type entity-type ...)]
        ctx.schema = Some(schema_json);
        if let Some(obj) = ctx.request.get_mut("payload").and_then(|v| v.as_object_mut()) {
            obj.insert("tenant_id".to_string(), Value::String(ctx.tenant_id.clone()));
        }
        // Bulk: inyectar tenant_id en cada row del array :data
        // [PORTED_FROM: (update-in [:request :data] #(mapv (fn [row] (assoc row :tenant_id ...)) %))]
        if let Some(Value::Array(rows)) = ctx.request.get_mut("data") {
            for row in rows.iter_mut() {
                if let Some(obj) = row.as_object_mut() {
                    obj.insert("tenant_id".to_string(), Value::String(ctx.tenant_id.clone()));
                }
            }
        }

        info!(
            engine = ?engine,
            entity = %entity_type,
            tenant = %ctx.tenant_id,
            "[JanusRouter] Despachando al canal"
        );

        // 6. Despachar al canal
        // [PORTED_FROM: (.route channel safe-ctx)]
        channel.route(ctx).await
    }
}
