// [PORTED_FROM: src/metri/iop/core.clj]
// iop/core.rs — IOP Orchestrator raíz del Metri Engine.
// En Clojure: ig/init-key :iop/orchestrator — coordina Cedar → Quota → Janus → Moira → Audit
// En Rust: IopOrchestrator struct con run() async.
//
// Arquitectura:
//   Paso 1: CedarAuthorizer  (Zero-Trust)
//   Paso 2: QuotaGuard       (control de recursos)
//   Paso 3: JanusRouter      (validación + ruteo)
//   → gRPC 200 OK al cliente
//   → (async fire-and-forget) MoiraEmitter  [solo en Ok]
//   → audit! SIEMPRE [Ok y Err]
//   → normalize_response (contrato de salida garantizado)

use std::sync::Arc;
use std::time::Instant;

use serde_json::{Map, Value};
use tracing::{error, info, warn};

use crate::domain::audit::protocol::IAuditInterceptor;
use crate::domain::errors::DomainError;
use crate::janus::normalizer::{normalize_response, ResponseType};

/// Contexto de una request IOP — viaja por todo el pipeline.
/// [PORTED_FROM: el mapa `ctx` / `request` que pasa por cada step]
#[derive(Debug, Clone)]
pub struct IopContext {
    pub tenant_id:  String,
    pub user_id:    String,
    pub request:    Map<String, Value>, // payload original gRPC
    pub entity_type: String,
    pub operation:  String,
    // Enriquecido por cada paso del pipeline:
    pub schema:     Option<Value>,
    pub model:      Option<Value>,
    pub roles:      Vec<String>,
    pub exec_start: Option<Instant>,
    pub execution_time_ms: Option<u64>,
    // ── Cedar (Paso 1) ─────────────────────────────────────────────────────────
    pub granted_action_keys: std::collections::HashSet<String>,
    pub is_super_master:     bool,
    pub cross_tenant_scope:  String,
    pub domain_boundaries:   Value,
    // ── Quota (Paso 2) ─────────────────────────────────────────────────────────
    pub quota_reservation:   Option<Value>,
}

impl IopContext {
    pub fn new(
        tenant_id:   impl Into<String>,
        user_id:     impl Into<String>,
        entity_type: impl Into<String>,
        operation:   impl Into<String>,
        request:     Map<String, Value>,
    ) -> Self {
        IopContext {
            tenant_id:   tenant_id.into(),
            user_id:     user_id.into(),
            entity_type: entity_type.into(),
            operation:   operation.into(),
            request,
            schema:      None,
            model:       None,
            roles:       Vec::new(),
            exec_start:  Some(Instant::now()),
            execution_time_ms: None,
            granted_action_keys: std::collections::HashSet::new(),
            is_super_master:     false,
            cross_tenant_scope:  "NONE".to_string(),
            domain_boundaries:   Value::Object(serde_json::Map::new()),
            quota_reservation:   None,
        }
    }
}

/// Trait del IOP Orchestrator — permite inyección de mocks en tests.
/// [PORTED_FROM: (fn run-iop [request] ...)]
#[async_trait::async_trait]
pub trait IIopOrchestrator: Send + Sync {
    async fn run(&self, ctx: IopContext) -> Value;
}

/// Implementación real del IOP Orchestrator.
/// [PORTED_FROM: ig/init-key :iop/orchestrator]
pub struct IopOrchestrator {
    /// Interceptores del pipeline en orden (Cedar → Quota → Janus)
    steps:             Vec<Arc<dyn IopStep>>,
    /// Emitter Moira — fire-and-forget tras Ok
    moira_emitter:     Option<Arc<dyn MoiraEmitter>>,
    /// Audit interceptor — se invoca siempre
    audit_interceptor: Option<Arc<dyn IAuditInterceptor>>,
}

impl IopOrchestrator {
    pub fn new(
        steps:             Vec<Arc<dyn IopStep>>,
        moira_emitter:     Option<Arc<dyn MoiraEmitter>>,
        audit_interceptor: Option<Arc<dyn IAuditInterceptor>>,
    ) -> Self {
        info!(
            "[IOP] Orchestrator activo | {} pasos síncronos",
            steps.len()
        );
        IopOrchestrator { steps, moira_emitter, audit_interceptor }
    }
}

#[async_trait::async_trait]
impl IIopOrchestrator for IopOrchestrator {
    /// Pipeline IOP completo.
    /// [PORTED_FROM: (fn run-iop [request] pipeline/run + moira + audit + normalizer)]
    #[tracing::instrument(name = "iop.pipeline.start", skip(self, ctx), fields(tenant_id = %ctx.tenant_id, request_id))]
    async fn run(&self, ctx: IopContext) -> Value {
        let start = Instant::now();
        let request_clone = ctx.request.clone(); // para audit
        let tenant_id = ctx.tenant_id.clone();
        let user_id   = ctx.user_id.clone();

        // ── Pipeline Railway: Cedar → Quota → Janus ──────────────────────────
        // [PORTED_FROM: (pipeline/run steps request)]
        // Los steps se inyectan en IopOrchestrator::new() — cero &[] vacío.
        let result = self.run_steps(ctx).await;

        let exec_ms = start.elapsed().as_millis() as u64;

        match &result {
            // ── Ok → Moira fire-and-forget ───────────────────────────────────
            Ok(final_ctx) => {
                if let Some(moira) = &self.moira_emitter {
                    let moira = Arc::clone(moira);
                    let ctx_clone = final_ctx.clone();
                    tokio::spawn(async move {
                        // [PORTED_FROM: (future (moira-emitter (second result+)))]
                        if let Err(e) = moira.emit(ctx_clone).await {
                            error!("[IOP] Moira fire-and-forget falló: {e:?}");
                        }
                    });
                }
            }
            Err(e) => {
                warn!("[IOP] Pipeline error: {e:?}");
            }
        }

        // ── Audit: SIEMPRE — Ok y Err ────────────────────────────────────────
        // [PORTED_FROM: (audit! audit-interceptor request result+)]
        if let Some(audit) = &self.audit_interceptor {
            let succeeded = result.is_ok();
            let req_val   = Value::Object(request_clone);
            audit.audit(&req_val, succeeded).await;
        }

        // ── Normalize + enriquecer exec_time_ms ─────────────────────────────
        // [PORTED_FROM: (normalizer/normalize-response result+)]
        match result {
            Ok(ctx) => {
                let mut body = Value::Object(ctx.request.clone());
                // Inyectar execution_time_ms en la respuesta
                if let Some(obj) = body.as_object_mut() {
                    obj.insert("execution_time_ms".to_string(), Value::Number(exec_ms.into()));
                }
                normalize_response(&body, ResponseType::Transaction)
            }
            Err(e) => {
                crate::iop::error_response::build_error_dto(
                    &e,
                    &tenant_id,
                    &user_id,
                    None,
                    None,
                )
            }
        }
    }
}

impl IopOrchestrator {
    /// Ejecuta los steps inyectados en orden Railway.
    async fn run_steps(&self, ctx: IopContext) -> Result<IopContext, DomainError> {
        let mut current = ctx;
        for step in &self.steps {
            current = step.execute(current).await?;
        }
        Ok(current)
    }
}

// ── Traits de extensión ───────────────────────────────────────────────────────

/// Paso del pipeline IOP — Cedar / Quota / JanusRouter implementan este trait.
/// [PORTED_FROM: cada step-fn en el vector steps]
#[async_trait::async_trait]
pub trait IopStep: Send + Sync {
    async fn execute(&self, ctx: IopContext) -> Result<IopContext, DomainError>;
}

/// Moira Emitter — fire-and-forget EDA tras Ok.
/// [PORTED_FROM: moira-emitter de ig/init-key :iop/orchestrator]
#[async_trait::async_trait]
pub trait MoiraEmitter: Send + Sync {
    async fn emit(&self, ctx: IopContext) -> Result<(), DomainError>;
}
