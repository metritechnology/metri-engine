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

    /// ¿Lleva este contexto algún efecto externo que habría que deshacer si un
    /// paso posterior falla?
    ///
    /// El orquestador lo consulta para no clonar el contexto cuando no hay nada
    /// que devolver, que es el caso de la mayoría de las peticiones. Vive aquí
    /// y no en `run_steps` para que añadir un efecto reversible nuevo sea una
    /// línea en un sitio, no una condición repartida por el pipeline.
    pub fn has_reversible_effects(&self) -> bool {
        self.quota_reservation.is_some()
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
    /// Notifier de fallos de Sherlog
    fault_notifier:    Arc<dyn crate::iop::sherlog::IFaultNotifier>,
    /// Canal OLAP para persistir domain_fault
    olap_channel:      Arc<dyn crate::janus_router::router::IWriteChannel>,
}

impl IopOrchestrator {
    pub fn new(
        steps:             Vec<Arc<dyn IopStep>>,
        moira_emitter:     Option<Arc<dyn MoiraEmitter>>,
        audit_interceptor: Option<Arc<dyn IAuditInterceptor>>,
        fault_notifier:    Arc<dyn crate::iop::sherlog::IFaultNotifier>,
        olap_channel:      Arc<dyn crate::janus_router::router::IWriteChannel>,
    ) -> Self {
        info!(
            "[IOP] Orchestrator activo | {} pasos síncronos",
            steps.len()
        );
        IopOrchestrator {
            steps,
            moira_emitter,
            audit_interceptor,
            fault_notifier,
            olap_channel,
        }
    }
}

#[async_trait::async_trait]
impl IIopOrchestrator for IopOrchestrator {
    /// Pipeline IOP completo.
    /// [PORTED_FROM: (fn run-iop [request] pipeline/run + moira + audit + normalizer)]
    #[tracing::instrument(
        name = "iop.pipeline.start",
        skip(self, ctx),
        fields(
            tenant_id = %ctx.tenant_id,
            user_id = %ctx.user_id,
            entity_type = %ctx.entity_type,
            operation = %ctx.operation,
            request_id,
            error.code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty
        )
    )]
    async fn run(&self, ctx: IopContext) -> Value {
        let start = Instant::now();
        let request_clone = ctx.request.clone(); // para audit
        let tenant_id = ctx.tenant_id.clone();
        let user_id   = ctx.user_id.clone();
        let entity_type = ctx.entity_type.clone();

        // ── Pipeline Railway: Cedar → Quota → Janus ──────────────────────────
        // [PORTED_FROM: (pipeline/run steps request)]
        // Los steps se inyectan en IopOrchestrator::new() — cero &[] vacío.
        let result = self.run_steps(ctx).await;

        let exec_ms = start.elapsed().as_millis() as u64;

        let error_dto = match &result {
            // ── Ok → Moira fire-and-forget ───────────────────────────────────
            Ok(final_ctx) => {
                self.spawn_moira_emitter(final_ctx.clone());
                None
            }
            // ── Err → Sherlog fault + build DTO ──────────────────────────────
            Err(e) => {
                let error_dto = self.process_pipeline_error(e, &tenant_id, &user_id, &entity_type);
                Some(error_dto)
            }
        };

        // ── Audit: SIEMPRE — Ok y Err ────────────────────────────────────────
        // [PORTED_FROM: (audit! audit-interceptor request result+)]
        self.perform_audit(request_clone, &user_id, &result).await;

        // ── Normalize + enriquecer exec_time_ms ─────────────────────────────
        // [PORTED_FROM: (normalizer/normalize-response result+)]
        self.normalize_and_enrich_response(result, error_dto, exec_ms)
    }
}

impl IopOrchestrator {
    /// Ejecuta los steps inyectados en orden Railway.
    async fn run_steps(&self, ctx: IopContext) -> Result<IopContext, DomainError> {
        let mut current = ctx;
        // Pasos ya completados, para deshacerlos en orden inverso si uno
        // posterior falla — la mitad «compensar» de una saga.
        let mut executed: Vec<Arc<dyn IopStep>> = Vec::new();
        // Copia del contexto tomada justo antes de cada paso, y SOLO cuando ya
        // lleva un efecto externo. En la petición normal —sin reserva— no se
        // clona nada; `execute` consume el contexto y en el camino de error se
        // habría perdido.
        let mut undo_ctx: Option<IopContext> = None;

        for step in &self.steps {
            if current.has_reversible_effects() {
                undo_ctx = Some(current.clone());
            }

            match step.execute(current).await {
                Ok(next) => {
                    executed.push(Arc::clone(step));
                    current = next;
                }
                Err(e) => {
                    if let Some(ref undo) = undo_ctx {
                        for done in executed.iter().rev() {
                            done.compensate(undo).await;
                        }
                    }
                    return Err(e);
                }
            }
        }
        Ok(current)
    }

    /// Dispara MoiraEmitter de forma asíncrona (fire-and-forget) ante el flujo exitoso.
    fn spawn_moira_emitter(&self, final_ctx: IopContext) {
        if let Some(moira) = &self.moira_emitter {
            let moira = Arc::clone(moira);
            tokio::spawn(async move {
                if let Err(e) = moira.emit(final_ctx).await {
                    error!("[IOP] Moira fire-and-forget falló: {e:?}");
                }
            });
        }
    }

    /// Procesa errores del pipeline abstrayendo la generación del DTO de error y escalando el fallo a Sherlog.
    fn process_pipeline_error(
        &self,
        e: &DomainError,
        tenant_id: &str,
        user_id: &str,
        entity_type: &str,
    ) -> Value {
        let error_code_str = e.code.canonical_code();
        let span = tracing::Span::current();
        span.record("error.code", error_code_str);
        span.record("otel.status_code", "ERROR");
        warn!("[IOP] Pipeline error: {e:?}");

        // Cargar el EntityModel para la sanitización de privacidad (redactado de campos sensitive: true)
        let model = crate::codice::registry::global_opt()
            .and_then(|r| r.get_model(entity_type));

        let error_dto = crate::iop::error_response::build_error_dto(
            e,
            tenant_id,
            user_id,
            e.context.clone(),
            model,
        );

        // Disparo de Sherlog (process_fault) fire-and-forget
        let notifier = Arc::clone(&self.fault_notifier);
        let olap = Arc::clone(&self.olap_channel);
        let error_clone = e.clone();
        let error_dto_clone = error_dto.clone();
        let entity_type_clone = entity_type.to_string();

        tokio::spawn(async move {
            crate::iop::sherlog::process_fault(
                notifier.as_ref(),
                olap.as_ref(),
                &error_clone,
                &error_dto_clone,
                Some(entity_type_clone),
            ).await;
        });

        error_dto
    }

    /// Envía logs de auditoría al Audit Interceptor para todas las transacciones.
    async fn perform_audit(
        &self,
        mut request_payload: Map<String, Value>,
        user_id: &str,
        result: &Result<IopContext, DomainError>,
    ) {
        if let Some(audit) = &self.audit_interceptor {
            let succeeded = result.is_ok();
            let error_stage = match result {
                Ok(_) => None,
                Err(e) => Some(e.stage.as_str()),
            };
            request_payload.insert("user_id".to_string(), Value::String(user_id.to_string()));
            let req_val = Value::Object(request_payload);
            audit.audit(&req_val, succeeded, error_stage).await;
        }
    }

    /// Normaliza la salida del pipeline garantizando el contrato de la respuesta e inyectando métricas de latencia.
    fn normalize_and_enrich_response(
        &self,
        result: Result<IopContext, DomainError>,
        error_dto: Option<Value>,
        exec_ms: u64,
    ) -> Value {
        match result {
            Ok(ctx) => {
                let mut body = Value::Object(ctx.request.clone());
                // Inyectar execution_time_ms en la respuesta
                if let Some(obj) = body.as_object_mut() {
                    obj.insert("execution_time_ms".to_string(), Value::Number(exec_ms.into()));
                }
                let response_type = ResponseType::infer(&body);
                normalize_response(&body, response_type)
            }
            Err(_) => error_dto.unwrap_or(Value::Null),
        }
    }
}

// ── Traits de extensión ───────────────────────────────────────────────────────

/// Paso del pipeline IOP — Cedar / Quota / JanusRouter implementan este trait.
/// [PORTED_FROM: cada step-fn en el vector steps]
#[async_trait::async_trait]
pub trait IopStep: Send + Sync {
    async fn execute(&self, ctx: IopContext) -> Result<IopContext, DomainError>;

    /// Deshace lo que `execute` hizo FUERA de este proceso, cuando un paso
    /// posterior del pipeline falla y la operación no va a completarse.
    ///
    /// Por defecto no hace nada: solo los pasos con efectos externos la
    /// implementan. Hoy es la cuota, que debita antes de que Janus escriba; sin
    /// esto, cada escritura fallida dejaba el contador un punto más alto y el
    /// tenant agotaba su plan sin haber creado nada.
    ///
    /// No devuelve error a propósito. Si una compensación falla, el fallo que
    /// la persona necesita leer sigue siendo el que tumbó el pipeline, no el
    /// de la limpieza.
    async fn compensate(&self, _ctx: &IopContext) {}
}

/// Moira Emitter — fire-and-forget EDA tras Ok.
/// [PORTED_FROM: moira-emitter de ig/init-key :iop/orchestrator]
#[async_trait::async_trait]
pub trait MoiraEmitter: Send + Sync {
    async fn emit(&self, ctx: IopContext) -> Result<(), DomainError>;
    async fn reset_orphaned_processing(&self, tenant_id: &str, ttl_ms: i64) -> Result<usize, DomainError>;
}

#[cfg(test)]
#[path = "tests/orchestrator_tests.rs"]
mod tests;
