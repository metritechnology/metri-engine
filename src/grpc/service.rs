//! Implementación principal del servicio gRPC `MetriService` (`MetriGrpcService`).
//!
//! Ensambla el orquestador IOP (`CedarAuthorizerStep` → `QuotaGuardStep` → `JanusRouterStep`)
//! y delega el cuerpo de cada operación RPC en los submódulos de `grpc::handlers`.

use std::sync::Arc;

use crate::grpc::pb::metri_service_server::MetriService;
use crate::grpc::pb::{
    BulkRequest, BulkResponse, CompositeTransactRequest, CompositeTransactResponse,
    DiscoveryRequest, DiscoveryResponse, ExploreRequest, ExploreResponse, ListEntitiesRequest,
    ListEntitiesResponse, ListSequenceConfigsRequest, ListSequenceConfigsResponse,
    MatchRoutingRulesBatchRequest, MatchRoutingRulesBatchResponse, QueryRequest, QueryResponse,
    TransactionRequest, TransactionResponse, UpsertSequenceConfigRequest,
    UpsertSequenceConfigResponse,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// Dependencias del servicio, resueltas una sola vez en la raíz de
/// composición (`grpc/server.rs` en producción). Sustituye al constructor de
/// once parámetros posicionales: cada campo tiene nombre y los `Option`
/// quedan a la vista.
pub struct ServiceDeps {
    /// Ejecutor de consultas OLTP sobre el almacén EAV/MBF.
    pub oltp_executor: crate::aegis::oltp::executor::OltpExecutor,
    /// Solo para fabricar el contador de cuota; el motor escribe por Janus.
    pub eav_writer: crate::eav::writer::EavWriter,
    /// Enrutador de canales de escritura Janus (`OltpChannel`).
    pub janus_router: std::sync::Arc<crate::janus_router::router::JanusRouter>,
    /// Caché del read path (Null Object si `QUERY_CACHE_MODE=off`).
    pub query_cache: std::sync::Arc<crate::janus::cache::QueryCacheFrontend>,
    /// Interceptor asíncrono de auditoría fire-and-forget.
    pub audit_interceptor:
        std::sync::Arc<crate::infrastructure::audit::interceptor::AuditInterceptorImpl>,
    /// Emisor opcional de eventos del transactional outbox hacia SQS FIFO (Moira).
    pub moira_emitter: Option<std::sync::Arc<dyn crate::iop::core::MoiraEmitter>>,
    /// Almacén de sesiones para verificación de tokens y lista negra de revocación.
    pub valkey_store: std::sync::Arc<dyn crate::domain::protocols::ISessionStore>,
    /// Caché de principals consolidados para el pipeline Cedar.
    pub principal_cache: std::sync::Arc<dyn crate::cedar::PrincipalCache>,
    /// Notificador de fallos operativos y de seguridad (Sherlog / EventBridge).
    pub fault_notifier: std::sync::Arc<dyn crate::iop::sherlog::IFaultNotifier>,
    /// Canal de escritura empleado para persistir telemetría de errores y auditoría.
    pub telemetry_channel: std::sync::Arc<dyn crate::janus_router::router::IWriteChannel>,
    /// Almacenamiento opcional para exportación de consultas masivas a CSV en S3.
    pub export_storage: Option<std::sync::Arc<dyn crate::domain::protocols::IExportStorage>>,
    /// Política de autenticación decidida en el arranque
    /// (`domain::config::resolve_dev_auth_bypass`, fail-closed);
    /// los tests lo fijan explícitamente. Nunca llega activo a producción.
    pub dev_auth_bypass: crate::cedar::AuthenticationPolicy,
    /// Bus de invalidación compartido con la caché de principals.
    pub invalidation_bus: Arc<dyn crate::cedar::ports::InvalidationBus>,
}

/// Servidor gRPC principal que implementa el contrato `MetriService`.
pub struct MetriGrpcService {
    pub(crate) oltp_executor: crate::aegis::oltp::executor::OltpExecutor,
    /// Escritor EAV inyectado: el composite lo usa directo (instanciación
    /// atómica — madre + hijos en una TransactWriteItems).
    pub(crate) eav_writer: crate::eav::writer::EavWriter,
    pub(crate) query_cache: std::sync::Arc<crate::janus::cache::QueryCacheFrontend>,
    pub(crate) iop_orchestrator: std::sync::Arc<dyn crate::iop::core::IIopOrchestrator>,
    pub(crate) valkey_store: std::sync::Arc<dyn crate::domain::protocols::ISessionStore>,
    pub(crate) principal_cache: std::sync::Arc<dyn crate::cedar::PrincipalCache>,
    pub(crate) cedar_engine: crate::cedar::CedarAuthorizer,
    pub(crate) policy_cache: std::collections::HashMap<String, cedar_policy::PolicySet>,
    pub(crate) fault_notifier: std::sync::Arc<dyn crate::iop::sherlog::IFaultNotifier>,
    pub(crate) telemetry_channel: std::sync::Arc<dyn crate::janus_router::router::IWriteChannel>,
    pub(crate) export_storage: Option<std::sync::Arc<dyn crate::domain::protocols::IExportStorage>>,
    pub(crate) dev_auth_bypass: crate::cedar::AuthenticationPolicy,
    pub(crate) invalidation_bus: Arc<dyn crate::cedar::ports::InvalidationBus>,
}

impl MetriGrpcService {
    /// Construye `MetriGrpcService` e inicializa el pipeline IOP con las dependencias provistas en `ServiceDeps`.
    pub fn new(deps: ServiceDeps) -> Self {
        let ServiceDeps {
            oltp_executor,
            eav_writer,
            janus_router,
            query_cache,
            audit_interceptor,
            moira_emitter,
            valkey_store,
            principal_cache,
            fault_notifier,
            telemetry_channel,
            export_storage,
            dev_auth_bypass,
            invalidation_bus,
        } = deps;

        // Inicializar pasos del IOP con dependencias reales de Cedar Zero-Trust
        let cedar_step = std::sync::Arc::new(crate::iop::cedar_step::CedarAuthorizerStep::new(
            valkey_store.clone(),
            oltp_executor.pull_reader().clone(),
            principal_cache.clone(),
        ));
        // El paso de cuota solo necesita el contador: desde la fase 3 no
        // escribe ninguna proyección, así que el writer no pinta nada aquí.
        let quota_step = std::sync::Arc::new(crate::iop::quota_step::QuotaGuardStep::new(
            oltp_executor.clone(),
            eav_writer.quota_ledger(),
        ));
        let janus_step =
            std::sync::Arc::new(crate::iop::janus_step::JanusRouterStep::new(janus_router));

        let steps: Vec<std::sync::Arc<dyn crate::iop::core::IopStep>> =
            vec![cedar_step, quota_step, janus_step];

        let iop_orchestrator = std::sync::Arc::new(crate::iop::core::IopOrchestrator::new(
            steps,
            moira_emitter,
            Some(audit_interceptor),
            fault_notifier.clone(),
            telemetry_channel.clone(),
        ));

        // Políticas Cedar compiladas para los roles conocidos — bootstrap
        // compartido con el CedarAuthorizerStep (D2).
        let policy_cache = crate::cedar::engine::default_policy_cache();

        Self {
            oltp_executor,
            eav_writer,
            query_cache,
            iop_orchestrator,
            valkey_store,
            principal_cache,
            cedar_engine: crate::cedar::CedarAuthorizer::new(),
            policy_cache,
            fault_notifier,
            telemetry_channel,
            export_storage,
            dev_auth_bypass,
            invalidation_bus,
        }
    }
}

#[tonic::async_trait]
impl MetriService for MetriGrpcService {
    type QueryStream = ReceiverStream<Result<QueryResponse, Status>>;

    async fn discovery(
        &self,
        request: Request<DiscoveryRequest>,
    ) -> Result<Response<DiscoveryResponse>, Status> {
        self.discovery_impl(request).await
    }

    async fn explore(
        &self,
        request: Request<ExploreRequest>,
    ) -> Result<Response<ExploreResponse>, Status> {
        self.explore_impl(request).await
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        self.query_impl(request).await
    }

    async fn list_entities(
        &self,
        request: Request<ListEntitiesRequest>,
    ) -> Result<Response<ListEntitiesResponse>, Status> {
        self.list_entities_impl(request).await
    }

    async fn transact(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<TransactionResponse>, Status> {
        self.transact_impl(request).await
    }

    async fn composite_transact(
        &self,
        request: Request<CompositeTransactRequest>,
    ) -> Result<Response<CompositeTransactResponse>, Status> {
        self.composite_transact_impl(request).await
    }

    async fn bulk_ingest(
        &self,
        request: Request<BulkRequest>,
    ) -> Result<Response<BulkResponse>, Status> {
        self.bulk_ingest_impl(request).await
    }

    async fn match_routing_rules_batch(
        &self,
        request: Request<MatchRoutingRulesBatchRequest>,
    ) -> Result<Response<MatchRoutingRulesBatchResponse>, Status> {
        self.match_routing_rules_batch_impl(request).await
    }

    async fn list_sequence_configs(
        &self,
        request: Request<ListSequenceConfigsRequest>,
    ) -> Result<Response<ListSequenceConfigsResponse>, Status> {
        self.list_sequence_configs_impl(request).await
    }

    async fn upsert_sequence_config(
        &self,
        request: Request<UpsertSequenceConfigRequest>,
    ) -> Result<Response<UpsertSequenceConfigResponse>, Status> {
        self.upsert_sequence_config_impl(request).await
    }
}

#[cfg(test)]
#[path = "tests/service_tests.rs"]
mod tests;
