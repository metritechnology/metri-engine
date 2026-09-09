//! MetriService gRPC implementation.
//!
//! Implementación de gRPC (Tonic)
//! Implementa la interfaz gRPC `MetriService`.

use std::sync::Arc;

use crate::grpc::pb::metri_service_server::MetriService;
use crate::grpc::pb::{
    BulkRequest, BulkResponse, DiscoveryRequest, DiscoveryResponse, ExploreRequest,
    ExploreResponse, ListEntitiesRequest, ListEntitiesResponse, MatchRoutingRulesBatchRequest,
    MatchRoutingRulesBatchResponse, QueryRequest, QueryResponse, TransactionRequest,
    TransactionResponse,
};
/// Dependencias del servicio, resueltas una sola vez en la raíz de
/// composición (`grpc/server.rs` en producción). Sustituye al constructor de
/// once parámetros posicionales: cada campo tiene nombre y los `Option`
/// quedan a la vista.
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct ServiceDeps {
    pub oltp_executor: crate::aegis::oltp::executor::OltpExecutor,
    /// Solo para fabricar el contador de cuota; el motor escribe por Janus.
    pub eav_writer: crate::eav::writer::EavWriter,
    pub janus_router: std::sync::Arc<crate::janus_router::router::JanusRouter>,
    pub audit_interceptor:
        std::sync::Arc<crate::infrastructure::audit::interceptor::AuditInterceptorImpl>,
    pub athena_engine: Option<std::sync::Arc<dyn crate::domain::protocols::IQueryEngine>>,
    pub moira_emitter: Option<std::sync::Arc<dyn crate::iop::core::MoiraEmitter>>,
    pub valkey_store: std::sync::Arc<dyn crate::domain::protocols::ISessionStore>,
    pub principal_cache: std::sync::Arc<dyn crate::cedar::PrincipalCache>,
    pub fault_notifier: std::sync::Arc<dyn crate::iop::sherlog::IFaultNotifier>,
    pub olap_channel: std::sync::Arc<dyn crate::janus_router::router::IWriteChannel>,
    pub export_storage: Option<std::sync::Arc<dyn crate::domain::protocols::IExportStorage>>,
    /// Bypass de autorización para desarrollo y pruebas. Lo decide
    /// Política de autenticación decidida en el arranque
    /// (`domain::config::resolve_dev_auth_bypass`, fail-closed);
    /// los tests lo fijan explícitamente. Nunca llega activo a producción.
    pub dev_auth_bypass: crate::cedar::AuthenticationPolicy,
    /// Bus de invalidación compartido con la caché de principals.
    pub invalidation_bus: Arc<dyn crate::cedar::ports::InvalidationBus>,
}

pub struct MetriGrpcService {
    pub(crate) oltp_executor: crate::aegis::oltp::executor::OltpExecutor,
    pub(crate) iop_orchestrator: std::sync::Arc<dyn crate::iop::core::IIopOrchestrator>,
    pub(crate) athena_engine: Option<std::sync::Arc<dyn crate::domain::protocols::IQueryEngine>>,
    pub(crate) valkey_store: std::sync::Arc<dyn crate::domain::protocols::ISessionStore>,
    pub(crate) principal_cache: std::sync::Arc<dyn crate::cedar::PrincipalCache>,
    pub(crate) cedar_engine: crate::cedar::CedarAuthorizer,
    pub(crate) policy_cache: std::collections::HashMap<String, cedar_policy::PolicySet>,
    pub(crate) fault_notifier: std::sync::Arc<dyn crate::iop::sherlog::IFaultNotifier>,
    pub(crate) olap_channel: std::sync::Arc<dyn crate::janus_router::router::IWriteChannel>,
    pub(crate) export_storage: Option<std::sync::Arc<dyn crate::domain::protocols::IExportStorage>>,
    pub(crate) dev_auth_bypass: crate::cedar::AuthenticationPolicy,
    pub(crate) invalidation_bus: Arc<dyn crate::cedar::ports::InvalidationBus>,
}

impl MetriGrpcService {
    pub fn new(deps: ServiceDeps) -> Self {
        let ServiceDeps {
            oltp_executor,
            eav_writer,
            janus_router,
            audit_interceptor,
            athena_engine,
            moira_emitter,
            valkey_store,
            principal_cache,
            fault_notifier,
            olap_channel,
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
            olap_channel.clone(),
        ));

        // Políticas Cedar compiladas para los roles conocidos — bootstrap
        // compartido con el CedarAuthorizerStep (D2).
        let policy_cache = crate::cedar::engine::default_policy_cache();

        Self {
            oltp_executor,
            iop_orchestrator,
            athena_engine,
            valkey_store,
            principal_cache,
            cedar_engine: crate::cedar::CedarAuthorizer::new(),
            policy_cache,
            fault_notifier,
            olap_channel,
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
}

#[cfg(test)]
#[path = "tests/service_tests.rs"]
mod tests;
