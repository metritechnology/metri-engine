use crate::grpc::service::MetriGrpcService;
use tonic::Status;
use tracing::error;

// Handlers del MetriGrpcService — fase 2: service.rs delega, aquí vive el cuerpo.

impl MetriGrpcService {
    /// Preámbulo de autorización compartido por los RPC de lectura.
    ///
    /// Una sola copia del camino Cedar: request sintético con sus cuatro
    /// cabeceras, `intercept`, y chequeo de aislamiento de tenant. Las
    /// divergencias legítimas entre RPC — el estado con que se reporta la
    /// denegación y si el conflicto de tenant emite a Sherlog — son parámetros
    /// explícitos, nunca copias de código.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn authorize_read(
        &self,
        rpc_label: &str,
        action: &str,
        entity_type: &str,
        domains: &str,
        auth_header: &str,
        tenant_id: &str,
        session_user_id: &str,
        error_entity: Option<String>,
        cedar_denial_is_permission: bool,
        emit_on_isolation: bool,
    ) -> Result<crate::cedar::authorizer::CedarContext, Status> {
        let mut dummy_req = tonic::Request::new(());
        if !auth_header.is_empty() {
            if let Ok(m_val) = auth_header.parse() {
                dummy_req.metadata_mut().insert("authorization", m_val);
            }
        }
        if let Ok(m_val) = action.parse() {
            dummy_req.metadata_mut().insert("x-metri-action", m_val);
        }
        if let Ok(m_val) = entity_type.parse() {
            dummy_req
                .metadata_mut()
                .insert("x-metri-entity-type", m_val);
        }
        if let Ok(m_val) = domains.parse() {
            dummy_req.metadata_mut().insert("x-metri-domains", m_val);
        }

        let ctx = match crate::cedar::authorizer::intercept(
            &dummy_req,
            self.valkey_store.as_ref(),
            self.oltp_executor.pull_reader(),
            self.principal_cache.as_ref(),
            &self.cedar_engine,
            &self.policy_cache,
        )
        .await
        {
            Ok(ctx) => ctx,
            Err(e) => {
                error!("[gRPC {rpc_label}] Acceso rechazado por Cedar: {:?}", e);
                let err_msg = e.detail.clone();
                let domain_err = crate::domain::errors::DomainError::auth(
                    crate::domain::errors::ErrorCode::InfraCedar002,
                    format!("{rpc_label} Access Denied by Cedar: {err_msg}"),
                );
                self.emit_read_error(tenant_id, session_user_id, domain_err, error_entity);
                let status = if cedar_denial_is_permission {
                    Status::permission_denied(format!("Acceso denegado: {err_msg}"))
                } else {
                    Status::unauthenticated(format!("Acceso denegado: {err_msg}"))
                };
                return Err(status);
            }
        };

        if let Err(e) = crate::cedar::authorizer::SystemSecurityRules::check_tenant_isolation(
            tenant_id,
            &ctx.tenant_id,
            &ctx.user_id,
        ) {
            error!(
                "[gRPC {rpc_label}] Conflicto de Tenant: Request={tenant_id:?} vs Session={:?}",
                ctx.tenant_id
            );
            if emit_on_isolation {
                let domain_err = crate::domain::errors::DomainError::auth(
                    crate::domain::errors::ErrorCode::GrpcTenant001,
                    format!(
                        "Tenant conflict in {rpc_label}: Request={tenant_id} vs Session={}",
                        ctx.tenant_id
                    ),
                );
                self.emit_read_error(tenant_id, &ctx.user_id.clone(), domain_err, error_entity);
            }
            return Err(Status::permission_denied(e.detail));
        }

        Ok(ctx)
    }

    pub(crate) fn emit_read_error(
        &self,
        tenant_id: &str,
        user_id: &str,
        error: crate::domain::errors::DomainError,
        entity_type: Option<String>,
    ) {
        Self::emit_read_error_static(
            self.fault_notifier.clone(),
            self.olap_channel.clone(),
            tenant_id,
            user_id,
            error,
            entity_type,
        );
    }

    pub(crate) fn emit_read_error_static(
        fault_notifier: std::sync::Arc<dyn crate::iop::sherlog::IFaultNotifier>,
        olap_channel: std::sync::Arc<dyn crate::janus_router::router::IWriteChannel>,
        tenant_id: &str,
        user_id: &str,
        error: crate::domain::errors::DomainError,
        entity_type: Option<String>,
    ) {
        let error_dto = crate::iop::error_response::build_error_dto(
            &error,
            tenant_id,
            user_id,
            error.context().cloned(),
            None,
        );

        let notifier = fault_notifier.clone();
        let olap = olap_channel.clone();
        let error_clone = error.clone();
        let entity_type_clone = entity_type.clone();

        tokio::spawn(async move {
            crate::iop::sherlog::process_fault(
                notifier.as_ref(),
                olap.as_ref(),
                &error_clone,
                &error_dto,
                entity_type_clone,
            )
            .await;
        });
    }
}
