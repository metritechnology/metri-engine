//! Query RPC body — server-side streaming.
use crate::grpc::handlers::query_support::*;
use crate::grpc::pb::{QueryRequest, QueryResponse};
use crate::grpc::service::MetriGrpcService;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{error, info};

// Handlers del MetriGrpcService — fase 2: service.rs delega, aquí vive el cuerpo.

use crate::grpc::translator;
use crate::janus::router::CedarCtx;

impl MetriGrpcService {
    pub(crate) async fn query_impl(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<ReceiverStream<Result<QueryResponse, Status>>>, Status> {
        let (session_tenant_id, session_user_id) = {
            let session = request
                .extensions()
                .get::<crate::grpc::interceptors::AuthenticatedSession>()
                .ok_or_else(|| Status::unauthenticated("Petición no autenticada [Fail-Closed]"))?;
            (session.tenant_id.clone(), session.user_id.clone())
        };

        let auth_header = request
            .metadata()
            .get("authorization")
            .or_else(|| request.metadata().get("sid"))
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let mut req = request.into_inner();
        let resolved_tenant_id = if session_tenant_id == "system" {
            if req.tenant_id.is_empty() {
                "system".to_string()
            } else {
                req.tenant_id.clone()
            }
        } else {
            session_tenant_id.clone()
        };
        req.tenant_id = resolved_tenant_id;

        // Extraer entidades implicadas en la consulta del gRPC Request
        let mut query_entities: Vec<String> = req
            .queries
            .values()
            .map(|q| q.entity.clone())
            .filter(|e| !e.is_empty())
            .collect();
        query_entities.sort();
        query_entities.dedup();
        let target_domains = if query_entities.is_empty() {
            "project".to_string()
        } else {
            query_entities.join(",")
        };
        let target_entity = query_entities
            .first()
            .cloned()
            .unwrap_or_else(|| "project".to_string());

        let is_csv_export = req.queries.values().any(|q| q.output_cast == 6);
        let action_str = if is_csv_export { "EXPORT" } else { "VIEW" };
        let authenticated_ctx = self
            .authorize_read(
                "Query",
                action_str,
                &target_entity,
                &target_domains,
                &auth_header,
                &req.tenant_id,
                &session_user_id,
                Some(target_entity.clone()),
                false,
                true,
            )
            .await?;

        // Only master tenant users (or system BFF) can query tenants or quotas.
        // Excepción de autoservicio: leer la fila `tenant_plugin` PROPIA es
        // parte del panel de módulos del tenant (rules.rs::is_self_service_row);
        // el scope de la consulta (`req.tenant_id`) es el tenant de la fila.
        for entity in &query_entities {
            if crate::cedar::SystemSecurityRules::is_self_service_row(
                entity,
                &req.tenant_id,
                &authenticated_ctx.tenant_id,
            ) {
                continue;
            }
            if let Err(e) = crate::cedar::SystemSecurityRules::check_crud_authorization(
                entity,
                &authenticated_ctx.tenant_id,
                &authenticated_ctx.user_id,
                "read",
            ) {
                return Err(Status::permission_denied(e.detail));
            }
        }

        info!("Query request for tenant: {}", req.tenant_id);

        let (tx, rx) = mpsc::channel(16);

        let exec_clone = self.oltp_executor.clone();
        let athena_clone = self.athena_engine.clone();
        let authenticated_ctx_clone = authenticated_ctx.clone();
        let fault_notifier_clone = self.fault_notifier.clone();
        let olap_channel_clone = self.olap_channel.clone();
        let export_storage_clone = self.export_storage.clone();

        tokio::spawn(async move {
            let tenant_id = req.tenant_id.clone();
            let user_id_str = authenticated_ctx_clone.user_id.clone();
            let queries_map = match translator::query_request_to_queries_map(&req) {
                Ok(q) => q,
                Err(e) => {
                    Self::emit_read_error_static(
                        fault_notifier_clone.clone(),
                        olap_channel_clone.clone(),
                        &tenant_id,
                        &user_id_str,
                        e.clone(),
                        Some(target_entity.clone()),
                    );
                    let _ = tx.send(Err(Status::invalid_argument(e.to_string()))).await;
                    return;
                }
            };
            tracing::debug!("QUERIES_MAP LEN: {}", queries_map.len());

            // Construir el contexto ABAC para este request a partir de la sesión autenticada real
            let is_master = crate::cedar::is_master_tenant(&authenticated_ctx_clone.tenant_id);
            let cedar_ctx = CedarCtx {
                tenant_id: tenant_id.clone(),
                user_id: authenticated_ctx_clone.user_id.clone(),
                roles: authenticated_ctx_clone.roles.iter().cloned().collect(),
                is_super_master: is_master,
                cross_tenant_scope: if is_master {
                    "FULL".to_string()
                } else {
                    "NONE".to_string()
                },
                domain_boundaries: authenticated_ctx_clone.domain_boundaries.clone(),
            };

            // Gate Zero-Trust
            if let Err(e) = crate::janus::validator::validate_tenant(&tenant_id) {
                error!("[Janus] Zero-Trust gate rechazó | tenant: {tenant_id}");
                Self::emit_read_error_static(
                    fault_notifier_clone.clone(),
                    olap_channel_clone.clone(),
                    &tenant_id,
                    &user_id_str,
                    e.clone(),
                    Some(target_entity.clone()),
                );
                let mut batch_results = std::collections::HashMap::new();
                batch_results.insert(
                    "__pipeline__".to_string(),
                    QueryResponse {
                        status: Some(crate::grpc::pb::Status {
                            success: false,
                            error_code: "JANUS_400".to_string(),
                            error_message: e.to_string(),
                            error_context: None,
                        }),
                        ..Default::default()
                    },
                );
                let pb_chunk = QueryResponse {
                    status: Some(crate::grpc::pb::Status {
                        success: false,
                        error_code: String::new(),
                        error_message: String::new(),
                        error_context: None,
                    }),
                    batch_results,
                    ..Default::default()
                };
                let _ = tx.send(Ok(pb_chunk)).await;
                return;
            }

            // Procesar cada subquery en paralelo y streamear inmediatamente
            let mut handles = Vec::new();
            for (query_key, query_map) in queries_map {
                let qk = query_key.clone();
                let qm = query_map.clone();
                let cedar = cedar_ctx.clone();
                let exec = exec_clone.clone();
                let athena = athena_clone.clone();
                let tx_clone = tx.clone();
                let explain_plan = req.explain_plan;
                let fault_notifier_qk = fault_notifier_clone.clone();
                let olap_channel_qk = olap_channel_clone.clone();
                let export_storage_qk = export_storage_clone.clone();
                let user_id_qk = user_id_str.clone();
                let target_entity_qk = target_entity.clone();

                handles.push(tokio::spawn(async move {
                    // Paso 1.5: Validación Semántica del Contrato FlatBuffers
                    if let Err(e) = crate::janus::validator::validate_analytics_request_fbs(&qm) {
                        error!("[Janus] Validación FBS falló en query_key '{}': {}", qk, e);
                        Self::emit_read_error_static(
                            fault_notifier_qk.clone(),
                            olap_channel_qk.clone(),
                            &cedar.tenant_id,
                            &user_id_qk,
                            e.clone(),
                            Some(target_entity_qk.clone()),
                        );
                        let mut batch_results = std::collections::HashMap::new();
                        batch_results.insert(
                            qk.clone(),
                            QueryResponse {
                                status: Some(crate::grpc::pb::Status {
                                    success: false,
                                    error_code: "JANUS_400".to_string(),
                                    error_message: e.to_string(),
                                    error_context: None,
                                }),
                                ..Default::default()
                            },
                        );
                        let pb_chunk = QueryResponse {
                            status: Some(crate::grpc::pb::Status {
                                success: false,
                                error_code: String::new(),
                                error_message: String::new(),
                                error_context: None,
                            }),
                            batch_results,
                            ..Default::default()
                        };
                        let _ = tx_clone.send(Ok(pb_chunk)).await;
                        return;
                    }

                    let is_system_bff = cedar.roles.iter().any(|r| r == "system-bff");
                    let chunks = crate::janus::router::process_single_query(
                        &qk,
                        &qm,
                        &cedar,
                        &exec,
                        athena.as_ref(),
                        explain_plan,
                    )
                    .await;
                    for chunk in chunks {
                        if !chunk.success {
                            let error_code_str = chunk
                                .body
                                .get("code")
                                .and_then(|c| c.as_str())
                                .unwrap_or("JANUS_500");
                            let reason = chunk
                                .body
                                .get("reason")
                                .and_then(|r| r.as_str())
                                .unwrap_or("Unknown Janus query error");

                            let domain_err = if error_code_str == "JANUS_400" {
                                crate::domain::errors::DomainError::new(
                                    crate::domain::errors::ErrorCode::JnsRef002,
                                    reason.to_string(),
                                )
                            } else {
                                crate::domain::errors::DomainError::new(
                                    crate::domain::errors::ErrorCode::InfraAthena005,
                                    reason.to_string(),
                                )
                            };

                            Self::emit_read_error_static(
                                fault_notifier_qk.clone(),
                                olap_channel_qk.clone(),
                                &cedar.tenant_id,
                                &user_id_qk,
                                domain_err,
                                Some(target_entity_qk.clone()),
                            );
                        }

                        let pb_chunk = map_chunk_to_response(
                            chunk,
                            is_system_bff,
                            export_storage_qk.as_ref(),
                            &cedar.tenant_id,
                        )
                        .await;

                        // Añadir pacing delay (150ms) en modo de desarrollo para visualización fluida
                        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

                        if tx_clone.send(Ok(pb_chunk)).await.is_err() {
                            break; // El cliente se desconectó
                        }
                    }
                }));
            }

            // Esperar que todas las subqueries terminen
            for handle in handles {
                let _ = handle.await;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
