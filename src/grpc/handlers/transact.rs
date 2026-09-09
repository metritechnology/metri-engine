//! Transact RPC body.
use crate::domain::errors::{DomainError, ErrorCode};
use crate::grpc::pb::{TransactionRequest, TransactionResponse};
use crate::grpc::service::MetriGrpcService;
use crate::grpc::translator;
use crate::janus_router::oltp_channel::extract_entity_id;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

// Handlers del MetriGrpcService — fase 2: service.rs delega, aquí vive el cuerpo.

impl MetriGrpcService {
    pub(crate) async fn transact_impl(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<TransactionResponse>, Status> {
        let principal = crate::cedar::get_principal_data(
            &request,
            self.valkey_store.as_ref(),
            self.oltp_executor.pull_reader(),
            self.principal_cache.as_ref(),
            self.dev_auth_bypass,
        )
        .await
        .map_err(|err| Status::unauthenticated(format!("Authentication failed: {}", err.detail)))?;

        let auth_header = request
            .metadata()
            .get("authorization")
            .or_else(|| request.metadata().get("sid"))
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let mut req = request.into_inner();
        let resolved_tenant_id = if principal.tenant_id == "system" {
            if req.tenant_id.is_empty() {
                "system".to_string()
            } else {
                req.tenant_id.clone()
            }
        } else {
            principal.tenant_id.clone()
        };
        req.tenant_id = resolved_tenant_id;

        let operation_str = match req.action {
            1 => "CREATE",
            2 => "UPDATE",
            3 => "DELETE",
            _ => "UNKNOWN",
        };

        info!(
            "Transaction request | tenant: {} | entity: {} | op: {}",
            req.tenant_id, req.entity_type, operation_str
        );

        let payload_json = if let Some(struct_payload) = req.payload {
            translator::struct_to_value(struct_payload)
        } else {
            serde_json::Value::Object(serde_json::Map::new())
        };

        let mut request_map = serde_json::Map::new();

        let mut actual_payload = if let Some(obj) = payload_json.as_object() {
            if let Some(data) = obj.get("data") {
                data.clone()
            } else {
                payload_json.clone()
            }
        } else {
            payload_json.clone()
        };

        let mut entity_id = req.entity_id.clone();
        if entity_id.is_empty() {
            entity_id = extract_entity_id(&actual_payload).unwrap_or_default();
        }

        // ADR-006: la identidad en el payload solo se inyecta para
        // UPDATE/DELETE (localizar la entidad). En CREATE reproduciría la
        // entidad fantasma: el cliente propone un id, el engine mintea otro,
        // y el UPDATE posterior opera sobre el propuesto. Un CREATE de
        // negocio con identidad del cliente la rechaza la ruta con
        // JANUS_VAL_001 (vía payload) o queda registrada aquí (vía campo RPC).
        let identity_proposed = !entity_id.is_empty();
        if operation_str == "CREATE" && identity_proposed {
            warn!(
                entity = %req.entity_type,
                id_propuesto = %entity_id,
                "[ADR-006] CREATE de entidad de negocio con id propuesto por el cliente — se ignora: el id real viaja en entity_id de la respuesta"
            );
        }
        if !entity_id.is_empty() && operation_str != "CREATE" {
            if let Some(obj) = actual_payload.as_object_mut() {
                if !obj.contains_key("id")
                    && !obj.contains_key("entity_id")
                    && !obj.contains_key("ulid")
                {
                    obj.insert(
                        "id".to_string(),
                        serde_json::Value::String(entity_id.clone()),
                    );
                }
            }
        }

        // --- Granular Validation ---
        if req.entity_type == "role" && (operation_str == "CREATE" || operation_str == "UPDATE") {
            if let Some(grants) = actual_payload.get("grants") {
                self.validate_role_grants(grants)?;
            }
        }

        if actual_payload.is_array() {
            if let Some(arr) = actual_payload.as_array() {
                for item in arr {
                    self.validate_single_mutation(
                        &req.tenant_id,
                        &req.entity_type,
                        operation_str,
                        item,
                        &principal,
                    )
                    .await?;
                }
            }
        } else {
            self.validate_single_mutation(
                &req.tenant_id,
                &req.entity_type,
                operation_str,
                &actual_payload,
                &principal,
            )
            .await?;
        }

        // --- Prevent cycle in user group hierarchy ---
        if req.entity_type == "user_group"
            && (operation_str == "CREATE" || operation_str == "UPDATE")
        {
            self.validate_user_group_hierarchy_cycle(&req.tenant_id, &entity_id, &actual_payload)
                .await?;
        }

        request_map.insert("payload".to_string(), actual_payload.clone());
        request_map.insert(
            "tenant_id".to_string(),
            serde_json::Value::String(req.tenant_id.clone()),
        );
        request_map.insert(
            "entity_type".to_string(),
            serde_json::Value::String(req.entity_type.clone()),
        );
        request_map.insert(
            "operation".to_string(),
            serde_json::Value::String(operation_str.to_string()),
        );
        // §10.4 (metri-schedulers): el ejecutor del Hub escribe bitácora del
        // Job sin disparar eventos — el campo ya estaba en el proto pero moría
        // aquí. El canal OLTP lo lee y suprime outbox + emisiones.
        request_map.insert(
            "suppress_events".to_string(),
            serde_json::Value::Bool(req.suppress_events),
        );

        // Inyección controlada y programática de metadatos para autorización Cedar
        if !auth_header.is_empty() {
            request_map.insert(
                "authorization".to_string(),
                serde_json::Value::String(auth_header),
            );
        }
        request_map.insert(
            "x-metri-action".to_string(),
            serde_json::Value::String(operation_str.to_string()),
        );
        request_map.insert(
            "x-metri-entity-type".to_string(),
            serde_json::Value::String(req.entity_type.clone()),
        );
        request_map.insert(
            "x-metri-domains".to_string(),
            serde_json::Value::String(req.entity_type.clone()),
        );
        if !entity_id.is_empty() {
            request_map.insert(
                "x-metri-entity-id".to_string(),
                serde_json::Value::String(entity_id),
            );
        }

        let ctx = crate::iop::core::IopContext::new(
            &req.tenant_id,
            &principal.user_id,
            &req.entity_type,
            operation_str,
            request_map,
        );

        // ── Ejecución del IOP Orchestrator (Railway + Async) ─────────────────────
        let response_value = self.iop_orchestrator.run(ctx).await;

        if let Some("error") = response_value.get("status").and_then(|s| s.as_str()) {
            let Some(error_obj) = response_value.get("error") else {
                // status=error sin objeto 'error': DTO malformado del
                // pipeline — Status interno vía el mapeo único (R6), nunca
                // pánico (R2).
                return Err(Status::from(DomainError::new(
                    ErrorCode::Janus500,
                    "IOP devolvió status=error sin objeto 'error'",
                )));
            };
            let code = error_obj
                .get("code")
                .and_then(|c| c.as_str())
                .unwrap_or("UNKNOWN")
                .to_string();
            let desc = error_obj
                .get("description")
                .and_then(|d| d.as_str())
                .unwrap_or("")
                .to_string();

            let response = TransactionResponse {
                status: Some(crate::grpc::pb::Status {
                    success: false,
                    error_code: code,
                    error_message: desc,
                    // El contexto que `build_error_dto` construyó y saneó. Antes
                    // se descartaba aquí, así que el cliente recibía el código y
                    // nada más.
                    error_context: error_obj
                        .get("context")
                        .filter(|c| !c.is_null())
                        .map(crate::grpc::translator::value_to_struct),
                }),
                entity_id: String::new(),
                ..Default::default()
            };
            return Ok(Response::new(response));
        }

        let entity_id = response_value
            .get("entity_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                response_value
                    .get("result")
                    .and_then(|res| res.get("entity_id"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("")
            .to_string();

        tracing::debug!("Raw IOP Response: {}", response_value);
        tracing::debug!("Extracted entity_id: {}", entity_id);

        // Cache Invalidation for Transact
        self.evict_caches_after_mutation(
            req.tenant_id.clone(),
            req.entity_type.clone(),
            entity_id.clone(),
            &actual_payload,
        );

        let response = TransactionResponse {
            status: Some(crate::grpc::pb::Status {
                success: true,
                error_code: String::new(),
                error_message: if entity_id.is_empty() {
                    response_value.to_string()
                } else {
                    String::new()
                },
                error_context: None,
            }),
            entity_id,
            result: Some(translator::value_to_struct(&response_value)),
        };
        Ok(Response::new(response))
    }
}
