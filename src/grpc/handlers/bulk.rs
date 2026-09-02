use crate::grpc::pb::{BulkRequest, BulkResponse};
use crate::grpc::service::MetriGrpcService;
use crate::grpc::translator;
use crate::janus_router::oltp_channel::extract_entity_id;
use tonic::{Request, Response, Status};
use tracing::{error, info};

// Handlers del MetriGrpcService — fase 2: service.rs delega, aquí vive el cuerpo.

impl MetriGrpcService {
    pub(crate) async fn bulk_ingest_impl(
        &self,
        request: Request<BulkRequest>,
    ) -> Result<Response<BulkResponse>, Status> {
        let principal = crate::cedar::authorizer::get_principal_data(
            &request,
            self.valkey_store.as_ref(),
            self.oltp_executor.pull_reader(),
            self.principal_cache.as_ref(),
            self.dev_auth_bypass,
        )
        .await
        .map_err(|err| Status::unauthenticated(format!("Authentication failed: {}", err.detail)))?;

        if principal.user_id != "usr_system_bff" {
            info!(
                "[gRPC BulkIngest] Solicitud de ingesta masiva por usuario regular: {}",
                principal.user_id
            );
        }

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
            "BulkIngest request | tenant: {} | entity: {} | op: {}",
            req.tenant_id, req.entity_type, operation_str
        );

        let row_set = req
            .data
            .ok_or_else(|| Status::invalid_argument("data (RowSet) is required"))?;
        let columns = row_set.columns;

        let payload_strategy = row_set
            .payload_strategy
            .ok_or_else(|| Status::invalid_argument("payload_strategy is required"))?;
        let mut ingested_count = 0;

        match payload_strategy {
            crate::grpc::pb::row_set::PayloadStrategy::RowsJson(data_row_list) => {
                let mut rows_vec = Vec::new();
                for row in data_row_list.iter {
                    let mut payload_map = serde_json::Map::new();
                    for (i, val) in row.values.into_iter().enumerate() {
                        if let Some(col) = columns.get(i) {
                            let json_val = match val.kind {
                                Some(prost_types::value::Kind::StringValue(s)) => {
                                    serde_json::Value::String(s)
                                }
                                Some(prost_types::value::Kind::NumberValue(n)) => {
                                    if n.fract() == 0.0 {
                                        serde_json::Value::Number(serde_json::Number::from(
                                            n as i64,
                                        ))
                                    } else if let Some(num) = serde_json::Number::from_f64(n) {
                                        serde_json::Value::Number(num)
                                    } else {
                                        serde_json::Value::Null
                                    }
                                }
                                Some(prost_types::value::Kind::BoolValue(b)) => {
                                    serde_json::Value::Bool(b)
                                }
                                Some(prost_types::value::Kind::StructValue(s)) => {
                                    translator::struct_to_value(s)
                                }
                                _ => serde_json::Value::Null,
                            };
                            payload_map.insert(col.key.clone(), json_val);
                        }
                    }
                    let row_val = serde_json::Value::Object(payload_map);

                    // Validate role grants if role is being created/updated
                    if req.entity_type == "role"
                        && (operation_str == "CREATE" || operation_str == "UPDATE")
                    {
                        if let Some(grants) = row_val.get("grants") {
                            self.validate_role_grants(grants)?;
                        }
                    }

                    rows_vec.push(row_val);
                }

                // [OPTIMIZATION 10x]: Validate Cedar permission ONCE for the entire bulk
                // instead of per-row. All rows share the same (tenant_id, entity_type, operation).
                if let Some(first_row) = rows_vec.first() {
                    self.validate_single_mutation(
                        &req.tenant_id,
                        &req.entity_type,
                        operation_str,
                        first_row,
                        &principal,
                    )
                    .await?;
                }

                // Gather entity IDs for invalidation
                let mut entity_ids = Vec::new();
                if req.entity_type == "user"
                    || req.entity_type == "role"
                    || req.entity_type == "user_group"
                {
                    for row_val in &rows_vec {
                        if let Some(eid) = extract_entity_id(row_val) {
                            entity_ids.push(eid);
                        }
                    }
                }

                let mut request_map = serde_json::Map::new();
                request_map.insert("data".to_string(), serde_json::Value::Array(rows_vec));
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

                // Inyección programática controlada para BulkIngest
                if !auth_header.is_empty() {
                    request_map.insert(
                        "authorization".to_string(),
                        serde_json::Value::String(auth_header),
                    );
                }
                request_map.insert(
                    "x-metri-action".to_string(),
                    serde_json::Value::String("BulkIngestData".to_string()),
                );
                request_map.insert(
                    "x-metri-entity-type".to_string(),
                    serde_json::Value::String(req.entity_type.clone()),
                );
                request_map.insert(
                    "x-metri-domains".to_string(),
                    serde_json::Value::String(req.entity_type.clone()),
                );

                let ctx = crate::iop::core::IopContext::new(
                    &req.tenant_id,
                    &principal.user_id,
                    &req.entity_type,
                    operation_str,
                    request_map,
                );

                let response_value = self.iop_orchestrator.run(ctx).await;
                info!("BulkIngest response value: {:?}", response_value);

                if let Some("error") = response_value.get("status").and_then(|s| s.as_str()) {
                    let error_obj = response_value.get("error").unwrap();
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
                    error!("Error bulk ingesting [{}]: {}", code, desc);

                    // Antes esto era `Status::internal(desc)`: un error de
                    // transporte, sin código. Una cuota agotada en una ingesta
                    // masiva llegaba al cliente indistinguible de una caída, y
                    // el predicado que la reconoce trabaja sobre el código.
                    //
                    // `BulkResponse` ya tiene su campo `Status`, y el cliente ya
                    // comprueba `status.success` y construye su rechazo a partir
                    // de `error_code` — así que esto es lo que esperaba recibir.
                    return Ok(Response::new(BulkResponse {
                        status: Some(crate::grpc::pb::Status {
                            success: false,
                            error_code: code,
                            error_message: desc,
                            error_context: error_obj
                                .get("context")
                                .filter(|c| !c.is_null())
                                .map(crate::grpc::translator::value_to_struct),
                        }),
                        ingested_count: 0,
                        ..Default::default()
                    }));
                } else {
                    if let Some(count) = response_value
                        .get("ingested_count")
                        .and_then(|c| c.as_u64())
                    {
                        ingested_count = count as i32;
                    }

                    // Invalidate caches upon successful ingest
                    for eid in entity_ids {
                        let msg = crate::cedar::authorizer::InvalidationMsg {
                            tenant_id: req.tenant_id.clone(),
                            entity_type: req.entity_type.clone(),
                            entity_id: eid,
                        };
                        let _ = crate::cedar::authorizer::INVALIDATION_TX.send(msg);
                    }
                }
            }
            _ => {
                return Err(Status::unimplemented(
                    "Only RowsJson is supported for BulkIngest right now",
                ));
            }
        }

        let response = BulkResponse {
            status: Some(crate::grpc::pb::Status {
                success: true,
                error_code: String::new(),
                error_message: String::new(),
                error_context: None,
            }),
            ingested_count,
            ..Default::default()
        };
        Ok(Response::new(response))
    }
}
