use crate::grpc::handlers::list_support::{sort_and_truncate, validate_list_filters};
use crate::grpc::pb::{
    DiscoveryRequest, DiscoveryResponse, ExploreRequest, ExploreResponse, ListEntitiesRequest,
    ListEntitiesResponse,
};
use crate::grpc::service::MetriGrpcService;
use tonic::{Request, Response, Status};
use tracing::{error, info};

// Handlers del MetriGrpcService — fase 2: service.rs delega, aquí vive el cuerpo.

impl MetriGrpcService {
    pub(crate) async fn discovery_impl(
        &self,
        request: Request<DiscoveryRequest>,
    ) -> Result<Response<DiscoveryResponse>, Status> {
        let (session_tenant_id, session_user_id) = {
            let session = request
                .extensions()
                .get::<crate::grpc::interceptors::AuthenticatedSession>()
                .ok_or_else(|| Status::unauthenticated("Petición no autenticada [Fail-Closed]"))?;
            (session.tenant_id.clone(), session.user_id.clone())
        };

        let accept_language = request
            .metadata()
            .get("accept-language")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("es")
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

        info!(
            "Discovery request: tenant={}, solicitante={}, accept-language={}",
            req.tenant_id, session_user_id, accept_language
        );

        if req.tenant_id != session_tenant_id && session_tenant_id != "system" {
            error!(
                "[gRPC Discovery] Acceso denegado: Conflicto de tenant. Req={} vs Token={}",
                req.tenant_id, session_tenant_id
            );
            let domain_err = crate::domain::errors::DomainError::auth(
                crate::domain::errors::ErrorCode::GrpcTenant001,
                format!(
                    "Acceso denegado: Conflicto de tenant. Req={} vs Token={}",
                    req.tenant_id, session_tenant_id
                ),
            );
            self.emit_read_error(
                &req.tenant_id,
                &session_user_id,
                domain_err,
                Some("discovery".to_string()),
            );
            return Err(Status::permission_denied(
                "Acceso denegado: Conflicto de organización",
            ));
        }

        let registry = crate::codice::global();
        let entity_names: Vec<String> = registry.entity_names().map(str::to_string).collect();
        let mut schemas = Vec::new();
        for name in &entity_names {
            // Filter by type if specified
            if !req.r#type.is_empty() && name != &req.r#type {
                continue;
            }
            if let Some(model) = registry.get_model(name) {
                let mut attrs = Vec::new();
                if req.include_attributes {
                    for attr in &model.attributes {
                        let label = registry
                            .get_localized_label(&accept_language, name, Some(&attr.name))
                            .unwrap_or_else(|| {
                                attr.label.clone().unwrap_or_else(|| attr.name.clone())
                            });
                        let enum_labels =
                            registry.get_localized_enum_labels(&accept_language, name, &attr.name);

                        attrs.push(crate::grpc::pb::AttributeSchema {
                            name: attr.name.clone(),
                            r#type: format!("{:?}", attr.attr_type).to_lowercase(),
                            label,
                            filterable: true,
                            sortable: true,
                            groupable: attr.is_dimension,
                            aggregatable: matches!(
                                attr.attr_type,
                                crate::codice::registry::AttrType::Number
                                    | crate::codice::registry::AttrType::Epoch
                            ),
                            fts: attr.fts,
                            entity_ref: attr.entity_ref.clone().unwrap_or_default(),
                            enum_labels,
                            required: attr.required,
                            validation_regex: attr.validation_regex.clone().unwrap_or_default(),
                            default_value: attr.default_value.clone().unwrap_or_default(),
                            enum_values: attr.options.clone(),
                            ..Default::default()
                        });
                    }
                }
                let label = registry
                    .get_localized_label(&accept_language, name, None)
                    .unwrap_or_else(|| model.label.clone().unwrap_or_else(|| model.entity.clone()));

                schemas.push(crate::grpc::pb::EntitySchema {
                    entity: model.entity.clone(),
                    attributes: attrs,
                    label,
                    icon: model.icon.clone().unwrap_or_default(),
                    primary_key: model
                        .primary_key
                        .clone()
                        .unwrap_or_else(|| "id".to_string()),
                    fts_fields: model.fts_fields.clone(),
                    ..Default::default()
                });
            }
        }
        let resp = DiscoveryResponse {
            status: Some(crate::grpc::pb::Status {
                success: true,
                error_code: String::new(),
                error_message: String::new(),
                error_context: None,
            }),
            schemas,
            ..Default::default()
        };
        Ok(Response::new(resp))
    }
}

impl MetriGrpcService {
    pub(crate) async fn explore_impl(
        &self,
        request: Request<ExploreRequest>,
    ) -> Result<Response<ExploreResponse>, Status> {
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

        let authenticated_ctx = self
            .authorize_read(
                "Explore",
                "VIEW",
                &req.entity,
                &req.entity,
                &auth_header,
                &req.tenant_id,
                &session_user_id,
                Some(req.entity.clone()),
                false,
                true,
            )
            .await?;

        // Only master tenant users (or system BFF) can explore tenants or quotas
        if let Err(e) = crate::cedar::SystemSecurityRules::check_crud_authorization(
            &req.entity,
            &authenticated_ctx.tenant_id,
            &authenticated_ctx.user_id,
            "explore",
        ) {
            return Err(Status::permission_denied(e.detail));
        }

        info!(
            "Explore request: tenant={} entity={} attr={}",
            req.tenant_id, req.entity, req.attribute
        );
        let exec = self.oltp_executor.clone();

        let registry = crate::codice::global();
        let is_olap = if let Some(model) = registry.get_model(&req.entity) {
            matches!(model.engine, crate::codice::registry::EngineChannel::Olap)
        } else {
            false
        };

        // Use AEVT scan to get distinct values for the attribute
        let limit = if req.limit > 0 {
            req.limit as usize
        } else {
            100
        };
        let ast_ir = crate::janus::fbs::AnalyticsRequestT {
            entity: Some(req.entity.clone()),
            dimensions: Some(vec![crate::janus::fbs::DimensionDefinitionT {
                entity: Some(req.entity.clone()),
                attribute: Some(req.attribute.clone()),
                ..Default::default()
            }]),
            metrics: Some(vec![crate::janus::fbs::MetricDefinitionT {
                entity: Some(req.entity.clone()),
                attribute: Some("id".to_string()),
                aggregation: crate::janus::fbs::AggregationFunction(1), // COUNT
                name: Some("count".to_string()),
                ..Default::default()
            }]),
            output_cast: crate::janus::fbs::OutputCastType(3), // PIE
            limit: limit as i32,
            ..Default::default()
        };

        let mut values = Vec::new();

        if is_olap {
            let is_master = crate::cedar::is_master_tenant(&authenticated_ctx.tenant_id);
            let cedar_ctx = crate::janus::router::CedarCtx {
                tenant_id: req.tenant_id.clone(),
                user_id: authenticated_ctx.user_id.clone(),
                roles: authenticated_ctx.roles.iter().cloned().collect(),
                is_super_master: is_master,
                cross_tenant_scope: if is_master {
                    "FULL".to_string()
                } else {
                    "NONE".to_string()
                },
                domain_boundaries: authenticated_ctx.domain_boundaries.clone(),
            };

            let chunks = crate::janus::router::process_single_query(
                "explore",
                &ast_ir,
                &cedar_ctx,
                &exec,
                self.athena_engine.as_ref(),
                false,
            )
            .await;

            if !chunks.is_empty() && chunks[0].success {
                let body = &chunks[0].body;
                let rows = body
                    .get("data")
                    .and_then(|d| d.as_array())
                    .cloned()
                    .unwrap_or_default();
                for row in &rows {
                    if let Some(v) = row.get(&req.attribute) {
                        let val_str = match v {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            _ => continue,
                        };
                        if !val_str.is_empty() && !values.contains(&val_str) {
                            values.push(val_str);
                        }
                    }
                }
            } else {
                let err_msg = if !chunks.is_empty() {
                    chunks[0]
                        .body
                        .get("reason")
                        .and_then(|r| r.as_str())
                        .unwrap_or("Unknown OLAP query error")
                        .to_string()
                } else {
                    "No OLAP response chunks".to_string()
                };
                error!("[gRPC Explore] OLAP query failed: {}", err_msg);
                let resp = ExploreResponse {
                    status: Some(crate::grpc::pb::Status {
                        success: false,
                        error_code: "EXPLORE_ERROR".to_string(),
                        error_message: err_msg,
                        error_context: None,
                    }),
                    values: vec![],
                };
                return Ok(Response::new(resp));
            }
        } else {
            match exec.run_oltp_query_fbs(&req.tenant_id, &ast_ir).await {
                Ok(result) => {
                    let rows = result
                        .get("data")
                        .and_then(|d| d.as_array())
                        .or_else(|| result.as_array())
                        .cloned()
                        .unwrap_or_default();
                    for row in &rows {
                        if let Some(v) = row.get(&req.attribute) {
                            let val_str = match v {
                                serde_json::Value::String(s) => s.clone(),
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                _ => continue,
                            };
                            if !val_str.is_empty() && !values.contains(&val_str) {
                                values.push(val_str);
                            }
                        }
                    }
                }
                Err(e) => {
                    self.emit_read_error(
                        &req.tenant_id,
                        &authenticated_ctx.user_id,
                        e.clone(),
                        Some(req.entity.clone()),
                    );
                    let resp = ExploreResponse {
                        status: Some(crate::grpc::pb::Status {
                            success: false,
                            error_code: "EXPLORE_ERROR".to_string(),
                            error_message: e.to_string(),
                            error_context: None,
                        }),
                        values: vec![],
                    };
                    return Ok(Response::new(resp));
                }
            }
        }

        let resp = ExploreResponse {
            status: Some(crate::grpc::pb::Status {
                success: true,
                error_code: String::new(),
                error_message: String::new(),
                error_context: None,
            }),
            values,
        };
        Ok(Response::new(resp))
    }
}

impl MetriGrpcService {
    pub(crate) async fn list_entities_impl(
        &self,
        request: Request<ListEntitiesRequest>,
    ) -> Result<Response<ListEntitiesResponse>, Status> {
        let (session_tenant_id, session_user_id) = {
            let session = request
                .extensions()
                .get::<crate::grpc::interceptors::AuthenticatedSession>()
                .ok_or_else(|| Status::unauthenticated("Peticion no autenticada [Fail-Closed]"))?;
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

        // El tenant de la SESION manda sobre el del cuerpo. Confiar en el del cuerpo
        // seria un salto de particion trivial entre inquilinos.
        req.tenant_id = if session_tenant_id == "system" {
            if req.tenant_id.is_empty() {
                "system".to_string()
            } else {
                req.tenant_id.clone()
            }
        } else {
            session_tenant_id.clone()
        };

        if req.entity_type.is_empty() {
            return Err(Status::invalid_argument("entity_type es obligatorio"));
        }

        // El limite es obligatorio mientras no exista paginacion (fase L4): sin el, un
        // tenant grande devolveria decenas de miles de ids en una sola respuesta.
        let max_limit: i32 = crate::domain::config::engine_config().max_list_limit;
        if req.limit <= 0 {
            return Err(Status::invalid_argument(
                "limit es obligatorio y debe ser > 0 mientras no exista paginacion",
            ));
        }
        if req.limit > max_limit {
            return Err(Status::invalid_argument(format!(
                "limit {} supera el maximo del servicio ({})",
                req.limit, max_limit
            )));
        }

        let authenticated_ctx = self
            .authorize_read(
                "ListEntities",
                "VIEW",
                &req.entity_type,
                &req.entity_type,
                &auth_header,
                &req.tenant_id,
                &session_user_id,
                Some(req.entity_type.clone()),
                true,
                false,
            )
            .await?;

        if let Err(e) = crate::cedar::SystemSecurityRules::check_crud_authorization(
            &req.entity_type,
            &authenticated_ctx.tenant_id,
            &authenticated_ctx.user_id,
            "list",
        ) {
            return Err(Status::permission_denied(e.detail));
        }

        // ── Validacion de filtros contra el Codice ──
        let registry = crate::codice::global();
        let model = registry.get_model(&req.entity_type).ok_or_else(|| {
            Status::invalid_argument(format!(
                "Entidad desconocida en el Codice: '{}'",
                req.entity_type
            ))
        })?;

        let filter_names: Vec<&str> = req.filters.keys().map(|k| k.as_str()).collect();
        if let Err(e) = validate_list_filters(model, &filter_names) {
            return Err(Status::from(e));
        }

        // ── Ejecucion ──
        // `entity_type` va SIEMPRE como primer filtro: es lo que acota el resultado a
        // la entidad pedida, porque los demas atributos se indexan por tenant y no por tipo.
        let mut filters: Vec<(String, crate::eav::types::datom::DatomValue)> = vec![(
            "entity_type".to_string(),
            crate::eav::types::datom::DatomValue::Str(req.entity_type.clone()),
        )];
        for (k, v) in &req.filters {
            filters.push((
                k.clone(),
                crate::eav::types::datom::DatomValue::Str(v.clone()),
            ));
        }

        let plan = crate::eav::reader::query::NativeQueryPlan::AvetIntersect {
            tenant_id: req.tenant_id.clone(),
            filters,
        };

        let mut ids = match self
            .oltp_executor
            .query_executor()
            .execute_native_plan(&plan)
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                error!("[gRPC ListEntities] Error de consulta: {:?}", e);
                return Err(Status::internal(format!(
                    "Error listando entidades: {}",
                    e.detail
                )));
            }
        };

        let truncated = sort_and_truncate(&mut ids, req.limit as usize);

        info!(
            "ListEntities: tenant={} entity={} filtros={} devueltos={} truncated={}",
            req.tenant_id,
            req.entity_type,
            req.filters.len(),
            ids.len(),
            truncated
        );

        Ok(Response::new(ListEntitiesResponse {
            status: Some(crate::grpc::pb::Status {
                success: true,
                error_code: String::new(),
                error_message: String::new(),
                error_context: None,
            }),
            entity_ids: ids,
            next_page_token: String::new(), // L4
            truncated,
        }))
    }
}
