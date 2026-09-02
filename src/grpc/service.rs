// grpc/service.rs — Implementación de gRPC (Tonic)
// SRP: Implementa la interfaz gRPC `MetriService`.

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use super::pb::metri_service_server::MetriService;
use super::pb::{
    BulkRequest, BulkResponse, DiscoveryRequest, DiscoveryResponse, ExploreRequest,
    ExploreResponse, ListEntitiesRequest, ListEntitiesResponse, MatchRoutingRulesBatchRequest,
    MatchRoutingRulesBatchResponse, MatchRoutingRulesResponse, MatchedRule, QueryRequest,
    QueryResponse, TransactionRequest, TransactionResponse, WebhookTarget,
};
use crate::janus::router::CedarCtx;
use crate::janus_router::oltp_channel::extract_entity_id;

use crate::grpc::translator;
pub struct MetriGrpcService {
    oltp_executor: crate::aegis::oltp::executor::OltpExecutor,
    iop_orchestrator: std::sync::Arc<dyn crate::iop::core::IIopOrchestrator>,
    athena_engine: Option<std::sync::Arc<dyn crate::domain::protocols::IQueryEngine>>,
    valkey_store: std::sync::Arc<dyn crate::domain::protocols::ISessionStore>,
    principal_cache: std::sync::Arc<dyn crate::cedar::authorizer::PrincipalCache>,
    cedar_engine: crate::cedar::authorizer::CedarAuthorizer,
    policy_cache: std::collections::HashMap<String, cedar_policy::PolicySet>,
    fault_notifier: std::sync::Arc<dyn crate::iop::sherlog::IFaultNotifier>,
    olap_channel: std::sync::Arc<dyn crate::janus_router::router::IWriteChannel>,
    export_storage: Option<std::sync::Arc<dyn crate::domain::protocols::IExportStorage>>,
}

impl MetriGrpcService {
    pub fn new(
        oltp_executor: crate::aegis::oltp::executor::OltpExecutor,
        // Solo para fabricar el contador de cuota; el motor escribe por Janus.
        eav_writer: crate::eav::writer::EavWriter,
        janus_router: std::sync::Arc<crate::janus_router::router::JanusRouter>,
        audit_interceptor: std::sync::Arc<
            crate::infrastructure::audit::interceptor::AuditInterceptorImpl,
        >,
        athena_engine: Option<std::sync::Arc<dyn crate::domain::protocols::IQueryEngine>>,
        moira_emitter: Option<std::sync::Arc<dyn crate::iop::core::MoiraEmitter>>,
        valkey_store: std::sync::Arc<dyn crate::domain::protocols::ISessionStore>,
        principal_cache: std::sync::Arc<dyn crate::cedar::authorizer::PrincipalCache>,
        fault_notifier: std::sync::Arc<dyn crate::iop::sherlog::IFaultNotifier>,
        olap_channel: std::sync::Arc<dyn crate::janus_router::router::IWriteChannel>,
        export_storage: Option<std::sync::Arc<dyn crate::domain::protocols::IExportStorage>>,
    ) -> Self {
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

        // Compilar políticas de Cedar para el path de consultas analíticas
        use std::str::FromStr;
        let policies_src = include_str!("../../docs/architecture/cedar/metri.cedar");
        let policies =
            cedar_policy::PolicySet::from_str(policies_src).expect("Failed to parse metri.cedar");

        let mut policy_cache = std::collections::HashMap::new();
        policy_cache.insert("admin".to_string(), policies.clone());
        policy_cache.insert("tenant-admin".to_string(), policies.clone());
        policy_cache.insert("contractor".to_string(), policies.clone());
        policy_cache.insert("user".to_string(), policies.clone());
        policy_cache.insert("system-bff".to_string(), policies.clone());
        policy_cache.insert("system-admin".to_string(), policies.clone());
        policy_cache.insert("role_super_master".to_string(), policies.clone());

        Self {
            oltp_executor,
            iop_orchestrator,
            athena_engine,
            valkey_store,
            principal_cache,
            cedar_engine: crate::cedar::authorizer::CedarAuthorizer::new(),
            policy_cache,
            fault_notifier,
            olap_channel,
            export_storage,
        }
    }

    fn emit_read_error(
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

    fn emit_read_error_static(
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
            error.context.clone(),
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

    fn validate_role_grants(&self, grants_value: &serde_json::Value) -> Result<(), Status> {
        use regex::Regex;
        // Strict regex pattern for grant format validation
        let re = Regex::new(r"^([a-zA-Z0-9_\-]+|\*):[a-zA-Z0-9_\-*]+$").unwrap();

        let validate_pair = |domain: &str, action: &str| -> Result<(), Status> {
            let pair = format!("{}:{}", domain, action);
            if !re.is_match(&pair) {
                return Err(Status::invalid_argument(format!(
                    "Invalid grant format '{}'. Grants must match pattern '^[a-zA-Z0-9_\\-]+:[a-zA-Z0-9_\\-*]+$'.",
                    pair
                )));
            }
            Ok(())
        };

        let elements = if let Some(s) = grants_value.as_str() {
            if s.starts_with('[') {
                serde_json::from_str::<serde_json::Value>(s).map_err(|e| {
                    Status::invalid_argument(format!("Failed to parse grants JSON string: {}", e))
                })?
            } else {
                serde_json::Value::Array(vec![serde_json::Value::String(s.to_string())])
            }
        } else {
            grants_value.clone()
        };

        if let Some(arr) = elements.as_array() {
            for item in arr {
                match item {
                    serde_json::Value::String(s) => {
                        if s.starts_with('{') {
                            if let Ok(serde_json::Value::Object(obj)) =
                                serde_json::from_str::<serde_json::Value>(s)
                            {
                                let domain = obj
                                    .get("domain")
                                    .and_then(|v| v.as_str())
                                    .ok_or_else(|| {
                                        Status::invalid_argument(
                                            "Grant object missing 'domain' field",
                                        )
                                    })?;

                                if let Some(actions_val) = obj.get("actions") {
                                    if let Some(actions_arr) = actions_val.as_array() {
                                        for act_val in actions_arr {
                                            let action = act_val.as_str().ok_or_else(|| {
                                                Status::invalid_argument(
                                                    "Grant action must be a string",
                                                )
                                            })?;
                                            validate_pair(domain, action)?;
                                        }
                                    } else if let Some(action_str) = actions_val.as_str() {
                                        validate_pair(domain, action_str)?;
                                    } else {
                                        return Err(Status::invalid_argument(
                                            "Grant 'actions' field must be an array or string",
                                        ));
                                    }
                                } else {
                                    validate_pair(domain, "*")?;
                                }
                                continue;
                            }
                        }
                        if !re.is_match(s) {
                            return Err(Status::invalid_argument(format!(
                                "Invalid grant format '{}'. Grants must match pattern '^[a-zA-Z0-9_\\-]+:[a-zA-Z0-9_\\-*]+$'.",
                                s
                            )));
                        }
                    }
                    serde_json::Value::Object(obj) => {
                        let domain =
                            obj.get("domain").and_then(|v| v.as_str()).ok_or_else(|| {
                                Status::invalid_argument("Grant object missing 'domain' field")
                            })?;

                        if let Some(actions_val) = obj.get("actions") {
                            if let Some(actions_arr) = actions_val.as_array() {
                                for act_val in actions_arr {
                                    let action = act_val.as_str().ok_or_else(|| {
                                        Status::invalid_argument("Grant action must be a string")
                                    })?;
                                    validate_pair(domain, action)?;
                                }
                            } else if let Some(action_str) = actions_val.as_str() {
                                validate_pair(domain, action_str)?;
                            } else {
                                return Err(Status::invalid_argument(
                                    "Grant 'actions' field must be an array or string",
                                ));
                            }
                        } else {
                            validate_pair(domain, "*")?;
                        }
                    }
                    _ => {
                        return Err(Status::invalid_argument(
                            "Grant item must be a string or object",
                        ));
                    }
                }
            }
        } else if let Some(obj) = elements.as_object() {
            let domain = obj
                .get("domain")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Status::invalid_argument("Grant object missing 'domain' field"))?;
            if let Some(actions_val) = obj.get("actions") {
                if let Some(actions_arr) = actions_val.as_array() {
                    for act_val in actions_arr {
                        let action = act_val.as_str().ok_or_else(|| {
                            Status::invalid_argument("Grant action must be a string")
                        })?;
                        validate_pair(domain, action)?;
                    }
                } else if let Some(action_str) = actions_val.as_str() {
                    validate_pair(domain, action_str)?;
                } else {
                    return Err(Status::invalid_argument(
                        "Grant 'actions' field must be an array or string",
                    ));
                }
            } else {
                validate_pair(domain, "*")?;
            }
        } else {
            return Err(Status::invalid_argument(
                "Grants field must be an array, string, or object",
            ));
        }

        Ok(())
    }

    async fn validate_single_mutation(
        &self,
        tenant_id: &str,
        entity_type: &str,
        action: &str,
        payload: &serde_json::Value,
        principal: &crate::cedar::authorizer::PrincipalData,
    ) -> Result<(), Status> {
        // Enforce tenant isolation (bypass for master tenant or system BFF account)
        if let Err(e) = crate::cedar::authorizer::SystemSecurityRules::check_tenant_isolation(
            tenant_id,
            &principal.tenant_id,
            &principal.user_id,
        ) {
            return Err(Status::permission_denied(e.detail));
        }

        let entity_id = extract_entity_id(payload).unwrap_or_default();

        if entity_type == "tenant" {
            if let Err(_) = crate::cedar::authorizer::SystemSecurityRules::check_tenant_isolation(
                &entity_id,
                &principal.tenant_id,
                &principal.user_id,
            ) {
                return Err(Status::permission_denied(
                    "Auth403: Cannot mutate other tenant",
                ));
            }
        }

        // Enforce that only master tenant users or system BFF account can mutate tenants and quotas
        if let Err(e) = crate::cedar::authorizer::SystemSecurityRules::check_crud_authorization(
            entity_type,
            &principal.tenant_id,
            &principal.user_id,
            "mutate",
        ) {
            return Err(Status::permission_denied(e.detail));
        }

        let mut resource = serde_json::json!({
            "entity_type": entity_type,
            "entity_id": entity_id,
            "domains": vec![entity_type.to_string()],
        });

        // Hydrate assigned_company_id if possible
        let mut assigned_company_id: Option<String> = None;
        for key in &[
            "assigned_company_id",
            "company_id",
            "assigned_company",
            "company",
        ] {
            if let Some(val) = payload.get(*key).and_then(|v| v.as_str()) {
                assigned_company_id = Some(val.to_string());
                break;
            }
        }

        if assigned_company_id.is_none()
            && (action == "UPDATE" || action == "DELETE")
            && !entity_id.is_empty()
        {
            let eav_reader = self.oltp_executor.pull_reader();
            if let Ok(entity_map) = eav_reader.pull(tenant_id, &entity_id, None).await {
                for key in &[
                    "assigned_company_id",
                    "company_id",
                    "assigned_company",
                    "company",
                ] {
                    if let Some(val) = entity_map
                        .get(*key)
                        .or_else(|| entity_map.get(&format!("{}/{}", entity_type, key)))
                    {
                        match val {
                            crate::eav::types::datom::DatomValue::Str(s) => {
                                assigned_company_id = Some(s.clone());
                                break;
                            }
                            crate::eav::types::datom::DatomValue::Ref(r) => {
                                assigned_company_id = Some(r.to_string());
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        if let Some(comp_id) = assigned_company_id {
            if let Some(obj) = resource.as_object_mut() {
                obj.insert(
                    "assigned_company_id".to_string(),
                    serde_json::json!(comp_id),
                );
            }
        }

        if std::env::var("METRI_TEST_MODE").unwrap_or_default() == "1" {
            return Ok(());
        }

        let body = serde_json::json!({});
        crate::cedar::authorizer::step4_evaluate_cedar(
            &self.cedar_engine,
            &self.policy_cache,
            principal,
            action,
            &resource,
            &body,
        )
        .map_err(|err| Status::permission_denied(format!("Auth403: {}", err.detail)))?;

        Ok(())
    }

    async fn validate_user_group_hierarchy_cycle(
        &self,
        tenant_id: &str,
        entity_id: &str,
        actual_payload: &serde_json::Value,
    ) -> Result<(), Status> {
        let parent_id = actual_payload
            .get("parent_user_group_id")
            .or_else(|| actual_payload.get("user_group/parent_user_group_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if !parent_id.is_empty() {
            if !entity_id.is_empty() && parent_id == entity_id {
                return Err(Status::invalid_argument(
                    "Circular dependency detected: a group cannot be its own parent",
                ));
            }

            let eav_reader = self.oltp_executor.pull_reader();
            let mut current_id = parent_id.to_string();
            let mut visited = std::collections::HashSet::new();
            visited.insert(entity_id.to_string());

            for _depth in 0..10 {
                if current_id.is_empty() {
                    break;
                }
                if !visited.insert(current_id.clone()) {
                    return Err(Status::invalid_argument(
                        "Circular dependency detected in user groups",
                    ));
                }

                if let Ok(group_map) = eav_reader.pull(tenant_id, &current_id, None).await {
                    if group_map.is_empty() {
                        break;
                    }
                    let next_parent = group_map
                        .get("parent_user_group_id")
                        .or_else(|| group_map.get("user_group/parent_user_group_id"))
                        .and_then(|v| match v {
                            crate::eav::types::datom::DatomValue::Str(s) => Some(s.clone()),
                            _ => None,
                        })
                        .unwrap_or_default();
                    current_id = next_parent;
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    fn evict_caches_after_mutation(
        &self,
        tenant_id: String,
        entity_type: String,
        entity_id: String,
        actual_payload: &serde_json::Value,
    ) {
        if entity_type == "user" || entity_type == "role" || entity_type == "user_group" {
            let entity_id_opt = if !entity_id.is_empty() {
                Some(entity_id)
            } else {
                extract_entity_id(actual_payload)
            };
            if let Some(eid) = entity_id_opt {
                let msg = crate::cedar::authorizer::InvalidationMsg {
                    tenant_id,
                    entity_type,
                    entity_id: eid,
                };
                let _ = crate::cedar::authorizer::INVALIDATION_TX.send(msg);
            }
        }
    }
}

/// Valida los filtros de `ListEntities` contra el Codice.
///
/// Rechaza atributos inexistentes y los de tipo no indexable en AVET: un filtro que
/// el indice no puede resolver degeneraria en un escaneo encubierto.
///
/// **Se valida por TIPO, no por el flag `indexed` del descriptor.** Ese flag es
/// declarativo y el camino de escritura lo ignora: `build_write_items` decide
/// escribir las claves AVET segun `ValueType::is_avet_indexable()`, que solo excluye
/// `Bytes`, `Array` y `Null`. Validar por el flag rechazaria filtros que funcionan
/// —`scheduled_job.status` no declara `index: true` y sin embargo esta en el indice—.
pub(crate) fn validate_list_filters(
    model: &crate::codice::registry::EntityModel,
    filter_names: &[&str],
) -> Result<(), String> {
    use crate::codice::registry::AttrType;
    for name in filter_names {
        match model.attributes.iter().find(|a| a.name == *name) {
            None => {
                return Err(format!(
                    "'{}' no es un atributo de '{}'",
                    name, model.entity
                ));
            }
            Some(a) => {
                if matches!(a.attr_type, AttrType::Bytes | AttrType::Array) {
                    return Err(format!(
                        "'{}' es de tipo no indexable en AVET: no se puede filtrar por el",
                        name
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Ordena y recorta. Devuelve true si quedaron resultados fuera.
///
/// El orden importa por correccion, no por estetica: el ejecutor devuelve el
/// resultado de un HashSet, cuyo orden no es determinista. Sin ordenar, dos llamadas
/// identicas con el mismo limite pueden devolver conjuntos DISTINTOS, y un consumidor
/// que reconcilie estado actuaria sobre una foto arbitraria.
pub(crate) fn sort_and_truncate(ids: &mut Vec<String>, limit: usize) -> bool {
    ids.sort();
    let truncated = ids.len() > limit;
    if truncated {
        ids.truncate(limit);
    }
    truncated
}

#[tonic::async_trait]
impl MetriService for MetriGrpcService {
    type QueryStream = ReceiverStream<Result<QueryResponse, Status>>;

    async fn discovery(
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

    async fn explore(
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

        let mut dummy_req = tonic::Request::new(());
        if !auth_header.is_empty() {
            if let Ok(m_val) = auth_header.parse() {
                dummy_req.metadata_mut().insert("authorization", m_val);
            }
        }
        if let Ok(m_val) = "VIEW".parse() {
            dummy_req.metadata_mut().insert("x-metri-action", m_val);
        }
        if let Ok(m_val) = req.entity.parse() {
            dummy_req
                .metadata_mut()
                .insert("x-metri-entity-type", m_val);
        }
        if let Ok(m_val) = req.entity.parse() {
            dummy_req.metadata_mut().insert("x-metri-domains", m_val);
        }

        let authenticated_ctx = match crate::cedar::authorizer::intercept(
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
                error!("[gRPC Explore] Acceso rechazado por Cedar: {:?}", e);
                let err_msg = e.detail.clone();
                let domain_err = crate::domain::errors::DomainError::auth(
                    crate::domain::errors::ErrorCode::InfraCedar002,
                    format!("Explore Access Denied by Cedar: {}", err_msg),
                );
                self.emit_read_error(
                    &req.tenant_id,
                    &session_user_id,
                    domain_err,
                    Some(req.entity.clone()),
                );
                return Err(Status::unauthenticated(format!(
                    "Acceso denegado: {}",
                    err_msg
                )));
            }
        };

        if let Err(e) = crate::cedar::authorizer::SystemSecurityRules::check_tenant_isolation(
            &req.tenant_id,
            &authenticated_ctx.tenant_id,
            &authenticated_ctx.user_id,
        ) {
            error!(
                "[gRPC Explore] Conflicto de Tenant: Request={:?} vs Session={:?}",
                req.tenant_id, authenticated_ctx.tenant_id
            );
            let domain_err = crate::domain::errors::DomainError::auth(
                crate::domain::errors::ErrorCode::GrpcTenant001,
                format!(
                    "Tenant conflict in Explore: Request={} vs Session={}",
                    req.tenant_id, authenticated_ctx.tenant_id
                ),
            );
            self.emit_read_error(
                &req.tenant_id,
                &authenticated_ctx.user_id,
                domain_err,
                Some(req.entity.clone()),
            );
            return Err(Status::permission_denied(e.detail));
        }

        // Only master tenant users (or system BFF) can explore tenants or quotas
        if let Err(e) = crate::cedar::authorizer::SystemSecurityRules::check_crud_authorization(
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
            let is_master =
                crate::cedar::authorizer::is_master_tenant(&authenticated_ctx.tenant_id);
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

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
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

        let mut dummy_req = tonic::Request::new(());
        if !auth_header.is_empty() {
            if let Ok(m_val) = auth_header.parse() {
                dummy_req.metadata_mut().insert("authorization", m_val);
            }
        }
        let is_csv_export = req.queries.values().any(|q| q.output_cast == 6);
        let action_str = if is_csv_export { "EXPORT" } else { "VIEW" };
        if let Ok(m_val) = action_str.parse() {
            dummy_req.metadata_mut().insert("x-metri-action", m_val);
        }
        if let Ok(m_val) = target_entity.parse() {
            dummy_req
                .metadata_mut()
                .insert("x-metri-entity-type", m_val);
        }
        if let Ok(m_val) = target_domains.parse() {
            dummy_req.metadata_mut().insert("x-metri-domains", m_val);
        }

        let authenticated_ctx = match crate::cedar::authorizer::intercept(
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
                error!("[gRPC Query] Acceso rechazado por Cedar: {:?}", e);
                let err_msg = e.detail.clone();
                let domain_err = crate::domain::errors::DomainError::auth(
                    crate::domain::errors::ErrorCode::InfraCedar002,
                    format!("Query Access Denied by Cedar: {}", err_msg),
                );
                self.emit_read_error(
                    &req.tenant_id,
                    &session_user_id,
                    domain_err,
                    Some(target_entity.clone()),
                );
                return Err(Status::unauthenticated(format!(
                    "Acceso denegado: {}",
                    err_msg
                )));
            }
        };

        if let Err(e) = crate::cedar::authorizer::SystemSecurityRules::check_tenant_isolation(
            &req.tenant_id,
            &authenticated_ctx.tenant_id,
            &authenticated_ctx.user_id,
        ) {
            error!(
                "[gRPC Query] Conflicto de Tenant: Request={:?} vs Session={:?}",
                req.tenant_id, authenticated_ctx.tenant_id
            );
            let domain_err = crate::domain::errors::DomainError::auth(
                crate::domain::errors::ErrorCode::GrpcTenant001,
                format!(
                    "Tenant conflict in Query: Request={} vs Session={}",
                    req.tenant_id, authenticated_ctx.tenant_id
                ),
            );
            self.emit_read_error(
                &req.tenant_id,
                &authenticated_ctx.user_id,
                domain_err,
                Some(target_entity.clone()),
            );
            return Err(Status::permission_denied(e.detail));
        }

        // Only master tenant users (or system BFF) can query tenants or quotas
        for entity in &query_entities {
            if let Err(e) = crate::cedar::authorizer::SystemSecurityRules::check_crud_authorization(
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
            let is_master =
                crate::cedar::authorizer::is_master_tenant(&authenticated_ctx_clone.tenant_id);
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

    /// Lista los ids de las entidades que cumplen filtros de igualdad.
    ///
    /// Cubre el hueco entre `Transact`+GET (una entidad por id ya conocido) y `Query`
    /// (consulta analitica de dashboards): ninguno responde "dame las entidades que
    /// cumplen X". Ver docs/architecture/PLAN_IMPLEMENTACION_LIST_ENTITIES.md
    ///
    /// La capacidad de consulta ya existia en el motor (`NativeQueryPlan::AvetIntersect`);
    /// esto la expone, la autoriza y la acota.
    async fn list_entities(
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
        const MAX_LIMIT: i32 = 5_000;
        if req.limit <= 0 {
            return Err(Status::invalid_argument(
                "limit es obligatorio y debe ser > 0 mientras no exista paginacion",
            ));
        }
        if req.limit > MAX_LIMIT {
            return Err(Status::invalid_argument(format!(
                "limit {} supera el maximo del servicio ({})",
                req.limit, MAX_LIMIT
            )));
        }

        // ── Autorizacion: mismo camino que cualquier otra lectura ──
        let mut dummy_req = tonic::Request::new(());
        if !auth_header.is_empty() {
            if let Ok(m_val) = auth_header.parse() {
                dummy_req.metadata_mut().insert("authorization", m_val);
            }
        }
        if let Ok(m_val) = "VIEW".parse() {
            dummy_req.metadata_mut().insert("x-metri-action", m_val);
        }
        if let Ok(m_val) = req.entity_type.parse() {
            dummy_req
                .metadata_mut()
                .insert("x-metri-entity-type", m_val);
        }
        if let Ok(m_val) = req.entity_type.parse() {
            dummy_req.metadata_mut().insert("x-metri-domains", m_val);
        }

        let authenticated_ctx = match crate::cedar::authorizer::intercept(
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
                error!("[gRPC ListEntities] Acceso rechazado por Cedar: {:?}", e);
                let err_msg = e.detail.clone();
                let domain_err = crate::domain::errors::DomainError::auth(
                    crate::domain::errors::ErrorCode::InfraCedar002,
                    format!("ListEntities Access Denied by Cedar: {}", err_msg),
                );
                self.emit_read_error(
                    &req.tenant_id,
                    &session_user_id,
                    domain_err,
                    Some(req.entity_type.clone()),
                );
                // Denegacion explicita, NO lista vacia: confundirlas convierte un
                // fallo de permisos en "no hay datos".
                return Err(Status::permission_denied(format!(
                    "Acceso denegado: {}",
                    err_msg
                )));
            }
        };

        if let Err(e) = crate::cedar::authorizer::SystemSecurityRules::check_tenant_isolation(
            &req.tenant_id,
            &authenticated_ctx.tenant_id,
            &authenticated_ctx.user_id,
        ) {
            error!(
                "[gRPC ListEntities] Conflicto de Tenant: Request={:?} vs Session={:?}",
                req.tenant_id, authenticated_ctx.tenant_id
            );
            return Err(Status::permission_denied(e.detail));
        }

        if let Err(e) = crate::cedar::authorizer::SystemSecurityRules::check_crud_authorization(
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
        if let Err(msg) = validate_list_filters(model, &filter_names) {
            return Err(Status::invalid_argument(msg));
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

    async fn transact(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<TransactionResponse>, Status> {
        let principal = crate::cedar::authorizer::get_principal_data(
            &request,
            self.valkey_store.as_ref(),
            &self.oltp_executor.pull_reader(),
            self.principal_cache.as_ref(),
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

        if !entity_id.is_empty() {
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

            let response = TransactionResponse {
                status: Some(super::pb::Status {
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
            status: Some(super::pb::Status {
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

    async fn bulk_ingest(
        &self,
        request: Request<BulkRequest>,
    ) -> Result<Response<BulkResponse>, Status> {
        let principal = crate::cedar::authorizer::get_principal_data(
            &request,
            self.valkey_store.as_ref(),
            &self.oltp_executor.pull_reader(),
            self.principal_cache.as_ref(),
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

    async fn match_routing_rules_batch(
        &self,
        request: Request<MatchRoutingRulesBatchRequest>,
    ) -> Result<Response<MatchRoutingRulesBatchResponse>, Status> {
        let (session_tenant_id, session_user_id) = {
            let session = request
                .extensions()
                .get::<crate::grpc::interceptors::AuthenticatedSession>()
                .ok_or_else(|| Status::unauthenticated("Petición no autenticada [Fail-Closed]"))?;
            (session.tenant_id.clone(), session.user_id.clone())
        };

        if session_tenant_id != "system" {
            error!(
                "[gRPC RoutingRules] Acceso denegado: Se requiere rol de sistema. Solicitante={}",
                session_user_id
            );
            return Err(Status::permission_denied(
                "Acceso denegado: Se requiere rol de sistema para la consulta de ruteo",
            ));
        }

        let batch = request.into_inner();
        let mut responses = Vec::with_capacity(batch.requests.len());

        for req in batch.requests {
            let tenant_id = &req.tenant_id;
            let entity_name = &req.entity_name;
            let trigger_type = &req.trigger_type;

            // 1. Decodificar CDC payload
            let cdc_payload: serde_json::Value = match serde_json::from_slice(&req.cdc_payload_json)
            {
                Ok(val) => val,
                Err(e) => {
                    return Err(Status::invalid_argument(format!(
                        "Payload JSON inválido: {e}"
                    )));
                }
            };

            // 2. Escanear reglas del tenant activas para la entidad y trigger
            let query_rules = serde_json::json!({
                "entity": "event_routing_rule",
                "select": ["id", "rule_code", "description", "target_entity_name", "event_trigger_type", "filter_conditions", "detail_type_output"],
                "where": [
                    "and",
                    ["=", "target_entity_name", entity_name],
                    ["=", "event_trigger_type", trigger_type]
                ],
                "limit": 100
            });

            let rules_rows = match self
                .oltp_executor
                .run_oltp_query(tenant_id, &query_rules)
                .await
            {
                Ok(val) => val,
                Err(e) => {
                    return Err(Status::internal(format!(
                        "Fallo al consultar reglas de ruteo: {e:?}"
                    )));
                }
            };

            let rules = rules_rows.as_array().cloned().unwrap_or_default();

            // 3. Consultar todos los webhook endpoints activos para filtrar por suscripción en memoria
            let query_webhooks = serde_json::json!({
                "entity": "webhook_endpoint",
                "select": ["id", "name", "target_url", "http_method", "authentication_type", "auth_token", "subscribed_rule_ids", "max_retries", "is_active"],
                "where": [
                    "and",
                    ["=", "is_active", true]
                ],
                "limit": 100
            });

            let webhooks_rows = match self
                .oltp_executor
                .run_oltp_query(tenant_id, &query_webhooks)
                .await
            {
                Ok(val) => val,
                Err(_) => serde_json::Value::Array(vec![]),
            };

            let active_webhooks = webhooks_rows.as_array().cloned().unwrap_or_default();

            let mut matched_rules = Vec::new();

            for rule in rules {
                let rule_id = rule.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let rule_code = rule.get("rule_code").and_then(|v| v.as_str()).unwrap_or("");
                let description = rule
                    .get("description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let detail_type_output = rule
                    .get("detail_type_output")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                // 4. Evaluación nativa de FilterNode
                if let Some(fc) = rule.get("filter_conditions") {
                    if let Some(fbs_node) = fc_to_fbs_filter_node(fc) {
                        let passes =
                            crate::aegis::oltp::filter::eval_filter_node(&cdc_payload, &fbs_node);
                        if !passes {
                            continue;
                        }
                    }
                }

                // 5. Mapear Webhooks suscritos
                let mut matched_webhooks = Vec::new();
                for webhook in &active_webhooks {
                    let subscribed_ids = webhook.get("subscribed_rule_ids");
                    let mut matches_rule = false;
                    if let Some(val) = subscribed_ids {
                        if let Some(arr) = val.as_array() {
                            matches_rule = arr.iter().any(|v| v.as_str() == Some(rule_id));
                        } else if let Some(s) = val.as_str() {
                            matches_rule = s == rule_id;
                        }
                    }

                    if matches_rule {
                        let target_url = webhook
                            .get("target_url")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let http_method = webhook
                            .get("http_method")
                            .and_then(|v| v.as_str())
                            .unwrap_or("POST")
                            .to_string();
                        let auth_type = webhook
                            .get("authentication_type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("NONE")
                            .to_string();
                        let resolved_auth_secret = webhook
                            .get("auth_token")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let max_retries = webhook
                            .get("max_retries")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(3) as i32;

                        matched_webhooks.push(WebhookTarget {
                            target_url,
                            http_method,
                            auth_type,
                            resolved_auth_secret,
                            max_retries,
                            timeout_seconds: 10,
                            headers: std::collections::HashMap::new(),
                        });
                    }
                }

                let condition_proto = rule
                    .get("filter_conditions")
                    .and_then(|fc| fc_to_proto_filter_node(fc));

                matched_rules.push(MatchedRule {
                    rule_code: rule_code.to_string(),
                    detail_type_output: detail_type_output.to_string(),
                    webhooks: matched_webhooks,
                    priority: 1,
                    name: description.to_string(),
                    condition: condition_proto,
                });
            }

            responses.push(MatchRoutingRulesResponse {
                status: Some(crate::grpc::pb::Status {
                    success: true,
                    error_code: String::new(),
                    error_message: String::new(),
                    error_context: None,
                }),
                matched_rules,
            });
        }

        Ok(Response::new(MatchRoutingRulesBatchResponse {
            status: Some(crate::grpc::pb::Status {
                success: true,
                error_code: String::new(),
                error_message: String::new(),
                error_context: None,
            }),
            responses,
        }))
    }
}

pub(crate) fn fc_to_fbs_filter_node(
    filter_conditions: &serde_json::Value,
) -> Option<crate::janus::fbs::FilterNodeT> {
    use crate::janus::fbs;

    let parsed_value;
    let fc = if let Some(s) = filter_conditions.as_str() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            parsed_value = v;
            &parsed_value
        } else {
            filter_conditions
        }
    } else {
        filter_conditions
    };

    let arr = fc.as_array()?;
    if arr.is_empty() {
        return None;
    }

    if arr.len() == 1 {
        return fc_item_to_fbs_node(&arr[0]);
    }

    let first = &arr[0];
    let parsed_first;
    let first_obj = if let Some(s) = first.as_str() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            parsed_first = v;
            &parsed_first
        } else {
            first
        }
    } else {
        first
    };

    let logical_op = first_obj
        .get("logical_operator")
        .and_then(|v| v.as_str())
        .unwrap_or("AND");
    let conjunction = match logical_op {
        "OR" => 2,
        _ => 1,
    };

    let mut child_nodes = Vec::new();
    for item in arr {
        if let Some(node) = fc_item_to_fbs_node(item) {
            child_nodes.push(node);
        }
    }

    if child_nodes.is_empty() {
        return None;
    }

    Some(fbs::FilterNodeT {
        group: Some(Box::new(fbs::FilterGroupT {
            conjunction: fbs::FilterGroup_Conjunction(conjunction),
            nodes: Some(child_nodes),
        })),
        ..Default::default()
    })
}

pub(crate) fn fc_item_to_fbs_node(
    item: &serde_json::Value,
) -> Option<crate::janus::fbs::FilterNodeT> {
    use crate::janus::fbs;

    let parsed_value;
    let obj = if let Some(s) = item.as_str() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            parsed_value = v;
            &parsed_value
        } else {
            item
        }
    } else {
        item
    };

    let field = obj.get("field_name").and_then(|v| v.as_str())?.to_string();
    let op_str = obj.get("operator").and_then(|v| v.as_str())?.to_lowercase();
    let target_val_str = obj.get("target_value").and_then(|v| v.as_str())?;
    let val_type = obj
        .get("target_value_type")
        .and_then(|v| v.as_str())
        .unwrap_or("string");

    let op = match op_str.as_str() {
        "eq" => fbs::FilterOperator::EQ,
        "neq" => fbs::FilterOperator::NEQ,
        "gt" => fbs::FilterOperator::GT,
        "lt" => fbs::FilterOperator::LT,
        "gte" => fbs::FilterOperator::GTE,
        "lte" => fbs::FilterOperator::LTE,
        "contains" => fbs::FilterOperator::CONTAINS,
        "in" => fbs::FilterOperator::IN,
        _ => fbs::FilterOperator::EQ,
    };

    let fval = match val_type {
        "long" | "double" => fbs::FilterValueT {
            number_val: target_val_str.parse::<f64>().unwrap_or(0.0),
            ..Default::default()
        },
        "boolean" => fbs::FilterValueT {
            bool_val: target_val_str.parse::<bool>().unwrap_or(false),
            ..Default::default()
        },
        _ => fbs::FilterValueT {
            string_val: Some(target_val_str.to_string()),
            ..Default::default()
        },
    };

    Some(fbs::FilterNodeT {
        criteria: Some(Box::new(fbs::FilterCriteriaT {
            field: Some(field),
            op_ref: op,
            value: Some(Box::new(fval)),
            ..Default::default()
        })),
        ..Default::default()
    })
}

fn fc_to_proto_filter_node(
    filter_conditions: &serde_json::Value,
) -> Option<crate::grpc::pb::FilterNode> {
    let parsed_value;
    let fc = if let Some(s) = filter_conditions.as_str() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            parsed_value = v;
            &parsed_value
        } else {
            filter_conditions
        }
    } else {
        filter_conditions
    };

    let arr = fc.as_array()?;
    if arr.is_empty() {
        return None;
    }

    if arr.len() == 1 {
        return fc_item_to_proto_node(&arr[0]);
    }

    let first = &arr[0];
    let parsed_first;
    let first_obj = if let Some(s) = first.as_str() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            parsed_first = v;
            &parsed_first
        } else {
            first
        }
    } else {
        first
    };

    let logical_op = first_obj
        .get("logical_operator")
        .and_then(|v| v.as_str())
        .unwrap_or("AND");
    let conjunction = match logical_op {
        "OR" => 2,
        _ => 1,
    };

    let mut child_nodes = Vec::new();
    for item in arr {
        if let Some(node) = fc_item_to_proto_node(item) {
            child_nodes.push(node);
        }
    }

    if child_nodes.is_empty() {
        return None;
    }

    use crate::grpc::pb::{filter_node::Node, FilterGroup, FilterNode};
    Some(FilterNode {
        node: Some(Node::Group(FilterGroup {
            conjunction,
            nodes: child_nodes,
        })),
    })
}

fn fc_item_to_proto_node(item: &serde_json::Value) -> Option<crate::grpc::pb::FilterNode> {
    let parsed_value;
    let obj = if let Some(s) = item.as_str() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
            parsed_value = v;
            &parsed_value
        } else {
            item
        }
    } else {
        item
    };

    let field = obj.get("field_name").and_then(|v| v.as_str())?.to_string();
    let op_str = obj.get("operator").and_then(|v| v.as_str())?.to_lowercase();
    let target_val_str = obj.get("target_value").and_then(|v| v.as_str())?;
    let val_type = obj
        .get("target_value_type")
        .and_then(|v| v.as_str())
        .unwrap_or("string");

    let op = match op_str.as_str() {
        "eq" => 1,
        "neq" => 2,
        "gt" => 3,
        "gte" => 4,
        "lt" => 5,
        "lte" => 6,
        "in" => 7,
        "not_in" => 8,
        "between" => 9,
        "like" => 10,
        "is_null" => 11,
        "is_not_null" => 12,
        "matches" => 13,
        "contains" => 14,
        _ => 1,
    };

    use crate::grpc::pb::{
        filter_node::Node, filter_value::Kind, FilterCriteria, FilterNode, FilterValue,
    };

    let kind = match val_type {
        "long" | "double" => Some(Kind::NumberVal(
            target_val_str.parse::<f64>().unwrap_or(0.0),
        )),
        "boolean" => Some(Kind::BoolVal(
            target_val_str.parse::<bool>().unwrap_or(false),
        )),
        _ => Some(Kind::StringVal(target_val_str.to_string())),
    };

    Some(FilterNode {
        node: Some(Node::Criteria(FilterCriteria {
            field,
            op_ref: op,
            value: Some(FilterValue { kind }),
        })),
    })
}

fn map_columns(
    columns_val: Option<&serde_json::Value>,
    is_system_bff: bool,
) -> (Vec<crate::grpc::pb::ColumnSchema>, Vec<String>) {
    let mut pb_columns = Vec::new();
    let mut col_keys = Vec::new();
    if let Some(cols) = columns_val.and_then(|c| c.as_array()) {
        for col in cols {
            let key = col
                .get("key")
                .and_then(|k| k.as_str())
                .unwrap_or("")
                .to_string();

            // FLS: Si el atributo es password_hash y el cliente NO tiene el rol system-bff, omitirlo
            if key == "password_hash" && !is_system_bff {
                continue;
            }

            col_keys.push(key.clone());
            pb_columns.push(crate::grpc::pb::ColumnSchema {
                key,
                label: col
                    .get("label")
                    .and_then(|l| l.as_str())
                    .unwrap_or("")
                    .to_string(),
                r#type: col
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("")
                    .to_string(),
                format: col
                    .get("format")
                    .and_then(|f| f.as_str())
                    .unwrap_or("")
                    .to_string(),
                is_dimension: col
                    .get("is_dimension")
                    .and_then(|b| b.as_bool())
                    .unwrap_or(false),
                is_measure: col
                    .get("is_measure")
                    .and_then(|b| b.as_bool())
                    .unwrap_or(false),
            });
        }
    }
    (pb_columns, col_keys)
}

fn map_data_rows(
    data_val: Option<&serde_json::Value>,
    col_keys: &[String],
) -> Vec<crate::grpc::pb::DataRow> {
    let mut pb_rows = Vec::new();
    if let Some(data) = data_val.and_then(|d| d.as_array()) {
        for r in data {
            if let Some(obj) = r.as_object() {
                let mut values = Vec::new();
                for k in col_keys {
                    let v = obj.get(k).unwrap_or(&serde_json::Value::Null);
                    let pb_val = translator::json_to_value(v);
                    values.push(pb_val);
                }
                pb_rows.push(crate::grpc::pb::DataRow { values });
            }
        }
    }
    pb_rows
}

fn map_metadata(meta_val: Option<&serde_json::Value>) -> Option<crate::grpc::pb::QueryMetadata> {
    meta_val.map(|m| crate::grpc::pb::QueryMetadata {
        execution_time_ms: m
            .get("execution_time_ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        total_count: m.get("total_count").and_then(|v| v.as_i64()).unwrap_or(0),
        engine: m
            .get("engine")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        is_semantic: m
            .get("is_semantic")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        total_queries: m.get("total_queries").and_then(|v| v.as_i64()).unwrap_or(1) as i32,
        parallelism_factor: m
            .get("parallelism_factor")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0),
        cache_hits: m.get("cache_hits").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
        query_id: m
            .get("query_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        cache_ttl_seconds: m
            .get("cache_ttl_seconds")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
    })
}

fn map_pagination(pag_val: Option<&serde_json::Value>) -> Option<crate::grpc::pb::Pagination> {
    pag_val.and_then(|p| {
        if p.is_null() {
            return None;
        }
        let mut links = Vec::new();
        if let Some(arr) = p.get("links").and_then(|l| l.as_array()) {
            for lnk in arr {
                links.push(crate::grpc::pb::Link {
                    rel: lnk
                        .get("rel")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    href: lnk
                        .get("href")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    method: lnk
                        .get("method")
                        .and_then(|v| v.as_str())
                        .unwrap_or("POST")
                        .to_string(),
                });
            }
        }
        Some(crate::grpc::pb::Pagination {
            next_cursor: p
                .get("next_cursor")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            previous_cursor: p
                .get("previous_cursor")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            page_size: p.get("page_size").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
            has_next: p.get("has_next").and_then(|v| v.as_bool()).unwrap_or(false),
            has_previous: p
                .get("has_previous")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            links,
        })
    })
}

fn map_viz_meta(viz_val: Option<&serde_json::Value>) -> Option<crate::grpc::pb::VizMeta> {
    viz_val.map(|vz| {
        let viz_type = vz
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let payload_json = vz.get("payload");

        let payload = if let Some(pj) = payload_json {
            if let Some(signal) = pj.get("signal") {
                Some(crate::grpc::pb::viz_meta::Payload::Signal(
                    crate::grpc::pb::AnalyticalSignal {
                        value: signal.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0),
                        previous_value: signal.get("previous_value").and_then(|v| v.as_f64()),
                        unit: signal
                            .get("unit")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        status_label: signal
                            .get("status_label")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        entity_ref: signal
                            .get("entity_ref")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        intelligence: signal.get("intelligence").map(|intel| {
                            crate::grpc::pb::IntelligenceSignal {
                                direction: intel
                                    .get("direction")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("neutral")
                                    .to_string(),
                                percentage: intel
                                    .get("percentage")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(0.0),
                                delta_abs: intel
                                    .get("delta_abs")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(0.0),
                                previous_value: intel
                                    .get("previous_value")
                                    .and_then(|v| v.as_f64()),
                                label: intel
                                    .get("label")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                is_anomaly: intel
                                    .get("is_anomaly")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false),
                                z_score: intel
                                    .get("z_score")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(0.0),
                                represents_initial: intel
                                    .get("represents_initial")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(false),
                            }
                        }),
                        ..Default::default()
                    },
                ))
            } else if let Some(chart) = pj.get("chart") {
                Some(crate::grpc::pb::viz_meta::Payload::Chart(
                    crate::grpc::pb::ChartDecoration {
                        x_dimension: chart
                            .get("x_dimension")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        y_dimensions: chart
                            .get("y_dimensions")
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_str().map(str::to_string))
                                    .collect()
                            })
                            .unwrap_or_default(),
                        color_scheme: chart
                            .get("color_scheme")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        show_legend: chart
                            .get("show_legend")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true),
                        show_tooltip: chart
                            .get("show_tooltip")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true),
                        title: chart
                            .get("title")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        stacked: chart
                            .get("stacked")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                        smooth: chart
                            .get("smooth")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                        label_template: chart
                            .get("label_template")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        fill_gaps: chart
                            .get("fill_gaps")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                        x_axis_label_template: chart
                            .get("x_axis_label_template")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        y_axis_label_template: chart
                            .get("y_axis_label_template")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        horizontal: chart
                            .get("horizontal")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                    },
                ))
            } else if let Some(breakdown) = pj.get("breakdown") {
                let mut pb_signals = std::collections::HashMap::new();
                if let Some(signals) = breakdown.get("signals").and_then(|s| s.as_object()) {
                    for (k, v) in signals {
                        pb_signals.insert(
                            k.clone(),
                            crate::grpc::pb::AnalyticalSignal {
                                value: v.get("value").and_then(|val| val.as_f64()).unwrap_or(0.0),
                                ..Default::default()
                            },
                        );
                    }
                }
                Some(crate::grpc::pb::viz_meta::Payload::Breakdown(
                    crate::grpc::pb::BreakdownSignal {
                        signals: pb_signals,
                    },
                ))
            } else if let Some(table) = pj.get("table") {
                let table_columns = table
                    .get("columns")
                    .and_then(|c| c.as_array())
                    .map(|cols| {
                        cols.iter()
                            .map(|col| crate::grpc::pb::TableColumn {
                                key: col
                                    .get("key")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                label: col
                                    .get("label")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                r#type: col
                                    .get("type")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("string")
                                    .to_string(),
                                sortable: col
                                    .get("sortable")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(true),
                                format: col
                                    .get("format")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                ..Default::default()
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                Some(crate::grpc::pb::viz_meta::Payload::Table(
                    crate::grpc::pb::TableMeta {
                        columns: table_columns,
                        row_actions: vec![],
                        global_links: vec![],
                    },
                ))
            } else if let Some(tree) = pj.get("tree") {
                Some(crate::grpc::pb::viz_meta::Payload::Tree(
                    crate::grpc::pb::TreeMeta {
                        id_key: tree
                            .get("id_key")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        parent_id_key: tree
                            .get("parent_id_key")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        label_key: tree
                            .get("label_key")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        has_children_key: tree
                            .get("has_children_key")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        icon_key: tree
                            .get("icon_key")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                    },
                ))
            } else {
                None
            }
        } else {
            None
        };

        crate::grpc::pb::VizMeta {
            r#type: viz_type,
            payload,
        }
    })
}

fn map_status(
    status_val: Option<&serde_json::Value>,
    chunk_success: bool,
) -> crate::grpc::pb::Status {
    status_val
        .map(|s| crate::grpc::pb::Status {
            success: s
                .get("success")
                .and_then(|v| v.as_bool())
                .unwrap_or(chunk_success),
            error_code: s
                .get("error_code")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            error_message: s
                .get("error_message")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            error_context: None,
        })
        .unwrap_or(crate::grpc::pb::Status {
            success: chunk_success,
            error_code: if !chunk_success {
                "JANUS_ERROR".to_string()
            } else {
                "".to_string()
            },
            error_message: String::new(),
            error_context: None,
        })
}

async fn map_chunk_to_response(
    chunk: crate::janus::router::QueryChunk,
    is_system_bff: bool,
    export_storage: Option<&std::sync::Arc<dyn crate::domain::protocols::IExportStorage>>,
    tenant_id: &str,
) -> crate::grpc::pb::QueryResponse {
    tracing::debug!("CHUNK BODY: {}", chunk.body);

    let mut batch_results = std::collections::HashMap::new();

    // ── 1. Columns & 2. DataRows ────────────────────────────────
    let columns_val = chunk.body.get("columns");
    let (pb_columns, col_keys) = map_columns(columns_val, is_system_bff);

    let data_val = chunk.body.get("data");
    let pb_rows = map_data_rows(data_val, &col_keys);

    let output_cast = chunk.body.get("output_cast").and_then(|v| v.as_str());

    let payload_strategy =
        if output_cast == Some("CSV_EXPORT") && pb_rows.len() > 5000 && export_storage.is_some() {
            tracing::info!(
                "[Service] Output is CSV_EXPORT with {} rows (> 5000), uploading to S3...",
                pb_rows.len()
            );
            match export_storage
                .unwrap()
                .generate_presigned_url(tenant_id, &chunk.query_key, &pb_columns, &pb_rows)
                .await
            {
                Ok(url) => {
                    tracing::info!(
                        "[Service] S3 export success. Presigned URL generated: {}",
                        url
                    );
                    Some(crate::grpc::pb::row_set::PayloadStrategy::PresignedCsvUrl(
                        url,
                    ))
                }
                Err(e) => {
                    tracing::error!(
                        "[Service] S3 export failed: {}. Falling back to inline JSON.",
                        e.detail
                    );
                    Some(crate::grpc::pb::row_set::PayloadStrategy::RowsJson(
                        crate::grpc::pb::DataRowList { iter: pb_rows },
                    ))
                }
            }
        } else {
            Some(crate::grpc::pb::row_set::PayloadStrategy::RowsJson(
                crate::grpc::pb::DataRowList { iter: pb_rows },
            ))
        };

    // ── 3. QueryMetadata ────────────────────────────────────────
    let pb_metadata = map_metadata(chunk.body.get("metadata"));

    // ── 4. Pagination ───────────────────────────────────────────
    let pb_pagination = map_pagination(chunk.body.get("pagination"));

    // ── 5. VizMeta ──────────────────────────────────────────────
    let pb_viz_ext = map_viz_meta(chunk.body.get("viz_ext"));

    // ── 6. Inner batch_result status ────────────────────────────
    let inner_status = map_status(chunk.body.get("status"), chunk.success);

    // ── 7. Assemble inner QueryResponse ─────────────────────────
    let query_key = chunk.query_key.clone();
    batch_results.insert(
        query_key,
        crate::grpc::pb::QueryResponse {
            status: Some(inner_status),
            data: Some(crate::grpc::pb::RowSet {
                columns: pb_columns,
                payload_strategy,
            }),
            viz_ext: pb_viz_ext.clone(),
            metadata: pb_metadata.clone(),
            pagination: pb_pagination.clone(),
            ..Default::default()
        },
    );

    // ── 8. Assemble outer (envelope) QueryResponse ──────────────
    crate::grpc::pb::QueryResponse {
        status: Some(crate::grpc::pb::Status {
            success: chunk.success,
            error_code: String::new(),
            error_message: String::new(),
            error_context: None,
        }),
        batch_results,
        metadata: pb_metadata,
        pagination: pb_pagination,
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "tests/service_tests.rs"]
mod tests;
