// grpc/service.rs — Implementación de gRPC (Tonic)
// SRP: Implementa la interfaz gRPC `MetriService`.

use tonic::{Request, Response, Status};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, error};

use super::pb::metri_service_server::MetriService;
use super::pb::{
    DiscoveryRequest, DiscoveryResponse, ExploreRequest, ExploreResponse,
    QueryRequest, QueryResponse, TransactionRequest, TransactionResponse,
    BulkRequest, BulkResponse, MatchRoutingRulesBatchRequest, MatchRoutingRulesBatchResponse
};
use crate::janus::router;
use crate::janus::router::CedarCtx;

use crate::grpc::translator;

pub struct MetriGrpcService {
    oltp_executor: crate::aegis::oltp::executor::OltpExecutor,
    iop_orchestrator: std::sync::Arc<dyn crate::iop::core::IIopOrchestrator>,
}

impl MetriGrpcService {
    pub fn new(
        oltp_executor: crate::aegis::oltp::executor::OltpExecutor,
        janus_router: std::sync::Arc<crate::janus_router::router::JanusRouter>,
        audit_interceptor: std::sync::Arc<crate::infrastructure::audit::interceptor::AuditInterceptorImpl>,
    ) -> Self {
        // Inicializar pasos del IOP
        let cedar_step = std::sync::Arc::new(crate::iop::cedar_step::CedarAuthorizerStep::new());
        let quota_step = std::sync::Arc::new(crate::iop::quota_step::QuotaGuardStep::new());
        let janus_step = std::sync::Arc::new(crate::iop::janus_step::JanusRouterStep::new(janus_router));
        
        let steps: Vec<std::sync::Arc<dyn crate::iop::core::IopStep>> = vec![
            cedar_step,
            quota_step,
            janus_step,
        ];
        
        let iop_orchestrator = std::sync::Arc::new(crate::iop::core::IopOrchestrator::new(
            steps,
            None, // FASE 4: MoiraEmitter
            Some(audit_interceptor),
        ));
        
        Self { oltp_executor, iop_orchestrator }
    }
}

#[tonic::async_trait]
impl MetriService for MetriGrpcService {
    type DiscoveryStream = ReceiverStream<Result<DiscoveryResponse, Status>>;
    type ExploreStream = ReceiverStream<Result<ExploreResponse, Status>>;
    type QueryStream = ReceiverStream<Result<QueryResponse, Status>>;

    async fn discovery(&self, request: Request<DiscoveryRequest>) -> Result<Response<Self::DiscoveryStream>, Status> {
        let req = request.into_inner();
        info!("Discovery request: tenant={}", req.tenant_id);
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
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
                            attrs.push(crate::grpc::pb::AttributeSchema {
                                name: attr.name.clone(),
                                r#type: format!("{:?}", attr.attr_type).to_lowercase(),
                                label: attr.label.clone().unwrap_or_else(|| attr.name.clone()),
                                filterable: true,
                                sortable: true,
                                groupable: true,
                                aggregatable: matches!(attr.attr_type, crate::codice::registry::AttrType::Number | crate::codice::registry::AttrType::Epoch),
                                fts: attr.fts,
                                entity_ref: attr.entity_ref.clone().unwrap_or_default(),
                                ..Default::default()
                            });
                        }
                    }
                    schemas.push(crate::grpc::pb::EntitySchema {
                        entity: model.entity.clone(),
                        attributes: attrs,
                        label: model.label.clone().unwrap_or_else(|| model.entity.clone()),
                        icon: model.icon.clone().unwrap_or_default(),
                        primary_key: model.primary_key.clone().unwrap_or_else(|| "id".to_string()),
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
                }),
                schemas,
                ..Default::default()
            };
            let _ = tx.send(Ok(resp)).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn explore(&self, request: Request<ExploreRequest>) -> Result<Response<Self::ExploreStream>, Status> {
        let req = request.into_inner();
        info!("Explore request: tenant={} entity={} attr={}", req.tenant_id, req.entity, req.attribute);
        let (tx, rx) = mpsc::channel(1);
        let exec = self.oltp_executor.clone();
        tokio::spawn(async move {
            // Use AEVT scan to get distinct values for the attribute
            let limit = if req.limit > 0 { req.limit as usize } else { 100 };
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

            match exec.run_oltp_query_fbs(&req.tenant_id, &ast_ir).await {
                Ok(result) => {
                    let mut values = Vec::new();
                    let rows = result.get("data").and_then(|d| d.as_array())
                        .or_else(|| result.as_array())
                        .cloned()
                        .unwrap_or_default();
                    for row in &rows {
                        if let Some(val) = row.get(&req.attribute).and_then(|v| v.as_str()) {
                            if !values.contains(&val.to_string()) {
                                values.push(val.to_string());
                            }
                        }
                    }
                    let resp = ExploreResponse {
                        status: Some(crate::grpc::pb::Status {
                            success: true, error_code: String::new(), error_message: String::new(),
                        }),
                        values,
                    };
                    let _ = tx.send(Ok(resp)).await;
                }
                Err(e) => {
                    let resp = ExploreResponse {
                        status: Some(crate::grpc::pb::Status {
                            success: false, error_code: "EXPLORE_ERROR".to_string(),
                            error_message: e.to_string(),
                        }),
                        values: vec![],
                    };
                    let _ = tx.send(Ok(resp)).await;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn query(&self, request: Request<QueryRequest>) -> Result<Response<Self::QueryStream>, Status> {
        let req = request.into_inner();
        info!("Query request for tenant: {}", req.tenant_id);

        let (tx, rx) = mpsc::channel(4);

        // FASE 4: Conexión a JanusRouter::run_query_pipeline
        let exec_clone = self.oltp_executor.clone();
        tokio::spawn(async move {
            let tenant_id = req.tenant_id.clone();
            let queries_map = match translator::query_request_to_queries_map(&req) {
                Ok(q) => q,
                Err(e) => {
                    let _ = tx.send(Err(Status::invalid_argument(e.to_string()))).await;
                    return;
                }
            };
            tracing::info!("DEBUG QUERIES_MAP LEN: {}", queries_map.len());
            
            // Construir el contexto ABAC para este request
            let cedar_ctx = CedarCtx {
                tenant_id: tenant_id.clone(),
                user_id: "grpc-user".to_string(), // Extraeríamos del JWT
                roles: vec!["admin".to_string()],
                is_super_master: false,
                cross_tenant_scope: "".to_string(),
            };

            let chunks = router::run_query_pipeline(&tenant_id, &queries_map, &cedar_ctx, &exec_clone).await;
            
            // Enviar cada chunk como parte del stream
            for chunk in chunks {
                tracing::info!("DEBUG CHUNK BODY: {}", chunk.body);
                
                let mut batch_results = std::collections::HashMap::new();
                let mut pb_rows = Vec::new();

                // ── 1. Columns (ColumnSchema[]) ─────────────────────────────
                let mut pb_columns = Vec::new();
                let mut col_keys = Vec::new();
                if let Some(cols) = chunk.body.get("columns").and_then(|c| c.as_array()) {
                    for col in cols {
                        let key = col.get("key").and_then(|k| k.as_str()).unwrap_or("").to_string();
                        col_keys.push(key.clone());
                        pb_columns.push(crate::grpc::pb::ColumnSchema {
                            key,
                            label: col.get("label").and_then(|l| l.as_str()).unwrap_or("").to_string(),
                            r#type: col.get("type").and_then(|t| t.as_str()).unwrap_or("").to_string(),
                            format: col.get("format").and_then(|f| f.as_str()).unwrap_or("").to_string(),
                            is_dimension: col.get("is_dimension").and_then(|b| b.as_bool()).unwrap_or(false),
                            is_measure: col.get("is_measure").and_then(|b| b.as_bool()).unwrap_or(false),
                        });
                    }
                }

                // ── 2. DataRows (deterministic column order) ─────────────────
                if let Some(data) = chunk.body.get("data").and_then(|d| d.as_array()) {
                    for r in data {
                        if let Some(obj) = r.as_object() {
                            let mut values = Vec::new();
                            for k in &col_keys {
                                let v = obj.get(k).unwrap_or(&serde_json::Value::Null);
                                let pb_val = match v.clone() {
                                    serde_json::Value::Null => prost_types::Value { kind: Some(prost_types::value::Kind::NullValue(0)) },
                                    serde_json::Value::Bool(b) => prost_types::Value { kind: Some(prost_types::value::Kind::BoolValue(b)) },
                                    serde_json::Value::Number(n) => prost_types::Value { kind: Some(prost_types::value::Kind::NumberValue(n.as_f64().unwrap_or(0.0))) },
                                    serde_json::Value::String(s) => prost_types::Value { kind: Some(prost_types::value::Kind::StringValue(s)) },
                                    _ => prost_types::Value { kind: Some(prost_types::value::Kind::StringValue(v.to_string())) },
                                };
                                values.push(pb_val);
                            }
                            pb_rows.push(crate::grpc::pb::DataRow { values });
                        }
                    }
                }

                let payload_strategy = Some(crate::grpc::pb::row_set::PayloadStrategy::RowsJson(
                    crate::grpc::pb::DataRowList { iter: pb_rows }
                ));

                // ── 3. QueryMetadata ────────────────────────────────────────
                let pb_metadata = chunk.body.get("metadata").map(|m| {
                    crate::grpc::pb::QueryMetadata {
                        execution_time_ms: m.get("execution_time_ms").and_then(|v| v.as_i64()).unwrap_or(0),
                        total_count: m.get("total_count").and_then(|v| v.as_i64()).unwrap_or(0),
                        engine: m.get("engine").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        is_semantic: m.get("is_semantic").and_then(|v| v.as_bool()).unwrap_or(false),
                        total_queries: m.get("total_queries").and_then(|v| v.as_i64()).unwrap_or(1) as i32,
                        parallelism_factor: m.get("parallelism_factor").and_then(|v| v.as_f64()).unwrap_or(1.0),
                        cache_hits: m.get("cache_hits").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
                        query_id: m.get("query_id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        cache_ttl_seconds: m.get("cache_ttl_seconds").and_then(|v| v.as_i64()).unwrap_or(0),
                    }
                });

                // ── 4. Pagination ───────────────────────────────────────────
                let pb_pagination = chunk.body.get("pagination").and_then(|p| {
                    if p.is_null() { return None; }
                    let mut links = Vec::new();
                    if let Some(arr) = p.get("links").and_then(|l| l.as_array()) {
                        for lnk in arr {
                            links.push(crate::grpc::pb::Link {
                                rel: lnk.get("rel").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                href: lnk.get("href").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                method: lnk.get("method").and_then(|v| v.as_str()).unwrap_or("POST").to_string(),
                            });
                        }
                    }
                    Some(crate::grpc::pb::Pagination {
                        next_cursor: p.get("next_cursor").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        previous_cursor: p.get("previous_cursor").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        page_size: p.get("page_size").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
                        has_next: p.get("has_next").and_then(|v| v.as_bool()).unwrap_or(false),
                        has_previous: p.get("has_previous").and_then(|v| v.as_bool()).unwrap_or(false),
                        links,
                    })
                });

                // ── 5. VizMeta ──────────────────────────────────────────────
                let pb_viz_ext = chunk.body.get("viz_ext").map(|vz| {
                    let viz_type = vz.get("type").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let payload_json = vz.get("payload");

                    let payload = if let Some(pj) = payload_json {
                        if let Some(signal) = pj.get("signal") {
                            Some(crate::grpc::pb::viz_meta::Payload::Signal(
                                crate::grpc::pb::AnalyticalSignal {
                                    value: signal.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0),
                                    previous_value: signal.get("previous_value").and_then(|v| v.as_f64()).unwrap_or(0.0),
                                    unit: signal.get("unit").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    status_label: signal.get("status_label").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    entity_ref: signal.get("entity_ref").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    intelligence: signal.get("intelligence").map(|intel| {
                                        crate::grpc::pb::IntelligenceSignal {
                                            direction: intel.get("direction").and_then(|v| v.as_str()).unwrap_or("neutral").to_string(),
                                            percentage: intel.get("percentage").and_then(|v| v.as_f64()).unwrap_or(0.0),
                                            delta_abs: intel.get("delta_abs").and_then(|v| v.as_f64()).unwrap_or(0.0),
                                            previous_value: intel.get("previous_value").and_then(|v| v.as_f64()).unwrap_or(0.0),
                                            label: intel.get("label").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                            is_anomaly: intel.get("is_anomaly").and_then(|v| v.as_bool()).unwrap_or(false),
                                            z_score: intel.get("z_score").and_then(|v| v.as_f64()).unwrap_or(0.0),
                                            represents_initial: intel.get("represents_initial").and_then(|v| v.as_bool()).unwrap_or(false),
                                        }
                                    }),
                                    ..Default::default()
                                }
                            ))
                        } else if let Some(chart) = pj.get("chart") {
                            Some(crate::grpc::pb::viz_meta::Payload::Chart(
                                crate::grpc::pb::ChartDecoration {
                                    x_dimension: chart.get("x_dimension").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    y_dimensions: chart.get("y_dimensions").and_then(|v| v.as_array())
                                        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                                        .unwrap_or_default(),
                                    color_scheme: chart.get("color_scheme").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    show_legend: chart.get("show_legend").and_then(|v| v.as_bool()).unwrap_or(true),
                                    show_tooltip: chart.get("show_tooltip").and_then(|v| v.as_bool()).unwrap_or(true),
                                    title: chart.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    stacked: chart.get("stacked").and_then(|v| v.as_bool()).unwrap_or(false),
                                    smooth: chart.get("smooth").and_then(|v| v.as_bool()).unwrap_or(false),
                                    label_template: chart.get("label_template").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    fill_gaps: chart.get("fill_gaps").and_then(|v| v.as_bool()).unwrap_or(false),
                                }
                            ))
                        } else if let Some(breakdown) = pj.get("breakdown") {
                            let mut pb_signals = std::collections::HashMap::new();
                            if let Some(signals) = breakdown.get("signals").and_then(|s| s.as_object()) {
                                for (k, v) in signals {
                                    pb_signals.insert(k.clone(), crate::grpc::pb::AnalyticalSignal {
                                        value: v.get("value").and_then(|val| val.as_f64()).unwrap_or(0.0),
                                        ..Default::default()
                                    });
                                }
                            }
                            Some(crate::grpc::pb::viz_meta::Payload::Breakdown(
                                crate::grpc::pb::BreakdownSignal {
                                    signals: pb_signals,
                                }
                            ))
                        } else if let Some(table) = pj.get("table") {
                            let table_columns = table.get("columns")
                                .and_then(|c| c.as_array())
                                .map(|cols| cols.iter().map(|col| crate::grpc::pb::TableColumn {
                                    key: col.get("key").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    label: col.get("label").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    r#type: col.get("type").and_then(|v| v.as_str()).unwrap_or("string").to_string(),
                                    sortable: col.get("sortable").and_then(|v| v.as_bool()).unwrap_or(true),
                                    format: col.get("format").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    ..Default::default()
                                }).collect())
                                .unwrap_or_default();
                            Some(crate::grpc::pb::viz_meta::Payload::Table(
                                crate::grpc::pb::TableMeta {
                                    columns: table_columns,
                                    row_actions: vec![],
                                    global_links: vec![],
                                }
                            ))
                        } else if let Some(tree) = pj.get("tree") {
                            Some(crate::grpc::pb::viz_meta::Payload::Tree(
                                crate::grpc::pb::TreeMeta {
                                    id_key: tree.get("id_key").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    parent_id_key: tree.get("parent_id_key").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    label_key: tree.get("label_key").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    has_children_key: tree.get("has_children_key").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                    icon_key: tree.get("icon_key").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                }
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
                });

                // ── 6. Inner batch_result status ────────────────────────────
                let inner_status = chunk.body.get("status").map(|s| {
                    crate::grpc::pb::Status {
                        success: s.get("success").and_then(|v| v.as_bool()).unwrap_or(chunk.success),
                        error_code: s.get("error_code").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                        error_message: s.get("error_message").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    }
                }).unwrap_or(crate::grpc::pb::Status {
                    success: chunk.success,
                    error_code: if !chunk.success { "JANUS_ERROR".to_string() } else { "".to_string() },
                    error_message: String::new(),
                });

                // ── 7. Assemble inner QueryResponse ─────────────────────────
                let query_key = chunk.query_key.clone();
                batch_results.insert(query_key, crate::grpc::pb::QueryResponse {
                    status: Some(inner_status.clone()),
                    data: Some(crate::grpc::pb::RowSet {
                        columns: pb_columns,
                        payload_strategy,
                    }),
                    viz_ext: pb_viz_ext.clone(),
                    metadata: pb_metadata.clone(),
                    pagination: pb_pagination.clone(),
                    ..Default::default()
                });

                // ── 8. Assemble outer (envelope) QueryResponse ──────────────
                let res = crate::grpc::pb::QueryResponse {
                    status: Some(crate::grpc::pb::Status {
                        success: chunk.success,
                        error_code: String::new(),
                        error_message: String::new(),
                    }),
                    batch_results,
                    metadata: pb_metadata,
                    pagination: pb_pagination,
                    ..Default::default()
                };

                if tx.send(Ok(res)).await.is_err() {
                    break; // El cliente se desconectó
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn transact(&self, request: Request<TransactionRequest>) -> Result<Response<TransactionResponse>, Status> {
        let req = request.into_inner();
        
        let operation_str = match req.action {
            1 => "CREATE",
            2 => "UPDATE",
            3 => "DELETE",
            _ => "UNKNOWN",
        };

        info!("Transaction request | tenant: {} | entity: {} | op: {}", req.tenant_id, req.entity_type, operation_str);

        let payload_json = if let Some(struct_payload) = req.payload {
            translator::struct_to_value(struct_payload)
        } else {
            serde_json::Value::Object(serde_json::Map::new())
        };

        let mut request_map = serde_json::Map::new();
        
        let actual_payload = if let Some(obj) = payload_json.as_object() {
            if let Some(data) = obj.get("data") {
                data.clone()
            } else {
                payload_json.clone()
            }
        } else {
            payload_json.clone()
        };

        request_map.insert("payload".to_string(), actual_payload);
        request_map.insert("tenant_id".to_string(), serde_json::Value::String(req.tenant_id.clone()));
        request_map.insert("entity_type".to_string(), serde_json::Value::String(req.entity_type.clone()));
        request_map.insert("operation".to_string(), serde_json::Value::String(operation_str.to_string()));

        let ctx = crate::iop::core::IopContext::new(
            &req.tenant_id,
            "grpc-user", // FASE 2: Extraer del JWT
            &req.entity_type,
            operation_str,
            request_map,
        );

        // ── Ejecución del IOP Orchestrator (Railway + Async) ─────────────────────
        let response_value = self.iop_orchestrator.run(ctx).await;

        if let Some("error") = response_value.get("status").and_then(|s| s.as_str()) {
            let error_obj = response_value.get("error").unwrap();
            let code = error_obj.get("code").and_then(|c| c.as_str()).unwrap_or("UNKNOWN").to_string();
            let desc = error_obj.get("description").and_then(|d| d.as_str()).unwrap_or("").to_string();

            let response = TransactionResponse {
                status: Some(super::pb::Status {
                    success: false,
                    error_code: code,
                    error_message: desc,
                }),
                entity_id: String::new(),
                ..Default::default()
            };
            return Ok(Response::new(response));
        }

        let entity_id = response_value.get("entity_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| response_value.get("result")
                .and_then(|res| res.get("entity_id"))
                .and_then(|v| v.as_str()))
            .unwrap_or("")
            .to_string();
        
        tracing::info!("Raw IOP Response: {}", response_value);
        tracing::info!("Extracted entity_id: {}", entity_id);

        let response = TransactionResponse {
            status: Some(super::pb::Status {
                success: true,
                error_code: String::new(),
                error_message: if entity_id.is_empty() { response_value.to_string() } else { String::new() },
            }),
            entity_id,
            result: Some(translator::value_to_struct(&response_value)),
        };
        Ok(Response::new(response))
    }

    async fn bulk_ingest(&self, request: Request<BulkRequest>) -> Result<Response<BulkResponse>, Status> {
        let req = request.into_inner();
        let operation_str = match req.action {
            1 => "CREATE",
            2 => "UPDATE",
            3 => "DELETE",
            _ => "UNKNOWN",
        };

        info!("BulkIngest request | tenant: {} | entity: {} | op: {}", req.tenant_id, req.entity_type, operation_str);

        let row_set = req.data.ok_or_else(|| Status::invalid_argument("data (RowSet) is required"))?;
        let columns = row_set.columns;
        
        let payload_strategy = row_set.payload_strategy.ok_or_else(|| Status::invalid_argument("payload_strategy is required"))?;
        let mut ingested_count = 0;
        
        match payload_strategy {
            crate::grpc::pb::row_set::PayloadStrategy::RowsJson(data_row_list) => {
                for row in data_row_list.iter {
                    let mut payload_map = serde_json::Map::new();
                    for (i, val) in row.values.into_iter().enumerate() {
                        if let Some(col) = columns.get(i) {
                            let json_val = match val.kind {
                                Some(prost_types::value::Kind::StringValue(s)) => serde_json::Value::String(s),
                                Some(prost_types::value::Kind::NumberValue(n)) => {
                                    if n.fract() == 0.0 {
                                        serde_json::Value::Number(serde_json::Number::from(n as i64))
                                    } else if let Some(num) = serde_json::Number::from_f64(n) {
                                        serde_json::Value::Number(num)
                                    } else {
                                        serde_json::Value::Null
                                    }
                                },
                                Some(prost_types::value::Kind::BoolValue(b)) => serde_json::Value::Bool(b),
                                Some(prost_types::value::Kind::StructValue(s)) => translator::struct_to_value(s),
                                _ => serde_json::Value::Null,
                            };
                            payload_map.insert(col.key.clone(), json_val);
                        }
                    }

                    // Extraer ID explícito si viene en el payload
                    let entity_id_opt = payload_map.remove("entity_id")
                        .or_else(|| payload_map.remove("id"));

                    let payload_json = serde_json::Value::Object(payload_map);
                    
                    let mut request_map = serde_json::Map::new();
                    request_map.insert("payload".to_string(), payload_json);
                    
                    if let Some(id) = entity_id_opt {
                        request_map.insert("entity_id".to_string(), id);
                    }
                    
                    request_map.insert("tenant_id".to_string(), serde_json::Value::String(req.tenant_id.clone()));
                    request_map.insert("entity_type".to_string(), serde_json::Value::String(req.entity_type.clone()));
                    request_map.insert("operation".to_string(), serde_json::Value::String(operation_str.to_string()));

                    let ctx = crate::iop::core::IopContext::new(
                        &req.tenant_id,
                        "grpc-user",
                        &req.entity_type,
                        operation_str,
                        request_map,
                    );

                    let response_value = self.iop_orchestrator.run(ctx).await;
                    
                    if let Some("error") = response_value.get("status").and_then(|s| s.as_str()) {
                        let error_obj = response_value.get("error").unwrap();
                        let desc = error_obj.get("description").and_then(|d| d.as_str()).unwrap_or("").to_string();
                        error!("Error bulk ingesting row: {}", desc);
                    } else {
                        ingested_count += 1;
                    }
                }
            },
            _ => {
                return Err(Status::unimplemented("Only RowsJson is supported for BulkIngest right now"));
            }
        }

        let response = BulkResponse {
            status: Some(crate::grpc::pb::Status {
                success: true,
                error_code: String::new(),
                error_message: String::new(),
            }),
            ingested_count,
            ..Default::default()
        };
        Ok(Response::new(response))
    }

    async fn match_routing_rules_batch(&self, request: Request<MatchRoutingRulesBatchRequest>) -> Result<Response<MatchRoutingRulesBatchResponse>, Status> {
        info!("MatchRoutingRulesBatch request: {:?}", request);
        Ok(Response::new(MatchRoutingRulesBatchResponse::default()))
    }
}
