use serde_json::json;
use std::sync::Arc;

use metri_engine::aegis::oltp::executor::OltpExecutor;
use metri_engine::cedar::authorizer::InMemoryPrincipalCache;
use metri_engine::codice::registry::EngineChannel;
use metri_engine::domain::error_catalog::{
    init_global as init_error_catalog, try_global, ErrorCatalog,
};
use metri_engine::eav::reader::pull::EavReader;
use metri_engine::eav::reader::query::EavQueryExecutor;
use metri_engine::eav::writer::EavWriter;
use metri_engine::grpc::interceptors::AuthenticatedSession;
use metri_engine::grpc::pb::metri_service_server::MetriService;
use metri_engine::grpc::pb::row_set::PayloadStrategy;
use metri_engine::grpc::pb::{
    AnalyticsRequest, OperationAction, OutputCastType, QueryRequest, QueryResponse,
    TransactionRequest, TransactionResponse,
};
use metri_engine::grpc::service::MetriGrpcService;
use metri_engine::grpc::translator;
use metri_engine::infrastructure::audit::interceptor::AuditInterceptorImpl;
use metri_engine::infrastructure::dynamodb::DynamoClient;
use metri_engine::infrastructure::kinesis::SpyStreamWriter;
use metri_engine::infrastructure::session_store::{issue_token, HmacTokenStore};
use metri_engine::janus_router::olap_channel::OlapChannel;
use metri_engine::janus_router::oltp_channel::OltpChannel;
use metri_engine::janus_router::router::{IWriteChannel, JanusRouter};

static INIT: std::sync::Once = std::sync::Once::new();
const HMAC_SECRET: &[u8] = b"secret-key-development-metri-256-bits!!!";

async fn setup_service() -> (MetriGrpcService, Arc<SpyStreamWriter>, String) {
    std::env::set_var("METRI_TEST_MODE", "1");

    if std::env::var("AWS_PROFILE").is_err() {
        if std::env::var("AWS_ACCESS_KEY_ID").is_err() {
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        }
        if std::env::var("AWS_SECRET_ACCESS_KEY").is_err() {
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        }
    }
    if std::env::var("AWS_DEFAULT_REGION").is_err() {
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
    }
    if std::env::var("DYNAMODB_ENDPOINT").is_err() {
        std::env::set_var("DYNAMODB_ENDPOINT", "http://127.0.0.1:8000");
    }

    INIT.call_once(|| {
        // Initialize global registry if not already done
        if metri_engine::codice::registry::global_opt().is_none() {
            let models_dir = std::path::Path::new("config/models");
            if let Ok((registry, _)) = metri_engine::codice::CodeRegistry::build(models_dir) {
                metri_engine::codice::init_global(registry);
            }
        }

        // Initialize global error catalog if not already done
        if try_global().is_none() {
            let catalog_path = std::path::Path::new("config/errors/error_catalog.toml");
            if let Ok(catalog) = ErrorCatalog::load(catalog_path) {
                init_error_catalog(catalog);
            }
        }
    });

    let table_name = std::env::var("EAV_TABLE").unwrap_or_else(|_| "metri-eav-local".to_string());
    let ddb_client = Arc::new(DynamoClient::new(&table_name).await);
    let query_exec = EavQueryExecutor::new(Arc::clone(&ddb_client), &table_name);
    let pull_read = EavReader::new(Arc::clone(&ddb_client), &table_name);
    let oltp_exec = OltpExecutor::new(query_exec, pull_read.clone());
    let eav_writer = EavWriter::new(Arc::clone(&ddb_client), &table_name);
    let oltp_channel: Arc<dyn IWriteChannel> = Arc::new(OltpChannel::new(eav_writer.clone()));

    let spy_writer = Arc::new(SpyStreamWriter::new());
    let olap_channel: Arc<dyn IWriteChannel> =
        Arc::new(OlapChannel::new("metri-olap-stream", spy_writer.clone()));

    let mut channel_registry = std::collections::HashMap::new();
    channel_registry.insert(EngineChannel::Oltp, Arc::clone(&oltp_channel));
    channel_registry.insert(EngineChannel::Olap, Arc::clone(&olap_channel));
    let janus_router = Arc::new(JanusRouter::new(channel_registry));

    let audit_interceptor = Arc::new(AuditInterceptorImpl::new(Arc::clone(&olap_channel)));
    let valkey_store = Arc::new(HmacTokenStore::new(
        HMAC_SECRET.to_vec(),
        Arc::clone(&ddb_client),
        table_name.clone(),
    ));
    let principal_cache = Arc::new(InMemoryPrincipalCache::new());

    let service = MetriGrpcService::new(
        oltp_exec,
        eav_writer,
        janus_router,
        audit_interceptor,
        None,
        None,
        valkey_store,
        principal_cache,
        Arc::new(metri_engine::iop::sherlog::NoopFaultNotifier),
        Arc::clone(&olap_channel),
        None,
    );

    // Generate a valid HMAC session token for the bypass credentials
    let token = issue_token(HMAC_SECRET, "system", "usr_system_bff", 3600, None);

    (service, spy_writer, token)
}

#[tokio::test]
#[ignore] // Requiere DynamoDB local o producción
async fn test_audit_interceptor_crud_e2e() {
    let (service, spy_writer, token) = setup_service().await;

    let tenant_id = "system";
    let user_id = "usr_system_bff";
    let asset_id = format!("ast_e2e_{}", uuid::Uuid::new_v4());

    let auth_header_val = format!("Bearer {}", token);

    // 1. CREATE asset
    let create_payload = json!({
        "id": asset_id,
        "name": "Bomba Centrifuga Audit E2E",
        "area": "Mecánica",
        "status": "ACTIVE",
        "location_id": "01JLOCATIONTEST0000000000",
        "timestamp": Utc::now_timestamp()
    });

    let mut req = tonic::Request::new(TransactionRequest {
        tenant_id: tenant_id.to_string(),
        entity_type: "asset".to_string(),
        entity_id: asset_id.clone(),
        action: OperationAction::Create as i32,
        payload: Some(translator::value_to_struct(&create_payload)),
        suppress_events: false,
    });
    req.metadata_mut()
        .insert("authorization", auth_header_val.parse().unwrap());
    req.metadata_mut()
        .insert("test-tenant", tenant_id.parse().unwrap());
    req.metadata_mut()
        .insert("test-user", user_id.parse().unwrap());
    req.metadata_mut()
        .insert("test-roles", "system-bff".parse().unwrap());
    req.extensions_mut().insert(AuthenticatedSession {
        tenant_id: tenant_id.to_string(),
        user_id: user_id.to_string(),
        jti: "test-jti".to_string(),
    });

    let res: Result<tonic::Response<TransactionResponse>, tonic::Status> =
        service.transact(req).await;
    assert!(res.is_ok(), "CREATE transaction failed: {:?}", res.err());
    let tx_resp = res.unwrap().into_inner();
    let status_obj = tx_resp
        .status
        .expect("Status missing from transaction response");
    assert!(
        status_obj.success,
        "CREATE transaction was not successful: {}",
        status_obj.error_message
    );

    // 2. UPDATE asset
    let update_payload = json!({
        "id": asset_id,
        "name": "Bomba Centrifuga Actualizada E2E",
    });

    let mut req = tonic::Request::new(TransactionRequest {
        tenant_id: tenant_id.to_string(),
        entity_type: "asset".to_string(),
        entity_id: asset_id.clone(),
        action: OperationAction::Update as i32,
        payload: Some(translator::value_to_struct(&update_payload)),
        suppress_events: false,
    });
    req.metadata_mut()
        .insert("authorization", auth_header_val.parse().unwrap());
    req.metadata_mut()
        .insert("test-tenant", tenant_id.parse().unwrap());
    req.metadata_mut()
        .insert("test-user", user_id.parse().unwrap());
    req.metadata_mut()
        .insert("test-roles", "system-bff".parse().unwrap());
    req.extensions_mut().insert(AuthenticatedSession {
        tenant_id: tenant_id.to_string(),
        user_id: user_id.to_string(),
        jti: "test-jti".to_string(),
    });

    let res: Result<tonic::Response<TransactionResponse>, tonic::Status> =
        service.transact(req).await;
    assert!(res.is_ok(), "UPDATE transaction failed: {:?}", res.err());
    let tx_resp = res.unwrap().into_inner();
    let status_obj = tx_resp
        .status
        .expect("Status missing from transaction response");
    assert!(
        status_obj.success,
        "UPDATE transaction was not successful: {}",
        status_obj.error_message
    );

    // 3. DELETE asset
    let delete_payload = json!({
        "id": asset_id,
    });

    let mut req = tonic::Request::new(TransactionRequest {
        tenant_id: tenant_id.to_string(),
        entity_type: "asset".to_string(),
        entity_id: asset_id.clone(),
        action: OperationAction::Delete as i32,
        payload: Some(translator::value_to_struct(&delete_payload)),
        suppress_events: false,
    });
    req.metadata_mut()
        .insert("authorization", auth_header_val.parse().unwrap());
    req.metadata_mut()
        .insert("test-tenant", tenant_id.parse().unwrap());
    req.metadata_mut()
        .insert("test-user", user_id.parse().unwrap());
    req.metadata_mut()
        .insert("test-roles", "system-bff".parse().unwrap());
    req.extensions_mut().insert(AuthenticatedSession {
        tenant_id: tenant_id.to_string(),
        user_id: user_id.to_string(),
        jti: "test-jti".to_string(),
    });

    let res: Result<tonic::Response<TransactionResponse>, tonic::Status> =
        service.transact(req).await;
    assert!(res.is_ok(), "DELETE transaction failed: {:?}", res.err());
    let tx_resp = res.unwrap().into_inner();
    let status_obj = tx_resp
        .status
        .expect("Status missing from transaction response");
    assert!(
        status_obj.success,
        "DELETE transaction was not successful: {}",
        status_obj.error_message
    );

    // Wait for the tokio::spawn fire-and-forget calls inside audit interceptor to finish
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Drain and assert captured records
    let captured = spy_writer.drain();
    assert_eq!(
        captured.len(),
        3,
        "Expected exactly 3 audit records to be captured (CREATE, UPDATE, DELETE)"
    );

    for (i, record) in captured.iter().enumerate() {
        assert_eq!(record.stream_name, "metri-olap-stream-audit-log");
        let data_json: serde_json::Value = serde_json::from_slice(&record.data)
            .expect("Failed to parse captured record data as JSON");

        // Verify system-level fields injected by OlapChannel
        assert!(
            data_json.get("id").and_then(|v| v.as_str()).is_some(),
            "id field missing"
        );
        assert_eq!(
            data_json.get("_tenant").and_then(|v| v.as_str()),
            Some(tenant_id)
        );
        assert!(
            data_json
                .get("created_at")
                .and_then(|v| v.as_i64())
                .is_some(),
            "created_at field missing"
        );

        // Verify audit log specific payload fields
        assert_eq!(
            data_json.get("action_type").and_then(|v| v.as_str()),
            Some("WRITE")
        );
        assert_eq!(
            data_json.get("resource_domain").and_then(|v| v.as_str()),
            Some("asset")
        );
        assert_eq!(
            data_json.get("tenant_id").and_then(|v| v.as_str()),
            Some(tenant_id)
        );
        assert_eq!(
            data_json.get("user_id").and_then(|v| v.as_str()),
            Some(user_id)
        );
        assert_eq!(
            data_json.get("status").and_then(|v| v.as_str()),
            Some("SUCCESS")
        );

        let req_payload = data_json
            .get("request_payload")
            .expect("request_payload missing");
        if i == 0 {
            assert_eq!(
                req_payload.get("name").and_then(|v| v.as_str()),
                Some("Bomba Centrifuga Audit E2E")
            );
        } else if i == 1 {
            assert_eq!(
                req_payload.get("name").and_then(|v| v.as_str()),
                Some("Bomba Centrifuga Actualizada E2E")
            );
        } else if i == 2 {
            assert_eq!(
                req_payload.get("id").and_then(|v| v.as_str()),
                Some(asset_id.as_str())
            );
        }
    }
}

#[tokio::test]
#[ignore]
async fn test_audit_time_travel_history() {
    let (service, _, token) = setup_service().await;

    let tenant_id = "system";
    let user_id = "usr_system_bff";
    let asset_id = format!("ast_e2e_{}", uuid::Uuid::new_v4());

    let auth_header_val = format!("Bearer {}", token);

    // 1. CREATE asset
    let create_payload = json!({
        "id": asset_id,
        "name": "Bomba Centrifuga Original",
        "area": "Mecánica",
        "status": "ACTIVE",
        "location_id": "01JLOCATIONTEST0000000000",
        "timestamp": Utc::now_timestamp()
    });

    let mut req = tonic::Request::new(TransactionRequest {
        tenant_id: tenant_id.to_string(),
        entity_type: "asset".to_string(),
        entity_id: asset_id.clone(),
        action: OperationAction::Create as i32,
        payload: Some(translator::value_to_struct(&create_payload)),
        suppress_events: false,
    });
    req.metadata_mut()
        .insert("authorization", auth_header_val.parse().unwrap());
    req.metadata_mut()
        .insert("test-tenant", tenant_id.parse().unwrap());
    req.metadata_mut()
        .insert("test-user", user_id.parse().unwrap());
    req.metadata_mut()
        .insert("test-roles", "system-bff".parse().unwrap());
    req.extensions_mut().insert(AuthenticatedSession {
        tenant_id: tenant_id.to_string(),
        user_id: user_id.to_string(),
        jti: "test-jti".to_string(),
    });

    let res: Result<tonic::Response<TransactionResponse>, tonic::Status> =
        service.transact(req).await;
    assert!(res.is_ok(), "CREATE failed: {:?}", res.err());
    let tx_resp = res.unwrap().into_inner();
    let status_obj = tx_resp.status.expect("Status missing from CREATE response");
    assert!(
        status_obj.success,
        "CREATE was not successful: {}",
        status_obj.error_message
    );

    // 2. UPDATE asset (change name)
    let update_payload = json!({
        "id": asset_id,
        "name": "Bomba Centrifuga Modificada",
    });

    let mut req = tonic::Request::new(TransactionRequest {
        tenant_id: tenant_id.to_string(),
        entity_type: "asset".to_string(),
        entity_id: asset_id.clone(),
        action: OperationAction::Update as i32,
        payload: Some(translator::value_to_struct(&update_payload)),
        suppress_events: false,
    });
    req.metadata_mut()
        .insert("authorization", auth_header_val.parse().unwrap());
    req.metadata_mut()
        .insert("test-tenant", tenant_id.parse().unwrap());
    req.metadata_mut()
        .insert("test-user", user_id.parse().unwrap());
    req.metadata_mut()
        .insert("test-roles", "system-bff".parse().unwrap());
    req.extensions_mut().insert(AuthenticatedSession {
        tenant_id: tenant_id.to_string(),
        user_id: user_id.to_string(),
        jti: "test-jti".to_string(),
    });

    let res: Result<tonic::Response<TransactionResponse>, tonic::Status> =
        service.transact(req).await;
    assert!(res.is_ok(), "UPDATE failed: {:?}", res.err());
    let tx_resp = res.unwrap().into_inner();
    let status_obj = tx_resp.status.expect("Status missing from UPDATE response");
    assert!(
        status_obj.success,
        "UPDATE was not successful: {}",
        status_obj.error_message
    );

    // 3. Query history timeline with select_tree: { id: <asset_id>, _history: true }
    let query_ast = json!({
        "entity": "asset",
        "select_tree": {
            "id": asset_id,
            "_history": true
        }
    });

    let req_queries = std::collections::HashMap::from([(
        "asset_history".to_string(),
        AnalyticsRequest {
            entity: "asset".to_string(),
            output_cast: OutputCastType::Table as i32,
            viz: "table".to_string(),
            limit: 100,
            select_tree: Some(translator::value_to_struct(
                query_ast.get("select_tree").unwrap(),
            )),
            ..Default::default()
        },
    )]);

    let req = QueryRequest {
        tenant_id: tenant_id.to_string(),
        queries: req_queries,
        ..Default::default()
    };
    let mut grpc_req = tonic::Request::new(req);
    grpc_req
        .metadata_mut()
        .insert("authorization", auth_header_val.parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-tenant", tenant_id.parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-user", user_id.parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-roles", "system-bff".parse().unwrap());
    grpc_req.extensions_mut().insert(AuthenticatedSession {
        tenant_id: tenant_id.to_string(),
        user_id: user_id.to_string(),
        jti: "test-jti".to_string(),
    });

    let res: Result<
        tonic::Response<
            tokio_stream::wrappers::ReceiverStream<Result<QueryResponse, tonic::Status>>,
        >,
        tonic::Status,
    > = service.query(grpc_req).await;
    assert!(res.is_ok(), "History query failed: {:?}", res.err());

    let response = res.unwrap().into_inner();
    let mut stream = response;
    let mut all_results = Vec::new();
    while let Some(chunk_res) = tokio_stream::StreamExt::next(&mut stream).await {
        let chunk: QueryResponse = chunk_res.expect("Stream chunk error");
        if let Some(res_item) = chunk.batch_results.get("asset_history") {
            all_results.push(res_item.clone());
        }
    }

    assert_eq!(
        all_results.len(),
        1,
        "Expected exactly 1 batch result for history query"
    );
    let batch_result = &all_results[0];
    if !batch_result
        .status
        .as_ref()
        .map(|s| s.success)
        .unwrap_or(false)
    {
        let err_msg = batch_result
            .status
            .as_ref()
            .map(|s| &s.error_message)
            .cloned()
            .unwrap_or_default();
        let err_code = batch_result
            .status
            .as_ref()
            .map(|s| &s.error_code)
            .cloned()
            .unwrap_or_default();
        panic!("Batch result indicates failure: {} ({})", err_msg, err_code);
    }

    let row_set = batch_result.data.as_ref().expect("RowSet missing");
    let columns = &row_set.columns;

    let mut attr_name_idx = None;
    let mut value_idx = None;
    let mut op_idx = None;

    for (idx, col) in columns.iter().enumerate() {
        if col.key == "attr_name" {
            attr_name_idx = Some(idx);
        } else if col.key == "value" {
            value_idx = Some(idx);
        } else if col.key == "op" {
            op_idx = Some(idx);
        }
    }

    let attr_name_idx = attr_name_idx.expect("attr_name column missing");
    let value_idx = value_idx.expect("value column missing");
    let op_idx = op_idx.expect("op column missing");

    let mut found_original = false;
    let mut found_modified = false;
    let rows_debug;

    if let Some(PayloadStrategy::RowsJson(row_list)) = &row_set.payload_strategy {
        rows_debug = format!("{:?}", row_list);
        for row in &row_list.iter {
            let vals = &row.values;

            let attr_name = match vals[attr_name_idx].kind.as_ref() {
                Some(prost_types::value::Kind::StringValue(s)) => Some(s.as_str()),
                _ => None,
            };

            let value = match vals[value_idx].kind.as_ref() {
                Some(prost_types::value::Kind::StringValue(s)) => Some(s.as_str()),
                _ => None,
            };

            let op = match vals[op_idx].kind.as_ref() {
                Some(prost_types::value::Kind::BoolValue(b)) => Some(*b),
                _ => None,
            };

            if attr_name == Some("name") {
                if value == Some("Bomba Centrifuga Original") && op == Some(true) {
                    found_original = true;
                }
                if value == Some("Bomba Centrifuga Modificada") && op == Some(true) {
                    found_modified = true;
                }
            }
        }
    } else {
        panic!("Expected RowsJson payload strategy");
    }

    assert!(
        found_original,
        "Should find history entry for original name. Filas recibidas: {:?}",
        rows_debug
    );
    assert!(
        found_modified,
        "Should find history entry for modified name"
    );
}

struct Utc;
impl Utc {
    fn now_timestamp() -> i64 {
        chrono::Utc::now().timestamp()
    }
}
