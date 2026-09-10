//! Tests for `grpc::service`.
use super::*;
use crate::grpc::handlers::query_support::*;
use crate::grpc::translator;
use serde_json::json;

#[tokio::test]
async fn test_map_chunk_to_response_field_level_security() {
    // Construct a sample chunk with 'password_hash' and 'username'
    let chunk = crate::janus::router::QueryChunk {
        query_key: "test_query".to_string(),
        success: true,
        body: json!({
            "columns": [
                {
                    "key": "username",
                    "label": "Username",
                    "type": "string",
                    "format": "",
                    "is_dimension": true,
                    "is_measure": false
                },
                {
                    "key": "password_hash",
                    "label": "Password Hash",
                    "type": "string",
                    "format": "",
                    "is_dimension": false,
                    "is_measure": false
                }
            ],
            "data": [
                {
                    "username": "john_doe",
                    "password_hash": "$2b$12$R9hKbEFzN5sS.."
                }
            ],
            "metadata": {
                "execution_time_ms": 10,
                "total_count": 1,
                "engine": "oltp",
                "is_semantic": false,
                "total_queries": 1,
                "parallelism_factor": 1.0,
                "cache_hits": 0,
                "query_id": "q-123",
                "cache_ttl_seconds": 0
            }
        }),
    };

    // 1. When is_system_bff is TRUE
    let resp_with_bff = map_chunk_to_response(chunk.clone(), true, None, "test-tenant").await;
    assert!(resp_with_bff.status.unwrap().success);
    let q1_res = resp_with_bff.batch_results.get("test_query").unwrap();
    let cols_with_bff = &q1_res.data.as_ref().unwrap().columns;
    // Should contain 2 columns: 'username' and 'password_hash'
    assert_eq!(cols_with_bff.len(), 2);
    assert_eq!(cols_with_bff[0].key, "username");
    assert_eq!(cols_with_bff[1].key, "password_hash");

    // The values list should also contain both the username and password_hash
    if let Some(crate::grpc::pb::row_set::PayloadStrategy::RowsJson(row_list)) =
        &q1_res.data.as_ref().unwrap().payload_strategy
    {
        assert_eq!(row_list.iter.len(), 1);
        let vals = &row_list.iter[0].values;
        assert_eq!(vals.len(), 2);
        // Verify username value
        if let Some(prost_types::value::Kind::StringValue(u)) = &vals[0].kind {
            assert_eq!(u, "john_doe");
        } else {
            panic!("Expected string value for username");
        }
        // Verify password_hash value
        if let Some(prost_types::value::Kind::StringValue(p)) = &vals[1].kind {
            assert_eq!(p, "$2b$12$R9hKbEFzN5sS..");
        } else {
            panic!("Expected string value for password_hash");
        }
    } else {
        panic!("Expected RowsJson payload strategy");
    }

    // 2. When is_system_bff is FALSE
    let resp_no_bff = map_chunk_to_response(chunk, false, None, "test-tenant").await;
    assert!(resp_no_bff.status.unwrap().success);
    let q1_res_no_bff = resp_no_bff.batch_results.get("test_query").unwrap();
    let cols_no_bff = &q1_res_no_bff.data.as_ref().unwrap().columns;
    // Should contain ONLY 'username' column
    assert_eq!(cols_no_bff.len(), 1);
    assert_eq!(cols_no_bff[0].key, "username");

    // The values list should contain ONLY 'username'
    if let Some(crate::grpc::pb::row_set::PayloadStrategy::RowsJson(row_list)) =
        &q1_res_no_bff.data.as_ref().unwrap().payload_strategy
    {
        assert_eq!(row_list.iter.len(), 1);
        let vals = &row_list.iter[0].values;
        assert_eq!(vals.len(), 1);
        // Verify username value
        if let Some(prost_types::value::Kind::StringValue(u)) = &vals[0].kind {
            assert_eq!(u, "john_doe");
        } else {
            panic!("Expected string value for username");
        }
    } else {
        panic!("Expected RowsJson payload strategy");
    }
}

#[tokio::test]
async fn test_group_cycle_prevention() {
    use crate::eav::reader::pull::{CacheEntry, EAV_CACHE};
    use crate::eav::types::datom::DatomValue;
    use std::collections::HashMap;
    use std::sync::Arc;

    // 1. Prepare EAV records maps for groups to create a path: A -> B -> C
    let mut group_a = HashMap::new();
    group_a.insert(
        "parent_user_group_id".to_string(),
        DatomValue::Str("group_b".to_string()),
    );

    let mut group_b = HashMap::new();
    group_b.insert(
        "parent_user_group_id".to_string(),
        DatomValue::Str("group_c".to_string()),
    );

    let group_c = HashMap::new();

    {
        let mut cache = EAV_CACHE.write().unwrap();
        cache.insert(
            "T#tnt_01#E#group_a".to_string(),
            CacheEntry {
                is_complete: true,
                map: group_a,
            },
        );
        cache.insert(
            "T#tnt_01#E#group_b".to_string(),
            CacheEntry {
                is_complete: true,
                map: group_b,
            },
        );
        cache.insert(
            "T#tnt_01#E#group_c".to_string(),
            CacheEntry {
                is_complete: true,
                map: group_c,
            },
        );
    }

    // 2. Instantiate MetriGrpcService
    let ddb_client =
        Arc::new(crate::infrastructure::dynamodb::DynamoClient::new("metri-eav-local").await);
    let query_exec = crate::eav::reader::query::EavQueryExecutor::new(
        Arc::clone(&ddb_client),
        "metri-eav-local",
    );
    let pull_read =
        crate::eav::reader::pull::EavReader::new(Arc::clone(&ddb_client), "metri-eav-local");
    let oltp_exec = crate::aegis::oltp::executor::OltpExecutor::new(query_exec, pull_read.clone());

    let eav_writer = crate::eav::writer::EavWriter::new(Arc::clone(&ddb_client), "metri-eav-local");
    let oltp_channel: Arc<dyn crate::janus_router::router::IWriteChannel> = Arc::new(
        crate::janus_router::oltp_channel::OltpChannel::new(eav_writer.clone()),
    );

    let mut channel_registry = std::collections::HashMap::new();
    channel_registry.insert(
        crate::codice::registry::EngineChannel::Oltp,
        Arc::clone(&oltp_channel),
    );
    let janus_router = Arc::new(crate::janus_router::router::JanusRouter::new(
        channel_registry,
    ));

    let audit_interceptor = Arc::new(
        crate::infrastructure::audit::interceptor::AuditInterceptorImpl::new(Arc::clone(
            &oltp_channel,
        )),
    );
    let valkey_store = Arc::new(crate::infrastructure::session_store::HmacTokenStore::new(
        "secret-key-development-metri-256-bits!!!"
            .to_string()
            .into_bytes(),
        Arc::clone(&ddb_client),
        "metri-eav-local".to_string(),
    ));
    let principal_cache = Arc::new(crate::cedar::InMemoryPrincipalCache::new(
        std::sync::Arc::new(crate::cedar::BroadcastBus::new(100)),
    ));

    let service = MetriGrpcService::new(crate::grpc::service::ServiceDeps {
        oltp_executor: oltp_exec,
        eav_writer,
        janus_router,
        audit_interceptor,
        athena_engine: None,
        moira_emitter: None,
        valkey_store,
        principal_cache,
        fault_notifier: Arc::new(crate::iop::sherlog::NoopFaultNotifier),
        olap_channel: Arc::clone(&oltp_channel),
        export_storage: None,
        dev_auth_bypass: crate::cedar::AuthenticationPolicy::DevBypass,
        invalidation_bus: std::sync::Arc::new(crate::cedar::BroadcastBus::new(100)),
    });

    // Scenario 1: A group cannot be its own parent
    {
        let payload = serde_json::json!({
            "id": "group_a",
            "parent_user_group_id": "group_a"
        });
        let req = crate::grpc::pb::TransactionRequest {
            tenant_id: "tnt_01".to_string(),
            entity_type: "user_group".to_string(),
            entity_id: "group_a".to_string(),
            action: 2, // UPDATE
            payload: Some(translator::value_to_struct(&payload)),
            suppress_events: false,
        };
        let res = service.transact(tonic::Request::new(req)).await;
        assert!(res.is_err(), "Expected error for self-parent group");
        let err = res.err().unwrap();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("cannot be its own parent"));
    }

    // Scenario 2: Cycle C -> A -> B -> C
    {
        let payload = serde_json::json!({
            "id": "group_c",
            "parent_user_group_id": "group_a"
        });
        let req = crate::grpc::pb::TransactionRequest {
            tenant_id: "tnt_01".to_string(),
            entity_type: "user_group".to_string(),
            entity_id: "group_c".to_string(),
            action: 2, // UPDATE
            payload: Some(translator::value_to_struct(&payload)),
            suppress_events: false,
        };
        let res = service.transact(tonic::Request::new(req)).await;
        assert!(res.is_err(), "Expected error for cyclic dependency");
        let err = res.err().unwrap();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("Circular dependency detected"));
    }
}

#[tokio::test]
async fn test_cache_invalidation_pubsub() {
    use crate::cedar::ports::InvalidationBus;
    use crate::cedar::{
        BroadcastBus, InMemoryPrincipalCache, InvalidationMsg, PrincipalCache, PrincipalData,
    };
    use crate::eav::reader::pull::{CacheEntry, EAV_CACHE};
    use std::collections::HashMap;

    // Populate EAV_CACHE
    let entry_key = "T#tnt_inval_01#E#usr_inval_001".to_string();
    {
        let mut cache = EAV_CACHE.write().unwrap();
        cache.insert(
            entry_key.clone(),
            CacheEntry {
                is_complete: true,
                map: HashMap::new(),
            },
        );
    }

    // Populate PrincipalCache — bus compartido entre publicador y caché
    let bus: std::sync::Arc<dyn InvalidationBus> = std::sync::Arc::new(BroadcastBus::new(100));
    let cache = InMemoryPrincipalCache::new(std::sync::Arc::clone(&bus));
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    let principal = PrincipalData {
        user_id: "usr_inval_001".to_string(),
        tenant_id: "tnt_inval_01".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: String::new(),
        roles: ["role_1".to_string()].into_iter().collect(),
        roles_boundaries: vec![],
        time_restrictions: vec![],
        group_allowed_locations: vec![],
        group_allowed_assets: vec![],
        groups: std::collections::HashSet::new(),
    };
    cache
        .store_principal("usr_inval_001", principal)
        .await
        .unwrap();

    // Assert they are present
    {
        let eav = EAV_CACHE.read().unwrap();
        assert!(eav.contains_key(&entry_key));
    }
    assert!(cache.lookup_principal("usr_inval_001").await.is_some());

    // Publish InvalidationMsg
    let msg = InvalidationMsg {
        tenant_id: "tnt_inval_01".to_string(),
        entity_type: "user".to_string(),
        entity_id: "usr_inval_001".to_string(),
    };
    bus.publish(msg);

    // Wait a bit for processing
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Assert they are evicted
    {
        let eav = EAV_CACHE.read().unwrap();
        assert!(!eav.contains_key(&entry_key));
    }
    assert!(cache.lookup_principal("usr_inval_001").await.is_none());
}

#[tokio::test]
async fn test_batch_transaction_granular_security() {
    use std::sync::Arc;

    // Instantiate MetriGrpcService
    let ddb_client =
        Arc::new(crate::infrastructure::dynamodb::DynamoClient::new("metri-eav-local").await);
    let query_exec = crate::eav::reader::query::EavQueryExecutor::new(
        Arc::clone(&ddb_client),
        "metri-eav-local",
    );
    let pull_read =
        crate::eav::reader::pull::EavReader::new(Arc::clone(&ddb_client), "metri-eav-local");
    let oltp_exec = crate::aegis::oltp::executor::OltpExecutor::new(query_exec, pull_read.clone());

    let eav_writer = crate::eav::writer::EavWriter::new(Arc::clone(&ddb_client), "metri-eav-local");
    let oltp_channel: Arc<dyn crate::janus_router::router::IWriteChannel> = Arc::new(
        crate::janus_router::oltp_channel::OltpChannel::new(eav_writer.clone()),
    );

    let mut channel_registry = std::collections::HashMap::new();
    channel_registry.insert(
        crate::codice::registry::EngineChannel::Oltp,
        Arc::clone(&oltp_channel),
    );
    let janus_router = Arc::new(crate::janus_router::router::JanusRouter::new(
        channel_registry,
    ));

    let audit_interceptor = Arc::new(
        crate::infrastructure::audit::interceptor::AuditInterceptorImpl::new(Arc::clone(
            &oltp_channel,
        )),
    );
    let valkey_store = Arc::new(crate::infrastructure::session_store::HmacTokenStore::new(
        "secret-key".to_string().into_bytes(),
        Arc::clone(&ddb_client),
        "metri-eav-local".to_string(),
    ));
    let principal_cache = Arc::new(crate::cedar::InMemoryPrincipalCache::new(
        std::sync::Arc::new(crate::cedar::BroadcastBus::new(100)),
    ));

    let service = MetriGrpcService::new(crate::grpc::service::ServiceDeps {
        oltp_executor: oltp_exec,
        eav_writer,
        janus_router,
        audit_interceptor,
        athena_engine: None,
        moira_emitter: None,
        valkey_store,
        principal_cache,
        fault_notifier: Arc::new(crate::iop::sherlog::NoopFaultNotifier),
        olap_channel: Arc::clone(&oltp_channel),
        export_storage: None,
        dev_auth_bypass: crate::cedar::AuthenticationPolicy::DevBypass,
        invalidation_bus: std::sync::Arc::new(crate::cedar::BroadcastBus::new(100)),
    });

    // Scenario: A transaction containing mixed valid (matching tenant) and invalid (mismatched tenant) operations
    // We set test-tenant = "tnt_01".
    // The transaction request is for tenant_id = "tnt_01".
    // One payload has entity_type = "tenant", id = "tnt_01" (valid, match principal's tenant).
    // Another payload has entity_type = "tenant", id = "tnt_02" (invalid, mismatches principal's tenant).
    // The granular security check should abort the entire batch atomically, returning Auth403.
    let payload = serde_json::json!([
        {
            "id": "tnt_01"
        },
        {
            "id": "tnt_02"
        }
    ]);

    let req = crate::grpc::pb::TransactionRequest {
        tenant_id: "tnt_01".to_string(),
        entity_type: "tenant".to_string(),
        entity_id: "".to_string(), // bulk/batch inside array payload
        action: 2,                 // UPDATE
        payload: Some(translator::value_to_struct(&payload)),
        suppress_events: false,
    };

    let mut grpc_req = tonic::Request::new(req);
    grpc_req
        .metadata_mut()
        .insert("test-tenant", "tnt_01".parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-user", "usr_001".parse().unwrap());

    let res = service.transact(grpc_req).await;
    assert!(
        res.is_err(),
        "Expected error for mixed transaction items due to tenant mismatch"
    );
    let err = res.err().unwrap();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert!(err
        .message()
        .contains("Auth403: Cannot mutate other tenant"));
}

#[tokio::test]
async fn test_invalid_role_grant_format() {
    use std::sync::Arc;

    let ddb_client =
        Arc::new(crate::infrastructure::dynamodb::DynamoClient::new("metri-eav-local").await);
    let query_exec = crate::eav::reader::query::EavQueryExecutor::new(
        Arc::clone(&ddb_client),
        "metri-eav-local",
    );
    let pull_read =
        crate::eav::reader::pull::EavReader::new(Arc::clone(&ddb_client), "metri-eav-local");
    let oltp_exec = crate::aegis::oltp::executor::OltpExecutor::new(query_exec, pull_read.clone());
    let eav_writer = crate::eav::writer::EavWriter::new(Arc::clone(&ddb_client), "metri-eav-local");
    let oltp_channel: Arc<dyn crate::janus_router::router::IWriteChannel> = Arc::new(
        crate::janus_router::oltp_channel::OltpChannel::new(eav_writer.clone()),
    );

    let mut channel_registry = std::collections::HashMap::new();
    channel_registry.insert(
        crate::codice::registry::EngineChannel::Oltp,
        Arc::clone(&oltp_channel),
    );
    let janus_router = Arc::new(crate::janus_router::router::JanusRouter::new(
        channel_registry,
    ));
    let audit_interceptor = Arc::new(
        crate::infrastructure::audit::interceptor::AuditInterceptorImpl::new(Arc::clone(
            &oltp_channel,
        )),
    );
    let valkey_store = Arc::new(crate::infrastructure::session_store::HmacTokenStore::new(
        "secret-key".to_string().into_bytes(),
        ddb_client,
        "table".to_string(),
    ));
    let principal_cache = Arc::new(crate::cedar::InMemoryPrincipalCache::new(
        std::sync::Arc::new(crate::cedar::BroadcastBus::new(100)),
    ));

    let service = MetriGrpcService::new(crate::grpc::service::ServiceDeps {
        oltp_executor: oltp_exec,
        eav_writer,
        janus_router,
        audit_interceptor,
        athena_engine: None,
        moira_emitter: None,
        valkey_store,
        principal_cache,
        fault_notifier: Arc::new(crate::iop::sherlog::NoopFaultNotifier),
        olap_channel: Arc::clone(&oltp_channel),
        export_storage: None,
        dev_auth_bypass: crate::cedar::AuthenticationPolicy::DevBypass,
        invalidation_bus: std::sync::Arc::new(crate::cedar::BroadcastBus::new(100)),
    });

    // Test invalid formats for grants in role payload
    let test_cases = vec![
        serde_json::json!({
            "id": "role_xyz",
            "grants": "domain:action\"invalid"
        }),
        serde_json::json!({
            "id": "role_xyz",
            "grants": ":action"
        }),
        serde_json::json!({
            "id": "role_xyz",
            "grants": "domain:"
        }),
        serde_json::json!({
            "id": "role_xyz",
            "grants": "domain/action"
        }),
    ];

    for payload in test_cases {
        let req = crate::grpc::pb::TransactionRequest {
            tenant_id: "tnt_01".to_string(),
            entity_type: "role".to_string(),
            entity_id: "role_xyz".to_string(),
            action: 1, // CREATE
            payload: Some(translator::value_to_struct(&payload)),
            suppress_events: false,
        };
        let mut grpc_req = tonic::Request::new(req);
        grpc_req
            .metadata_mut()
            .insert("test-tenant", "tnt_01".parse().unwrap());

        let res = service.transact(grpc_req).await;
        assert!(
            res.is_err(),
            "Expected error for invalid grant: {:?}",
            payload
        );
        let err = res.err().unwrap();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("Invalid grant format"));
    }

    // Test a valid grant format that should NOT fail on validation
    let valid_payload = serde_json::json!({
        "id": "role_xyz",
        "grants": "domain-name:action-name"
    });
    let req = crate::grpc::pb::TransactionRequest {
        tenant_id: "tnt_01".to_string(),
        entity_type: "role".to_string(),
        entity_id: "role_xyz".to_string(),
        action: 1, // CREATE
        payload: Some(translator::value_to_struct(&valid_payload)),
        suppress_events: false,
    };
    let mut grpc_req = tonic::Request::new(req);
    grpc_req
        .metadata_mut()
        .insert("test-tenant", "tnt_01".parse().unwrap());
    // Should compile and run, failing on EAV writing or event router since those are not mocked here,
    // but it should NOT return InvalidArgument "Invalid grant format".
    let res = service.transact(grpc_req).await;
    if let Err(err) = res {
        assert_ne!(err.code(), tonic::Code::InvalidArgument);
    }
}

#[tokio::test]
async fn test_tenant_and_quota_master_crud_gates() {
    use crate::cedar::PrincipalCache;
    use std::sync::Arc;

    std::env::set_var("HMAC_SECRET", "secret-key-development-metri-256-bits!!!");

    // Instantiate MetriGrpcService
    let ddb_client =
        Arc::new(crate::infrastructure::dynamodb::DynamoClient::new("metri-eav-local").await);
    let query_exec = crate::eav::reader::query::EavQueryExecutor::new(
        Arc::clone(&ddb_client),
        "metri-eav-local",
    );
    let pull_read =
        crate::eav::reader::pull::EavReader::new(Arc::clone(&ddb_client), "metri-eav-local");
    let oltp_exec = crate::aegis::oltp::executor::OltpExecutor::new(query_exec, pull_read.clone());
    let eav_writer = crate::eav::writer::EavWriter::new(Arc::clone(&ddb_client), "metri-eav-local");
    let oltp_channel: Arc<dyn crate::janus_router::router::IWriteChannel> = Arc::new(
        crate::janus_router::oltp_channel::OltpChannel::new(eav_writer.clone()),
    );

    let mut channel_registry = std::collections::HashMap::new();
    channel_registry.insert(
        crate::codice::registry::EngineChannel::Oltp,
        Arc::clone(&oltp_channel),
    );
    let janus_router = Arc::new(crate::janus_router::router::JanusRouter::new(
        channel_registry,
    ));
    let audit_interceptor = Arc::new(
        crate::infrastructure::audit::interceptor::AuditInterceptorImpl::new(Arc::clone(
            &oltp_channel,
        )),
    );
    let valkey_store = Arc::new(crate::infrastructure::session_store::HmacTokenStore::new(
        "secret-key-development-metri-256-bits!!!"
            .to_string()
            .into_bytes(),
        Arc::clone(&ddb_client),
        "metri-eav-local".to_string(),
    ));
    let principal_cache = Arc::new(crate::cedar::InMemoryPrincipalCache::new(
        std::sync::Arc::new(crate::cedar::BroadcastBus::new(100)),
    ));

    // Popular Principal Cache para usr_regular
    let regular_principal = crate::cedar::PrincipalData {
        user_id: "usr_regular".to_string(),
        tenant_id: "tnt_regular".to_string(),
        status: "ACTIVE".to_string(),
        user_type: "INTERNAL".to_string(),
        company_id: String::new(),
        roles: ["regular-role".to_string()].into_iter().collect(),
        roles_boundaries: vec![crate::cedar::RoleBoundary {
            role_id: "regular-role".to_string(),
            grants: vec![serde_json::json!({
                "domain": "*",
                "actions": ["VIEW", "CREATE", "UPDATE", "DELETE"],
                "scope": "ALL"
            })],
            permitted_locations: vec![],
            permitted_assets: vec![],
        }],
        time_restrictions: vec![],
        group_allowed_locations: vec![],
        group_allowed_assets: vec![],
        groups: std::collections::HashSet::new(),
    };
    principal_cache
        .store_principal("usr_regular", regular_principal)
        .await
        .unwrap();

    // Helper token generator
    fn generate_test_token(tenant_id: &str, user_id: &str) -> String {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;

        let now = chrono::Utc::now().timestamp();
        let claims = serde_json::json!({
            "exp": now + 3600,
            "iat": now,
            "jti": format!("test-{}", now),
            "tid": tenant_id,
            "uid": user_id,
        });
        let payload_bytes = serde_json::to_vec(&claims).unwrap();
        let payload_b64 = URL_SAFE_NO_PAD.encode(&payload_bytes);

        let hmac_secret = std::env::var("HMAC_SECRET")
            .unwrap_or_else(|_| "secret-key-development-metri-256-bits!!!".to_string());
        let mut mac = HmacSha256::new_from_slice(hmac_secret.as_bytes()).unwrap();
        mac.update(&payload_bytes);
        let sig_bytes = mac.finalize().into_bytes();
        let sig_b64 = URL_SAFE_NO_PAD.encode(sig_bytes);

        format!("Bearer mk_{}.{}", payload_b64, sig_b64)
    }

    let service = MetriGrpcService::new(crate::grpc::service::ServiceDeps {
        oltp_executor: oltp_exec,
        eav_writer,
        janus_router,
        audit_interceptor,
        athena_engine: None,
        moira_emitter: None,
        valkey_store,
        principal_cache,
        fault_notifier: Arc::new(crate::iop::sherlog::NoopFaultNotifier),
        olap_channel: Arc::clone(&oltp_channel),
        export_storage: None,
        dev_auth_bypass: crate::cedar::AuthenticationPolicy::DevBypass,
        invalidation_bus: std::sync::Arc::new(crate::cedar::BroadcastBus::new(100)),
    });

    // 1. Mutate 'tenant' as non-master user -> Expect PermissionDenied (Auth403)
    let payload = serde_json::json!({
        "id": "tnt_regular"
    });
    let req = crate::grpc::pb::TransactionRequest {
        tenant_id: "tnt_regular".to_string(),
        entity_type: "tenant".to_string(),
        entity_id: "tnt_regular".to_string(),
        action: 1, // CREATE
        payload: Some(translator::value_to_struct(&payload)),
        suppress_events: false,
    };
    let mut grpc_req = tonic::Request::new(req);
    grpc_req
        .metadata_mut()
        .insert("test-tenant", "tnt_regular".parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-user", "usr_regular".parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-roles", "regular-role".parse().unwrap());

    let res = service.transact(grpc_req).await;
    assert!(res.is_err());
    let err = res.err().unwrap();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    // F4: la fila `tenant` PROPIA ya no la bloquea el gate maestro sino el
    // GRANT de autoservicio (según la acción: CREATE) — mensaje accionable.
    assert!(err.message().contains("requiere el permiso tenant:CREATE"));

    // 2. Mutate 'domain_quota' as non-master user -> Expect PermissionDenied (Auth403)
    let payload = serde_json::json!({
        "id": "quota_regular"
    });
    let req = crate::grpc::pb::TransactionRequest {
        tenant_id: "tnt_regular".to_string(),
        entity_type: "domain_quota".to_string(),
        entity_id: "quota_regular".to_string(),
        action: 1, // CREATE
        payload: Some(translator::value_to_struct(&payload)),
        suppress_events: false,
    };
    let mut grpc_req = tonic::Request::new(req);
    grpc_req
        .metadata_mut()
        .insert("test-tenant", "tnt_regular".parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-user", "usr_regular".parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-roles", "regular-role".parse().unwrap());

    let res = service.transact(grpc_req).await;
    assert!(res.is_err());
    let err = res.err().unwrap();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert!(err
        .message()
        .contains("Only master tenant users can mutate domain_quota"));

    // 3.5 Autoservicio CON GRANT (F4): un tenant NO maestro muta SU PROPIA fila
    // 'tenant_plugin' — el gate maestro y el ABAC de dominio no aplican, pero
    // el usuario debe llevar el grant `tenant_plugin:UPDATE` en su rol. El
    // harness 'regular-role' no tiene grants: sin ellos → 403 ACCIONABLE.
    {
        let payload = serde_json::json!({
            "tenant_id": "tnt_regular",
            "plugin_id": "cmms",
            "status": "active",
            "config": "{}"
        });
        let req = crate::grpc::pb::TransactionRequest {
            tenant_id: "tnt_regular".to_string(),
            entity_type: "tenant_plugin".to_string(),
            entity_id: "tplug_self".to_string(),
            action: 2, // UPDATE
            payload: Some(translator::value_to_struct(&payload)),
            suppress_events: false,
        };
        let mut grpc_req = tonic::Request::new(req);
        grpc_req
            .metadata_mut()
            .insert("test-tenant", "tnt_regular".parse().unwrap());
        grpc_req
            .metadata_mut()
            .insert("test-user", "usr_regular".parse().unwrap());
        grpc_req
            .metadata_mut()
            .insert("test-roles", "regular-role".parse().unwrap());

        let res = service.transact(grpc_req).await;
        let err = res
            .err()
            .expect("sin grant el self-service debe rechazarse");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(
            err.message()
                .contains("requiere el permiso tenant_plugin:UPDATE"),
            "mensaje sin guía de acción: {}",
            err.message()
        );
    }

    // 3.5b Autoservicio de la fila `tenant` PROPIA sin grant → 403 accionable
    // (editar Cuenta Empresarial exige tenant:UPDATE).
    {
        let payload = serde_json::json!({
            "id": "tnt_regular",
            "name": "Regular"
        });
        let req = crate::grpc::pb::TransactionRequest {
            tenant_id: "tnt_regular".to_string(),
            entity_type: "tenant".to_string(),
            entity_id: "tnt_regular".to_string(),
            action: 2, // UPDATE
            payload: Some(translator::value_to_struct(&payload)),
            suppress_events: false,
        };
        let mut grpc_req = tonic::Request::new(req);
        grpc_req
            .metadata_mut()
            .insert("test-tenant", "tnt_regular".parse().unwrap());
        grpc_req
            .metadata_mut()
            .insert("test-user", "usr_regular".parse().unwrap());
        grpc_req
            .metadata_mut()
            .insert("test-roles", "regular-role".parse().unwrap());

        let res = service.transact(grpc_req).await;
        let err = res
            .err()
            .expect("sin grant la mutación de tenant propia debe rechazarse");
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(
            err.message().contains("requiere el permiso tenant:UPDATE"),
            "mensaje sin guía de acción: {}",
            err.message()
        );
    }

    // 3.5c Autoservicio como BFF de sistema → el gate y el grant no aplican
    // (puede fallar después por EAV, jamás por PermissionDenied del grant).
    {
        let payload = serde_json::json!({
            "tenant_id": "tnt_regular",
            "plugin_id": "cmms",
            "status": "active",
            "config": "{}"
        });
        let req = crate::grpc::pb::TransactionRequest {
            tenant_id: "tnt_regular".to_string(),
            entity_type: "tenant_plugin".to_string(),
            entity_id: "tplug_self_admin".to_string(),
            action: 2, // UPDATE
            payload: Some(translator::value_to_struct(&payload)),
            suppress_events: false,
        };
        let mut grpc_req = tonic::Request::new(req);
        grpc_req
            .metadata_mut()
            .insert("test-tenant", "tnt_regular".parse().unwrap());
        grpc_req
            .metadata_mut()
            .insert("test-user", "usr_system_bff".parse().unwrap());
        grpc_req
            .metadata_mut()
            .insert("test-roles", "admin".parse().unwrap());

        let res = service.transact(grpc_req).await;
        if let Err(err) = res {
            assert_ne!(
                err.code(),
                tonic::Code::PermissionDenied,
                "el BFF de sistema no debe bloquearse por grant en self-service: {}",
                err.message()
            );
        }
    }

    // 3.6 Lectura propia: un tenant NO maestro consulta SU 'tenant_plugin'
    // -> el gate de lectura NO debe bloquearlo (el panel de módulos carga así
    // su configuración guardada). El acceso cruzado ni siquiera existe a nivel
    // de handler: para no-maestros `req.tenant_id` se reescribe con el del
    // principal (transact_impl/query_impl).
    {
        let q_req = crate::grpc::pb::QueryRequest {
            tenant_id: "tnt_regular".to_string(),
            queries: {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "q1".to_string(),
                    crate::grpc::pb::AnalyticsRequest {
                        tenant_id: "tnt_regular".to_string(),
                        entity: "tenant_plugin".to_string(),
                        ..Default::default()
                    },
                );
                m
            },
            ..Default::default()
        };
        let mut grpc_req = tonic::Request::new(q_req);
        grpc_req
            .metadata_mut()
            .insert("test-tenant", "tnt_regular".parse().unwrap());
        grpc_req
            .metadata_mut()
            .insert("test-user", "usr_regular".parse().unwrap());
        grpc_req
            .metadata_mut()
            .insert("test-roles", "regular-role".parse().unwrap());
        let token = generate_test_token("tnt_regular", "usr_regular");
        grpc_req
            .metadata_mut()
            .insert("authorization", token.parse().unwrap());
        grpc_req
            .extensions_mut()
            .insert(crate::grpc::interceptors::AuthenticatedSession {
                tenant_id: "tnt_regular".to_string(),
                user_id: "usr_regular".to_string(),
                jti: "test-jti-self".to_string(),
            });

        let res = service.query(grpc_req).await;
        if let Err(err) = &res {
            assert_ne!(
                err.code(),
                tonic::Code::PermissionDenied,
                "leer la fila tenant_plugin propia no debe bloquearse por el gate: {}",
                err
            );
        }
    }

    // 3. Mutate 'tenant' as master user -> Expect success (or at least EAV error, not gate block)
    let payload = serde_json::json!({
        "id": "tnt_regular"
    });
    let req = crate::grpc::pb::TransactionRequest {
        tenant_id: "tnt_regular".to_string(),
        entity_type: "tenant".to_string(),
        entity_id: "tnt_regular".to_string(),
        action: 1, // CREATE
        payload: Some(translator::value_to_struct(&payload)),
        suppress_events: false,
    };
    let mut grpc_req = tonic::Request::new(req);
    grpc_req
        .metadata_mut()
        .insert("test-tenant", "system".parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-user", "usr_master".parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-roles", "role_super_master".parse().unwrap());

    let res = service.transact(grpc_req).await;
    if let Err(err) = &res {
        // Gate is passed, it should not fail with PermissionDenied "Only master tenant users can mutate"
        assert_ne!(
            err.code(),
            tonic::Code::PermissionDenied,
            "Gate should let master user pass: {:?}",
            err
        );
    }

    // 4. Query 'tenant' as non-master user -> Expect PermissionDenied
    let q_req = crate::grpc::pb::QueryRequest {
        tenant_id: "tnt_regular".to_string(),
        queries: {
            let mut m = std::collections::HashMap::new();
            m.insert(
                "q1".to_string(),
                crate::grpc::pb::AnalyticsRequest {
                    tenant_id: "tnt_regular".to_string(),
                    entity: "tenant".to_string(),
                    ..Default::default()
                },
            );
            m
        },
        ..Default::default()
    };
    let mut grpc_req = tonic::Request::new(q_req);
    grpc_req
        .metadata_mut()
        .insert("test-tenant", "tnt_regular".parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-user", "usr_regular".parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-roles", "regular-role".parse().unwrap());
    let token = generate_test_token("tnt_regular", "usr_regular");
    grpc_req
        .metadata_mut()
        .insert("authorization", token.parse().unwrap());
    grpc_req
        .extensions_mut()
        .insert(crate::grpc::interceptors::AuthenticatedSession {
            tenant_id: "tnt_regular".to_string(),
            user_id: "usr_regular".to_string(),
            jti: "test-jti".to_string(),
        });

    let res = service.query(grpc_req).await;
    assert!(res.is_err());
    let err = res.err().unwrap();
    println!(
        "DEBUG QUERY ERROR: code={:?}, message={}",
        err.code(),
        err.message()
    );
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert!(err
        .message()
        .contains("Only master tenant users can read tenants or quotas"));

    // 5. Explore 'domain_quota' as non-master user -> Expect PermissionDenied
    let exp_req = crate::grpc::pb::ExploreRequest {
        tenant_id: "tnt_regular".to_string(),
        entity: "domain_quota".to_string(),
        attribute: "id".to_string(),
        limit: 10,
    };
    let mut grpc_req = tonic::Request::new(exp_req);
    grpc_req
        .metadata_mut()
        .insert("test-tenant", "tnt_regular".parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-user", "usr_regular".parse().unwrap());
    grpc_req
        .metadata_mut()
        .insert("test-roles", "regular-role".parse().unwrap());
    let token = generate_test_token("tnt_regular", "usr_regular");
    grpc_req
        .metadata_mut()
        .insert("authorization", token.parse().unwrap());
    grpc_req
        .extensions_mut()
        .insert(crate::grpc::interceptors::AuthenticatedSession {
            tenant_id: "tnt_regular".to_string(),
            user_id: "usr_regular".to_string(),
            jti: "test-jti".to_string(),
        });

    let res = service.explore(grpc_req).await;
    assert!(res.is_err());
    let err = res.err().unwrap();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert!(err
        .message()
        .contains("Only master tenant users can explore"));
}

#[tokio::test]
async fn test_map_chunk_to_response_csv_export_s3() {
    use crate::domain::protocols::IExportStorage;
    use crate::infrastructure::s3_export::StubExportStorage;
    use std::sync::Arc;

    let columns = json!([
        {
            "key": "id",
            "label": "ID",
            "type": "string",
            "format": "",
            "is_dimension": true,
            "is_measure": false
        }
    ]);

    // Create 5001 rows
    let mut data = Vec::new();
    for i in 0..5001 {
        data.push(json!({
            "id": format!("id_{}", i)
        }));
    }

    let chunk = crate::janus::router::QueryChunk {
        query_key: "export_query".to_string(),
        success: true,
        body: json!({
            "columns": columns,
            "data": data,
            "output_cast": "CSV_EXPORT",
            "metadata": {
                "execution_time_ms": 10,
                "total_count": 5001,
                "engine": "olap",
                "is_semantic": false,
                "total_queries": 1,
                "parallelism_factor": 1.0,
                "cache_hits": 0,
                "query_id": "q-export",
                "cache_ttl_seconds": 0
            }
        }),
    };

    let export_storage: Arc<dyn IExportStorage> = Arc::new(StubExportStorage::new());

    // 1. Scenario: > 5000 rows (S3 export triggered)
    let resp =
        map_chunk_to_response(chunk.clone(), false, Some(&export_storage), "tnt_export").await;
    assert!(resp.status.unwrap().success);
    let q_res = resp.batch_results.get("export_query").unwrap();

    if let Some(crate::grpc::pb::row_set::PayloadStrategy::PresignedCsvUrl(url)) =
        &q_res.data.as_ref().unwrap().payload_strategy
    {
        assert!(url.contains("metri-mock-exports.s3.amazonaws.com/exports/mock_file_"));
    } else {
        panic!("Expected PresignedCsvUrl payload strategy for > 5000 rows");
    }

    // 2. Scenario: <= 5000 rows (no S3 export, fallback to RowsJson)
    let mut small_body = chunk.body.clone();
    if let Some(arr) = small_body.get_mut("data").and_then(|v| v.as_array_mut()) {
        arr.truncate(100); // 100 rows
    }
    let small_chunk = crate::janus::router::QueryChunk {
        query_key: "export_query".to_string(),
        success: true,
        body: small_body,
    };

    let resp_small =
        map_chunk_to_response(small_chunk, false, Some(&export_storage), "tnt_export").await;
    assert!(resp_small.status.unwrap().success);
    let q_res_small = resp_small.batch_results.get("export_query").unwrap();

    if let Some(crate::grpc::pb::row_set::PayloadStrategy::RowsJson(rows)) =
        &q_res_small.data.as_ref().unwrap().payload_strategy
    {
        assert_eq!(rows.iter.len(), 100);
    } else {
        panic!("Expected RowsJson payload strategy for <= 5000 rows");
    }
}

// ── ListEntities ─────────────────────────────────────────────────────────────

fn list_test_registry() -> crate::codice::registry::CodeRegistry {
    let dir = std::path::Path::new("config/models");
    let (registry, _rules) =
        crate::codice::CodeRegistry::build(dir).expect("config/models debe compilar");
    registry
}

#[test]
fn list_filters_acepta_status_aunque_no_declare_index() {
    // El caso que motiva validar por TIPO y no por el flag `indexed`:
    // scheduled_job.status sólo declara is_dimension, pero SÍ está en el índice AVET
    // porque el camino de escritura decide por ValueType. Validar por el flag
    // rechazaría justo la consulta que necesita el reconciliador.
    let reg = list_test_registry();
    let model = reg
        .get_model("scheduled_job")
        .expect("scheduled_job en el Códice");
    assert!(
        crate::grpc::handlers::list_support::validate_list_filters(model, &["status"]).is_ok(),
        "status debe aceptarse como filtro"
    );
}

#[test]
fn list_filters_rechaza_atributo_inexistente() {
    let reg = list_test_registry();
    let model = reg.get_model("scheduled_job").unwrap();
    let err = crate::grpc::handlers::list_support::validate_list_filters(model, &["no_existe"]);
    assert!(err.is_err(), "un atributo inexistente debe rechazarse");
    assert!(err.unwrap_err().detail.contains("no_existe"));
}

#[test]
fn list_filters_rechaza_tipos_no_indexables() {
    // Un filtro que el índice AVET no puede resolver degeneraría en un escaneo
    // encubierto: mejor rechazarlo que servirlo caro y en silencio.
    let reg = list_test_registry();
    let model = reg.get_model("scheduled_job").unwrap();
    let arrays: Vec<&str> = model
        .attributes
        .iter()
        .filter(|a| matches!(a.attr_type, crate::codice::registry::AttrType::Array))
        .map(|a| a.name.as_str())
        .collect();
    for name in arrays {
        assert!(
            crate::grpc::handlers::list_support::validate_list_filters(model, &[name]).is_err(),
            "'{}' es Array y no debe admitirse como filtro",
            name
        );
    }
}

#[test]
fn list_filters_acepta_varios_a_la_vez() {
    let reg = list_test_registry();
    let model = reg.get_model("scheduled_job").unwrap();
    assert!(crate::grpc::handlers::list_support::validate_list_filters(
        model,
        &["status", "trigger_type"]
    )
    .is_ok());
}

#[test]
fn sort_and_truncate_marca_lo_que_deja_fuera() {
    // Sin la bandera, un resultado recortado es indistinguible de uno completo, y un
    // consumidor que reconcilie estado borraría lo que corresponde a los ids ausentes.
    let mut ids = vec!["c".into(), "a".into(), "b".into()];
    let truncated = crate::grpc::handlers::list_support::sort_and_truncate(&mut ids, 2);
    assert!(truncated, "3 ids con limit 2 deben marcarse truncados");
    assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn sort_and_truncate_no_marca_si_cabe_entero() {
    let mut ids = vec!["b".into(), "a".into()];
    assert!(!crate::grpc::handlers::list_support::sort_and_truncate(
        &mut ids, 5
    ));
    assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
}

#[test]
fn sort_and_truncate_es_determinista() {
    // El ejecutor devuelve el resultado de un HashSet, cuyo orden no es determinista.
    // Sin ordenar, dos llamadas idénticas recortarían conjuntos DISTINTOS y el
    // consumidor actuaría sobre una foto arbitraria.
    let mut a = vec!["z".into(), "m".into(), "a".into(), "k".into()];
    let mut b = vec!["k".into(), "a".into(), "z".into(), "m".into()];
    crate::grpc::handlers::list_support::sort_and_truncate(&mut a, 2);
    crate::grpc::handlers::list_support::sort_and_truncate(&mut b, 2);
    assert_eq!(
        a, b,
        "el mismo conjunto en otro orden debe recortarse igual"
    );
}

// ── CompositeTransact — instanciación atómica (Fase 9) ──────────────────────

/// Servicio con DevBypass, el mismo armado de los tests de granular security.
async fn servicio_composite() -> MetriGrpcService {
    use std::sync::Arc;
    let ddb_client =
        Arc::new(crate::infrastructure::dynamodb::DynamoClient::new("metri-eav-local").await);
    let query_exec = crate::eav::reader::query::EavQueryExecutor::new(
        Arc::clone(&ddb_client),
        "metri-eav-local",
    );
    let pull_read =
        crate::eav::reader::pull::EavReader::new(Arc::clone(&ddb_client), "metri-eav-local");
    let oltp_exec = crate::aegis::oltp::executor::OltpExecutor::new(query_exec, pull_read.clone());
    let eav_writer =
        crate::eav::writer::EavWriter::new(Arc::clone(&ddb_client), "metri-eav-local");
    let oltp_channel: Arc<dyn crate::janus_router::router::IWriteChannel> = Arc::new(
        crate::janus_router::oltp_channel::OltpChannel::new(eav_writer.clone()),
    );
    let mut channel_registry = std::collections::HashMap::new();
    channel_registry.insert(
        crate::codice::registry::EngineChannel::Oltp,
        Arc::clone(&oltp_channel),
    );
    let janus_router =
        Arc::new(crate::janus_router::router::JanusRouter::new(channel_registry));
    let audit_interceptor = Arc::new(
        crate::infrastructure::audit::interceptor::AuditInterceptorImpl::new(Arc::clone(
            &oltp_channel,
        )),
    );
    let valkey_store = Arc::new(crate::infrastructure::session_store::HmacTokenStore::new(
        "secret-key-development-metri-256-bits!!!".to_string().into_bytes(),
        Arc::clone(&ddb_client),
        "metri-eav-local".to_string(),
    ));
    let principal_cache = Arc::new(crate::cedar::InMemoryPrincipalCache::new(
        std::sync::Arc::new(crate::cedar::BroadcastBus::new(100)),
    ));
    let olap_channel = Arc::clone(&oltp_channel);
    MetriGrpcService::new(crate::grpc::service::ServiceDeps {
        oltp_executor: oltp_exec,
        eav_writer,
        janus_router,
        audit_interceptor,
        athena_engine: None,
        moira_emitter: None,
        valkey_store,
        principal_cache,
        fault_notifier: Arc::new(crate::iop::sherlog::NoopFaultNotifier),
        olap_channel,
        export_storage: None,
        dev_auth_bypass: crate::cedar::AuthenticationPolicy::DevBypass,
        invalidation_bus: std::sync::Arc::new(crate::cedar::BroadcastBus::new(100)),
    })
}

fn peticion_compuesta(
    entities: Vec<crate::grpc::pb::TransactionRequest>,
) -> tonic::Request<crate::grpc::pb::CompositeTransactRequest> {
    let mut grpc_req = tonic::Request::new(crate::grpc::pb::CompositeTransactRequest {
        tenant_id: "tnt_01".to_string(),
        entities,
        suppress_events: true,
    });
    grpc_req.metadata_mut().insert("test-tenant", "tnt_01".parse().unwrap());
    grpc_req.metadata_mut().insert("test-user", "usr_001".parse().unwrap());
    grpc_req
}

fn entidad_create(
    entity_type: &str,
    entity_id: &str,
    payload: serde_json::Value,
) -> crate::grpc::pb::TransactionRequest {
    crate::grpc::pb::TransactionRequest {
        tenant_id: "tnt_01".to_string(),
        entity_type: entity_type.to_string(),
        entity_id: entity_id.to_string(),
        action: 1, // CREATE
        payload: Some(translator::value_to_struct(&payload)),
        suppress_events: true,
    }
}

#[tokio::test]
async fn composite_sin_entidades_es_invalido() {
    let service = servicio_composite().await;
    let res = service.composite_transact(peticion_compuesta(vec![])).await;
    let err = res.err().expect("vacío debe ser rechazado");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn composite_rechaza_operaciones_que_no_son_create() {
    let service = servicio_composite().await;
    let mut e = entidad_create("work_order_procedure", "01A", json!({}));
    e.action = 2; // UPDATE
    let err = service
        .composite_transact(peticion_compuesta(vec![e]))
        .await
        .err()
        .expect("UPDATE no entra por composite");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("solo soporta CREATE"));
}

#[tokio::test]
async fn composite_exige_ids_preasignados() {
    let service = servicio_composite().await;
    let mut e = entidad_create(
        "work_order_procedure",
        "",
        json!({ "work_order_id": "01WO", "procedure_id": "01PROC" }),
    );
    e.entity_id = String::new();
    let err = service
        .composite_transact(peticion_compuesta(vec![e]))
        .await
        .err()
        .expect("sin id preasignado debe ser rechazado");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("entity_id preasignado"));
}

#[tokio::test]
async fn composite_rechaza_entidad_fuera_del_catalogo() {
    let service = servicio_composite().await;
    let err = service
        .composite_transact(peticion_compuesta(vec![entidad_create(
            "fantasma_que_no_existe",
            "01A",
            json!({ "x": 1 }),
        )]))
        .await
        .err()
        .expect("entidad fuera del Códice debe ser rechazada");
    assert!(err.message().contains("no en registry"), "{}", err.message());
}

#[tokio::test]
async fn composite_valida_la_estructura_antes_de_escribir() {
    // Inicializar el Códice global: el composite valida contra el catálogo real.
    static CODICE: std::sync::Once = std::sync::Once::new();
    CODICE.call_once(|| {
        if crate::codice::registry::global_opt().is_none() {
            let models = std::path::Path::new("config/models");
            let (registry, _) =
                crate::codice::CodeRegistry::build(models).expect("registry de config/models");
            crate::codice::init_global(registry);
        }
    });

    let service = servicio_composite().await;
    // work_order_procedure sin work_order_id (required): debe fallar en
    // planificación y no tocar la base.
    let err = service
        .composite_transact(peticion_compuesta(vec![entidad_create(
            "work_order_procedure",
            "01A",
            json!({ "name": "huerfana" }),
        )]))
        .await
        .err()
        .expect("violación estructural debe rechazar el composite");
    assert!(
        err.message().contains("work_order_id"),
        "el error debe nombrar el campo faltante: {}",
        err.message()
    );
}

/// Puerta 9 (Fase 9): raíz + hijos en una sola transacción; el reintento del
/// mismo composite lo rechaza la constraint unique y no duplica nada.
/// Exige DynamoDB Local: `make test-integration`.
#[tokio::test]
#[ignore]
async fn composite_instancia_procedure_y_campos_atomicamente() {
    use std::sync::Arc;
    static CODICE: std::sync::Once = std::sync::Once::new();
    CODICE.call_once(|| {
        std::env::set_var("DYNAMODB_ENDPOINT", "http://localhost:8000");
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_REGION", "us-east-1");
        if crate::codice::registry::global_opt().is_none() {
            let models = std::path::Path::new("config/models");
            let (registry, _) =
                crate::codice::CodeRegistry::build(models).expect("registry");
            crate::codice::init_global(registry);
        }
    });

    let service = servicio_composite().await;
    let tenant = format!("tnt_ctest_{}", ulid::Ulid::new());

    // Semilla: procedure PUBLISHED (requisito del ref_state).
    let proc_id = format!("01PROC{}", &ulid::Ulid::new().to_string()[..8]);
    let semilla = crate::grpc::pb::TransactionRequest {
        tenant_id: tenant.clone(),
        entity_type: "procedure_template".to_string(),
        entity_id: proc_id.clone(),
        action: 1,
        payload: Some(translator::value_to_struct(&json!({
            "name": "Inspección de planta",
            "lifecycle_state": "PUBLISHED",
            "max_score": 100
        }))),
        suppress_events: true,
    };
    let mut req_semilla = tonic::Request::new(semilla);
    req_semilla.metadata_mut().insert("test-tenant", tenant.clone().parse().unwrap());
    req_semilla.metadata_mut().insert("test-user", "usr_001".parse().unwrap());
    service
        .transact(req_semilla)
        .await
        .expect("la semilla procedure debe crear");

    // Composite: WOP + 2 campos, ids preasignados.
    let wop_id = format!("01WOP{}", &ulid::Ulid::new().to_string()[..8]);
    let f1 = format!("01F{}", &ulid::Ulid::new().to_string()[..10]);
    let f2 = format!("01F{}", &ulid::Ulid::new().to_string()[..10]);
    let entities = vec![
        entidad_create(
            "work_order_procedure",
            &wop_id,
            json!({
                "work_order_id": "01WO",
                "procedure_template_id": proc_id,
                "name": "Inspección de planta",
                "procedure_order": 1,
                "status": "PENDING"
            }),
        ),
        entidad_create(
            "work_order_procedure_field",
            &f1,
            json!({
                "work_order_procedure_id": wop_id,
                "label": "Sin fugas",
                "field_type": "INSPECTION_CHECK",
                "field_order": 1
            }),
        ),
        entidad_create(
            "work_order_procedure_field",
            &f2,
            json!({
                "work_order_procedure_id": wop_id,
                "label": "Voltaje",
                "field_type": "NUMBER",
                "field_order": 2
            }),
        ),
    ];

    let res = service
        .composite_transact(peticion_compuesta(entities))
        .await
        .expect("el composite debe escribir las 3 entidades");
    let resp = res.into_inner();
    assert!(resp.status.as_ref().unwrap().success);
    assert_eq!(resp.results.len(), 3);
    assert_eq!(resp.results[0].entity_id, wop_id);
    assert_eq!(resp.results[1].entity_id, f1);
    assert_eq!(resp.results[2].entity_id, f2);

    // Puerta 9: el reintento del MISMO composite choca contra la constraint
    // unique(work_order_id, procedure_id) — la transacción entera se aborta.
    let entities_duplicado = vec![
        entidad_create(
            "work_order_procedure",
            &wop_id,
            json!({
                "work_order_id": "01WO",
                "procedure_template_id": proc_id,
                "name": "Inspección de planta",
                "procedure_order": 1,
                "status": "PENDING"
            }),
        ),
        entidad_create(
            "work_order_procedure_field",
            &format!("01F{}", &ulid::Ulid::new().to_string()[..10]),
            json!({
                "work_order_procedure_id": wop_id,
                "label": "Otro campo",
                "field_type": "TEXT",
                "field_order": 3
            }),
        ),
    ];
    // UPDATE sobre la misma entidad: la reclamación es idempotente para el
    // dueño, así que el reintento CREATE falla por la condición
    // attribute_not_exists del claim — y NO escribe el hijo.
    let reintento = service.composite_transact(peticion_compuesta(entities_duplicado)).await;
    assert!(reintento.is_err(), "el duplicado debe abortar");
}
