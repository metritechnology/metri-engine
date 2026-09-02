use super::*;
use crate::domain::protocols::SqsMessage;
use crate::eav::writer::TransactResult;
use std::sync::Mutex;

struct MockPullReader {
    // maps entity_id -> map of attribute -> DatomValue
    store: Mutex<HashMap<String, HashMap<String, DatomValue>>>,
}

#[async_trait]
impl EavPullReader for Arc<MockPullReader> {
    async fn pull(
        &self,
        _tenant_id: &str,
        entity_id: &str,
        _attributes: Option<&[&str]>,
    ) -> Result<HashMap<String, DatomValue>, DomainError> {
        let store = self.store.lock().unwrap();
        if let Some(event) = store.get(entity_id) {
            Ok(event.clone())
        } else {
            Ok(HashMap::new())
        }
    }
}

struct MockTransacter {
    transacts: Mutex<Vec<TransactPayload>>,
    store: Arc<MockPullReader>,
}

#[async_trait]
impl EavTransacter for MockTransacter {
    async fn transact(&self, payload: TransactPayload) -> Result<TransactResult, DomainError> {
        self.transacts.lock().unwrap().push(payload.clone());
        if let Some(entity_id) = &payload.entity_id {
            let mut store = self.store.store.lock().unwrap();
            let entry = store.entry(entity_id.clone()).or_insert_with(HashMap::new);
            for (k, v) in payload.attrs {
                entry.insert(k, v);
            }
        }
        Ok(TransactResult {
            entity_id: payload.entity_id.unwrap_or_default(),
            tx_id: 1,
            datoms: 1,
            outbox_count: 0,
        })
    }
}

struct MockOltpExecutor {
    rows: Mutex<Vec<Value>>,
}

#[async_trait]
impl OltpQueryRunner for MockOltpExecutor {
    async fn run_oltp_query(&self, _tenant_id: &str, ast_ir: &Value) -> Result<Value, DomainError> {
        let r = self.rows.lock().unwrap().clone();
        let status = ast_ir
            .get("where")
            .and_then(|w| w.get(1))
            .and_then(|cond| cond.get(2))
            .and_then(|v| v.as_str())
            .unwrap_or("PENDING");

        let filtered: Vec<Value> = r
            .into_iter()
            .filter(|row| {
                let row_status = row
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("PENDING");
                row_status == status
            })
            .collect();

        Ok(Value::Array(filtered))
    }
}

#[tokio::test]
async fn test_moira_emit_no_pending_events() {
    let pull = Arc::new(MockPullReader {
        store: Mutex::new(HashMap::new()),
    });
    let transacter = MockTransacter {
        transacts: Mutex::new(vec![]),
        store: pull.clone(),
    };
    let oltp = MockOltpExecutor {
        rows: Mutex::new(vec![]),
    };
    let sqs = Arc::new(crate::infrastructure::sqs::StubSqsBus::new());

    let emitter = MoiraEmitterImpl::new(pull, transacter, sqs.clone(), oltp);

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "outbox_event",
        "CREATE",
        serde_json::Map::new(),
    );
    let res = emitter.emit(ctx).await;

    assert!(res.is_ok());
    assert!(sqs.messages.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_moira_emit_success_workflow() {
    let mut event_store = HashMap::new();
    let mut attrs = HashMap::new();
    attrs.insert("id".to_string(), DatomValue::Str("evt_01".to_string()));
    attrs.insert("status".to_string(), DatomValue::Str("PENDING".to_string()));
    attrs.insert(
        "detail_type".to_string(),
        DatomValue::Str("test_event".to_string()),
    );
    event_store.insert("evt_01".to_string(), attrs);

    let pull = Arc::new(MockPullReader {
        store: Mutex::new(event_store),
    });
    let transacter = MockTransacter {
        transacts: Mutex::new(vec![]),
        store: pull.clone(),
    };

    let pending_list = vec![json!({
        "id": "evt_01",
        "status": "PENDING",
        "detail_type": "test_event",
        "created_at": 1000
    })];
    let oltp = MockOltpExecutor {
        rows: Mutex::new(pending_list),
    };
    let sqs = Arc::new(crate::infrastructure::sqs::StubSqsBus::new());

    let emitter = MoiraEmitterImpl::new(pull.clone(), transacter, sqs.clone(), oltp);

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "outbox_event",
        "CREATE",
        serde_json::Map::new(),
    );
    let res = emitter.emit(ctx).await;

    assert!(res.is_ok());

    // SQS should have 1 message
    let msgs = sqs.messages.lock().unwrap();
    assert_eq!(msgs.len(), 1);
    assert!(msgs[0].body.contains("evt_01"));

    // Pull should now have status = DELIVERED
    let final_event = pull.store.lock().unwrap().get("evt_01").unwrap().clone();
    if let Some(DatomValue::Str(status)) = final_event.get("status") {
        assert_eq!(status, "DELIVERED");
    } else {
        panic!("Expected status to be DELIVERED");
    }
}

#[tokio::test]
async fn test_moira_emit_fail_and_backoff() {
    let mut event_store = HashMap::new();
    let mut attrs = HashMap::new();
    attrs.insert("id".to_string(), DatomValue::Str("evt_01".to_string()));
    attrs.insert("status".to_string(), DatomValue::Str("PENDING".to_string()));
    attrs.insert("retry_count".to_string(), DatomValue::Long(0));
    attrs.insert(
        "detail_type".to_string(),
        DatomValue::Str("test_event".to_string()),
    );
    event_store.insert("evt_01".to_string(), attrs);

    let pull = Arc::new(MockPullReader {
        store: Mutex::new(event_store),
    });
    let transacter = MockTransacter {
        transacts: Mutex::new(vec![]),
        store: pull.clone(),
    };

    let pending_list = vec![json!({
        "id": "evt_01",
        "status": "PENDING",
        "detail_type": "test_event",
        "created_at": 1000
    })];
    let oltp = MockOltpExecutor {
        rows: Mutex::new(pending_list),
    };

    // A failing SQS bus that returns error
    struct FailSqsBus;
    #[async_trait]
    impl ISqsBus for FailSqsBus {
        async fn publish(&self, _p: &str, _g: &str, _d: &str) -> Result<String, DomainError> {
            Err(DomainError::infra(
                ErrorCode::Infra003,
                "SQS simulated fail".to_string(),
            ))
        }
        async fn receive_messages(&self, _m: u32) -> Result<Vec<SqsMessage>, DomainError> {
            Ok(vec![])
        }
        async fn delete_message(&self, _r: &str) -> Result<(), DomainError> {
            Ok(())
        }
    }
    let sqs = Arc::new(FailSqsBus);

    let emitter = MoiraEmitterImpl::new(pull.clone(), transacter, sqs, oltp);

    let ctx = IopContext::new(
        "tnt_01",
        "usr_01",
        "outbox_event",
        "CREATE",
        serde_json::Map::new(),
    );
    let res = emitter.emit(ctx).await;

    assert!(res.is_ok());

    // Event should be marked as FAILED with retry_count = 1
    let final_event = pull.store.lock().unwrap().get("evt_01").unwrap().clone();
    if let Some(DatomValue::Str(status)) = final_event.get("status") {
        assert_eq!(status, "FAILED");
    } else {
        panic!("Expected status to be FAILED");
    }
    if let Some(DatomValue::Long(retries)) = final_event.get("retry_count") {
        assert_eq!(*retries, 1);
    } else {
        panic!("Expected retry_count to be 1");
    }
}

#[tokio::test]
async fn test_moira_watchdog_resets_orphans() {
    let mut event_store = HashMap::new();

    let mut attrs1 = HashMap::new();
    attrs1.insert("id".to_string(), DatomValue::Str("evt_stuck".to_string()));
    attrs1.insert(
        "status".to_string(),
        DatomValue::Str("PROCESSING".to_string()),
    );
    attrs1.insert("claimed_at".to_string(), DatomValue::Instant(100)); // Very old
    event_store.insert("evt_stuck".to_string(), attrs1);

    let mut attrs2 = HashMap::new();
    attrs2.insert("id".to_string(), DatomValue::Str("evt_fresh".to_string()));
    attrs2.insert(
        "status".to_string(),
        DatomValue::Str("PROCESSING".to_string()),
    );
    attrs2.insert(
        "claimed_at".to_string(),
        DatomValue::Instant(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        ),
    ); // Now
    event_store.insert("evt_fresh".to_string(), attrs2);

    let pull = Arc::new(MockPullReader {
        store: Mutex::new(event_store),
    });
    let transacter = MockTransacter {
        transacts: Mutex::new(vec![]),
        store: pull.clone(),
    };

    // Oltp returns both candidates
    let candidates = vec![
        json!({
            "id": "evt_stuck",
            "status": "PROCESSING",
            "claimed_at": 100
        }),
        json!({
            "id": "evt_fresh",
            "status": "PROCESSING",
            "claimed_at": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
        }),
    ];
    let oltp = MockOltpExecutor {
        rows: Mutex::new(candidates),
    };
    let sqs = Arc::new(crate::infrastructure::sqs::StubSqsBus::new());

    let emitter = MoiraEmitterImpl::new(pull.clone(), transacter, sqs, oltp);

    let reset_count = emitter
        .reset_orphaned_processing("tnt_01", 600_000)
        .await
        .unwrap();

    assert_eq!(reset_count, 1);

    let store = pull.store.lock().unwrap();
    // stuck event is now PENDING
    let stuck_status = store
        .get("evt_stuck")
        .unwrap()
        .get("status")
        .unwrap()
        .clone();
    assert_eq!(stuck_status, DatomValue::Str("PENDING".to_string()));

    // fresh event is still PROCESSING
    let fresh_status = store
        .get("evt_fresh")
        .unwrap()
        .get("status")
        .unwrap()
        .clone();
    assert_eq!(fresh_status, DatomValue::Str("PROCESSING".to_string()));
}

#[tokio::test]
async fn test_moira_emitter_concurrency_10_cases() {
    use crate::infrastructure::sqs::StubSqsBus;
    use crate::iop::core::IopContext;
    use tokio::sync::Mutex as TokioMutex;

    // 1. Definición de Stubs livianos e in-memory para EavReader y EavWriter
    #[derive(Clone)]
    struct MockEavState {
        events: Arc<TokioMutex<HashMap<String, HashMap<String, DatomValue>>>>,
    }

    #[async_trait::async_trait]
    impl EavPullReader for MockEavState {
        async fn pull(
            &self,
            _tenant_id: &str,
            entity_id: &str,
            _attributes: Option<&[&str]>,
        ) -> Result<HashMap<String, DatomValue>, DomainError> {
            let mut state = self.events.lock().await;
            let entry = state.entry(entity_id.to_string()).or_insert_with(|| {
                let mut def = HashMap::new();
                def.insert("id".to_string(), DatomValue::Str(entity_id.to_string()));
                def.insert("status".to_string(), DatomValue::Str("PENDING".to_string()));
                def.insert(
                    "detail_type".to_string(),
                    DatomValue::Str("work_order.create".to_string()),
                );
                def.insert("retry_count".to_string(), DatomValue::Long(0));
                def.insert("created_at".to_string(), DatomValue::Instant(1622000000000));
                def
            });
            Ok(entry.clone())
        }
    }

    #[async_trait::async_trait]
    impl EavTransacter for MockEavState {
        async fn transact(
            &self,
            payload: TransactPayload,
        ) -> Result<crate::eav::writer::TransactResult, DomainError> {
            let mut state = self.events.lock().await;
            let entity_id = payload
                .entity_id
                .clone()
                .unwrap_or_else(|| "mock-entity".to_string());
            let datom_count = payload.attrs.len();
            if let Some(id) = &payload.entity_id {
                let entry = state.entry(id.clone()).or_insert_with(HashMap::new);
                for (k, v) in payload.attrs {
                    entry.insert(k, v);
                }
            }
            Ok(crate::eav::writer::TransactResult {
                entity_id,
                tx_id: 1622000000000,
                datoms: datom_count,
                outbox_count: 0,
            })
        }
    }

    #[async_trait::async_trait]
    impl OltpQueryRunner for MockEavState {
        async fn run_oltp_query(
            &self,
            tenant_id: &str,
            ast_ir: &Value,
        ) -> Result<Value, DomainError> {
            let is_pending = ast_ir
                .get("where")
                .and_then(|w| w.get(1))
                .and_then(|cond| cond.get(2))
                .and_then(|v| v.as_str())
                .map(|s| s == "PENDING")
                .unwrap_or(false);

            if !is_pending {
                return Ok(Value::Array(vec![]));
            }

            let suffix = tenant_id.strip_prefix("tnt_").unwrap_or("00");
            let event_id = format!("outbox-uuid-{}", suffix);
            let mock_outbox = json!([{
                "id": event_id,
                "status": "PENDING",
                "detail_type": "work_order.create",
                "retry_count": 0,
                "payload": "{\"test\": \"data\"}",
                "claimed_at": null,
                "created_at": 1622000000000i64
            }]);
            Ok(mock_outbox)
        }
    }

    let mock_state = MockEavState {
        events: Arc::new(TokioMutex::new(HashMap::new())),
    };
    let stub_sqs = Arc::new(StubSqsBus::new());

    let emitter = Arc::new(MoiraEmitterImpl::new(
        mock_state.clone(),
        mock_state.clone(),
        Arc::clone(&stub_sqs) as Arc<dyn ISqsBus>,
        mock_state.clone(),
    ));

    let mut handles: Vec<tokio::task::JoinHandle<(String, Result<(), DomainError>)>> = Vec::new();

    for i in 1..=10 {
        let emitter_clone = Arc::clone(&emitter);
        let tenant_id = format!("tnt_{:02}", i);
        let outbox_id = format!("outbox-uuid-{:02}", i);

        let mut request_map = serde_json::Map::new();
        request_map.insert("id".to_string(), json!(outbox_id));
        request_map.insert("entity_type".to_string(), json!("work_order"));
        request_map.insert("operation".to_string(), json!("create"));
        request_map.insert(
            "payload".to_string(),
            json!({ "cost": 1000 * i, "status": "ACTIVE" }),
        );

        let ctx = IopContext::new(tenant_id, "usr_admin", "work_order", "create", request_map);

        let handle = tokio::spawn(async move {
            let result = emitter_clone.emit(ctx).await;
            (outbox_id, result)
        });
        handles.push(handle);
    }

    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await);
    }

    for res in results {
        let res: Result<(String, Result<(), DomainError>), tokio::task::JoinError> = res;
        let (outbox_id, emit_result) = res.expect("Fallo crítico en Tokio Task");
        assert!(
            emit_result.is_ok(),
            "Fallo al emitir el evento para outbox_id {}: {:?}",
            outbox_id,
            emit_result.err()
        );
    }

    let sqs_messages = stub_sqs.messages.lock().unwrap().clone();
    assert_eq!(sqs_messages.len(), 10);
    for i in 1..=10 {
        let expected_outbox_id = format!("outbox-uuid-{:02}", i);
        let found = sqs_messages
            .iter()
            .any(|msg| msg.body.contains(&expected_outbox_id));
        assert!(
            found,
            "No se encontró el mensaje SQS correspondiente al outbox_id {}",
            expected_outbox_id
        );
    }
}

#[tokio::test]
async fn test_moira_emitter_concurrency_10_cases_real() {
    if std::env::var("REAL_ENV").is_err() {
        println!("Saltando test de entorno real (REAL_ENV no configurado)");
        return;
    }

    use crate::aegis::oltp::executor::OltpExecutor;
    use crate::eav::reader::pull::EavReader;
    use crate::eav::reader::query::EavQueryExecutor;
    use crate::eav::writer::{EavWriter, TransactOp, TransactPayload};
    use crate::infrastructure::dynamodb::DynamoClient;
    use crate::infrastructure::sqs::SqsFifoBus;
    use crate::iop::core::IopContext;

    // 1. Inicializar Códice si no está inicializado (silenciando la salida de pánico temporalmente)
    let has_registry = {
        let prev_hook = std::panic::take_hook();
        let res = std::panic::catch_unwind(|| {
            crate::codice::global();
        });
        std::panic::set_hook(prev_hook);
        res.is_ok()
    };

    if !has_registry {
        let models_dir = std::path::Path::new("config/models");
        let (registry, _event_rules) = crate::codice::CodeRegistry::build(models_dir).unwrap();
        crate::codice::init_global(registry);
    }

    // 2. Override de configuraciones de entorno para local
    std::env::set_var("AWS_ENDPOINT_URL", "http://localhost:4566");
    std::env::set_var("DYNAMODB_ENDPOINT", "http://localhost:8000");
    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
    std::env::set_var("AWS_REGION", "us-east-1");

    let eav_table = "metri-eav-local";
    let ddb_client = Arc::new(DynamoClient::new(eav_table).await);
    assert!(
        ddb_client.health_check().await,
        "¡DynamoDB local no está listo!"
    );

    let query_exec = EavQueryExecutor::new(Arc::clone(&ddb_client), eav_table);
    let pull_read = Arc::new(EavReader::new(Arc::clone(&ddb_client), eav_table));
    let oltp_exec = OltpExecutor::new(query_exec, (*pull_read).clone());
    let eav_writer = Arc::new(EavWriter::new(Arc::clone(&ddb_client), eav_table));

    let queue_url = "http://localhost:4566/000000000000/metri-outbox.fifo";
    let sqs_bus = Arc::new(SqsFifoBus::new(queue_url).await);
    assert!(
        sqs_bus.health_check().await,
        "¡Localstack SQS no está listo!"
    );

    let emitter = Arc::new(MoiraEmitterImpl::new(
        pull_read.clone(),
        eav_writer.clone(),
        Arc::clone(&sqs_bus) as Arc<dyn ISqsBus>,
        oltp_exec,
    ));

    // 3. Pre-seed/Limpiar base de datos y cola SQS
    // Intentar recibir y eliminar mensajes existentes para asegurar limpieza
    let _ = sqs_bus.receive_messages(10).await;

    let mut handles: Vec<tokio::task::JoinHandle<(String, Result<(), DomainError>)>> = Vec::new();
    let run_id = uuid::Uuid::new_v4().to_string();
    let run_suffix = &run_id[..8];

    for i in 1..=10 {
        let emitter_clone = Arc::clone(&emitter);
        let eav_writer_clone = Arc::clone(&eav_writer);
        let tenant_id = format!("tnt_real_{:02}", i);
        let outbox_id = format!("outbox-{}-real-{:02}", run_suffix, i);

        // Seed del evento en la base de datos real
        let mut attrs = HashMap::new();
        attrs.insert("status".to_string(), DatomValue::Str("PENDING".to_string()));
        attrs.insert(
            "detail_type".to_string(),
            DatomValue::Str("work_order.create".to_string()),
        );
        attrs.insert(
            "payload".to_string(),
            DatomValue::Str(format!(
                "{{\"cost\": {}, \"status\": \"ACTIVE\"}}",
                1000 * i
            )),
        );
        attrs.insert("retry_count".to_string(), DatomValue::Long(0));
        attrs.insert(
            "created_at".to_string(),
            DatomValue::Instant(1622000000000 + i as i64),
        );

        let transact = TransactPayload {
            tenant_id: tenant_id.clone(),
            entity_id: Some(outbox_id.clone()),
            entity_type: "outbox_event".to_string(),
            attrs,
            op: TransactOp::Create,
        };
        eav_writer_clone.transact(transact).await.unwrap();

        // Preparar contexto para la llamada emit
        let mut request_map = serde_json::Map::new();
        request_map.insert("id".to_string(), json!(outbox_id));
        request_map.insert("entity_type".to_string(), json!("work_order"));
        request_map.insert("operation".to_string(), json!("create"));
        request_map.insert(
            "payload".to_string(),
            json!({ "cost": 1000 * i, "status": "ACTIVE" }),
        );

        let ctx = IopContext::new(tenant_id, "usr_admin", "work_order", "create", request_map);

        let handle = tokio::spawn(async move {
            let result = emitter_clone.emit(ctx).await;
            (outbox_id, result)
        });
        handles.push(handle);
    }

    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await);
    }

    for res in results {
        let res: Result<(String, Result<(), DomainError>), tokio::task::JoinError> = res;
        let (outbox_id, emit_result) = res.expect("Fallo crítico en Tokio Task");
        assert!(
            emit_result.is_ok(),
            "Fallo al emitir el evento real para outbox_id {}: {:?}",
            outbox_id,
            emit_result.err()
        );
    }

    // 4. Verificaciones de entorno real:
    // A) Validar que los estados en la base de datos real cambiaron a "DELIVERED"
    for i in 1..=10 {
        let outbox_id = format!("outbox-{}-real-{:02}", run_suffix, i);
        let tenant_id = format!("tnt_real_{:02}", i);
        let pulled = pull_read.pull(&tenant_id, &outbox_id, None).await.unwrap();
        let status = pulled
            .get("status")
            .expect("Status no encontrado en entidad real");
        assert_eq!(status, &DatomValue::Str("DELIVERED".to_string()));
    }

    // B) Validar que la cola de SQS FIFO recibió los 10 mensajes correspondientes
    let sqs_messages = sqs_bus.receive_messages(10).await.unwrap();
    assert_eq!(
        sqs_messages.len(),
        10,
        "¡La cola SQS real no recibió los 10 mensajes!"
    );
    for i in 1..=10 {
        let expected_outbox_id = format!("outbox-{}-real-{:02}", run_suffix, i);
        let found_msg = sqs_messages
            .iter()
            .find(|msg| msg.body.contains(&expected_outbox_id));
        assert!(
            found_msg.is_some(),
            "¡No se encontró el mensaje SQS real para outbox_id {}!",
            expected_outbox_id
        );
        // Eliminar para dejar limpia la cola
        sqs_bus
            .delete_message(&found_msg.unwrap().receipt_handle)
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore]
async fn test_seed_production_quotas() {
    use crate::eav::writer::{EavWriter, TransactOp, TransactPayload};
    use crate::infrastructure::dynamodb::DynamoClient;

    // 1. Inicializar Códice
    let models_dir = std::path::Path::new("config/models");
    let (registry, _event_rules) = crate::codice::CodeRegistry::build(models_dir).unwrap();
    crate::codice::init_global(registry);

    // 2. Set credentials for production
    std::env::set_var("AWS_PROFILE", "metri-dev");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::remove_var("DYNAMODB_ENDPOINT");

    let eav_table = "metri-dynamo";
    let ddb_client = Arc::new(DynamoClient::new(eav_table).await);
    let eav_writer = EavWriter::new(ddb_client, eav_table);

    println!("Seeding location quota in production...");
    let mut attrs_loc = HashMap::new();
    attrs_loc.insert("tenant_id".to_string(), DatomValue::Ref(0));
    attrs_loc.insert(
        "resource_domain".to_string(),
        DatomValue::Str("location".to_string()),
    );
    attrs_loc.insert(
        "limit_type".to_string(),
        DatomValue::Str("WRITE_COUNT".to_string()),
    );
    attrs_loc.insert(
        "reset_strategy".to_string(),
        DatomValue::Str("FIXED".to_string()),
    );
    attrs_loc.insert(
        "period_key".to_string(),
        DatomValue::Str("LIFETIME".to_string()),
    );
    attrs_loc.insert("max_limit".to_string(), DatomValue::Long(1000000));
    attrs_loc.insert("current_usage".to_string(), DatomValue::Long(0));

    let transact_loc = TransactPayload {
        tenant_id: "golden-tenant-benchmark".to_string(),
        entity_id: Some("quota-loc-prod-benchmark".to_string()),
        entity_type: "domain_quota".to_string(),
        attrs: attrs_loc,
        op: TransactOp::Create,
    };
    eav_writer.transact(transact_loc).await.unwrap();
    println!("✓ Location quota seeded!");

    println!("Seeding asset quota in production...");
    let mut attrs_asset = HashMap::new();
    attrs_asset.insert("tenant_id".to_string(), DatomValue::Ref(0));
    attrs_asset.insert(
        "resource_domain".to_string(),
        DatomValue::Str("asset".to_string()),
    );
    attrs_asset.insert(
        "limit_type".to_string(),
        DatomValue::Str("WRITE_COUNT".to_string()),
    );
    attrs_asset.insert(
        "reset_strategy".to_string(),
        DatomValue::Str("FIXED".to_string()),
    );
    attrs_asset.insert(
        "period_key".to_string(),
        DatomValue::Str("LIFETIME".to_string()),
    );
    attrs_asset.insert("max_limit".to_string(), DatomValue::Long(1000000));
    attrs_asset.insert("current_usage".to_string(), DatomValue::Long(0));

    let transact_asset = TransactPayload {
        tenant_id: "golden-tenant-benchmark".to_string(),
        entity_id: Some("quota-asset-prod-benchmark".to_string()),
        entity_type: "domain_quota".to_string(),
        attrs: attrs_asset,
        op: TransactOp::Create,
    };
    eav_writer.transact(transact_asset).await.unwrap();
    println!("✓ Asset quota seeded!");
}

#[tokio::test]
#[ignore]
async fn test_seed_and_emit_production_outbox_events() {
    use crate::aegis::oltp::executor::OltpExecutor;
    use crate::eav::reader::pull::EavReader;
    use crate::eav::reader::query::EavQueryExecutor;
    use crate::eav::writer::{EavWriter, TransactOp, TransactPayload};
    use crate::infrastructure::dynamodb::DynamoClient;
    use crate::infrastructure::sqs::SqsFifoBus;
    use crate::iop::core::IopContext;

    // 1. Inicializar Códice
    let models_dir = std::path::Path::new("config/models");
    let (registry, _event_rules) = crate::codice::CodeRegistry::build(models_dir).unwrap();
    crate::codice::init_global(registry);

    // 2. Set credentials for production
    std::env::set_var("AWS_PROFILE", "metri-dev");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::remove_var("DYNAMODB_ENDPOINT");

    let eav_table = "metri-dynamo";
    let ddb_client = Arc::new(DynamoClient::new(eav_table).await);
    let pull_read = Arc::new(EavReader::new(Arc::clone(&ddb_client), eav_table));
    let query_exec = EavQueryExecutor::new(Arc::clone(&ddb_client), eav_table);
    let oltp_exec = OltpExecutor::new(query_exec, (*pull_read).clone());
    let eav_writer = Arc::new(EavWriter::new(ddb_client, eav_table));

    let queue_url = "https://sqs.us-east-1.amazonaws.com/982592308819/metri-outbox.fifo";
    let sqs_bus = Arc::new(SqsFifoBus::new(queue_url).await);

    let emitter = MoiraEmitterImpl::new(
        pull_read.clone(),
        eav_writer.clone(),
        Arc::clone(&sqs_bus) as Arc<dyn ISqsBus>,
        oltp_exec,
    );

    let tenant_id = "golden-tenant-benchmark";
    let run_id = uuid::Uuid::new_v4().to_string();
    let run_suffix = &run_id[..8];

    println!("Seeding 10 outbox events in production...");
    for i in 1..=10 {
        let outbox_id = format!("outbox-prod-{}-{}", run_suffix, i);
        let mut attrs = HashMap::new();
        attrs.insert("status".to_string(), DatomValue::Str("PENDING".to_string()));
        attrs.insert(
            "detail_type".to_string(),
            DatomValue::Str("asset.create".to_string()),
        );
        attrs.insert("payload".to_string(), DatomValue::Str(format!(
            "{{\"event_type\": \"asset.create\", \"asset_id\": \"{}\", \"status\": \"ACTIVE\", \"timestamp\": {}}}",
            outbox_id,
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()
        )));
        attrs.insert("retry_count".to_string(), DatomValue::Long(0));
        attrs.insert(
            "created_at".to_string(),
            DatomValue::Instant(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64
                    + i as i64,
            ),
        );

        let transact = TransactPayload {
            tenant_id: tenant_id.to_string(),
            entity_id: Some(outbox_id.clone()),
            entity_type: "outbox_event".to_string(),
            attrs,
            op: TransactOp::Create,
        };
        eav_writer.transact(transact).await.unwrap();
        println!("  ✓ Seeded event {i}: {outbox_id}");
    }

    println!("Triggering Moira emitter to process and publish those 10 events...");
    let ctx = IopContext::new(
        tenant_id,
        "benchmark-user",
        "outbox_event",
        "CREATE",
        serde_json::Map::new(),
    );

    emitter.emit(ctx).await.unwrap();
    println!("Moira emitter completed execution!");

    println!("Verifying that outbox events were successfully delivered (marked DELIVERED in production)...");
    let mut delivered_count = 0;
    for i in 1..=10 {
        let outbox_id = format!("outbox-prod-{}-{}", run_suffix, i);
        let pulled = pull_read.pull(tenant_id, &outbox_id, None).await.unwrap();
        if let Some(DatomValue::Str(status)) = pulled.get("status") {
            println!("  - Event {}: {} is {}", i, outbox_id, status);
            if status == "DELIVERED" {
                delivered_count += 1;
            }
        } else {
            println!("  - Event {}: {} status not found!", i, outbox_id);
        }
    }
    assert_eq!(
        delivered_count, 10,
        "Not all events were delivered successfully!"
    );
    println!("Verification SUCCESS: All 10 events were published to SQS FIFO and marked DELIVERED in production DynamoDB!");
}

#[tokio::test]
#[ignore]
async fn test_seed_rules_and_webhooks_production() {
    use crate::eav::writer::{EavWriter, TransactOp, TransactPayload};
    use crate::infrastructure::dynamodb::DynamoClient;

    // 1. Inicializar Códice
    let models_dir = std::path::Path::new("config/models");
    let (registry, _event_rules) = crate::codice::CodeRegistry::build(models_dir).unwrap();
    crate::codice::init_global(registry);

    // 2. Set credentials for production
    std::env::set_var("AWS_PROFILE", "metri-dev");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::remove_var("DYNAMODB_ENDPOINT");

    let eav_table = "metri-dynamo";
    let ddb_client = Arc::new(DynamoClient::new(eav_table).await);
    let eav_writer = EavWriter::new(ddb_client, eav_table);

    let tenant_id = "golden-tenant-benchmark";

    println!("Seeding event routing rule in production...");
    let mut attrs_rule = HashMap::new();
    attrs_rule.insert(
        "rule_code".to_string(),
        DatomValue::Str("BENCHMARK_ASSET_CREATE".to_string()),
    );
    attrs_rule.insert(
        "description".to_string(),
        DatomValue::Str("Benchmark asset creation rule".to_string()),
    );
    attrs_rule.insert("is_system_seeded".to_string(), DatomValue::Bool(true));
    attrs_rule.insert(
        "target_entity_name".to_string(),
        DatomValue::Str("asset".to_string()),
    );
    attrs_rule.insert(
        "event_trigger_type".to_string(),
        DatomValue::Str("CREATE".to_string()),
    );
    attrs_rule.insert("filter_conditions".to_string(), DatomValue::Array(vec![]));
    attrs_rule.insert(
        "detail_type_output".to_string(),
        DatomValue::Str("benchmark.asset.created".to_string()),
    );

    let transact_rule = TransactPayload {
        tenant_id: tenant_id.to_string(),
        entity_id: Some("rule-benchmark-asset-create".to_string()),
        entity_type: "event_routing_rule".to_string(),
        attrs: attrs_rule,
        op: TransactOp::Create,
    };
    eav_writer.transact(transact_rule).await.unwrap();
    println!("✓ Event routing rule seeded!");

    println!("Seeding webhook endpoint in production...");
    let mut attrs_webhook = HashMap::new();
    attrs_webhook.insert(
        "name".to_string(),
        DatomValue::Str("Webhook Benchmark Target".to_string()),
    );
    attrs_webhook.insert(
        "target_url".to_string(),
        DatomValue::Str("https://httpbin.org/post".to_string()),
    );
    attrs_webhook.insert(
        "http_method".to_string(),
        DatomValue::Str("POST".to_string()),
    );
    attrs_webhook.insert(
        "authentication_type".to_string(),
        DatomValue::Str("NONE".to_string()),
    );
    attrs_webhook.insert(
        "auth_token".to_string(),
        DatomValue::Str("my-secret-token".to_string()),
    );
    attrs_webhook.insert(
        "subscribed_rule_ids".to_string(),
        DatomValue::Array(vec!["rule-benchmark-asset-create".to_string()]),
    );
    attrs_webhook.insert("is_active".to_string(), DatomValue::Bool(true));

    let transact_webhook = TransactPayload {
        tenant_id: tenant_id.to_string(),
        entity_id: Some("webhook-benchmark".to_string()),
        entity_type: "webhook_endpoint".to_string(),
        attrs: attrs_webhook,
        op: TransactOp::Create,
    };
    eav_writer.transact(transact_webhook).await.unwrap();
    println!("✓ Webhook endpoint seeded!");
}

#[tokio::test]
#[ignore]
async fn test_seed_cmms_rules_and_events_production() {
    use crate::aegis::oltp::executor::OltpExecutor;
    use crate::eav::reader::pull::EavReader;
    use crate::eav::reader::query::EavQueryExecutor;
    use crate::eav::writer::{EavWriter, TransactOp, TransactPayload};
    use crate::infrastructure::dynamodb::DynamoClient;
    use crate::infrastructure::sqs::SqsFifoBus;
    use crate::iop::core::IopContext;

    // 1. Inicializar Códice
    let models_dir = std::path::Path::new("config/models");
    let (registry, _event_rules) = crate::codice::CodeRegistry::build(models_dir).unwrap();
    crate::codice::init_global(registry);

    // 2. Set credentials for production
    std::env::set_var("AWS_PROFILE", "metri-dev");
    std::env::set_var("AWS_REGION", "us-east-1");
    std::env::remove_var("DYNAMODB_ENDPOINT");

    let eav_table = "metri-dynamo";
    let ddb_client = Arc::new(DynamoClient::new(eav_table).await);
    let pull_read = Arc::new(EavReader::new(Arc::clone(&ddb_client), eav_table));
    let query_exec = EavQueryExecutor::new(Arc::clone(&ddb_client), eav_table);
    let oltp_exec = OltpExecutor::new(query_exec, (*pull_read).clone());
    let eav_writer = Arc::new(EavWriter::new(ddb_client, eav_table));

    let queue_url = "https://sqs.us-east-1.amazonaws.com/982592308819/metri-outbox.fifo";
    let sqs_bus = Arc::new(SqsFifoBus::new(queue_url).await);

    let emitter = MoiraEmitterImpl::new(
        pull_read.clone(),
        eav_writer.clone(),
        Arc::clone(&sqs_bus) as Arc<dyn ISqsBus>,
        oltp_exec,
    );

    let tenant_id = "golden-tenant-benchmark";

    // Definimos las 8 reglas con sus IDs
    let rule_cases = vec![
        ("rule-cmms-lc01", "LOCATION_CREATED", "location", "CREATE", vec![], "system.location.created", "Notificación sistémica global al crear una nueva ubicación."),
        ("rule-cmms-lc02", "LOCATION_UPDATED", "location", "UPDATE", vec![], "system.location.updated", "Notificación sistémica global al actualizar una ubicación."),
        ("rule-cmms-lc03", "LOCATION_DELETED", "location", "DELETE", vec![], "system.location.deleted", "Notificación sistémica global al eliminar una ubicación."),
        ("rule-cmms-as01", "ASSET_CREATED", "asset", "CREATE", vec![], "system.asset.created", "Notificación sistémica global al crear cualquier activo nuevo."),
        ("rule-cmms-as02", "ASSET_STATUS_CHANGED", "asset", "UPDATE", vec![
            json!({
                "field_name": "status",
                "operator": "neq",
                "target_value": "",
                "target_value_type": "string"
            }).to_string()
        ], "system.asset.status_changed", "Auditoría global de cambios de estado en activos."),
        ("rule-cmms-wo01", "WO_CREATED", "work_order", "CREATE", vec![], "system.work_order.created", "Notificación sistémica global al crear una orden de trabajo."),
        ("rule-cmms-wo02", "WO_CLOSED", "work_order", "UPDATE", vec![
            json!({
                "field_name": "status",
                "operator": "eq",
                "target_value": "CLOSED",
                "target_value_type": "string"
            }).to_string()
        ], "system.work_order.closed", "Evento crítico de cierre de orden de trabajo para auditoría y KPIs."),
        ("rule-cmms-wo03", "WO_HIGH_COST_ALERT", "work_order", "UPDATE", vec![
            json!({
                "field_name": "total_cost_cents",
                "operator": "gt",
                "target_value": "5000000",
                "target_value_type": "long"
            }).to_string()
        ], "system.alerts.high_cost_work_order", "Alerta sistémica automática para órdenes de trabajo que superan los 50k en costo (5,000,000 céntimos)."),
    ];

    println!("Seeding 8 CMMS routing rules in production DynamoDB...");
    let mut seeded_rule_ids = Vec::new();
    for (id, code, entity, trigger, filter_conds, detail_type, desc) in &rule_cases {
        // Intentar borrar primero la regla si ya existe para asegurar un clean seed
        let _ = eav_writer
            .transact(TransactPayload {
                tenant_id: tenant_id.to_string(),
                entity_id: Some(id.to_string()),
                entity_type: "event_routing_rule".to_string(),
                attrs: HashMap::new(),
                op: TransactOp::Delete,
            })
            .await;

        let mut attrs = HashMap::new();
        attrs.insert("rule_code".to_string(), DatomValue::Str(code.to_string()));
        attrs.insert("description".to_string(), DatomValue::Str(desc.to_string()));
        attrs.insert("is_system_seeded".to_string(), DatomValue::Bool(true));
        attrs.insert(
            "target_entity_name".to_string(),
            DatomValue::Str(entity.to_string()),
        );
        attrs.insert(
            "event_trigger_type".to_string(),
            DatomValue::Str(trigger.to_string()),
        );
        attrs.insert(
            "filter_conditions".to_string(),
            DatomValue::Array(filter_conds.clone()),
        );
        attrs.insert(
            "detail_type_output".to_string(),
            DatomValue::Str(detail_type.to_string()),
        );

        let transact_rule = TransactPayload {
            tenant_id: tenant_id.to_string(),
            entity_id: Some(id.to_string()),
            entity_type: "event_routing_rule".to_string(),
            attrs,
            op: TransactOp::Create,
        };
        eav_writer.transact(transact_rule).await.unwrap();
        seeded_rule_ids.push(id.to_string());
        println!("  ✓ Rule seeded: {code} ({id})");
    }

    println!("Seeding webhook endpoint subscribed to all 8 CMMS rules...");
    let _ = eav_writer
        .transact(TransactPayload {
            tenant_id: tenant_id.to_string(),
            entity_id: Some("webhook-cmms-testing".to_string()),
            entity_type: "webhook_endpoint".to_string(),
            attrs: HashMap::new(),
            op: TransactOp::Delete,
        })
        .await;

    let mut attrs_webhook = HashMap::new();
    attrs_webhook.insert(
        "name".to_string(),
        DatomValue::Str("Webhook CMMS Testing Target".to_string()),
    );
    attrs_webhook.insert(
        "target_url".to_string(),
        DatomValue::Str("https://httpbin.org/post".to_string()),
    );
    attrs_webhook.insert(
        "http_method".to_string(),
        DatomValue::Str("POST".to_string()),
    );
    attrs_webhook.insert(
        "authentication_type".to_string(),
        DatomValue::Str("NONE".to_string()),
    );
    attrs_webhook.insert(
        "auth_token".to_string(),
        DatomValue::Str("my-cmms-secret-token".to_string()),
    );
    attrs_webhook.insert(
        "subscribed_rule_ids".to_string(),
        DatomValue::Array(seeded_rule_ids),
    );
    attrs_webhook.insert("is_active".to_string(), DatomValue::Bool(true));

    let transact_webhook = TransactPayload {
        tenant_id: tenant_id.to_string(),
        entity_id: Some("webhook-cmms-testing".to_string()),
        entity_type: "webhook_endpoint".to_string(),
        attrs: attrs_webhook,
        op: TransactOp::Create,
    };
    eav_writer.transact(transact_webhook).await.unwrap();
    println!("✓ Webhook endpoint seeded!");

    // Ahora vamos a sembrar e ingerir los 8 outbox_events correspondientes a los 8 casos de prueba.
    // Usamos un run_id único para poder identificarlos y evitar colisiones de deduplicación.
    let run_id = uuid::Uuid::new_v4().to_string();
    let run_suffix = &run_id[..8];

    let event_cases = vec![
        (
            "LC-01",
            "location.create",
            json!({
                "id": format!("loc-create-{run_suffix}"),
                "name": "Central Office",
                "code": "LOC-CO-001",
                "type": "plant",
                "address": "123 Main St"
            }),
        ),
        (
            "LC-02",
            "location.update",
            json!({
                "id": format!("loc-update-{run_suffix}"),
                "name": "Central Office Updated",
                "code": "LOC-CO-001",
                "type": "plant"
            }),
        ),
        (
            "LC-03",
            "location.delete",
            json!({
                "id": format!("loc-delete-{run_suffix}"),
                "code": "LOC-CO-001"
            }),
        ),
        (
            "AS-01",
            "asset.create",
            json!({
                "id": format!("asset-create-{run_suffix}"),
                "name": "HVAC Compressor 1",
                "status": "active",
                "criticality": "A",
                "manufacturer": "Carrier"
            }),
        ),
        (
            "AS-02",
            "asset.update",
            json!({
                "id": format!("asset-status-{run_suffix}"),
                "name": "HVAC Compressor 1",
                "status": "maintenance"
            }),
        ),
        (
            "WO-01",
            "work_order.create",
            json!({
                "id": format!("wo-create-{run_suffix}"),
                "title": "Fix HVAC Leaks",
                "status": "OPEN",
                "priority": "HIGH",
                "total_cost_cents": 250000,
                "currency": "USD"
            }),
        ),
        (
            "WO-02",
            "work_order.update",
            json!({
                "id": format!("wo-closed-{run_suffix}"),
                "title": "Fix HVAC Leaks",
                "status": "CLOSED",
                "priority": "HIGH",
                "total_cost_cents": 4500000,
                "currency": "USD"
            }),
        ),
        (
            "WO-03",
            "work_order.update",
            json!({
                "id": format!("wo-alert-{run_suffix}"),
                "title": "Major Overhaul",
                "status": "IN_PROGRESS",
                "priority": "CRITICAL",
                "total_cost_cents": 6500000,
                "currency": "USD"
            }),
        ),
    ];

    println!("Seeding 8 outbox events associated with CMMS events in production...");
    let mut seeded_outbox_ids = Vec::new();
    for (case_id, detail_type, payload) in &event_cases {
        let outbox_id = format!("outbox-cmms-{case_id}-{run_suffix}");

        // Borrar primero si existe
        let _ = eav_writer
            .transact(TransactPayload {
                tenant_id: tenant_id.to_string(),
                entity_id: Some(outbox_id.clone()),
                entity_type: "outbox_event".to_string(),
                attrs: HashMap::new(),
                op: TransactOp::Delete,
            })
            .await;

        let mut attrs = HashMap::new();
        attrs.insert("status".to_string(), DatomValue::Str("PENDING".to_string()));
        attrs.insert(
            "detail_type".to_string(),
            DatomValue::Str(detail_type.to_string()),
        );
        attrs.insert("payload".to_string(), DatomValue::Str(payload.to_string()));
        attrs.insert("retry_count".to_string(), DatomValue::Long(0));
        attrs.insert(
            "created_at".to_string(),
            DatomValue::Instant(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            ),
        );

        let transact = TransactPayload {
            tenant_id: tenant_id.to_string(),
            entity_id: Some(outbox_id.clone()),
            entity_type: "outbox_event".to_string(),
            attrs,
            op: TransactOp::Create,
        };
        eav_writer.transact(transact).await.unwrap();
        seeded_outbox_ids.push(outbox_id.clone());
        println!("  ✓ Seeded event for {case_id}: {outbox_id}");
    }

    println!("Triggering Moira emitter to process and publish the 8 CMMS events...");
    let ctx = IopContext::new(
        tenant_id,
        "benchmark-user",
        "outbox_event",
        "CREATE",
        serde_json::Map::new(),
    );

    emitter.emit(ctx).await.unwrap();
    println!("Moira emitter completed execution!");

    println!("Verifying that all 8 outbox events were successfully delivered (marked DELIVERED in production)...");
    let mut delivered_count = 0;
    for outbox_id in &seeded_outbox_ids {
        let pulled = pull_read.pull(tenant_id, outbox_id, None).await.unwrap();
        if let Some(DatomValue::Str(status)) = pulled.get("status") {
            println!("  - Event {}: is {}", outbox_id, status);
            if status == "DELIVERED" {
                delivered_count += 1;
            }
        } else {
            println!("  - Event {}: status not found!", outbox_id);
        }
    }
    assert_eq!(
        delivered_count, 8,
        "Not all 8 CMMS events were delivered successfully!"
    );
    println!("E2E Seeding & Emit Verification SUCCESS: All 8 CMMS events were transacted, published, and marked DELIVERED!");
}
