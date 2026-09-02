use serde_json::Value;
use std::sync::{Arc, Mutex};

use metri_engine::domain::errors::{DomainError, ErrorCode};
use metri_engine::iop::core::IopContext;
use metri_engine::iop::sherlog::{process_fault, FaultSeverity, IFaultNotifier};
use metri_engine::janus_router::router::IWriteChannel;

#[derive(Clone)]
struct MockFaultNotifier {
    calls: Arc<Mutex<Vec<(Value, FaultSeverity)>>>,
}

impl MockFaultNotifier {
    fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait::async_trait]
impl IFaultNotifier for MockFaultNotifier {
    async fn notify(&self, error_dto: &Value, severity: &FaultSeverity) -> Result<(), DomainError> {
        self.calls
            .lock()
            .unwrap()
            .push((error_dto.clone(), severity.clone()));
        Ok(())
    }
}

#[derive(Clone)]
struct MockWriteChannel {
    calls: Arc<Mutex<Vec<IopContext>>>,
}

impl MockWriteChannel {
    fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait::async_trait]
impl IWriteChannel for MockWriteChannel {
    async fn route(&self, ctx: IopContext) -> Result<Value, DomainError> {
        self.calls.lock().unwrap().push(ctx);
        Ok(serde_json::json!({ "status": "success" }))
    }
}

static INIT: std::sync::Once = std::sync::Once::new();

fn setup_catalog() {
    INIT.call_once(|| {
        // Initialize CodeRegistry
        let models_dir = std::path::Path::new("config/models");
        if let Ok((registry, _)) = metri_engine::codice::CodeRegistry::build(models_dir) {
            metri_engine::codice::init_global(registry);
        }

        // Initialize ErrorCatalog
        if metri_engine::domain::error_catalog::try_global().is_none() {
            let catalog =
                metri_engine::domain::ErrorCatalog::load("config/errors/error_catalog.toml")
                    .unwrap();
            metri_engine::domain::init_error_catalog(catalog);
        }
    });
}

#[tokio::test]
async fn test_process_fault_double_action() {
    setup_catalog();

    let notifier = MockFaultNotifier::new();
    let olap_channel = MockWriteChannel::new();

    // Create a DomainError with a code that exists in catalog (e.g. Jns001 -> severity: "error" in catalog)
    let error = DomainError::janus(ErrorCode::Jns001, "Test error detail");
    println!(
        "DEBUG: Serialized code: {:?}",
        serde_json::to_value(&error.code)
    );
    println!(
        "DEBUG: try_global is_some: {}",
        metri_engine::domain::error_catalog::try_global().is_some()
    );
    if let Some(catalog) = metri_engine::domain::error_catalog::try_global() {
        println!("DEBUG: Catalog entry JNS_001: {:?}", catalog.get("JNS_001"));
        println!("DEBUG: Catalog entry JNS001: {:?}", catalog.get("JNS001"));
        let code_str = serde_json::to_value(&error.code)
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap();
        println!(
            "DEBUG: Catalog entry with code_str ({}): {:?}",
            code_str,
            catalog.get(&code_str)
        );
    }

    // Build mock error DTO representing rich context error
    let error_dto = serde_json::json!({
        "status": "error",
        "error": {
            "code": "JNS_001",
            "description": "Test error detail",
            "trace_id": "test_trace_12345",
            "span_id": "0000000000000000",
            "correlation_id": "REQ-test",
            "tenant_id": "tnt_test",
            "user_id": "usr_test",
            "timestamp": 1718221000i64,
            "stage": "janus",
            "retryable": false,
            "context": {
                "field": "test_field"
            },
            "component": "metri-engine"
        }
    });

    // Execute process_fault
    process_fault(
        &notifier,
        &olap_channel,
        &error,
        &error_dto,
        Some("asset".to_string()),
    )
    .await;

    // 1. Verify EventBridge notification calls
    let notifier_calls = notifier.calls.lock().unwrap();
    assert_eq!(notifier_calls.len(), 1);
    let (notified_dto, notified_severity) = &notifier_calls[0];
    assert_eq!(*notified_severity, FaultSeverity::Error);
    assert_eq!(notified_dto["error"]["code"], "JNS_001");

    // 2. Verify OLAP route calls
    let olap_calls = olap_channel.calls.lock().unwrap();
    assert_eq!(olap_calls.len(), 1);
    let fault_ctx = &olap_calls[0];
    assert_eq!(fault_ctx.entity_type, "domain_fault");
    assert_eq!(fault_ctx.operation, "BULK_CREATE");
    assert_eq!(fault_ctx.tenant_id, "tnt_test");
    assert_eq!(fault_ctx.user_id, "usr_test");

    let data_array = fault_ctx
        .request
        .get("data")
        .and_then(|d| d.as_array())
        .unwrap();
    assert_eq!(data_array.len(), 1);
    let fault_record = data_array[0].as_object().unwrap();

    assert_eq!(
        fault_record.get("trace_id").unwrap().as_str().unwrap(),
        "test_trace_12345"
    );
    assert_eq!(
        fault_record.get("tenant_id").unwrap().as_str().unwrap(),
        "tnt_test"
    );
    assert_eq!(
        fault_record.get("user_id").unwrap().as_str().unwrap(),
        "usr_test"
    );
    assert_eq!(
        fault_record.get("error_code").unwrap().as_str().unwrap(),
        "JNS_001"
    );
    assert_eq!(
        fault_record.get("severity").unwrap().as_str().unwrap(),
        "ERROR"
    );
    assert_eq!(
        fault_record.get("stage").unwrap().as_str().unwrap(),
        "janus"
    );
    assert_eq!(
        fault_record.get("component").unwrap().as_str().unwrap(),
        "metri-engine"
    );
    assert_eq!(
        fault_record.get("entity_type").unwrap().as_str().unwrap(),
        "asset"
    );
    assert!(!fault_record.get("retryable").unwrap().as_bool().unwrap());
    assert_eq!(
        fault_record.get("occurred_at").unwrap().as_i64().unwrap(),
        1718221000i64
    );
    assert_eq!(
        fault_record.get("context").unwrap()["field"]
            .as_str()
            .unwrap(),
        "test_field"
    );
}
