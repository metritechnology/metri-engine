use super::*;
use std::sync::Mutex;
use crate::domain::errors::{DomainError, ErrorCode};

#[test]
fn test_fault_severity_parsing() {
    assert_eq!(FaultSeverity::from_catalog_str("info"), FaultSeverity::Info);
    assert_eq!(FaultSeverity::from_catalog_str("error"), FaultSeverity::Error);
    assert_eq!(FaultSeverity::from_catalog_str("fatal"), FaultSeverity::Fatal);
    assert_eq!(FaultSeverity::from_catalog_str("anything_else"), FaultSeverity::Warning);
}

#[test]
fn test_severity_requires_notification() {
    assert!(!FaultSeverity::Info.requires_notification());
    assert!(FaultSeverity::Warning.requires_notification());
    assert!(FaultSeverity::Error.requires_notification());
    assert!(FaultSeverity::Fatal.requires_notification());
}

struct MockWriteChannel {
    contexts: Mutex<Vec<IopContext>>,
}

#[async_trait::async_trait]
impl IWriteChannel for MockWriteChannel {
    async fn route(&self, ctx: IopContext) -> Result<Value, DomainError> {
        self.contexts.lock().unwrap().push(ctx);
        Ok(Value::Null)
    }
}

struct MockFaultNotifier {
    notifications: Mutex<Vec<(Value, FaultSeverity)>>,
}

#[async_trait::async_trait]
impl IFaultNotifier for MockFaultNotifier {
    async fn notify(&self, error_dto: &Value, severity: &FaultSeverity) -> Result<(), DomainError> {
        self.notifications.lock().unwrap().push((error_dto.clone(), severity.clone()));
        Ok(())
    }
}

#[tokio::test]
async fn test_process_fault_success() {
    let notifier = MockFaultNotifier { notifications: Mutex::new(vec![]) };
    let olap = MockWriteChannel { contexts: Mutex::new(vec![]) };
    
    let error = DomainError::new(ErrorCode::Janus400, "Bad input data");
    
    let error_dto = serde_json::json!({
        "error": {
            "code": "JANUS_400",
            "trace_id": "trace-100",
            "tenant_id": "tnt_01",
            "user_id": "usr_01",
            "stage": "janus",
            "component": "metri-engine",
            "retryable": false,
            "timestamp": 1622000000000i64,
            "context": null
        }
    });

    process_fault(&notifier, &olap, &error, &error_dto, Some("asset".to_string())).await;
    
    // Notifier should have received 1 notification
    let notifications = notifier.notifications.lock().unwrap();
    assert_eq!(notifications.len(), 1);
    let (notif_val, notif_sev) = &notifications[0];
    assert_eq!(notif_sev, &FaultSeverity::Warning); // default because try_global is None in test
    assert_eq!(notif_val.get("error").unwrap().get("trace_id").unwrap().as_str(), Some("trace-100"));
    
    // OLAP channel should have received 1 fault context to route
    let contexts = olap.contexts.lock().unwrap();
    assert_eq!(contexts.len(), 1);
    let ctx = &contexts[0];
    assert_eq!(ctx.tenant_id, "tnt_01");
    assert_eq!(ctx.entity_type, "domain_fault");
    assert_eq!(ctx.operation, "BULK_CREATE");
}
