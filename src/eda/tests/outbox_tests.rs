use super::*;
use serde_json::json;

#[tokio::test]
async fn test_outbox_save_for_retry() {
    let manager = OutboxManager::new();
    let payload = json!({"id": "event_1"});
    let res = manager.save_for_retry("work_order.create", &payload).await;
    assert!(res.is_ok());
}
