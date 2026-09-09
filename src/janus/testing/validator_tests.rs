//! Tests for `janus::validator`.
use crate::janus::validator::{validate_query_request, validate_tenant};
use serde_json::json;

#[test]
fn validate_tenant_rejects_empty() {
    assert!(validate_tenant("").is_err());
    assert!(validate_tenant("unknown-tenant").is_err());
}

#[test]
fn validate_tenant_accepts_valid() {
    assert!(validate_tenant("tenant-abc-123").is_ok());
}

#[test]
fn validate_query_request_requires_queries() {
    let payload = json!({"tenant_id": "t1", "queries": {}});
    assert!(validate_query_request(&payload).is_err());

    let payload = json!({"tenant_id": "t1", "queries": {"q1": {"entity": "WorkOrder"}}});
    assert!(validate_query_request(&payload).is_ok());
}
